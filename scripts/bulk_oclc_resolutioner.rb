require 'htph';

# Performs OCLC resolution on the tables hathi_oclc and hathi_str.
# Unused hathi strings should be removed with post_index_remove_unused_str.rb

log  = HTPH::Hathilog::Log.new();
log.d("Started");
db   = HTPH::Hathidb::Db.new();
conn = db.get_conn();
slice_size = 100;
x_qmarks   = ['?'] * slice_size;
oclc_map   = {};

log.d("Prepping queries");

@get_oclcs_query = conn.prepare("SELECT DISTINCT ho.str_id, hs.str AS oclc FROM hathi_oclc AS ho JOIN hathi_str AS hs ON (ho.str_id = hs.id) ORDER BY oclc");

map_sql    = %W[
    SELECT z.a, MIN(z.b) AS min_b FROM (
        SELECT DISTINCT CAST(x.oclc AS UNSIGNED) AS a, CAST(y.oclc AS UNSIGNED) AS b
        FROM holdings_htitem_oclc AS x
        JOIN holdings_htitem_oclc AS y ON (x.volume_id = y.volume_id)
        WHERE x.oclc IN(#{x_qmarks.join(',')})
    ) AS z GROUP BY a
].join(" ");

@map_query         = conn.prepare(map_sql);
@get_str_ids_query = conn.prepare("SELECT str, id FROM hathi_str WHERE str IN (#{x_qmarks.join(',')})");
@insert_str_query  = conn.prepare("INSERT INTO hathi_str (str) VALUES (?)");
@last_id_query     = conn.prepare("SELECT LAST_INSERT_ID() AS id");
@count_oclcs_query = conn.prepare("SELECT COUNT(DISTINCT str_id) AS c FROM hathi_oclc");

log.d("Getting a before-count on hathi_oclc...");
before_count = 0;
@count_oclcs_query.enumerate() do |row|
  before_count = row[:c];
end
log.d(before_count);

log.d("Getting oclcs and their str_ids from hathi_oclc");
old_oclc_to_str_id_map = {};
# Get all oclcs, 100 at a time
@get_oclcs_query.enumerate().each_slice(slice_size) do |slice|
  oclc_slice = [];
  slice.each do |row|
    old_oclc_to_str_id_map[row[:oclc].to_s] = row[:str_id].to_s;
    oclc_slice << row[:oclc];
  end

  # Get all known mappings, for 100 oclcs at a time
  @map_query.enumerate(*oclc_slice) do |row|
    if row[:a] != row[:min_b] then
      oclc_map[row[:a]] = row[:min_b];
    end
  end
end

log.d("Checking results.");
oclc_map.keys.each do |k|
  puts "#{k} --> #{oclc_map[k]}";
  if k.to_i < oclc_map[k].to_i then
    raise "why is #{k} less than #{oclc_map[k]} ?";
  end
end

log.d("#{oclc_map.keys.size} mapped pairs");

# Get str_ids for new oclcs and put in a map.
# They are not guaranteed to exist, so we need
# to insert some records to get 100% coverage.
new_oclc_to_str_id_map = {};
log.d("Getting existing str_ids for new oclcs");
oclc_map.values.uniq.each_slice(slice_size) do |slice|
  @get_str_ids_query.enumerate(*slice) do |row|
    new_oclc_to_str_id_map[row[:str].to_s] = row[:id].to_s;
  end
end

# Here are the ones that need a new str_id:
log.d("Getting str_ids for the new oclcs that don't have any.");
need_str_id = oclc_map.values.uniq - new_oclc_to_str_id_map.keys;
need_str_id.each do |oclc|
  @insert_str_query.execute(oclc);
  str_id = nil;
  @last_id_query.enumerate() do |row|
    str_id = row[:id];
  end
  new_oclc_to_str_id_map[oclc.to_s] = str_id.to_s;
end

log.d("Generating loadfile for temp table");
tmp_table_data = HTPH::Hathidata::Data.new('tmp_oclc.dat').open('w');
oclc_map.keys.each do |k|
  old_oclc   = k.to_s;
  new_oclc   = oclc_map[k].to_s;
  old_str_id = old_oclc_to_str_id_map[old_oclc];
  new_str_id = new_oclc_to_str_id_map[new_oclc];
  tmp_table_data.file.puts([old_str_id, new_str_id].join("\t"));
end
tmp_table_data.close();

# Create a temp table and load the contents of tmp_table_data.
iconn    = db.get_interactive();
create   = "CREATE TEMPORARY TABLE tmp_oclc_resolution (old_str_id INT(11), new_str_id INT(11))";
load_sql = "LOAD DATA LOCAL INFILE ? INTO TABLE tmp_oclc_resolution (old_str_id, new_str_id)"
log.d("Loading data into temp table");
iconn.execute(create);
load_query = iconn.prepare(load_sql);
load_query.execute(tmp_table_data.path);
iconn.query("SELECT COUNT(*) AS c FROM tmp_oclc_resolution") do |row|
  log.d("#{row[:c]} records in tmp_oclc_resolution");
end

# Use the temp table to update the str_ids in hathi_oclc
update_from_tmp_sql = %W[
  UPDATE IGNORE hathi_oclc       AS ho
  INNER JOIN tmp_oclc_resolution AS tmp
  ON ho.str_id  = tmp.old_str_id
  SET ho.str_id = tmp.new_str_id;
].join(" ");
log.d(update_from_tmp_sql);
iconn.execute(update_from_tmp_sql);

@count_oclcs_query.enumerate() do |row|
  log.d("Before-count was #{before_count}");
  log.d("After-count is #{row[:c]}");
end

log.d("Finished");
