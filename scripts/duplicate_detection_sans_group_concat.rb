require 'htph';
require 'set';

db    = HTPH::Hathidb::Db.new();
@conn = db.get_conn();

def run
  # Clean the table of related ids.
  sql_delete_related = "DELETE FROM hathi_related";
  q_delete_related   = @conn.prepare(sql_delete_related);
  puts sql_delete_related;
  q_delete_related.execute();

  # Get related ids and the checksum of the things that make them related.
  # Enum-chrons are NOT included in this pass.
  sql_get_related = %w[
    SELECT
      h.id AS gd_id,
      MD5(CONCAT_WS(',', d.str_id, p.str_id, s.str_id, o.str_id, t.str_id, l.str_id, i.str_id)) AS checksum
    FROM
      hathi_gd                  AS h
      LEFT JOIN hathi_pubdate   AS d ON (h.id = d.gd_id)
      LEFT JOIN hathi_publisher AS p ON (h.id = p.gd_id)
      LEFT JOIN hathi_sudoc     AS s ON (h.id = s.gd_id)
      LEFT JOIN hathi_oclc      AS o ON (h.id = o.gd_id)
      LEFT JOIN hathi_title     AS t ON (h.id = t.gd_id)
      LEFT JOIN hathi_lccn      AS l ON (h.id = l.gd_id)
      LEFT JOIN hathi_issn      AS i ON (h.id = i.gd_id)
  ].join(" ");
  q_get_related = @conn.prepare(sql_get_related);

  # Put related ids into a table.
  sql_load_related = "LOAD DATA LOCAL INFILE ? INTO TABLE hathi_related (gd_id, checksum)";
  q_load_related   = @conn.prepare(sql_load_related);

  sql_get_checksums = "SELECT checksum FROM hathi_related GROUP BY checksum HAVING COUNT(checksum) >= 2";
  q_get_checksums   = @conn.prepare(sql_get_checksums);

  sql_get_ids = %w[SELECT gd_id FROM hathi_related WHERE checksum = ?].join(' ');
  q_get_ids   = @conn.prepare(sql_get_ids);

  # Similar query to get the actual records.
  qmarks_a = ['?'] * 50;
  qmarks   = qmarks_a.join(',');
  sql_get_values = %W[
    SELECT
      h.id,
      d.str_id AS date_id,
      p.str_id AS publisher_id,
      s.str_id AS sudoc_id,
      o.str_id AS oclc_id,
      t.str_id AS title_id,
      l.str_id AS lccn_id,
      i.str_id AS issn_id,
      e.str_id AS enumc_id
    FROM
      hathi_gd                  AS h
      LEFT JOIN hathi_pubdate   AS d ON (h.id = d.gd_id)
      LEFT JOIN hathi_publisher AS p ON (h.id = p.gd_id)
      LEFT JOIN hathi_sudoc     AS s ON (h.id = s.gd_id)
      LEFT JOIN hathi_oclc      AS o ON (h.id = o.gd_id)
      LEFT JOIN hathi_title     AS t ON (h.id = t.gd_id)
      LEFT JOIN hathi_lccn      AS l ON (h.id = l.gd_id)
      LEFT JOIN hathi_issn      AS i ON (h.id = i.gd_id)
      LEFT JOIN hathi_enumc     AS e ON (h.id = e.gd_id)
    WHERE
      h.id IN (#{qmarks})
  ].join(' ');
  q_get_values = @conn.prepare(sql_get_values);

  seen = Set.new();

  hdout = HTPH::Hathidata::Data.new('related_ids_by_hash.dat').open('w');
  q_get_related.enumerate() do |row|
    hdout.file.puts("#{row[:gd_id]}\t#{row[:checksum]}");
  end
  hdout.close();
  puts sql_load_related;
  q_load_related.execute(hdout.path);
  
  # Loop over checksums that are known to have 2+ ids.
  q_get_checksums.enumerate() do |ch_row|
    checksum = ch_row[:checksum];

    ids = [];
    # Get ids connected to checksum.
    q_get_ids.enumerate(checksum) do |gi_row|
      ids << gi_row[:gd_id];
    end

    ids.sort!;
    ids_clone = ids.clone;
    next if seen.include?(ids);
    seen << ids;
    count_ids    = ids.size;
    chunk_max_size = 50;
    enumc_id_map = {};

    # For a cluster, get a hash of all values for sudoc, oclc, etc., 
    # and how many of each. {:sudoc_id=>{nil=>5}, :oclc_id=>{498486=>5}, ... }
    uniq_attr_set  = {
      :sudoc_id => {},
      :oclc_id  => {},
      :title_id => {},
      :lccn_id  => {},
      :issn_id  => {},
      :enumc_id => {},
    };

    # In case there are more than 50 ids.
    while ids.size > 0 do
      ids_chunk = [];
      1.upto(50).each do
        if ids.size > 0 then
          ids_chunk << ids.shift;
        end
      end
      padding = [nil] * (qmarks_a.size - ids_chunk.size);
      q_args  = [ids_chunk, padding].flatten;
      q_get_values.enumerate(*q_args) do |vals|
        [:sudoc_id, :oclc_id, :title_id, :lccn_id, :issn_id, :enumc_id].each do |x|
          uniq_attr_set[x][vals[x]] = 1;
        end
        # Keep track of which enumc goes with which id(s).
        enumc_id_map[vals[:enumc_id]] ||= Set.new();
        enumc_id_map[vals[:enumc_id]] << vals[:id];
      end
    end

    rel = "";
    if uniq_attr_set[:enumc_id].keys == [nil] then
      # None of the docs have enumchrons.
      rel = "duplicates";
    elsif uniq_attr_set[:enumc_id].keys.size == 1 then
      # All of the docs have the same enumchron.
      rel = "duplicates";
    elsif uniq_attr_set[:enumc_id].keys.size == count_ids then
      # There are as many enumchrons as there are documents.
      rel = "related";
    else
      # Look for duplicates inside the cluster.
      # If so, then mark duplicates with asterisk,
      # and the cluster as a whole with asterisk.
      subclusters = false;
      enumc_id_map.keys.each do |enumc| 
        if enumc_id_map[enumc].size > 1 then
          subclusters = true;
          puts "duplicates*\t#{enumc_id_map[enumc].sort.join(',')}";
        end
      end
      if subclusters == true then
        rel = "related*";
      else
        rel = "unclear";
      end
    end
    puts "#{rel}\t#{ids_clone.join(',')}";
  end
end

run();
