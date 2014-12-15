require 'htph';
require 'java';

BEGIN {
  java_import 'java.lang.Runtime';
  mxm = Runtime.getRuntime.maxMemory.to_i;
  if mxm <= (999 * 1024 * 1024) then
    puts "I recommend you start with -J-Xmx1000m. You currently have #{mxm}.";
    raise "You're gonna need a bigger boat.";
  end
}

def prepare_qmarks (sql, bindparams)
  s = bindparams.size;
  q = sql.sub('__?__', (['?'] * s).join(','));
  return q;
end

db    = HTPH::Hathidb::Db.new();
@conn = db.get_interactive();

oclc_to_volid = {};
item_to_oclcs = {};
uniq_oclc     = {};
oclc_to_val   = {};

last_id_sql = "SELECT LAST_INSERT_ID() AS id";
last_id_q  = @conn.prepare(last_id_sql);

get_oclc_prop_sql = "SELECT prop FROM mwarin_ht.v_gd_prop_str WHERE str = 'oclc'";
oclc_prop = nil;
@conn.query(get_oclc_prop_sql) do |row|
  oclc_prop = row[:prop];
end

if oclc_prop.nil? then
  puts "Cannot go on if oclc_prop is nil";
  exit(1);
end

q1 = %w[
  SELECT DISTINCT
    gs.str, hho.volume_id
  FROM
    mwarin_ht.gd_prop         AS gp
    JOIN mwarin_ht.gd_str     AS gs  ON (gp.val  = gs.gd_str_id)
    JOIN holdings_htitem_oclc AS hho ON (gs.str  = hho.oclc)
  WHERE
    gp.prop = ?
].join(' ');

q2 = %w[
  SELECT DISTINCT
    oclc
  FROM
    holdings_htitem_oclc
  WHERE
    volume_id IN (__?__)
].join(' ');

# Idea for improvement: look up what the gd_str_id for 'oclc' is in a 
# separate step and use that instead of looking up the string each time.
q3 = %w[
  SELECT DISTINCT
    gp.gd_item_id
  FROM
    mwarin_ht.gd_prop AS gp
    JOIN mwarin_ht.gd_str AS gs1 ON (gp.prop = gs1.gd_str_id)
    JOIN mwarin_ht.gd_str AS gs2 ON (gp.val  = gs2.gd_str_id)
  WHERE
    gs1.str = 'oclc'
    AND
    gs2.str IN (__?__)
].join(' ');

q4  = "SELECT gd_str_id FROM mwarin_ht.gd_str WHERE str = ?";
pq4 = @conn.prepare(q4);

q5  = "INSERT INTO mwarin_ht.gd_str (str) VALUES (?)";
pq5 = @conn.prepare(q5);

q6  = "DELETE FROM mwarin_ht.gd_prop WHERE prop = ? AND gd_item_id = ?";
pq6 = @conn.prepare(q6);

q7  = "LOAD DATA LOCAL INFILE ? INTO TABLE mwarin_ht.gd_prop (gd_item_id, prop, val)";
pq7 = @conn.prepare(q7);

# Get all the oclcs that need to be checked for mapping.
pq1 = @conn.prepare(q1);
pq1.enumerate(oclc_prop) do |row|
  oclc  = row[:str];
  volid = row[:volume_id];
  oclc_to_volid[oclc] ||= [];
  oclc_to_volid[oclc] << volid;
end

# For each oclc, check if there is more than a 1:1 mapping.
oclc_to_volid.keys.each do |oclc|
  mapped = [];
  pq2 = @conn.prepare(prepare_qmarks(q2, oclc_to_volid[oclc]));
  pq2.enumerate(*oclc_to_volid[oclc]) do |row|
    mapped << row[:oclc];
  end
  mapped = mapped.sort.uniq;

  # If there is more than 1:1 mapping, find all documents that need
  # to be updated with additional oclcs.
  if mapped.size > 1 then
    puts "#{oclc} --> #{mapped.join(',')}";
    pq3 = @conn.prepare(prepare_qmarks(q3, mapped));
    pq3.enumerate(*mapped) do |row|
      gd_item_id = row[:gd_item_id];
      item_to_oclcs[gd_item_id] ||= [];
      item_to_oclcs[gd_item_id].concat(mapped).uniq!;
    end
    mapped.each do |oclc|
      uniq_oclc[oclc] = 1;
    end
  end
end
puts "OK, now we know all the OCLCs...";

# Don't need you any more.
oclc_to_volid = nil;

# Now go through oclcs and create gd_str records if needed,
# then write a loadfile for gd_prop
uniq_oclc.keys.sort.each do |oclc|
  # When incorporated in index_json, just ise get_str_id() here
  gd_str_id = nil;
  pq4.enumerate(oclc) do |row|
    gd_str_id = row[:gd_str_id];
  end
  if gd_str_id.nil? then
    puts "inserting new oclc #{oclc}";
    pq5.execute(oclc);
    last_id_q.enumerate() do |row|
      gd_str_id = row[:id];
    end
  end
  oclc_to_val[oclc] = gd_str_id;
end

# Don't need you anymore.
uniq_oclc = nil;

# Generate loadfile:
HTPH::Hathidata.write('items_to_oclc.txt') do |hdout|
  item_to_oclcs.keys.each do |item|
    item_to_oclcs[item].each do |oclc|
      hdout.file.puts [item, oclc_prop, oclc_to_val[oclc]].join("\t");
      pq6.execute(oclc_prop, item);
    end
  end
end

# Load from file:
pq7.execute(HTPH::Hathidata::Data.new('items_to_oclc.txt').path);
