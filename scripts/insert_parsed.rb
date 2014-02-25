$LOAD_PATH << './../lib/'
require 'gddb';
require 'json';

@conn          = nil;
@insert_item_q = nil;
@last_id_q     = nil;
@insert_prop_q = nil;
@str_exist_q   = nil;
@str_insert_q  = nil;
@max_str_len   = 499;

=begin

-- Get all:
-- props:
    SELECT gp.prop, vp.str, COUNT(gp.prop) AS cp FROM gd_prop AS gp JOIN v_gd_prop_str AS vp ON (gp.prop = vp.prop) GROUP BY gp.prop, vp.str ORDER BY cp;
-- vals:
    SELECT gp.val, vv.str, COUNT(gp.val) AS cv FROM gd_prop AS gp JOIN v_gd_val_str AS vv ON (gp.val = vv.val) GROUP BY gp.val, vv.str HAVING cv > 99 ORDER BY cv;

-- Show clusters:
    select gd_cluster_id, count(gd_cluster_id) as cc from gd_item_cluster group by gd_cluster_id order by cc desc;

-- Show actual properties of an item:
SELECT
    gp.gd_item_id,
    gp.prop,
    vp.str,
    gp.val,
    vv.str
FROM
    gd_prop            AS gp
    JOIN v_gd_prop_str AS vp ON (gp.prop = vp.prop)
    JOIN v_gd_val_str  AS vv ON (gp.val  = vv.val)
WHERE
    gp.gd_item_id = ?

-- Show the titles for a cluster:
SELECT
    gp.gd_item_id,
    gic.gd_cluster_id,
    vp.str AS prop,
    vv.str AS val
FROM
    gd_item_cluster    AS gic
    JOIN gd_prop       AS gp ON (gic.gd_item_id = gp.gd_item_id)
    JOIN v_gd_prop_str AS vp ON (gp.prop = vp.prop)
    JOIN v_gd_val_str  AS vv ON (gp.val = vv.val)
WHERE
    gic.gd_cluster_id = 18
    AND
    vp.str = 'Title';

-- Show strings for a property-value pair (mostly for human eyes):
SELECT 'prop' AS str_type, str FROM v_gd_prop_str WHERE prop = 18 
UNION 
SELECT 'val' AS str_type, str FROM v_gd_val_str WHERE val = 2046;

=end

def setup ()
  db    = Gddb::Db.new();
  @conn = db.get_interactive();

  delete_prop_sql = "DELETE FROM mwarin_ht.gd_prop";
  delete_item_sql = "DELETE FROM mwarin_ht.gd_item";
  insert_item_sql = "INSERT INTO mwarin_ht.gd_item (raw) VALUES (?)";
  last_id_sql     = "SELECT LAST_INSERT_ID() AS id";
  insert_prop_sql = "INSERT INTO mwarin_ht.gd_prop (gd_item_id, prop, val) VALUES (?, ?, ?)";
  str_exist_sql   = "SELECT gd_str_id, str FROM mwarin_ht.gd_str WHERE str = ?";
  str_insert_sql  = "INSERT INTO mwarin_ht.gd_str (str) VALUES (?)";

  puts delete_prop_sql;
  @conn.update(delete_prop_sql);
  puts delete_item_sql;
  @conn.update(delete_item_sql);

  # Prepared queries
  @insert_item_q = @conn.prepare(insert_item_sql);
  @last_id_q     = @conn.prepare(last_id_sql);
  @insert_prop_q = @conn.prepare(insert_prop_sql);
  @str_exist_q   = @conn.prepare(str_exist_sql);
  @str_insert_q  = @conn.prepare(str_insert_sql);
end

def shutdown ()
  @conn.close();
end

def parse_file (infile)
  c = 0;
  File.open(infile) do |f|
    f.each_line do |line|
      c += 1;
      break if c >= 100000;
      if c % 10000 == 0 then
        puts "zzz #{Time.new()}";
        sleep 1;
      end
      puts c if c % 100 == 0;
      line.strip!;
      insert_item(line);
    end
  end
end

def insert_item (line)
  y = @insert_item_q.execute(line);
  gd_item_id = nil;
  @last_id_q.query do |last|
    gd_item_id = last.id;
  end
  j = JSON.parse(line);
  j.each do |key,val|
    insert_prop(key, val, gd_item_id);
  end
end

def insert_prop (key, val, gd_item_id)
  if val.class != [].class then
    val = [val];
  end

  key_str_id = get_str_id(key);
  val.each do |v|
    v = v.to_s.strip;
    if v.length > (@max_str_len + 1) then
      puts "Truncated #{v}";
      v = v[0 .. @max_str_len];
    end

    if key == 'agency' then
      # Do some scrubbing to normalize agencies
      # Remove all versions of e.g. "for sale by the Supt. of Docs., U.S. Govt. Print Off"
      if v =~ /for sale by/i then
        v.sub!(/for sale by.+$/i, '')
      end
      vnormalized = v.downcase.gsub(/[^a-z]/, '');
      if (vnormalized == 'usgovtprintoff' || vnormalized == 'usgpo') then
        puts "Skipping #{v}";
        next;
      end
    end
    v_str_id = get_str_id(v);
    next if v_str_id.nil?;
    @insert_prop_q.execute(gd_item_id, key_str_id, v_str_id);
  end
end

def get_str_id (str)
  str_id = nil;

  str = str.gsub(/ +/, ' ');
  str = str.sub(/^ /, '');
  str = str.sub(/ $/, '');

  return str_id if str == '';

  @str_exist_q.enumerate(str) do |res|
    str_id = res[:gd_str_id];
  end

  if str_id.nil? then
    @str_insert_q.execute(str);
    @last_id_q.enumerate do |res|
      str_id = res[:id];
    end
  end

  return str_id.to_i;
end

if $0 == __FILE__ then
  infile = ARGV.shift;
  if infile.nil? then
    raise "Need infile";
  elsif !File.exists?(infile) then
    raise "Need infile that actually exists.";
  end

  setup();
  parse_file(infile);
  shutdown();
end
