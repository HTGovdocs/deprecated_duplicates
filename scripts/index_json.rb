require 'digest';
require 'htph';
require 'json';

=begin

Takes a list of preprocessed newline-delimited json files, does some
more processing, and creates records in the database.

=end

@conn          = nil;
@schema        = 'mwarin_ht';
@insert_item_q = nil;
@last_id_q     = nil;
@insert_prop_q = nil;
@str_exist_q   = nil;
@str_insert_q  = nil;
@max_str_len   = 499;
@str_mem       = {};
@max_str_mem   = 1000;
@str_mem_hit   = 0;
@str_mem_miss  = 0;
@sha_digester  = nil;
@usgporx       = /usgovtprintoff|govtprintoff|usgpo|gpo/;

def setup ()
  db    = HTPH::Hathidb::Db.new();
  @conn = db.get_interactive();

  insert_item_sql = "INSERT INTO mwarin_ht.gd_item (raw, hashsum) VALUES (?, ?)";
  last_id_sql     = "SELECT LAST_INSERT_ID() AS id";
  insert_prop_sql = "INSERT INTO mwarin_ht.gd_prop (gd_item_id, prop, val) VALUES (?, ?, ?)";
  str_exist_sql   = "SELECT gd_str_id, str FROM mwarin_ht.gd_str WHERE str = ?";
  str_insert_sql  = "INSERT INTO mwarin_ht.gd_str (str) VALUES (?)";

  # Prepared queries
  @insert_item_q = @conn.prepare(insert_item_sql);
  @last_id_q     = @conn.prepare(last_id_sql);
  @insert_prop_q = @conn.prepare(insert_prop_sql);
  @str_exist_q   = @conn.prepare(str_exist_sql);
  @str_insert_q  = @conn.prepare(str_insert_sql);

  @sha_digester  = Digest::SHA256.new();
end

def shutdown ()
  @conn.close();
end

# Triggered if the commandline args contain the string 'clean'.
# Performs some DELETEs.

def clean_tables
  deletes_from_table = %w[gd_cluster_weights gd_item_cluster gd_cluster gd_prop gd_item gd_str];
  deletes_from_table.each do |tbl|
   q = "DELETE FROM #{@schema}.#{tbl}";
   puts q;
   @conn.update(q);
  end
end

# Reads input file line by line and calls deeper methods.
def index_file (infile)
  puts "Indexing #{infile}";
  puts "Started #{Time.new()}";
  c = 0;
  File.open(infile) do |f|
    f.each_line do |line|
      break if c >= 10000;
      c += 1;
      if c % 10000 == 0 then
        puts "zzz #{Time.new()}";
        sleep 1;
      end
      if c % 1000 == 0 then
        puts "#{Time.new()} | #{c} records | #{@str_mem_hit} cache hits / #{@str_mem_miss} cache misses";
      end
      line.strip!;
      process_line(line);
    end
  end
end

# Finds items in a line and calls deeper methods to insert them into db.
def process_line (line)
  j = JSON.parse(line);

  if j.has_key?('enum_chron') && j['enum_chron'].class == [].class && j['enum_chron'].size > 1 then
    enum_chrons = j['enum_chron'];
    enum_chrons.each do |ec|
      ecj = j;
      ecj['enum_chron'] = [ec];
      insert_item(ecj);
    end
  else
    insert_item(j);
  end
end

# Inserts an item into the db and calls deeper methods to insert its properties.
def insert_item (json)
  jstr     = json.to_s;
  sha_hash = @sha_digester.hexdigest(jstr);
  @insert_item_q.execute(jstr, sha_hash);
  gd_item_id = nil;
  @last_id_q.query do |last|
    gd_item_id = last.id;
  end
  json.each do |key,val|
    insert_prop(key, val, gd_item_id);
  end
end

# Inserts a single property into the database. Calls deeper methods to look up str_ids.
def insert_prop (key, val, gd_item_id)
  if val.class != [].class then
    val = [val];
  end

  key_str_id = get_str_id(key);
  val.each do |v|
    v = v.to_s.strip;
    if v.length > (@max_str_len + 1) then
      v = v[0 .. @max_str_len];
    end

    # Do some scrubbing to normalize agencies
    # Remove all versions of e.g. "for sale by the Supt. of Docs., U.S. Govt. Print Off"
    if key == 'agency' then
      if v =~ /for sale by/i then
        v.sub!(/for sale by.+$/i, '');
      end
      vnorm = v.downcase.gsub(/[^a-z]/, '');
      next if vnorm =~ @usgporx;
    end

    v_str_id = get_str_id(v);
    next if v_str_id.nil?;
    @insert_prop_q.execute(gd_item_id, key_str_id, v_str_id);
  end
end

# Takes a string as input and returns a gd_str_id
# from gd_str. Uses an internal hash to cache the
# last 1000 seen strings. Inserts in db if not
# cached and not already in db.

def get_str_id (str)
  str_id = nil;
  str = str.gsub(/ +/, ' ');
  str = str.sub(/^ /, '');
  str = str.sub(/ $/, '');

  return str_id if str == '';
  if @str_mem.has_key?(str) then
    @str_mem_hit += 1;
    return @str_mem[str];
  end
  @str_mem_miss += 1;

  @str_exist_q.enumerate(str) do |res|
    str_id = res[:gd_str_id];
  end

  if str_id.nil? then
    @str_insert_q.execute(str);
    @last_id_q.enumerate do |res|
      str_id = res[:id];
    end
  end

  if @str_mem.keys.size >= @max_str_mem then
    # Mem hash is full, make some room, delete 10% of keys.
    @str_mem.keys[0 .. (@str_mem.keys.size / 10)].each do |k|
      @str_mem.delete(k);
    end
  end

  @str_mem[str] = str_id.to_i;

  return str_id.to_i;
end

# Main:
# Pass 'clean' as commandline arg to empty the relevant db tables
# and/or a list of paths to input files full of newlined-delimited
# json.
if $0 == __FILE__ then
  if ARGV.size == 0 then
    raise "Need infile";
  end

  setup();
  ARGV.each do |infile|
    if infile == 'clean' then
      clean_tables();
      next;
    end
    if !File.exists?(infile) then
      raise "Need infile that actually exists.";
    end
    index_file(infile);
  end
  shutdown();
end
