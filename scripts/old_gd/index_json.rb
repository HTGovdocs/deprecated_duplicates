require 'digest';
require 'htph';
require 'json';

=begin

Takes a list of preprocessed newline-delimited json files, does some
more processing, and creates records in the database.

=end

@conn           = nil;
@schema         = 'mwarin_ht';
@insert_item_q  = nil;
@last_id_q      = nil;
@str_exist_q    = nil;
@str_insert_q   = nil;
@max_str_len    = 499;
@str_mem        = {};
@max_str_mem    = 1000;
@str_mem_hit    = 0;
@str_mem_miss   = 0;
@sha_digester   = nil;
@usgporx        = /usgovtprintoff|govtprintoff|usgpo|gpo/;
@issnrx         = /(\d+{4}\-\d{3}[0-9X])/;
@hd_prop        = nil;
@oclc_map_cache = {};
@did_clean      = false;

def setup ()
  db    = HTPH::Hathidb::Db.new();
  @conn = db.get_interactive();

  insert_item_sql = "INSERT INTO mwarin_ht.gd_item (raw, hashsum) VALUES (?, ?)";
  last_id_sql     = "SELECT LAST_INSERT_ID() AS id";
  str_exist_sql   = "SELECT gd_str_id, str FROM mwarin_ht.gd_str WHERE str = ?";
  str_insert_sql  = "INSERT INTO mwarin_ht.gd_str (str) VALUES (?)";
  load_props_sql  = "LOAD DATA LOCAL INFILE ? INTO TABLE mwarin_ht.gd_prop (gd_item_id, prop, val)";

  # Non distinct, we need to uniq the results.
  @get_oclc_mapping_sql = [
                           'SELECT o2.oclc FROM',
                           'holdings_htitem_oclc AS o1',
                           'JOIN holdings_htitem_oclc AS o2',
                           'ON (o1.volume_id = o2.volume_id AND o1.oclc != o2.oclc)',
                           'WHERE o1.oclc IN (__?__)'
                          ].join(' ');

  # Prepared queries
  @insert_item_q = @conn.prepare(insert_item_sql);
  @last_id_q     = @conn.prepare(last_id_sql);
  @str_exist_q   = @conn.prepare(str_exist_sql);
  @str_insert_q  = @conn.prepare(str_insert_sql);
  @load_props_q  = @conn.prepare(load_props_sql);

  @sha_digester  = Digest::SHA256.new();
end

def shutdown ()
  if @did_clean == true then
    restore_weights();
  end
  @conn.close();
end

# Triggered if the commandline args contain the string 'clean'.
# Performs some DELETEs and resets the auto-increment value.

def clean_tables
  @did_clean = true;
  # Not deleting gd_str at this point, that could save some time.
  # Rather, add a function to remove any unused gd_str row AFTERwards.
  deletes_from_table = %w[gd_cluster_weights gd_item_cluster gd_cluster gd_weights gd_prop gd_item];
  deletes_from_table.each do |tbl|
    q1 = "DELETE FROM #{@schema}.#{tbl}";
    puts q1;
    @conn.update(q1);
    q2 = "ALTER TABLE #{@schema}.#{tbl} AUTO_INCREMENT = 1";
    puts q2;
    @conn.update(q2);
  end
end

# ... I guess keep them in for now.
def restore_weights
  prop_q      = "SELECT prop FROM mwarin_ht.v_gd_prop_str WHERE str = ?";
  p_prop_q    = @conn.prepare(prop_q);
  restore_q   = "INSERT INTO mwarin_ht.gd_weights (prop_str, weight) VALUES (?, ?)";
  p_restore_q = @conn.prepare(restore_q);
  HTPH::Hathidata.read('settings.tsv') do |line|
    prop_str,weight = *line.strip.split("\t");
    prop_str = nil;

    p_prop_q.enumerate(str) do |row|
      prop_str = row[:prop_str];
    end

    if prop_str.nil? then
      puts "Found no prop_str = #{prop_str}";
    else
      puts "restoring #{prop_str} #{weight}";
      p_restore_q.execute(prop_str, weight);
    end
  end
end

# Reads input file line by line and calls deeper methods.
def index_file (infile)
  puts "Indexing #{infile}";
  @hd_prop = HTPH::Hathidata::Data.new('prop_infile.tsv').open('w');
  @hd_prop.file.sync = false; # Turning off autoflush should make it faster?
  puts "Started #{Time.new()}";
  c = 0;
  File.open(infile) do |f|
    f.each_line do |line|
      break if c >= 40000;
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

  @hd_prop.close();
  puts "Loading #{@hd_prop.path}";
  @load_props_q.execute(@hd_prop.path);
  @hd_prop.delete();
end

# Finds items in a line and calls deeper methods to insert them into db.
def process_line (line)
  j = JSON.parse(line);

  if j.has_key?('issn') then
    issns = [];
    j['issn'].each do |issn|
      issn.split(/\s+/).each do |i|
        if i =~ @issnrx then
          issns << $1;
        end
      end
    end
    j['issn'] = issns.sort.uniq;
  end

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
  json_sans_who = json.select{|k,v| k != 'who'};
  jstr = json_sans_who.to_s;
  # The hash is based on the string representation
  # which contains everything except the who-property.
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

  # This is just too damn slow!
  #if key == 'oclc' then
  #  # If there is no mappings then we get empty back.
  #  oclcs = get_oclc_mapping(val);
  #  if oclcs != [] then
  #    val = oclcs;
  #  end
  #end

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

    # Remove leading zeroes in OCLC.
    if key == 'oclc' && v[0] == '0' then
      v.sub!(/^0+/, '');
    end

    v_str_id = get_str_id(v);
    next if v_str_id.nil?;
    @hd_prop.file.puts [gd_item_id, key_str_id, v_str_id].join("\t");
  end
end

# for the oclcs [x,y,z] look if there are any mappings,
# and return the set of all input and mapped oclcs.
# The last 1000 mappings are cached.
def get_oclc_mapping (oclc_in)
  oclc_mapped = [];
  oclc_in.sort!.map!{|o| o.to_i};

  cache_key = oclc_in.to_s;
  if @oclc_map_cache.has_key?(cache_key) then
    return @oclc_map_cache[cache_key];
  end

  if @oclc_map_cache.keys.size >= 1000 then
    puts "making room in oclc cache"
    @oclc_map_cache.keys.first(100).each do |k|
      @oclc_map_cache.delete(k);
    end
  end

  q = prepare_qmarks(@get_oclc_mapping_sql, oclc_in);
  pq = @conn.prepare(q);
  pq.query(*oclc_in) do |row|
    oclc_mapped << row[:oclc].to_i;
  end

  if oclc_mapped.size == 0 then
    oclc_out = oclc_in;
  else
    oclc_out = [oclc_in, oclc_mapped].flatten.uniq.sort;
    if oclc_out.size != oclc_in.size then
      puts "OCLC map #{oclc_in.join(',')} -> #{oclc_out.join(',')}";
    end
  end

  @oclc_map_cache[cache_key] = oclc_out;

  return oclc_out;
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

def prepare_qmarks (sql, bindparams)
  sql.sub('__?__', (['?'] * bindparams.size).join(','));
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
