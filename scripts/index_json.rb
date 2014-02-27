$LOAD_PATH << './../lib/'
require 'gddb';
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

def setup ()
  db    = Gddb::Db.new();
  @conn = db.get_interactive();

  deletes_from_table = %w[gd_cluster_weights gd_item_cluster gd_cluster gd_prop gd_item gd_str];
  deletes_from_table.each do |tbl|
    q = "DELETE FROM #{@schema}.#{tbl}";
    puts q;
    @conn.update(q);
  end

  insert_item_sql = "INSERT INTO mwarin_ht.gd_item (raw) VALUES (?)";
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
end

def shutdown ()
  @conn.close();
end

def index_file (infile)
  puts "Indexing #{infile}";
  c = 0;
  File.open(infile) do |f|
    f.each_line do |line|
      # break if c >= 10000;
      c += 1;
      if c % 10000 == 0 then
        puts "zzz #{Time.new()}";
        sleep 1;
      end
      if c % 1000 == 0 then
        puts "#{Time.new()} | #{c} records";
      end
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
      v = v[0 .. @max_str_len];
    end

    # Do some scrubbing to normalize agencies
    # Remove all versions of e.g. "for sale by the Supt. of Docs., U.S. Govt. Print Off"
    if key == 'agency' then
      if v =~ /for sale by/i then
        v.sub!(/for sale by.+$/i, '')
      end
      vnormalized = v.downcase.gsub(/[^a-z]/, '');
      if (vnormalized == 'usgovtprintoff' || vnormalized == 'usgpo' || vnormalized == 'gpo') then
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

# Main:
if $0 == __FILE__ then
  if ARGV.size == 0 then
    raise "Need infile";
  end

  setup();
  ARGV.each do |infile|
    index_file(infile);
    if !File.exists?(infile) then
      raise "Need infile that actually exists.";
    end
  end
  shutdown();
end
