require 'htph';

@db   = HTPH::Hathidb::Db.new();
@conn = @db.get_conn();

@log = HTPH::Hathilog::Log.new();
@command_log = HTPH::Hathilog::Log.new({:file_name => 'rewrite_str.log'});

# Get the strings that match the rewrite criterion.
# The rewrite criterion is a WHERE-clause in a JOIN between hathi_str 
# and hathi_x (AS hx).

# rewrite_str.rb <table_name> <WHERE-contents> <gsub-from> <gsub-to>

# For instance, to rewrite all pubdate strings ending with a period to NOT end with a period, do:
# bundle exec ruby scripts/rewrite_str.rb hathi_pubdate "hs.str LIKE '%.'" "\.$" ""

def run
  # Keep commands for future reference.
  @command_log.i([$0, ARGV].flatten.join("\t"));

  hathi_x      = ARGV.shift; # hathi_table name
  where_clause = ARGV.shift; # WHERE-clause that fits sql_template.
  gsub_from    = Regexp.new(ARGV.shift); # First half of a ruby gsub (regex)
  gsub_to      = ARGV.shift; # Second half of a ruby gsub (str)

  @get_str_id    = @conn.prepare("SELECT id FROM hathi_str WHERE str = ?");
  @insert_str_id = @conn.prepare("INSERT INTO hathi_str (str) VALUES (?)");
  @update_x      = @conn.prepare("UPDATE IGNORE #{hathi_x} SET str_id = ? WHERE str_id = ?");
  @del_x         = @conn.prepare("DELETE FROM #{hathi_x} WHERE str_id = ?");

  sql_template = %W[
    SELECT DISTINCT hx.str_id, hs.str 
    FROM #{hathi_x} AS hx 
    JOIN hathi_str AS hs ON (hx.str_id = hs.id) 
    WHERE #{where_clause}
  ].join(' ');

  puts sql_template;

  i = 0;
  mapped_to = {};
  @conn.query(sql_template) do |row|
    i += 1;

    if i % 1000 == 0 then
      @log.d("#{i}\t#{mapped_to.keys.size}");
    end

    original_str    = row[:str];
    original_str_id = row[:str_id];

    updated_str    = original_str.gsub(gsub_from, gsub_to);
    updated_str_id = get_str_id(updated_str);

    mapped_to[updated_str_id] = 1;

    puts "#{original_str} --> #{updated_str}";
    puts "UPDATE #{hathi_x} SET str_id = #{updated_str_id} WHERE str_id = #{original_str_id}";
    @update_x.execute(updated_str_id, original_str_id);
    @del_x.execute(original_str_id);
  end
  @log.d("#{i}\t#{mapped_to.keys.size}");
end

def get_str_id (str)
  str_id = nil;
  @get_str_id.enumerate(str) do |row|
    str_id = row[:id];
  end
  if str_id.nil? then
    @insert_str_id.execute(str);
    @get_str_id.enumerate(str) do |row|
      str_id = row[:id];
    end
  end
  return str_id;
end

if __FILE__ == $0 then
  run();
end
