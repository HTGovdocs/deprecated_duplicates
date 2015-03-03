require 'htph';
require 'set';

# Take ids from argv, look them up and print.

@db     = HTPH::Hathidb::Db.new();
@conn   = @db.get_conn();
@tables = %w[isbn issn lccn oclc title enumc pubdate publisher sudoc].sort;

union  = @tables.map { |x|
  "SELECT '#{x}' AS t, hx.marc_field, hx.str_id FROM hathi_#{x} AS hx WHERE hx.gd_id = ?"
}.join("\nUNION\n");

big_sql = %W[
SELECT
    hif.file_path,
    hg.record_id,
    hg.item_id,
    u.t,
    u.marc_field,
    u.str_id
FROM
    hathi_gd         AS hg,
    hathi_input_file AS hif,
    (\n #{union} \n) AS u
WHERE
    hg.id = ?
    AND
    hif.id = hg.file_id
].join(' ');

# puts big_sql;

@q_get_values = @conn.prepare(big_sql);
sql_get_str = "SELECT str FROM hathi_str WHERE id = ?";
@q_get_str  = @conn.prepare(sql_get_str);

def main ()
  # Takes a list of ids to look up, or if ARGV[0] is -f then a file with one id per line.
  if ARGV.first == '-f' then
    ARGV.shift;
    ARGV.each do |arg|
      HTPH::Hathidata.read(arg) do |line|
        line.strip!;
        puts "\n## #{line} ##";
        get_doc(line);
        puts "\n";
      end
    end
  else
    ARGV.each do |id|
      puts "\n## #{id} ##";
      get_doc(id);
    end
  end
    puts "\n";
end

def get_doc (id)
  printed_header = false;
  @q_get_values.enumerate(*([id] * (@tables.size + 1))) do |row|
    if !printed_header then
      printed_header = true;
      puts "file_path\t\t#{row[:file_path]}";
      puts "record_id\t\t#{row[:record_id]}";
      puts "item_id\t\t#{row[:item_id] || 'NULL'}";
    end
    puts [row[:t], row[:marc_field], get_str(row[:str_id])].join("\t");
  end
end

def get_str (str_id)
  str = 'NULL';
  @q_get_str.enumerate(str_id) do |row|
    str = row[:str];
  end
  return str;
end

if __FILE__ == $0 then
  main();
end
