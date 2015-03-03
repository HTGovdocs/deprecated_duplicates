require 'htph';

# Just get out all distinct strings for a table, sudoc for hathi_sudoc, issn for hathi_issn, etc. 

db = HTPH::Hathidb::Db.new();
conn = db.get_conn();

sql = "SELECT DISTINCT hs.str FROM hathi_str AS hs JOIN hathi_xxx AS hx ON (hs.id = hx.str_id) ORDER BY hs.str";
sql.sub!('xxx', ARGV.shift);
q   = conn.prepare(sql);

q.enumerate() do |row|
  puts row[:str];
end
