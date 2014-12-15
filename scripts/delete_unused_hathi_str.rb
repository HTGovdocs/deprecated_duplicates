require 'htph';

# Get all the strings in hathi_str which aren't used and remove them.

db      = HTPH::Hathidb::Db.new();
conn    = db.get_conn();
# If you add any new tables, they must be reflected here.
tables  = %w[enumc isbn issn lccn oclc pubdate publisher sudoc title];

# Check counts before.
count = "SELECT COUNT(*) AS c FROM hathi_str";
puts count;
conn.query(count) do |r|
  puts r[:c];
end

# Build subquery
selects =  tables.map { |x| 
  "\nSELECT DISTINCT str_id FROM hathi_#{x}\n"
}.join("UNION")

# Put it all together and run:
sql = %W[
  DELETE FROM hathi_str WHERE id NOT IN (
    #{selects}
  )
].join(' ');

puts sql;
conn.execute(sql);

# Check counts after.
puts count;
conn.query(count) do |r|
  puts r[:c];
end
