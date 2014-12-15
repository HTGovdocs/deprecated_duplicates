require 'htph';
# Testing that sqlite works.
sqlite_file = HTPH::Hathidata::Data.new('hathifiles.db');
sqlite_conn = JDBCHelper::SQLite.connect(sqlite_file.path);

q = %w[
    SELECT b.htid, n.num FROM bibs AS b
    JOIN nums AS n ON (b.bibid = n.bibid)
    WHERE n.type = "sudoc"
].join(' ');

i = 0;
HTPH::Hathidata.write("htid_sudoc.tsv") do |hdout|
  sqlite_conn.query(q) do |row|
    i += 1;
    hdout.file.puts [:htid, :num].map{ |x| row[x].strip }.join("\t");
  end
end
puts "#{i} rows";
