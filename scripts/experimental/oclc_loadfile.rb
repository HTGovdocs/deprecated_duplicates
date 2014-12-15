require 'htph';

db    = HTPH::Hathidb::Db.new();
conn  = db.get_conn();
bench = HTPH::Hathibench::Benchmark.new();
hd    = HTPH::Hathidata::Data.new('hathi_oclc.dat').open('w');

bench.time("select") do
  conn.query("SELECT gd_id, str_id FROM hathi_oclc") do |row|
    hd.file.puts("#{row[:gd_id]}\t#{row[:str_id]}");
  end
end
hd.close();

bench.time("delete") do
  conn.execute("DELETE FROM hathi_oclc");
end

insert_sql   = "INSERT INTO hathi_oclc (gd_id, str_id) VALUES (?, ?)";
insert_query = conn.prepare(insert_sql);

hd.open('r').file.each_line do |line|
  (gd_id,str_id) = line.split("\t");
  bench.time("insert") do
    insert_query.execute(gd_id, str_id);
  end
end
hd.close();

bench.time("delete") do
  conn.execute("DELETE FROM hathi_oclc");
end

load_sql   = "LOAD DATA LOCAL INFILE ? INTO TABLE hathi_oclc (gd_id, str_id)";
load_query = conn.prepare(load_sql);

bench.time("load") do
  load_query.execute(hd.path);
end

puts bench;
