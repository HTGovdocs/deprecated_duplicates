require 'htph';

=begin

Rather specific script, for adding sudoc to the hathi records. Only
useful as long as we're using the hathifile for input. Run before
adding any other members' data.

bundle exec ruby scripts/load_hathi_sudocs.rb

=end

log = HTPH::Hathilog::Log.new();
log.d("Started");

db   = HTPH::Hathidb::Db.new();
conn = db.get_conn();

get_id_sql = "SELECT id FROM hathi_gd WHERE record_id = ?";
get_id_q   = conn.prepare(get_id_sql);

insert_str_sql = "INSERT INTO hathi_str (str) VALUES (?)";
insert_str_q   = conn.prepare(insert_str_sql);

insert_sudoc_sql = "INSERT INTO hathi_sudoc (gd_id, str_id) VALUES (?, ?)";
insert_sudoc_q   = conn.prepare(insert_sudoc_sql);

last_id_sql = "SELECT LAST_INSERT_ID() AS id";
last_id_q   = conn.prepare(last_id_sql);

delete_sudocs_sql = "DELETE FROM hathi_sudoc";
delete_sudocs_q   = conn.prepare(delete_sudocs_sql);
delete_sudocs_q.execute();

i = 0;
HTPH::Hathidata.read("htid_sudoc.tsv") do |line|
  i += 1;
  if i % 5000 == 0 then
    log.d(i) ;
  end
  (htid, sudoc) = line.strip.split("\t");  
  get_id_q.query(htid) do |get_id_row|
    gd_id = get_id_row[:id];
    insert_str_q.execute(sudoc);
    last_id_q.query() do |get_last_row|
      str_id = get_last_row[:id];
      insert_sudoc_q.execute(gd_id, str_id);
    end
  end
end

log.d("Finished");
