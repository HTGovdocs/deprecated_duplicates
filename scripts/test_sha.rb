require 'htph';
require 'digest';

db     = HTPH::Hathidb::Db.new();
conn   = db.get_conn();
q      = "SELECT gd_item_id, raw FROM mwarin_ht.gd_item";
sha256 = Digest::SHA256.new();

log = HTPH::Hathilog::Log.new({:file_name => 'digest_time.log'});

conn.query(q) do |row|
  digest = sha256.hexdigest(row[:raw]);
  log.d("#{row[:gd_item_id]} : #{digest}");
end
log.close();
conn.close();
