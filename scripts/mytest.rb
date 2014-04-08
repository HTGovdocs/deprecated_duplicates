require 'dotenv';
Dotenv.load;
require 'htph';

db = HTPH::Hathidb::Db.new();
conn = db.get_conn();
