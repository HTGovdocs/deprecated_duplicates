require 'htph';

log = HTPH::Hathilog::Log.new();

q = "SELECT oclc, isbn, issn, lccn, gov_doc FROM hathi_files";

f_gd_oclc = HTPH::Hathidata::Data.new("gd_oclc.dat").open("w");
f_gd_isbn = HTPH::Hathidata::Data.new("gd_isbn.dat").open("w");
f_gd_issn = HTPH::Hathidata::Data.new("gd_issn.dat").open("w");
f_gd_lccn = HTPH::Hathidata::Data.new("gd_lccn.dat").open("w");

f_oclc = HTPH::Hathidata::Data.new("oclc.dat").open("w");
f_isbn = HTPH::Hathidata::Data.new("isbn.dat").open("w");
f_issn = HTPH::Hathidata::Data.new("issn.dat").open("w");
f_lccn = HTPH::Hathidata::Data.new("lccn.dat").open("w");

db = HTPH::Hathidb::Db.new();
conn = db.get_conn();

i = 0;

conn.query(q) do |row|
  i += 1;
  if i % 100000 == 0 then
    log.d(i);
  end

  gov_doc = row[:gov_doc];
  oclc    = row[:oclc];
  isbn    = row[:isbn];
  issn    = row[:issn];
  lccn    = row[:lccn];

  if gov_doc == 1 then
    if oclc != '' then
      f_gd_oclc.file.puts oclc;
    end
    if isbn != '' then
      f_gd_isbn.file.puts isbn;
    end
    if issn != '' then
      f_gd_issn.file.puts issn;
    end
    if lccn != '' then
      f_gd_lccn.file.puts lccn;
    end
  else
    if oclc != '' then
      f_oclc.file.puts oclc;
    end
    if isbn != '' then
      f_isbn.file.puts isbn;
    end
    if issn != '' then
      f_issn.file.puts issn;
    end
    if lccn != '' then
      f_lccn.file.puts lccn;
    end
  end
end

[f_gd_oclc, f_gd_isbn, f_gd_issn, f_gd_lccn, f_oclc, f_isbn, f_issn, f_lccn].each do |f|
  f.close();
end
