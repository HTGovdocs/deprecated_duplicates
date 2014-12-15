require 'htph';

db = HTPH::Hathidb::Db.new();
conn = db.get_conn();

chunks = [[]];
qm100 = (['?'] * 100).join(',')
sql   = "SELECT DISTINCT hs.str FROM hathi_lccn AS hl JOIN hathi_str AS hs ON (hl.str_id = hs.id) WHERE hs.str IN (#{qm100})";
q     = conn.prepare(sql);

HTPH::Hathidata.read('lccn_with_r.txt') do |line|
  line.strip!;
  if chunks.last.size >= 100 then
    chunks << [];
  end
  chunks.last << line;
end

chunks.each do |chunk|
  if chunk.size < 100 then
    chunk = [chunk, ([nil] * (100 - chunk.size))].flatten;
  end
  q.query(*chunk) do |row|
    puts row[:str];
  end
end
