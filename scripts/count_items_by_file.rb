require 'htph';

db   = HTPH::Hathidb::Db.new();
conn = db.get_conn();

sql = %w[
  SELECT
    hif.file_path,
    hif.date_read,
    COUNT(hg.id) AS c
  FROM
  hathi_input_file AS hif
    LEFT JOIN
    hathi_gd AS hg ON (hif.id = hg.file_id)
  GROUP BY hif.file_path, hif.date_read
  ORDER BY c DESC;
].join(' ');

print_header = true;
conn.enumerate(sql) do |row|
  if print_header then
    puts row.to_h.keys.join("\t");
    print_header = false;
  end
  puts row.to_a.join("\t");
end
