require 'htph';

db   = HTPH::Hathidb::Db.new();
conn = db.get_conn();

sql = %w[
  SELECT
    hif.id,
    hif.file_path,
    DATE_FORMAT(hif.date_read, "%y-%m-%d %T") AS t,
    COUNT(hg.id) AS c
  FROM
  hathi_input_file AS hif
    LEFT JOIN
    hathi_gd AS hg ON (hif.id = hg.file_id)
  GROUP BY hif.file_path, hif.date_read
  ORDER BY t ASC, c DESC;
].join(' ');

print_header = true;
total = 0;
conn.enumerate(sql) do |row|
  if print_header then
    puts row.to_h.keys.join("\t");
    print_header = false;
  end
  total += row[:c].to_i;
  puts row.to_a.join("\t");
end
puts "Total:\t#{total}";
