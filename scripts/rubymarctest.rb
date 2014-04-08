require 'marc';
require 'zlib';
require 'json';

Zlib::GzipReader.open("/htapps/mwarin.babel/govdocs/input_data/minnesota/all_minn_records.ndj.gz") do |gzr|
  gzr.each_line do |line|
    puts line;
    r = MARC::Record.new_from_hash(JSON.parse(line));
    puts r['245'];
    break;
  end
end

reader = MARC::Reader.new("/htapps/mwarin.babel/govdocs/input_data/umich/frist.marc21")
for record in reader
  # print out field 245 subfield a
  puts record['245']
end
