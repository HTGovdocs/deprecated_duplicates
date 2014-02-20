require 'marc';
require 'zlib';
require 'json';

Zlib::GzipReader.open("/htapps/mwarin.babel/govdocs/CIC/CIC_all.ndj.gz") do |gzr|
  gzr.each_line do |line|
    puts line;
    r = MARC::Record.new_from_hash(JSON.parse(line));
    puts r['245'];
    break;
  end
end
