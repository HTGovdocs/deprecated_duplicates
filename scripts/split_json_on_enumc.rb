require 'htph';
require 'json';

=begin

Takes a .ndj file as input and turns records with several enumc
values into separate records. Should be done on all files that are
output of traject before they are indexed.

Input:

  {"record_id":["976"], "enumc":["vol 1", "volume 2"]}
  {"record_id":["977"], "enumc":["v.3", "vol. 4"]}
  {"record_id":["978"], "enumc":["v5"]}
  {"record_id":["979"]}

Output:

  {"record_id":["976"]," enumc":["vol 1"]}
  {"record_id":["976"]," enumc":["volume 2"]}
  {"record_id":["977"]," enumc":["v.3"]}
  {"record_id":["977"]," enumc":["vol. 4"]}
  {"record_id":["978"]," enumc":["v5"]}
  {"record_id":["979"]}

=end

in_path  = ARGV.shift;
out_path = in_path.sub(/$/, '.enumsplit');

hdin  = HTPH::Hathidata::Data.new(in_path).open('r');
hdout = HTPH::Hathidata::Data.new(out_path).open('w');

hdin.file.each_line do |line|
  line_json = JSON.parse(line);
  if line_json.has_key?('enumc') then
    enum_size = line_json['enumc'].size;
    twin = line_json.clone;
    line_json['enumc'].sort.uniq.each do |e|
      twin['enumc'] = [e];
      
      hdout.file.puts(twin.to_json);
    end
  else
    hdout.file.puts(line);
  end
end

hdin.close();
hdout.close();
