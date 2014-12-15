require 'htph';
require 'set';

s = Set.new();
i = 0;

HTPH::Hathidata.read('all_enumc.txt') do |line|
  line.chomp!;
  enumc = HTPH::Hathinormalize.enumc(line);
  i    += 1;
  s    << enumc;
  if !enumc.nil? then
    puts enumc;
  end
end

total       = i;
unique      = s.size;
compression = (total - unique).to_f / unique;

$stderr.puts "Total:#{total}, Unique:#{unique}, Compression:#{100 * compression}%";

# 2.5% compression ((total - unique) / unique).
