# Read an agencymap and count how many agencies map to how many sudocs?

require 'htph';

agency_count = {};

HTPH::Hathidata.read('agency.map') do |line|
  next if !line.start_with?("\t");
  line.strip!;
  agency_count[line] ||= 0;
  agency_count[line]  += 1;
end

agency_count.keys.each do |k|
  puts "#{agency_count[k]}\t#{k}";
end
