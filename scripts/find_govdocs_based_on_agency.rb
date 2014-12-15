require 'htph';
require 'json';
require 'set';

agency_sudoc_map = {};
sudoc            = nil;

# Read sudoc-agency map.
HTPH::Hathidata.read(ARGV.shift) do |line|
  line.chomp!;
  (s,agency) = line.split("\t");
  if !agency.nil? then
    agency_sudoc_map[agency] ||= Set.new();
    agency_sudoc_map[agency] << sudoc;
  else
    # Break up X1 into X 1 
    sudoc = s.gsub(/^([A-Z]+)([0-9]+)$/, "\\1 \\2");
  end
end

i = 0;

# Read hathidocs (non_govdoc_minimarc.ndj produced by get_non_govdocs.rb)
HTPH::Hathidata.read(ARGV.shift) do |line|
  i += 1;
  # if i > 100 then
  #   throw :break;
  # end
  j        = JSON.parse(line);
  id       = j["HOL_p"]; 
  agencies = [j["110"], j["260_b"], j["710"]].flatten.compact.uniq; 
  stems    = Set.new();
  # For each agency field, normalize it, look up in agency-sudoc map
  agencies.each do |a| 
    sudoc_stems = agency_sudoc_map[HTPH::Hathinormalize.agency(a)];
    if !sudoc_stems.nil? then
      sudoc_stems.each do |stem|
        stems << stem;
      end 
    end
  end
  if stems.size > 0 then
    puts "#{id}\t#{stems.to_a.sort.join("\t")}";
  end
end
