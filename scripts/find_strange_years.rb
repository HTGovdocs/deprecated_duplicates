require 'htph';

found_something = 0;
found_nothing   = 0;
found_in_col    = {};

=begin
Found a bunch of weird years in the hathifile, and this is trying
to figure out where these years come from.
=end

HTPH::Hathidata.read('strange_pubdates.tsv') do |line|
  bits = line.strip.split("\t");
  pubyear = bits[16];
  rx = Regexp.new(/\b#{pubyear}\b/);
  found = false;
  bits.each_with_index do |b,i|
    if i != 16 then
      if b =~ rx then
        puts "column #{i} contains #{pubyear}: #{b}";
        found = true;
        found_in_col[i] ||= 0;
        found_in_col[i] += 1;
      end
    end
  end
  if found then
    found_something += 1;
  else
    puts "found no occurrence of #{pubyear}";
    found_nothing += 1;
  end
end

puts "#{found_something} could be found, #{found_nothing} couldnt be found.";
puts "Of the ones that were found, they were in these columns:";

found_in_col.keys.sort.each do |k|
  puts "#{k} #{found_in_col[k]}";
end
