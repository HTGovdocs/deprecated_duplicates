require 'htph';

partials = {};

# Take first file and read all non-normalized lines (starts with a tab).
i = 0;
HTPH::Hathidata.read(ARGV.shift) do |line|
  if line =~ /^\t(.+)/ then
    partials[$1] = 1;
    i += 1;
    puts i if i % 10000 == 0;
  end
end

# read the full hathifile and print lines whose col 12 is in partials.
HTPH::Hathidata.read(ARGV.shift) do |line| 
  bits = line.split("\t");
  if partials.has_key?(bits[12]) then
    puts line;
  end
end
