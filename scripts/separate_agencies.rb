require 'htph';

=begin
bundle exec ruby -J-Xmx2048m scripts/separate_agencies.rb hathi_full_20140801.txt

Read a whole hathifile, pick out the govdoc indicator and imprint,
and put publisher into different files based on whether they exist
in only govdocs, only non-govdocs or both.
=end

gds        = {};
non_gds    = {};
# Used to find and remove pubdate from imprint.
pubdate_rx = /c?-?[0-9u]{4}\??(\s*(-|i.\s*e.|or)?\s*c?[0-9u]{0,4})\??/;
# Remove any occurences of any of these.
junk_rx    = /[,:;\.\-\(\)\[\]\{\}<>\|\$!"'#\*\+\/\\]/;
# Find any length of spaces (so we can replace with a single space)
space_rx   = / +/;
# 'for sale by' and anything that follows, so we can remove it.
forsale_rx = /for sale by.+/i;
# if we cant find a single a-z, then it can't be a thing.
az_rx      = /[a-z]/;

HTPH::Hathidata.read(ARGV.shift) do |line|
  cols     = line.split("\t");
  gd       = cols[15];
  imprint  = cols[12];
  original = "#{imprint}";

  imprint.gsub!(pubdate_rx, '');
  imprint.gsub!(junk_rx,    ' ');
  imprint.gsub!(forsale_rx, '');
  imprint.gsub!(space_rx,   ' ');
  imprint.strip!;
  imprint.downcase!;
  next if imprint !~ az_rx;

  if gd == '1'
    if !gds.has_key?(imprint) then
      gds[imprint] = {};
    end
    gds[imprint][original] = 1;
  elsif gd == '0'
    if !non_gds.has_key?(imprint) then
      non_gds[imprint] = {};
    end
    non_gds[imprint][original] = 1;
  else
    puts "#{gd} ???";
  end
end

hd_solo_gd     = HTPH::Hathidata::Data.new("solo_gd.txt").open("w");
hd_solo_non_gd = HTPH::Hathidata::Data.new("solo_non_gd.txt").open("w");
hd_overlap     = HTPH::Hathidata::Data.new('agency_overlaps.txt').open("w");
j = 0;

gds.keys.sort.each do |k|
  if non_gds.has_key?(k) then
    # Step 1: Put overlaps in separate file.
    j += 1;
    hd_overlap.file.puts k;
    [gds[k].keys, non_gds[k].keys].flatten.sort.uniq.each do |v|
      hd_overlap.file.puts("\t#{v}");
    end
    non_gds.delete(k); # Makes step 3 easier.
  else
    # Step 2: Put the only-gd in separate file.
    hd_solo_gd.file.puts(k);
    gds[k].keys.sort.each do |v|
      hd_solo_gd.file.puts("\t#{v}");
    end
  end
end

# Step 3: Put non-gd in separate file.
non_gds.keys.sort.each do |k|
  hd_solo_non_gd.file.puts(k);
  non_gds[k].keys.sort.each do |v|
    hd_solo_non_gd.file.puts("\t#{v}");
  end
end

hd_solo_gd.close();
hd_solo_non_gd.close();

puts "#{j} overlaps";
