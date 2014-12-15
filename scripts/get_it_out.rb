require 'htph';

# Reads hathi file into an intermediate format with just the stuff we care about.

cols = {
  :oclc       => 7 ,
  :isbn       => 8 ,
  :issn       => 9 ,
  :lccn       => 10,
  :title      => 11,
  :enum_chron => 4 ,
  :imprint    => 12, # publisher + pubdate
  :gov_doc    => 15,
  :pubdate    => 16,
  :htid       => 0 ,
};

i = 0;
hdout = HTPH::Hathidata::Data.new('hathi_selected_govdocs').open('w');
hdout.file.puts cols.keys.join("\t");
# hathi_full_YYYYMMDD.txt
HTPH::Hathidata.read(ARGV.shift) do |line|
  i += 1;
  if i % 50000 == 0 then
    puts i;
  end
  arr = line.split("\t");
  if arr[cols[:gov_doc]] == '1' then
    out = [];
    cols.values.each do |c|
      out << arr[c];
    end
    hdout.file.puts out.join("\t");
  end
end
hdout.close();
