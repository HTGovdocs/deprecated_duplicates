require 'htph';
require 'json';

# Turn hathi file into an intermediate format (newline-delimited json) 
# with just the stuff we care about.

cols = { # which col is which
  :oclc       => 7 ,
  :isbn       => 8 ,
  :issn       => 9 ,
  :lccn       => 10,
  :title      => 11,
  :enum_chron => 4 ,
  :imprint    => 12, # publisher + pubdate
  :gov_doc    => 15,
  :pubdate    => 16,
  :record_id  => 0 ,
};

i = 0;
hdout = HTPH::Hathidata::Data.new('hathi_extract_$ymd.ndj').open('w');
# hathi_full_YYYYMMDD.txt
HTPH::Hathidata.read(ARGV.shift) do |line|
  i += 1;
  if i % 50000 == 0 then
    puts i;
  end
  arr = line.split("\t");
  if arr[cols[:gov_doc]] == '1' then
    out = {};
    cols.keys.each do |k|
      c        = cols[k];
      out[k] ||= [];
      out[k]  << arr[c];
    end
    hdout.file.puts(out.to_json);
  end
end
hdout.close();
