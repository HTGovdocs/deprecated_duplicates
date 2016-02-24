require 'htph';

class DisjointSetForest
  attr_reader :trunk;
  attr_reader :index;
  def initialize
    @trunk = {}; # Main storage.
    @index = {}; # Reverse lookup, which key does a value belong to?
  end

  def find (x)
    found = [];
    x = @index[x] || x;
    if @trunk.has_key?(x) then
      found = [x, @trunk[x].keys].flatten.sort;
    end
    return found;
  end

  def add (k, v)
    k,v = [k,v].sort;   # Always use the lowest value as k.
    k = @index[k] || k; # Use indexed if available.
    v = @index[v] || v;
    return self if k == v;
    @trunk[k]  ||= {};
    @trunk[k][v] = 1;
    @index[v]    = k;
    # merge if @trunk.has_key?(k) and @trunk.has_key?(v)
    if @trunk.has_key?(v) then
      r = @trunk.delete(v);
      r.keys.each do |s|
        @trunk[k][s] = 1;
        @index[s]    = k;
      end
    end
    return self;
  end

  def get_sets
    # Turn:
    # {:a => {:b=>1, :c=>1, ... :n=>1}, :x => {:y=>1}}
    # ... into an array of key arrays:
    # [[:a, :b:, :c, ... :n], [:x, :y]]
    sets = [];
    @trunk.keys.each do |k|
      sets << [k, @trunk[k].keys].flatten.sort;
    end
    return sets;
  end

  def to_s
    self.get_sets.join(",");
  end
end

high = DisjointSetForest.new();
low  = DisjointSetForest.new();

# Input file containing pairs.
pairs_filename = ARGV.shift;
if pairs_filename.nil? then
  puts "Filename for input pairs file required as 1st arg.";
  exit(1);
end

# If these are given, write 2 new files with updated solo- and related-clusters.
related_filename = ARGV.shift;
solos_filename = ARGV.shift;

hdin = HTPH::Hathidata::Data.new(pairs_filename).open('r');
hdin.file.each_line do |line|
  # Look for lines like:
  # 555<tab>123<tab>0.765
  if line =~ /^(\d+)\t(\d+)\t(\d\.\d+)/ then
    pt1   = $1.to_i;
    pt2   = $2.to_i;
    score = $3.to_f;

    # Put pair into one of the collection, based on score.
    collection = low;
    if score > 0.75 then
      collection = high;
    end

    collection.add(pt1, pt2);
  end
end
hdin.close();

high.get_sets.each do |set|
  puts "high: " + set.join(", ");
end
low.get_sets.each do |set|
  puts "low: " + set.join(", ");
end

# If related_filename was given as 2nd arg, copy lines to new file
# and also append new clusters to new file.
if !related_filename.nil? then
  if related_filename !~ /related_\d+/ then
    puts "2nd arg should, if given, be a filename that matches /related_\d+/";
    exit(1);
  end
  # Read related file
  rel_hdin  = HTPH::Hathidata::Data.new(related_filename).open('r');
  # Write to new related file
  rel_hdout = HTPH::Hathidata::Data.new("related_$ymd_with_titlematch.tsv").open('w');
  rel_hdin.file.each_line do |line|
    rel_hdout.file.print line;
  end
  [high,low].each do |forest|
    forest.get_sets.each do |set|
      rel_hdout.file.puts "related\t" + set.join(",");
    end
  end
  rel_hdout.close();
  rel_hdin.close();
end

# If solos_filename was given as a 3rd arg, make a new version of solos_filename
# which does not contain any of the ids in high or low.
if !solos_filename.nil? then
  if solos_filename !~ /solos_\d+/ then
    puts "3rd arg should, if given, be a filename that matches /solos_\d+/";
    exit(1);
  end
  # Read solos file
  solo_hdin  = HTPH::Hathidata::Data.new(solos_filename).open('r');
  # Write to new solos file.
  solo_hdout = HTPH::Hathidata::Data.new("solos_$ymd_sans_titlematch.tsv").open('w');
  solo_hdin.file.each_line do |line|
    if line =~ /^solo\t(\d+)/ then
      id = $1.to_i;
      # Only write to new file if id is not in high or low.
      if high.find(id) == [] && low.find(id) == [] then
        solo_hdout.file.puts "solo\t#{id}";
      end
    end
  end
  solo_hdout.close();
  solo_hdin.close();
end

