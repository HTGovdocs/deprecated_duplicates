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

hdin = HTPH::Hathidata::Data.new(ARGV.shift).open('r');
hdin.file.each_line do |line|
  if line =~ /^(\d+)\t(\d+)\t(\d\.\d+)/ then
    pt1   = $1.to_i;
    pt2   = $2.to_i;
    score = $3.to_f;

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
