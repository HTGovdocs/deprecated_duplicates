require 'htph';

set_file   = ARGV.shift; # Name of file in /data/
pubdate_rx = /c?-?\d{4}\??(\s*(-|i.\s*e.|or)?\s*c?\d{0,4})\??/;
set        = [];

hd_in = HTPH::Hathidata::Data.new(set_file);
if !hd_in.exists? then
  raise 'WhereThatFileException';
end

results = {
  :dup => [],
  :rel => [],
  :unr => [],
  :rev => [],
};

cols = {
  :id         =>  3,
  :oclc       =>  7,
  :isbn       =>  8,
  :issn       =>  9,
  :lccn       => 10,
  :title      => 11,
  :enum_chron =>  4,
  :imprint    => 12, # publisher + pubdate
  :gov_doc    => 15,
};

# Helper class used in putting sets together.
class DisjointSetForest
  attr_reader :trunk;
  attr_reader :index;
  def initialize
    @trunk = {}; # Main storage.
    @index = {}; # Reverse lookup, which key does a value belong to?
  end

  def find (x)
    puts "find: #{x}";
    found = [];
    x = @index[x] || x;
    if @trunk.has_key?(x) then
      found = [x, @trunk[x].keys].flatten.sort;
    end
    puts "found: #{found.join(',')}";
    return found;
  end

  def add (k, v)
    k = @index[k] || k; # Use indexed if available.
    v = @index[v] || v;
    return self if k == v;
    @trunk[k]  ||= {};
    @trunk[k][v] = 1;
    @index[v]    = k;
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
    sets = [];
    @trunk.keys.each do |k|
      sets << [k, @trunk[k].keys].flatten.sort;
    end
    return sets;
  end

  def to_s
    self.get_sets.to_s;
  end
end

def normalize_publisher (str)
  if str.nil? then
    return '';
  end
  str.gsub!(/[,\.:;]|\'S?/, '');   # punctuations
  str.gsub!(/[\(\)\{\}\[\]]/, ''); # Brackets
  str.gsub!(/FOR SALE BY.*/, '');  # I AM NOT INTERESTED IN WHAT YOU ARE SELLING KTHXBYE.
  str.gsub!(/\b(THE) /, '');       # Stop words

  # Abbreviations et cetera.
  str.gsub!(/DEPARTMENT/, 'DEPT');
  str.gsub!(/DEPTOF/, 'DEPT OF'); # Strangely common typo(?)

  str.gsub!(/UNITED STATES( OF AMERICA)?/, 'US');
  str.gsub!(/U\sS\s|U S$/, 'US ');
  str.gsub!(/GOVERNMENT/, 'GOVT');
  str.gsub!(/ SPN$/, '');

  # US GOVT PRINT OFF, which is so common yet has so many variations.
  str.sub!(/(US\s?)?GOVT\s?PRINT(ING)?\s?OFF(ICE)?/, 'USGPO');
  str.sub!(/U\s?S\s?G\s?P\s?O/, 'USGPO');
  str.sub!(/US.*GO.+P.*O/, 'USGPO');
  str.sub!(/^GPO$/, 'USGPO');

  str.gsub!(/ +/, ' '); # whitespace
  str.sub!(/^ /,  '');
  str.sub!(/ $/,  '');

  return str;
end

# unravel({1=>2, 2=>3, 3=>4, 4=>5, 6=>7}) ==>> [[1, 2, 3, 4, 5], [6, 7]]
def unravel(h)
  dsf = DisjointSetForest.new();
  h.each do |k|
    dsf.add(k[0][:id], k[1][:id]);
  end
  return dsf.get_sets();
end

def score (set, anss)
  # Takes a set:
  # [ {:id=>["009585881"], :oclc=>[], :isbn=>[], :issn=>[], :lccn=>["war10000163"], ... } ,
  #   {:id=>["001257028"], :oclc=>["9177545"], :isbn=>[], :issn=>[], :lccn=>["war10000163"], ... } ]
  #
  # ... and an array of "correct answer" arrays, and looks up if there is an answer that matches the input set, and if so, how well.
  f_max   = 0;
  anss.each do |ans|
    if set == ans.sort then
      # They are just the same. Perfect match.
      return 1;
    end
    tp = 0;
    fp = 0;
    # tn = 0; # Ain't nobody don't care about no tn.
    fn = 0;
    set.each do |si|
      if ans.include?(si) then
        tp += 1;
      else
        fp += 1;
      end
    end
    ans.each do |a|
      if !set.include?(a) then
        fn += 1;
      end
    end
    precision = tp.to_f / (tp + fp);
    recall    = tp.to_f / (tp + fn);
    f         = 2 * ((precision * recall) / (precision + recall));
    if !f.nan? then
      puts "Score against answer: " + ans.join(', ');
      puts "tp #{tp}, fp #{fp}, fn #{fn}";
      puts "p #{precision}, r #{recall}, f #{f}";
    end
    if f > f_max then
      f_max = f;
    end
  end

  return f_max;
end

def veq (ri, rj, k) # Verbose equality method.
  puts "Does #{k} match for #{ri[k]} and #{rj[k]} ?";
  if k == :enum_chron && ri[k].empty? && rj[k].empty? then
    # Only for enum_chron does 'there is nothing' mean they are the same.
    return true;
  end

  tf        = false;
  subset_tf = false;
  if ri.has_key?(k) && !ri[k].empty? && ri[k] == rj[k] then
    # Straight up identical
    tf = true;
    puts "Exact match!";
  elsif !ri[k].nil? then
    # Subset match
    ri[k].each do |rik|
      if rj[k].include?(rik) then
        subset_tf = true;
        puts "Subset match on #{rik}!";
      end
    end
  end
  puts   (tf || subset_tf);
  return (tf || subset_tf);
end

def veqs1 (ri, rj, results)
  decided = false;
  if veq(ri, rj, :oclc) then
    if veq(ri, rj, :enum_chron) then
      puts "they are duplicates";
      results[:dup] << [ri ,rj];
    else
      puts "they are related";
      results[:rel] << [ri, rj];
    end
    decided = true;
  elsif veq(ri, rj, :lccn) then
    if veq(ri, rj, :enum_chron) then
      puts "they are duplicates";
      results[:dup] << [ri, rj];
      decided = true;
    end
  elsif veq(ri, rj, :issn) then
    if veq(ri, rj, :enum_chron) then
      puts "they are duplicates";
      results[:dup] << [ri, rj];
    else
      puts "they are related";
      results[:rel] << [ri, rj];
    end
    decided = true;
  elsif veq(ri, rj, :sudoc) then
    if veq(ri, rj, :title) then
      if veq(ri, rj, :enum_chron) then
        puts "they are duplicates";
        results[:dup] << [ri, rj];
        decided = true;
      end
    end
  elsif veq(ri, rj, :title) then
    if veq(ri, rj, :agency) then
      if veq(ri, rj, :pub_date) then
        if veq(ri, rj, :enum_chron) then
          puts "they are duplicates";
          results[:dup] << [ri, rj];
          decided = true;
        end
      end
    end
  end
  if !decided then
    puts "they are unrelated";
    results[:unr] << [ri, rj];
  end
end

def veqs2 (ri, rj, results)
  decided = false;
  if veq(ri, rj, :lccn) then
    if veq(ri, rj, :title) then
      if veq(ri, rj, :enum_chron) then
        puts "They are duplicates";
        results[:dup] << [ri, rj];
      else
        results[:rev] << [ri, rj];
      end
      decided = true;
    end
  elsif veq(ri, rj, :oclc) then
    if veq(ri, rj, :title) then
      if veq(ri, rj, :enum_chron) then
        results[:dup] << [ri, rj];
        puts "They are duplicates";
      else
        results[:rel] << [ri, rj];
        puts "They are related";
      end
      decided = true;
    end
  elsif veq(ri, rj, :title) then
    if veq(ri, rj, :publisher) then
      if veq(ri, rj, :sudoc) then
        if veq(ri, rj, :enum_chron) then
          results[:dup] << [ri, rj];
          puts "They are duplicates";
        else
          results[:rev] << [ri, rj];
        end
        decided = true;
      else
        results[:rev] << [ri, rj];
        decided = true;
      end
    else
      results[:unr] << [ri, rj];
      puts "They are unrelated";
      decided = true;
    end
  else
    results[:unr] << [ri, rj];
    puts "They are unrelated";
    decided = true;
  end
  if !decided then
    puts "they are unrelated";
    results[:unr] << [ri, rj];
  end
end

hd_in.open('r').file.each_line do |line|
  record = {};
  cols.keys.each do |k|
    record[k] = line.split("\t")[cols[k]].split(",");
    # Special cases.
    case k
    when :title
      r = record[k].join(',');
      r.downcase!;
      r.gsub!(/[^a-z0-9 ]/, '');
      r.gsub!(/ +/, ' ');
      record[k] = [r];
    when :imprint
      imprints = record[k].join(',').split(/[,:;]/).map{|x| x.strip.gsub(/[\[\]<>\(\)]/, '')}.uniq;
      record[:pub_date]  = [];
      record[:publisher] = [];
      if imprints.size > 0 then
        if !imprints.last.nil? && !imprints.last.match(pubdate_rx).nil? then
          pubdate = imprints.last.match(pubdate_rx)[0];
          imprints.last.gsub!(pubdate, '');
        end
      end
      if !pubdate.nil? then
        record[:pub_date] << pubdate;
      end
      imprints.map{|x| normalize_publisher(x)}.uniq.each do |publisher|
        if publisher != '' then
          record[:publisher] << publisher;
        end
      end
      record.delete(k);
    end
  end
  set << record;
end
hd_in.close();

puts "Set size: #{set.size}";
set.each_with_index do |ri,i|
  set.each_with_index do |rj,j|
    next if j <= i;
    puts "comparing (#{i} #{j}) #{ri[:id]} and #{rj[:id]}";
    puts "... \n\t#{ri} \n=?=\n\t#{rj}";
    veqs1(ri, rj, results);

    puts '---------';
  end
end

[:unr, :rel, :dup].each do |relation| # Read in the answers.
  hd_answers = HTPH::Hathidata::Data.new("#{relation}_answers.tsv");
  if !hd_answers.exists? then
    puts "We have no answers for #{relation}";
    next;
  end
  puts "=== #{relation}";
  ans    = [];
  scores = [];
  hd_answers.open('r').file.each_line do |line|
    ans << line.strip.split("\t");
  end
  hd_answers.close();
  # Compare results to answers & score.
  unraveled = unravel(results[relation]);
  unraveled.each do |set|
    s = score(set, ans);
    puts "Score for #{set} = #{s}";
    scores << s;
  end
  puts "#{relation} scores found: #{scores.size}, answers sought: #{ans.size}";
  puts "Avg #{relation} score : #{(scores.inject(:+).to_f / scores.size) * (scores.size.to_f / ans.size)}";
  puts "\n\n";
end

# List the ones we need to ponder.
puts "=== rev";
puts "These are tricky, need human revision.";
results[:rev].each do |x|
  puts x.map{|y| y[:id]}.join(' <?> ');
end
