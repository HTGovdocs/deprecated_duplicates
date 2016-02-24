require 'htph';
require 'threach';

# Use this script for documents that lack good identifiers.
# Once you have a list of documents that couldn't be clustered
# based on their identifiers, run this.

# Turn on verbosity with -v. Defaults to false.
$verbose = false;
v_flag = ARGV.select{|arg| arg =~ /^-v$/}
if !v_flag.empty? then
  ARGV.delete(v_flag.first);
  $verbose = true;
end

# Set the thread count with -t=<int>, defaults to 4.
$thread_count = 4;
t_flag = ARGV.select{|arg| arg =~ /^-t=\d+$/}
if !t_flag.empty? then
  ARGV.delete(t_flag[0]);
  t_flag.first =~ /(\d+)/;
  $thread_count = Integer($1);
end

# If you want to keep the word freqs.
$keep_freqs = false;
k_flag = ARGV.select{|arg| arg =~ /^-k$/}
if !k_flag.empty? then
  ARGV.delete(k_flag[0]);
  $keep_freqs = true;
end

# Only bother analyzing document pairs that have at least overlap_min_size words in common.
overlap_min_size = 5;

# Only output pairs with a score greater than score_cutoff.
score_cutoff     = 0.5;

# Take a file of ids as input.
file = ARGV.shift;
hdin = HTPH::Hathidata::Data.new(file).open('r');

db   = HTPH::Hathidb::Db.new();
conn = db.get_conn();
log  = HTPH::Hathilog::Log.new();

# Look up ids and make bags of words (one for title, one for publisher, etc for all categories we want)
categories  = %w[publisher title pubdate enumc];
cat_queries = {};
word_freqs  = {};
word_2_id   = {};
stop_words  = [];

# Read stop words from file.
stop_words_f = HTPH::Hathidata::Data.new('title_matching_stop_words.txt');
if stop_words_f.exists? then
  stop_words_f.open('r').file.each_line do |line|
    line.strip!;
    stop_words << line;
  end
  stop_words_f.close();
end

# Get the string value of XXX (each of categories) given a gd_id.
cat_sql_template = %w<
  SELECT hs.str
  FROM hathi_XXX AS hx
  JOIN hathi_str AS hs
  ON (hx.str_id = hs.id)
  WHERE hx.gd_id = ?
>.join(" ");

categories.each do |c|
  cat_queries[c] = conn.prepare(cat_sql_template.sub('XXX', c));
end

# Populate id bags
id_bags = {};
i = 0;
# A line can be just a gd_id or "solo\t<gd_id>"
hdin.file.each_line do |line|
  i += 1;
  log.d("Read #{i} lines") if i % 1000 == 0;
  line.strip!;
  line.sub!("solo\t", '');
  id = Integer(line);
  # Make bag.
  id_bags[id] = {};
  # For each categort, get all the words an put in bag.
  categories.each do |c|
    id_bags[id][c] = [];
    cat_queries[c].enumerate(id) do |row|
      words = row[:str].split(' ').sort.uniq - stop_words;
      # Gather word freqs while we're at it.
      words.each do |w|
        word_freqs[w] ||= 0;
        word_freqs[w]  += 1;
        # Get reverse mapping too.
        word_2_id[w]  ||= {};
        word_2_id[w][id] = 1;
      end
      id_bags[id][c] = words;
    end
  end
end
hdin.close();
conn.close();

tot_freq = 0.0;
if $keep_freqs then
  hdout = HTPH::Hathidata::Data.new('title_matching_word_freqs.tsv').open('w');
  word_freqs.sort_by{|word, freq| freq}.reverse.each do |word, freq|
    freq = word_freqs[word];
    hdout.file.puts "#{word}\t#{freq}";
    tot_freq += freq;
  end
  hdout.close();
else
  word_freqs.values.each do |freq|
    tot_freq += freq;
  end
end

# Inverse word/doc freq (because a word only occurs 0 or 1 times per doc, so word freq and doc freq are the same)
# would be 1 - (word_freq.to_f / tot_freq)
inv_freq    = lambda {|word| 1 - (word_freqs[word].to_f / tot_freq).round(3)};
get_words   = lambda {|id|   categories.map{|c| id_bags[id][c]}.flatten.sort.uniq};
comparisons = 0;
outputted   = 0;
hdout       = HTPH::Hathidata::Data.new("title_word_matches_$ymd.tsv").open('w');

# Writing to outfile needs to be synchronized.
mutex = Mutex.new();

# N.B. threaded loop.
id_bags.keys.sort.threach($thread_count) do |id|
  # Get all the words per id
  # Sort so the first word is the most relevant/rare
  words = get_words.call(id).sort_by{|w| inv_freq.call(w)}.reverse;
  # Get all ids for all words
  other_ids = words.map{|w| word_2_id[w].keys}.flatten.sort.uniq - [id];
  # Check the words for the other ids
  # So we don't compare x & y and then later y & x.
  other_ids.select{|x| x > id}.each do |other_id|
    other_words = get_words.call(other_id);
    # & works as set intersection
    overlap = words & other_words;
    # Don't bother unless they have enough words in common.
    if (overlap.size == words.size || overlap.size > overlap_min_size) then
      # The words that only occur in one of the sets
      misses = (words + other_words) - overlap;
      overlap_tot_freq = overlap.map{|w| inv_freq.call(w)}.reduce(:+) || 0;
      misses_tot_freq  = misses.map{|w|  inv_freq.call(w)}.reduce(:+) || 0;
      score            = (overlap_tot_freq - misses_tot_freq) / (overlap.size + misses.size);

      if $verbose == true then
        # Verbosity will slow things down a bit since there'd be more to sync.
        mutex.synchronize {
          puts "#{id} vs #{other_id}:";
          puts "words #{words.join(',')}";
          puts "other #{other_words.join(',')}";
          puts "overlap #{overlap.join(',')}";
          puts "misses #{misses.join(',')}";
          puts "score (#{overlap_tot_freq} - #{misses_tot_freq}) / (#{overlap.size} + #{misses.size}) = #{score}";
          puts "---";
        }
      end

      # Arbitrary cutoff. Only output if the score is high enough.
      if score > score_cutoff then
        # Output pair of ids, score and overlapping words.
        outstr = [id, other_id, score.round(3), overlap.join(',')].join("\t");
        mutex.synchronize {
          hdout.file.puts(outstr);
          outputted += 1;
        }
      end

      mutex.synchronize {
        comparisons += 1;
      }

      if comparisons % 10000 == 0 then
        log.d("#{comparisons} record pairs compared, #{outputted} outputted");
      end
    end
  end
end
hdout.close();

log.d("#{comparisons} record pairs compared, #{outputted} outputted");
log.d("Donzo.")
