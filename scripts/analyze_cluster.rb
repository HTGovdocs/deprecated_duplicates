require 'htph';

# Go through the output from only_join_on_ids.rb.
# For each cluster there, break it apart on the things they actually all have in common.
# For each resulting set, say that its members are related, and then look for duplicates inside.

def main
  db = HTPH::Hathidb::Db.new();
  conn = db.get_conn();
  @log    = HTPH::Hathilog::Log.new();
  @tables = %w[isbn issn lccn oclc title enumc pubdate publisher sudoc].sort;

  # Look up attributes and values for the documents.
  union_sql  = @tables.map { |x|
    format('SELECT \'%s\' AS t, hx.str_id, hx.gd_id FROM hathi_%s AS hx WHERE hx.gd_id = ?', x, x)
  }.join("\nUNION\n");

  @union_q = conn.prepare(union_sql);
  # File with ^(cluster|solo)\t\d+(,(\d+,)\d*)$ on each line
  hdin   = HTPH::Hathidata::Data.new(ARGV.shift).open('r');
  @solos = HTPH::Hathidata::Data.new("solos_$ymd.tsv").open('w');
  @rels  = HTPH::Hathidata::Data.new("related_$ymd.tsv").open('w');
  @dups  = HTPH::Hathidata::Data.new("duplicates_$ymd.tsv").open('w');
  @huge  = HTPH::Hathidata::Data.new("huge_$ymd.tsv").open('w');
  i = 0;
  # Any bigger than this and we can't even.
  cluster_max_size = 25000;

  catch :outer do
    hdin.file.each_line do |line|
      i += 1;
      if i % 1000 == 0 then
        @log.d(i);
      end
      line.strip!;
      (type, idstr) = line.split("\t");
      if type == 'cluster' then
        ids = idstr.split(',');
        if ids.size > cluster_max_size then
          # This is too big. We have to deal with them differently.
          @huge.file.puts(line);
        else
          analyze_cluster(ids);
        end
      elsif type == 'solo' then
        @solos.file.puts(line);
      end
    end
    hdin.close();
    [@solos, @rels, @dups, @huge].each do |f|
      f.close();
    end
  end
end

def analyze_cluster (ids)
  # 2d hash where [doc + attr] -> vals
  @doc_attr_vals = {};
  ids.each do |id|
    @doc_attr_vals[id] = {};
    id_args = ([id] * @tables.size);
    @union_q.enumerate(*id_args) do |row|
      attr = row[:t];
      val  = row[:str_id];
      doc  = row[:gd_id];
      @doc_attr_vals[id][attr] ||= [];
      @doc_attr_vals[id][attr] << val;
    end
    if @doc_attr_vals.keys.size % 1000 == 0 then
      @log.d("Big cluster, #{@doc_attr_vals.keys.size} / #{ids.size}");
    end
  end

  skip    = [];
  related = [];
  subsets = [];

  # Get related subset(s).
  get_related(ids, skip, related);

  related.sort_by!{|x| x.size};
  related.each_with_index do |x,i|
    related.each_with_index do |y,j|
      next if i >= j;
      if (x - y).empty? then
        # Each set which is a subset of another is marked for deletion.
        subsets << x;
      end
    end
  end

  subsets.each do |s| # Delete subsets.
    related.delete(s);
  end

  related.each do |r|
    # Print related to file.
    @rels.file.puts("related\t#{r.join(',')}");
    # Now let's get out the ones that are dups from each set.
    get_duplicates(r).each do |d|
      # Print dups to file.
      s = score(d);
      @dups.file.puts("duplicates\t#{s}\t#{d.join(',')}");
    end
  end
end

def get_related (ids, skip, related)
  if ids.size <= 1 then
    # puts "No point in looking at a single id. (#{ids.first})";
    return nil;
  end

  attrs       = %w[oclc sudoc lccn issn title];
  attrs_order = attrs - skip;

  # build up a different 2d hash where [attr + val] -> docs
  attr_val_docs = {};
  ids.each do |doc|
    # memoize this?
    @doc_attr_vals[doc].keys.each do |attr|
      @doc_attr_vals[doc][attr].each do |val|
        attr_val_docs[attr]         ||= {};
        attr_val_docs[attr][val]    ||= {};
        attr_val_docs[attr][val][doc] = 1;
      end
    end
  end

  # Any attr-val that only occurs once could be discarded, we're not going to be able
  # to use it in any comparison, and it is possible for 2 duplicate docs to have different
  # granularity, i.e. one may have oclc and lccn, the other just oclc.
  attr_val_docs.keys.each do |attr|
    next if attr == 'enumc';
    attr_val_docs[attr].keys.each do |val|
      if  attr_val_docs[attr][val].keys.size == 1 then
        # puts "#{attr} #{val} #{attr_val_docs[attr][val].keys} is just one, remove.";
        attr_val_docs[attr].delete(val);
      end
    end
  end

  # Now drill down. Is there anything they all have in common?
  # Look at attrs in descending order of importance.
  biggest_key_attr = '';
  biggest_key_val  = '';
  biggest_key_size = 0;
  common           = '';

  catch :b0rk do
    attrs_order.each do |attr|
      if attr_val_docs.has_key?(attr) then
        attr_val_docs[attr].keys.each do |val|
          key_size = attr_val_docs[attr][val].keys.size;
          # puts "check #{attr}, #{key_size} == #{ids.size}?";
          if key_size == ids.size then
            common = attr;
            skip << common;
            throw :b0rk;
          elsif key_size > biggest_key_size then
            biggest_key_attr = attr;
            biggest_key_val  = val;
            biggest_key_size = key_size;
          end
        end
      end
    end
  end

  if common == '' then
    if biggest_key_size == 0 then
      # puts "Nothing in common!";
    else
      # puts "biggest key was #{biggest_key_attr} #{biggest_key_val}, shared by #{attr_val_docs[biggest_key_attr][biggest_key_val].keys.join(',')}";
      biggest_key_ids = attr_val_docs[biggest_key_attr][biggest_key_val].keys;
      remaining_ids   = ids - biggest_key_ids;
      (related << biggest_key_ids) if (get_related(biggest_key_ids, (skip << biggest_key_attr), related) == nil);
      if remaining_ids.size > 1 then
        (related << remaining_ids) if (get_related(remaining_ids, [], related) == nil);
      end
    end
  else
    # puts "They all have #{common} in common:";
    related << ids;
  end
end

def get_duplicates (ids)
  enumc_doc  = {};
  duplicates = [];
  ids.each do |doc|
    @doc_attr_vals[doc]['enumc'] ||= [nil];
    @doc_attr_vals[doc]['enumc'].each do |val|
      enumc_doc[val]    ||= {};
      enumc_doc[val][doc] = 1;
    end
  end
  enumc_doc.keys.each do |enumc|
    if enumc_doc[enumc].keys.size > 1 then
      duplicates << enumc_doc[enumc].keys;
    end
  end

  return duplicates;
end

def score (ids)
  count_vals = {};
  ids.each do |doc|
    @doc_attr_vals[doc].keys.each do |attr|
      val = @doc_attr_vals[doc][attr];
      count_vals[val] ||= 0;
      count_vals[val]  += 1;
    end
  end

  val_counts = count_vals.values;
  sum_vals   = val_counts.inject(:+).to_f;
  tot        = 0.0;

  val_counts.each do |vc|
    score = 1 - ((ids.size - vc) / sum_vals);
    tot += score;
  end

  return (tot / val_counts.size) ** 3;
end

main if $0 == __FILE__;
