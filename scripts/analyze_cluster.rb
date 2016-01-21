require 'htph';

# Go through the output from only_join_on_ids.rb.
# For each cluster there, break it apart on the things they actually all have in common.
# For each resulting set, say that its members are related, and then look for duplicates inside.

# Call like:
#  bundle exec ruby -J-Xmx10g scripts/analyze_cluster.rb merged_yyyymmdd.tsv
# Needs a lot of ram if there are millions of items in +100K of clusters.

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
  @ping_q  = conn.prepare("SELECT 1");
  @ping_t  = Time.new();
  # File with ^(cluster|solo)\t\d+(,(\d+,)\d*)$ on each line
  # output from only_join_on_ids.rb.
  hdin   = HTPH::Hathidata::Data.new(ARGV.shift).open('r');
  @solos = HTPH::Hathidata::Data.new("solos_$ymd.tsv").open('w');
  @rels  = HTPH::Hathidata::Data.new("related_$ymd.tsv").open('w');
  @dups  = HTPH::Hathidata::Data.new("duplicates_$ymd.tsv").open('w');
  @huge  = HTPH::Hathidata::Data.new("huge_$ymd.tsv").open('w');
  i = 0;
  # Any bigger than this and we can't even.
  cluster_max_size = 25000;

  # Go through input file
  hdin.file.each_line do |line|
    next if line !~ /^(solo|cluster)\t/;
    line.strip!;
    (type, idstr) = line.split("\t");
    ids = idstr.nil? ? [] : idstr.split(',').map{|x| x.to_i};
    i += 1;
    # if i % 1000 == 0 then
    @log.d("input line #{i} has #{ids.size} ids");
    # end

    if type == 'cluster' then
      if ids.size > cluster_max_size then
        # This is too big. We have to deal with them differently.
        @huge.file.puts(line);
      else
        # This is where we actually look closer.
        analyze_cluster(ids);
      end
    elsif type == 'solo' then
      # Just put solos in solo file.
      @solos.file.puts(line);
    end
  end
  hdin.close();
  [@solos, @rels, @dups, @huge].each do |f|
    f.close();
  end
end

# Take a line from input file and do stuff to it.
def analyze_cluster (ids)
  # 2d hash where [doc + attr] -> vals
  # Refresh for each cluster.
  @doc_attr_vals = {};
  ids.each do |id|
    @doc_attr_vals[id] = {};
    id_args = ([id] * @tables.size);
    # Look up all the attribute values for each id in the ids array.
    @union_q.enumerate(*id_args) do |row|
      attr = row[:t];
      val  = row[:str_id].to_i;
      doc  = row[:gd_id].to_i;
      @doc_attr_vals[id][attr] ||= [];
      # Remember that doc 555 has title = 123.
      @doc_attr_vals[id][attr] << val;
    end
    if @doc_attr_vals.keys.size % 1000 == 0 then
      @log.d("Big cluster, #{@doc_attr_vals.keys.size} / #{ids.size}");
    end
  end

  related = [];

  # Get related subset(s).
  get_related(ids, [], related);

  related.sort_by!{|x| x.size};
  related.each_with_index do |x,i|
    related.each_with_index do |y,j|
      next if i >= j;
      next if x.empty?;
      next if y.empty?;
      if (x - y).empty? then
        # Each set which is a subset of another is to be forgotten.
        # I.e. if we have {a,b,c,d,e}, {a,b} and {c,d} then forget {a,b} and {c,d}
        if Time.new() - @ping_t > 1 then
          # Don't ping more than once per second.
          @ping_t = Time.new();
          @ping_q.enumerate do |x|
            @log.d("Ping!");
          end
        end
        x = [];
      end
    end
  end

  related.each do |r|
    next if r.empty?;
    # Print related to file.
    @rels.file.puts("related\t#{r.join(',')}");
    # Now let's get out the ones that are dups from each set.
    get_duplicates(r).each do |d|
      # Print dups to file, with score.
      s = score(d);
      @dups.file.puts("duplicates\t#{s}\t#{d.join(',')}");
    end
  end
end

# Given an array of ids, get subsets that are more closely related.
def get_related (ids, skip, related)
  # ids is a list of ids in a cluster.
  # skip is an array of things we've already looked at, so we don't have to look at them again.
  # related is an array of arrays of ids that we deem related.
  if ids.size <= 1 then
    # No point in looking for related ids if given a single id.
    return nil;
  end

  @ping_q.enumerate do |x|
    @log.d("Ping!");
  end

  # Each time we recurse we're going to add something to skip,
  # so we don't keep looking for the same attribute type.
  attrs       = %w[oclc sudoc lccn issn title];
  attrs_order = attrs - skip;

  # build up a different 2d hash where [attr + val] -> docs
  attr_val_docs = {};
  ids.each do |doc|
    @doc_attr_vals[doc].keys.each do |attr|
      @doc_attr_vals[doc][attr].each do |val|
        attr_val_docs[attr]         ||= {};
        attr_val_docs[attr][val]    ||= {};
        # Remember that title=123 occurs in document 555:
        # attr_val_docs['title'][123][555]
        attr_val_docs[attr][val][doc] = true;
      end
    end
  end

  # Any attr-val that only occurs once could be discarded, we're not going to be able
  # to use it in any comparison, and it is possible for 2 duplicate docs to have different
  # granularity, i.e. one may have oclc and lccn, the other just oclc.
  attr_val_docs.keys.each do |attr|
    next if attr == 'enumc';
    attr_val_docs[attr].keys.each do |val|
      if attr_val_docs[attr][val].keys.size == 1 then
        # puts "#{attr} #{val} #{attr_val_docs[attr][val].keys} only occurs once in this set, remove.";
        attr_val_docs[attr].delete(val);
      end
    end
  end

  biggest_key_attr = '';
  biggest_key_val  = '';
  biggest_key_size = 0;
  common           = '';

  # Now drill down. Is there anything they all have in common?
  # Look at attrs in descending order of importance.
  catch :b0rk do
    attrs_order.each do |attr|
      if attr_val_docs.has_key?(attr) then
        attr_val_docs[attr].keys.each do |val|
          key_size = attr_val_docs[attr][val].keys.size;
          # puts "check #{attr}, #{key_size} == #{ids.size}?";
          if key_size == ids.size then
            # This is something that they all have in common, stop looking.
            common = attr;
            skip << common;
            throw :b0rk;
          elsif key_size > biggest_key_size then
            # This is the thing that most (so far) docs have in common.
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
      # This should not happen. At this point there has to be things that some members in
      # the set have in common, or they wouldn't have been clustered together in the first place.
      # puts "Nothing in common!";
    else
      # The biggest key is the thing that most docs have in common.
      # Separate the set of ids into: 
      # * one set of ids that all have the biggest thing in common (biggest_key_ids)
      # * one set of ids that DONT have the biggest thing in common (remaining_ids)
      biggest_key_ids = attr_val_docs[biggest_key_attr][biggest_key_val].keys;
      remaining_ids    = ids - biggest_key_ids;
      # Save this set of ids as an array in the related array if you can't find anything more related in them...
      (related << biggest_key_ids) if (get_related(biggest_key_ids, (skip << biggest_key_attr), related) == nil);
      if remaining_ids.size > 1 then
        # Also recurse down on the ids that were NOT in the set which had biggest_key_attr in common.
        (related << remaining_ids) if (get_related(remaining_ids, [], related) == nil);
      end
    end
  else
    # puts "They all have #{common} in common:";
    related << ids;
  end
end

# Take a list of ids and return an array of arrays of ids that have the same (or nil) enumchron.
def get_duplicates (ids)
  enumc_doc  = {};
  duplicates = [];

  # At this point we know that the cluster has a bunch of things in common.
  # So really, it's just a matter of checking enumcs.

  # For example, assume input ids = [1,2,3,4] 
  # and @doc_attr_vals[1]['enumc'] = 'v1',
  #     @doc_attr_vals[2]['enumc'] = 'v2',
  #     @doc_attr_vals[3]['enumc'] = 'v1',
  #     @doc_attr_vals[4]['enumc'] = 'v2'

  @ping_q.enumerate do |x|
    @log.d("Ping!");
  end

  # For each enumchron in the set, reverse map enumc -> docs.
  ids.each do |doc|
    @doc_attr_vals[doc]['enumc'] ||= [nil];
    @doc_attr_vals[doc]['enumc'].each do |val|
      enumc_doc[val]    ||= {};
      enumc_doc[val][doc] = true;
    end
  end

  # At the end of this loop, we'll have:
  # enumc_doc = {
  #   'v1' => {1 => true, 3 => true}, 
  #   'v2' => {2 => true, 4 => true},     
  # }
  # Now, for each key in enumc_doc that leads to more than one id,
  # take those ids and put in an array in duplicates.

  enumc_doc.keys.each do |enumc|
    if enumc_doc[enumc].keys.size > 1 then
      duplicates << enumc_doc[enumc].keys;
    end
  end

  return duplicates;
end

# Take a list of ids and assign a score based on how much their values have in common.
# Examples in comments below.
def score (ids)
  # Assume in this example that ids is an array with 3 ids.
  # They all have the same oclc but 2 are from congress and 1 from the senate.
  # For each distinct attribute value in the set we get based on the ids,
  # count how many times they occur and store in count_vals.
  # So, count_vals = {"555" => 3, "congress" => 2, "senate" => 1}

  count_vals = {};
  ids.each do |doc|
    @doc_attr_vals[doc].keys.each do |attr|
      val = @doc_attr_vals[doc][attr];
      count_vals[val] ||= 0;
      count_vals[val]  += 1;
    end
  end

  # val_counts is how many values there are in the set.
  # so since
  # count_vals = {"555" => 3, "congress" => 2, "senate" => 1}
  # then sum_vals = 6.0

  val_counts = count_vals.values;
  sum_vals   = val_counts.inject(:+).to_f;
  tot        = 0.0;

  # If there are 3 ids and count_vals = {"555" => 3, "congress" => 2, "senate" => 1}
  # then add into tot:
  # 1 - ((3 - 3) / 6.0) = 1
  # 1 - ((3 - 2) / 6.0) = 0.8333333333333334
  # 1 - ((3 - 1) / 6.0) = 0.6666666666666667

  val_counts.each do |vc|
    score = 1 - ((ids.size - vc) / sum_vals);
    tot += score;
  end

  # In this example, we end up with tot = 2.5
  # Divide this by the number of values, which is 3 ("555", "congress", "senate")
  # 2.5 / 3 = 0.8333333333333334
  # Raise to the power of 3 just to punish values more the further away they are from 1.

  return (tot / val_counts.size) ** 3;
end

main if $0 == __FILE__;
