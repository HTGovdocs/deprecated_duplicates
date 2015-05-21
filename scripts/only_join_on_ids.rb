require 'htph';

def main
  @log = HTPH::Hathilog::Log.new();
  @log.d("Started");

  db   = HTPH::Hathidb::Db.new();
  conn = db.get_conn();

  # Get each distinct value of a given attribute (oclc, sudoc, ...)
  sql_get_str_ids_xxx = %w[
    SELECT hx.str_id, COUNT(DISTINCT hx.gd_id) AS c
    FROM hathi_xxx AS hx
    GROUP BY str_id
    ORDER BY c DESC
  ].join(' ');

  # Get each distinct document that has a given attribute value (e.g. oclc=555)
  sql_get_gd_ids_xxx = %w[
    SELECT DISTINCT gd_id
    FROM hathi_xxx
    WHERE str_id = ?
  ].join(' ');

  @item_to_set = {};
  @set_to_item = {};
  ignore_top_x = 10;
  i = 0; # Use i in set_id below, instead attribute name, to save memory.

  # These are the kinds of ids we are interested in, look them up with sql_get_str_ids_xxx.
  the_ids = %w[oclc sudoc lccn issn];
  the_ids.each do |attr|
    @log.d(attr);
    # Replace xxx in the queries with the current attribute.
    sql_get_str_ids = sql_get_str_ids_xxx.sub('xxx', attr);
    sql_get_gd_ids  = sql_get_gd_ids_xxx.sub('xxx', attr);
    q_get_gd_ids    = conn.prepare(sql_get_gd_ids);
    j = 0;
    # Get attribute values (e.g. oclc=555).
    conn.query(sql_get_str_ids) do |str_id_row|
      j += 1;
      # The x top most common strings we assume are bad predictors. 
      # But is it safe to just skip them? We might actually lose some docs between the cracks.
      # For now, just skip. If we get clusters in the 100K size, then we'll run out of memory.
      next if j <= ignore_top_x;
      @log.d(j) if j % 50000 == 0;
      str_id = str_id_row[:str_id];
      set_id = "#{i}_#{str_id}";
      @set_to_item[set_id] = {};
      # For each attribute value, get docs with that value for that attribute.
      q_get_gd_ids.enumerate(str_id) do |gd_id_row|
        gd_id = gd_id_row[:gd_id].to_i;
        @set_to_item[set_id][gd_id] = 1;
        @item_to_set[gd_id]       ||= {};
        @item_to_set[gd_id][set_id] = 1;
      end
    end
    i += 1;
  end

  @log.d("Started merging.");
  @item_to_set.keys.sort.each do |item|
    # We are deleting from the hash we are looping over,
    # so make sure we don't do anything with a non-existing key.
    next unless @item_to_set.has_key?(item);
    merge_sets  = {};
    merge_items = {};
    r_get_overlap(merge_sets, merge_items, item);
    # puts "returned merge_sets.keys: #{merge_sets.keys.join(",")}";
    # puts "returned merge_items.keys: #{merge_items.keys.join(",")}";
    if merge_items.keys.size > 1 then
      puts "cluster\t#{merge_items.keys.sort.join(',')}";
    else
      puts "solo\t#{item}";
    end
    # Through power of transitivity, if we're recursing correctly then we never need to revisit these:
    merge_sets.keys.each do |s|
      @set_to_item.delete(s);
    end
    merge_items.keys.each do |i|
      @item_to_set.delete(i);
    end
  end
  @log.d("Finished merging.");
end

def r_get_overlap (seen_sets, seen_items, item)
  @item_to_set[item].map{|x| x.first}.each do |set|
    next if seen_sets.has_key?(set);
    # puts "\tgot #{set}";
    seen_sets[set] = 1;
    # puts "look up #{set} in set_to_item:"
    items = @set_to_item[set];
    # puts items;
    items.keys.each do |ii|
      next if seen_items.has_key?(ii);
      # puts "\tgot #{ii}";
      seen_items[ii] = 1;
      r_get_overlap(seen_sets, seen_items, ii);
    end
  end
  return;
end

main() if $0 == __FILE__;
