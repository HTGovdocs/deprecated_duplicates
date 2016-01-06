require 'htph';

# Produces lists of gd_ids where those ids can be joined by one or more
# of oclc, sudoc, lccn, and/or issn.

def main
  @log = HTPH::Hathilog::Log.new();
  @log.d("Started");

  db   = HTPH::Hathidb::Db.new();
  conn = db.get_conn();

  # Get each distinct value of a given attribute (oclc, sudoc, ...)
  # and a count how many distinct records contain it,
  # ordered highest count to lowest count
  sql_get_str_ids_xxx = %w[
    SELECT hx.str_id, COUNT(DISTINCT hx.gd_id) AS c
    FROM hathi_xxx AS hx
    GROUP BY str_id
    ORDER BY c DESC
  ].join(' ');

  # Get each distinct document that has a given attribute value (e.g. oclc str_id 174003)
  sql_get_gd_ids_xxx = %w[
    SELECT DISTINCT gd_id
    FROM hathi_xxx
    WHERE str_id = ?
  ].join(' ');

  @item_to_set = {};
  @set_to_item = {};
  ignore_top_x = 10;

  # Use i in set_id below, instead attribute name, to save memory.
  i = 0;

  # These are the kinds of ids we are interested in, look them up with sql_get_str_ids_xxx.
  the_ids = %w[oclc sudoc lccn issn];
  the_ids.each do |attr|
    @log.d(attr);
    # Replace xxx in the queries with the current attribute.
    sql_get_str_ids = sql_get_str_ids_xxx.sub('xxx', attr);
    sql_get_gd_ids  = sql_get_gd_ids_xxx.sub('xxx', attr);
    q_get_gd_ids    = conn.prepare(sql_get_gd_ids);
    j = 0;

=begin

Get attribute values and counts.

SELECT hx.str_id, COUNT(DISTINCT hx.gd_id) AS c
FROM hathi_oclc AS hx
GROUP BY str_id
ORDER BY c DESC

+----------+-------+
| str_id   | c     |
+----------+-------+
|   321862 | 51592 |
|  3911324 | 13461 |
| 12573018 | 13125 |
| 12575725 | 11807 |
|   137944 |  8969 |
|   174008 |  8883 |
|  5469957 |  8455 |
|   128444 |  7755 |
| 12590898 |  7399 |
| 12576231 |  7072 |
|   133757 |  6649 |
|   134555 |  6374 |
|   174003 |  6137 |
...
|  6971475 |     1 |
+----------+-------+

=end

    conn.query(sql_get_str_ids) do |str_id_row|
      j += 1;
      # The ignore_top_x most common strings we assume are bad predictors.
      # But is it safe to just skip them? We might actually lose some docs between the cracks.
      # For now, just skip. If we get clusters in the 100K size, then we'll run out of memory.
      next if j <= ignore_top_x;

      if j % 50000 == 0 then
        @log.d(j);
      end

      str_id = str_id_row[:str_id];

      # For each value in each category (oclc, sudoc, etc) create a set id.
      # Use i instead of category name to save memory.
      # So for oclc 174003, create set_id "0_174003".
      set_id = "#{i}_#{str_id}";
      @set_to_item[set_id] = {};
      # For each attribute value, get gd_ids for all docs with that value for that attribute.
      # E.g, find the gd_id of all records with oclc str_id 174003.
      q_get_gd_ids.enumerate(str_id) do |gd_id_row|
        gd_id = gd_id_row[:gd_id].to_i;
        @set_to_item[set_id][gd_id] = 1; # => @set_to_item["0_174003"][555] = 1;
        # Do reverse indexing also.
        @item_to_set[gd_id]       ||= {};
        @item_to_set[gd_id][set_id] = 1; # => @item_to_set[555]["0_174003"] = 1;
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
    # r_get_overlap will populate merge_items with gd_ids as keys.
    r_get_overlap(merge_sets, merge_items, item);

    # Determine if we got any overlap (output "cluster") or not (output "solo").
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

# seen_sets and seen_items start out as empty hashes.
# Start with an item (gd_id).
# Look up all the sets the item occurs in. Remember those sets so we don't look them up again.
# Look up all the items in those sets. Add those items to seen_items. For each found item, recurse.
# (... rubs eyes and yawns ...)
# So, in more detail, we call 
# r_get_overlap ({}, {}, 555)
# Then, look up all the sets: @item_to_set[555].
# This gives us some set ids, like "0_174003"
# Look up what are the ids in that set using the reverse index, @set_to_item["0_174003"].
# Add those to seen_items and recurse for each item. 
# As we go deeper, ignore any previously seen item or set.

def r_get_overlap (seen_sets, seen_items, item)
  @item_to_set[item].map{|x| x.first}.each do |set|
    # Make sure we only look at a set once.
    next if seen_sets.has_key?(set);
    seen_sets[set] = 1;
    # look up set in set_to_item
    items = @set_to_item[set];
    # items is an array of gd_ids
    items.keys.each do |other_item|
      # other_item is also a gd_id
      # Make sure we only look at an item once.
      next if seen_items.has_key?(other_item);
      seen_items[other_item] = 1;
      r_get_overlap(seen_sets, seen_items, other_item);
    end
  end
  # When we aren't getting any previously unseen items or sets, we're done recursing.
  # The payload is the keys (gd_ids) in the seen_items hash.
  return;
end

main() if $0 == __FILE__;
