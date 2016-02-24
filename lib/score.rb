class Score  
  # Take a list of ids (a cluster) and assign a score based on how much their values have in common.
  # Examples in comments below.
  def Score.cluster (ids, doc_attr_vals)
    # Assume in this example that ids is an array with 3 ids.
    # They all have the same oclc but 2 are from congress and 1 from the senate.
    # For each distinct attribute value in the set we get based on the ids,
    # count how many times they occur and store in count_vals.
    # So, count_vals = {"555" => 3, "congress" => 2, "senate" => 1}

    count_vals = {};
    ids.each do |doc|
      doc_attr_vals[doc].keys.each do |attr|
        val = doc_attr_vals[doc][attr];
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
end
