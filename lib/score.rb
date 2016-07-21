class Score  
  # Take a list of ids (a cluster) and assign a score based on how much their values have in common.
  # Examples in comments below.

  @@debug = false;

  def Score.set_debug (bool)
    @@debug = bool;
  end
  
  def Score.cluster (ids, doc_attr_vals)
    # Assume in this example that:
    #   * ids is an array with 3 ids: [22,33,44]
    #   * doc_attr_vals is a 2D hash with ids values for keys:
    #   {
    #    22 => {:oclc => '555', :publisher => 'congress'},
    #    33 => {:oclc => '555', :publisher => 'congress'},
    #    44 => {:oclc => '555', :publisher => 'senate'},
    #   }
    # They all have the same oclc but 2 have publisher congress and 1 has senate.
    # For each distinct attribute value in the set we get based on the ids,
    # count how many times they occur and store in count_vals.
    # So, count_vals = {'555' => 3, 'congress' => 2, 'senate' => 1}

    if @@debug then
      puts "ids:#{ids}";
      puts "doc_attr_vals:#{doc_attr_vals}";
    end
    
    count_vals = {};
    ids.each do |doc|
      doc_attr_vals[doc].keys.each do |attr|
        val = doc_attr_vals[doc][attr];
        count_vals[val] ||= 0;
        count_vals[val]  += 1;
      end
    end

    if @@debug then
      puts "count_vals:#{count_vals}";
    end
    
    # val_counts is how many values there are in the set.
    # so since
    # count_vals = {'555' => 3, 'congress' => 2, 'senate' => 1}
    # then sum_vals = 6.0

    val_counts = count_vals.values;
    sum_vals   = val_counts.inject(:+).to_f;
    tot        = 0.0;

    if @@debug then
      puts "val_counts:#{val_counts}";
      puts "sum_vals:#{sum_vals}";      
    end
    
       # If there are 3 ids and count_vals = {'555' => 3, 'congress' => 2, 'senate' => 1}
    # then add into tot:
    # 1 - ((3 - 3) / 6.0) = 1
    # 1 - ((3 - 2) / 6.0) = 0.8333333333333334
    # 1 - ((3 - 1) / 6.0) = 0.6666666666666667

    val_counts.each do |vc|
      score = 1 - ((ids.size - vc) / sum_vals);
      tot += score;
      if @@debug then
        puts "#{score} = 1 - ((#{ids.size} - #{vc}) / #{sum_vals})";
        puts "tot:#{tot}";
      end
    end

    # In this example, we end up with tot = 2.5
    # Divide this by the number of distinct values, which is 3 ('555', 'congress', 'senate')
    # 2.5 / 3 = 0.8333333333333334
    # Raise to the power of 3 just to punish values more the further away they are from 1.

    final_score = (tot / val_counts.size) ** 3;
    
    if @@debug then
      puts "returning (#{tot} / #{val_counts.size}) ** 3 == #{final_score}";
    end
    
    return final_score;
  end
end

if $0 == __FILE__ then
  if ARGV.include?('--test') then
    if ARGV.include?('--debug') then
      puts Score.set_debug(true);
    end
    ids = [22,33,44];
    doc_attr_vals = {
      22 => {:oclc => '555', :publisher => 'congress'},
      33 => {:oclc => '555', :publisher => 'congress'},
      44 => {:oclc => '555', :publisher => 'senate'},
    };
    s = Score.cluster(ids, doc_attr_vals);
    puts s;
  end
end
