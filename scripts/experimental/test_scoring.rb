set = [
 {:id=>["009220688"], :oclc=>["117824"], :isbn=>[], :issn=>[], :lccn=>[], :title=>["Proceedings."], :enum_chron=>[], :pub_date=>["1970"], :publisher=>["Federal Water Pollution Control Administration"], :gov_doc=>["1"]},
 {:id=>["001515451"], :oclc=>["117824"], :isbn=>[], :issn=>[], :lccn=>["78607102"], :title=>["Proceedings."], :enum_chron=>[], :pub_date=>["1970"], :publisher=>["US Federal Water Pollution Control Administration"], :gov_doc=>["0"]}
]

ans = [['009220688', '001515451', '999'], ['009220688', '00151545199']];

def score (set, anss)
  set_ids = set.map{|x| x[:id]}.flatten.sort;
  fs      = [0];
  anss.each do |ans|
    if set_ids == ans.sort then
      puts "same!";
      return 1;
    end
    tp = 0;
    fp = 0;
    # tn = 0; # Ain't nobody don't care about no tn.
    fn = 0;
    set_ids.each do |si|
      if ans.include?(si) then
        tp += 1;
      else
        fp += 1;
      end
    end
    ans.each do |a|
      if !set_ids.include?(a) then
        fn += 1;
      end
    end
    precision = tp.to_f / (tp + fp);
    next if precision == 0;
    recall    = tp.to_f / (tp + fn);
    f         = 2 * ((precision * recall) / (precision + recall));
    fs << f;
  end
  return fs.inject(:+) / fs.size.to_f;
end

puts score(set, ans);
