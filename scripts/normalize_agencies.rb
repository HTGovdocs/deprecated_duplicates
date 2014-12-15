require 'htph';

agencies = {};

HTPH::Hathidata.read('agencies_raw_count.tsv') do |line|
  line.strip!
  (ct, ag) = line.split("\t");

  if ag.nil? then
    next;
  end

  ag.upcase!;
  ag.gsub!(/[,\.:;]|\'S?/, '');   # punctuations
  ag.gsub!(/[\(\)\{\}\[\]]/, ''); # Brackets
  ag.gsub!(/FOR SALE BY.*/, '');  # I AM NOT INTERESTED IN WHAT YOU ARE SELLING KTHXBYE.
  ag.gsub!(/\b(THE) /, '');       # Stop words

  # Abbreviations et cetera.
  ag.gsub!(/DEPARTMENT/, 'DEPT');
  ag.gsub!(/DEPTOF/, 'DEPT OF'); # Strangely common typo(?)

  ag.gsub!(/UNITED STATES( OF AMERICA)?/, 'US');
  ag.gsub!(/U\sS\s|U S$/, 'US ');
  ag.gsub!(/GOVERNMENT/, 'GOVT');
  ag.gsub!(/ SPN$/, '');

  # US GOVT PRINT OFF, which is so common yet has so many variations.
  ag.sub!(/(US\s?)?GOVT\s?PRINT(ING)?\s?OFF(ICE)?/, 'USGPO');
  ag.sub!(/U\s?S\s?G\s?P\s?O/, 'USGPO');
  ag.sub!(/^GPO$/, 'USGPO');
  
  ag.gsub!(/ +/, ' '); # whitespace
  ag.sub!(/^ +/,  '');
  ag.sub!(/ +$/,  '');

  agencies[ag] ||= 0;
  agencies[ag]  += ct.to_i;
end

HTPH::Hathidata.write('agencies_normalized_count.tsv') do |hdout|
  agencies.keys.each do |k|
    if k != '' then
      hdout.file.puts "#{agencies[k]}\t#{k}";
    end
  end
end
