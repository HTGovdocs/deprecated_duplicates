=begin
Get all the records with an agency and a sudoc.
Stem the sudoc and normalize the agency.
Make hash with key=sudoc stem and value=agency.
=end

require 'htph';
require 'json';
require 'set';

class Array
  def slurp (filename) # for slurping lists of lines from file into array
    HTPH::Hathidata.read(filename) do |line|
      self << line.strip;
    end
    return self;
  end
end

stopwords         = [].slurp('stopwords.txt');
keep_single_words = [].slurp('keep_singles.txt');
known_acronyms    = [].slurp('gov_agency_acronyms.txt');
us_states         = [].slurp('states.txt');

# Precompile some regexes.
@single_word_stop_rx = Regexp.new(/^(#{stopwords.join("|")})$/);
@known_acronyms_rx   = Regexp.new(/^(#{known_acronyms.join("|")})$/);
@state_agency_rx     = Regexp.new(/(#{us_states.join("|")})\s*(STATE)?\s*DEPT OF/);
@state_first_rx      = Regexp.new(/^((NORTH|EAST|SOUTH|WEST)(ERN)?|CENTRAL| )*(#{us_states.join("|")})/);
@city_state_rx       = Regexp.new(/^[^ ]+ (#{us_states.join("|")})/); # AKRON OHIO
@office_dept_rx      = Regexp.new(/OFFICE|US DEPT|ADMINISTRATION/);
@university_of_rx    = Regexp.new(/UNIVERSITY OF/);
@university_rx       = Regexp.new(/UNIVERSITY/);
@college_rx          = Regexp.new(/COLLEGE|SCHOOL/);
@college_ok_rx       = Regexp.new(/DEFENSE|NAVAL|ARMED FORCES|USAF|INFANTRY|COMMITTEE|COMMISSION|OFFICE/);
@corporation_rx      = Regexp.new(/CORPORATION/);
@corporation_ok_rx   = Regexp.new(/^US.+(AGENCY|COMMITTEE|CONGRESS|DEPT|SENATE)/);
@co_inc_ltd_rx       = Regexp.new(/\b((AND)? CO(MPANY)?|INC(ORPORATED)?|L(IMI)?TE?D|ASSOCIATES|FIRM|LLC)\b/);
@foundation_rx       = Regexp.new(/FOUNDATION/);
@foundation_ok_rx    = Regexp.new(/(AFRICAN DEVELOPMENT|INTER-AMERICAN|NATIONAL SCIENCE)/);
@burns_and_roe_rx    = Regexp.new(/^[^ ]+ AND [^ ]+$/);
@society_rx          = Regexp.new(/(SOCIETY (OF|FOR)|SOCIETY( US)?$)/);
@gp_pr_rx            = Regexp.new(/GP|PR/);
@keep_singles_rx     = Regexp.new(/(#{keep_single_words.join("|")}|\b(#{known_acronyms.join('|')})\b)/);

def main
  map   = {};
  i     = 0;
  hdout = HTPH::Hathidata::Data.new('agency.map').open('w');
  ag_counter = {};
  ag_count_threshold = 2;

  HTPH::Hathidata.read(ARGV.shift) do |line|
    i += 1;
    js = JSON.parse(line);
    next if js["sudoc"].nil?;
    next if js["agency"].nil?;
    sudoc_roots = js["sudoc"].map{  |x| stem_sudoc(x) };
    agencies    = js["agency"].map{ |x| HTPH::Hathinormalize.agency(x) };
    sudoc_roots.each do |su|
      next if su.nil?;
      map[su] ||= Set.new();
      agencies.each do |ag|
        ag_counter[ag] ||= 0;
        ag_counter[ag]  += 1;
        next if ag.nil?;
        next if ag == '';
        next if ag =~ @single_word_stop_rx;
        # Single word agencies are only ok if in a preapproved list of acronyms.
        next if (ag !~ /\s/ && ag !~ @known_acronyms_rx)
        next if ag =~ @university_of_rx;
        # Only allow GPO as agency if the sudoc has GP or PR in it.
        next if (su !~ @gp_pr_rx && ag =~ /GPO/);
        # Most agencies with "UNIVERSITY" or "COLLEGE" in them gotta go.
        next if (ag =~ @university_rx && ag !~ @office_dept_rx);
        next if (ag =~ @college_rx    && ag !~ @college_ok_rx);
        # Skip most corporations & companies.
        next if (ag =~ @corporation_rx && ag !~ @corporation_ok_rx)
        next if ag =~ @co_inc_ltd_rx;
        # Foundations are no fun. Except when they are.
        next if (ag =~ @foundation_rx  && ag !~ @foundation_ok_rx);
        # Don't care about STATE gov docs.
        next if ag =~ @state_agency_rx;
        next if ag =~ @state_first_rx;
        next if ag =~ @city_state_rx;
        next if ag =~ @burns_and_roe_rx;
        next if ag =~ @society_rx;
        # Don't care about county docs unless they mention the US.
        next if (ag =~ /\bCOUNTY\b/ && ag !~ /\bUS\b/);
        map[su] << ag;
      end
    end
  end

  # Remove the agencies that occur fewer than X times
  puts "Going through agencies with fewer than #{ag_count_threshold} occurrences...";
  map.keys.each do |k|
    map[k].each do |ag|
      # Unless they contain enough matches of certain trigger words.
      if ag_counter[ag] < ag_count_threshold then
        puts ag;
        uniq_matches = ag.scan(@keep_singles_rx).flatten.uniq.map{ |x| x == '' ? nil : x }.compact;
        if uniq_matches.size >= 2 then
          puts "keeping #{ag} (#{uniq_matches.join(', ')})";
        else
          puts "dropping #{ag}";
          map[k].delete(ag);
        end
      end
    end
  end

  map.keys.sort.each do |k|
    if !map[k].empty? then
      hdout.file.puts(k);
      map[k].sort.each do |v|
        hdout.file.puts("\t#{v}");
      end
    end
  end
  hdout.close();
end

def stem_sudoc (sudoc)
  # Return the substring from start to (but excluding) the first period, trim spaces.
  md = sudoc.match(/^([^.]+)/);
  if md.nil? then
    return nil;
  end
  stem = md[0];
  # Sudoc stems cannot contain '-', if they do they are something else.
  if stem =~ /\-/ then
    return nil;
  end
  stem.upcase!;
  stem.gsub!(/\s\[\]\"/ , '');
  stem.gsub!(/II0\s*A/ , ''); # Occurs often and is junk?
  if stem !~ /[a-zA-Z]/ then
    return nil;
  end
  return stem;
end

main();
