require 'htph';
require 'json';
require 'logger';
require 'traject';
require 'traject/ndj_reader';

# Turns marcjson (marc as newline-delimited json) into line-by-line marc:
# I.e., turns:
# {"leader":"foo","fields":[{"001":"ocm123"}, {"010":{"ind1":" ","ind2":" ","subfields":[{"a":"846"}]}}] ... }
# ... into:
# 001  <TAB> ocm123
# 010a <TAB> 846
# ...
# Separates records with \n---\n
# Nota bene: Ignores subfield indicators.

# Grep-like function: add -cTAG=VAL, and we'll only output records where said tag matches said value.
#  bundle exec ruby read_json_as_marc.rb marc.ndj -c110b=Central.Intelligence.Agency
# Can be chained:
#  bundle exec ruby read_json_as_marc.rb marc.ndj -c110b=Central.Intelligence.Agency -c260c=1972

# Add -o to only output the lines matched by a -cTAG=VAL expression.
# Add -n to get input file line number with each output record.

conditions = [];

# Set $x to true if '-x' is in ARGV, and delete it from there. Otherwise false.
$o = !ARGV.delete('-o').nil?;
$n = !ARGV.delete('-n').nil?;

# Pick out any -c from ARGV and put in conditions[].
ARGV.each do |arg|
  if arg =~ /-c.+=.+/ then
    conditions << arg;
  end
end
conditions.each do |c|
  ARGV.delete(c);
  # Remove -c from each value.
  c.sub!(/^-c/, '');
end

# Loop through remaining ARGVs, which hopefully are input file paths.
ARGV.each do |arg|
  reader = Traject::NDJReader.new(File.new(arg), {});
  # Go through each input file line by line as Marc records.
  reader.each_with_index do |marcrecord,i| # Marc::Record
    out_record = [];
    catch :next_record do
      # Put record, line by line, into out_record.
      marcrecord.fields.each do |f| # MARC::DataField
        if f.class == MARC::DataField then
          f.subfields.each do |subfield| # MARC::SubField
            out_record << [f.tag + subfield.code, subfield.value].join("\t");
          end
        elsif f.class == MARC::ControlField then
          out_record << [f.tag, f.value].join("\t");
        end
      end

      # If there are conditions, test here and see if record should be output or not.
      # Each condition must be met for a record to be output.
      if conditions.size > 0 then
        conditions_met = {};
        conditions.map do |x|
          conditions_met[x] = 0;
        end
        filtered_out = [];
        out_record.each do |line|
          conditions.each do |condition|
            (ctag,cval) = condition.split('=');
            if line =~ /#{ctag}\t#{cval}/i then
              conditions_met[condition] = 1;
              if $o then # Only show matches if -o.
                filtered_out << line;
              end
            end
          end
        end
        if conditions_met.values.reduce(:+) < conditions_met.values.size;
          throw :next_record;
        end
        puts conditions_met.values.join(',');
      end
      puts "# line #{i+1}" if $n;
      if $o then # Only show matches if -o.
        out_record = filtered_out;
      end
      puts out_record.join("\n");
      puts "---";
    end
  end
end
