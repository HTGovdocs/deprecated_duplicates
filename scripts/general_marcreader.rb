# encoding: utf-8
require 'htph';
require 'json';
require 'logger';
require 'traject';
require 'traject/alephsequential_reader';
require 'traject/macros/marc21';
require 'traject/macros/marc21_semantics';
require 'traject/marc_reader';
require 'traject/ndj_reader';
require 'zlib';
require_relative '../ext/icu4j-56_1.jar';

# Call thusly:
#   bundle exec ruby general_marcreader.rb CIC.ndj profile=/path/to/marc/profile.tsv
# Add 'aleph' as commandline argument if you want to use Traject::AlephSequentialReader
# Will transparently handle .gz extension.
# Use 'mongo' and file_path to read from mongodb instead of file on disk:
#  bundle exec ruby general_marcreader.rb mongo <mongo_file_path> profile=/path/to/marc/profile.tsv
# If no profile is given, use default profile.

@@spec        = {}; # Store marc profile, i.e. what to look for in each marc record and where to look.
@@item_id_tag = nil;
@@enumc_tag   = nil;
@@require_fu  = false;
@@logger      = HTPH::Hathilog::Log.new();
# @@text_filter does, to a string:
# unicode decomposition,
# case fold to lowercase
# transliterate everything to a latin script
# translate that into ASCII.
@@text_filter = com.ibm.icu.text.Transliterator.get_instance "NFKC; Lower(); Any-Latin; Latin-ASCII";

# Without this we can't parse zephir_full_* files, which have a FMT in them.
MARC::ControlField.control_tags.delete('FMT');

class HathiMarcReader
  attr_accessor :infile;
  def main ()
    # Use the reader to read marc records.
    @reader.each_with_index do |record,i|
      marcrecord = record;
      mongo_id   = nil;

      if self.class.to_s == 'MongoReader' then
        marcrecord = MARC::Record.new_from_hash(record['source']);
        i          = record['line_number'];
        mongo_id   = record['source_id'];
      end

      out      = {'infile' => @infile, 'lineno' => i, 'mongo_id' => mongo_id}; # 1 json line per marc record
      holding  = {}; # A single holding, goes into holdings ...
      holdings = []; # ... the holdings (pairs of item_id and enumc), if any.

      # Get the 008.
      # get_from_marc (rec, tag)
      values_008 = get_from_marc(marcrecord, '008');

      # Check that we got exactly 1 008.
      if values_008.class == [].class then
        if values_008.size != 1 then
          @@logger.d("Record #{i} has a weird number of 008s (#{values_008.size}), skipping.");
          values_008 = [];
        end
      end
      # value_008 is nil if values_008 is [].
      value_008 = values_008.first;

      if @@require_fu then
        # If we set @@require_fu, then
        # throw out records whose 008 isn't strictly us fed doc.
        if value_008.nil? then
          @@logger.d("Record #{i} has a nil 008");
          next;
        end
        if value_008[17] != 'u' || value_008[28] != 'f' then
          @@logger.d("Record #{i} has a 008 that doesn't look like a us fed doc: [#{value_008[17]}#{value_008[28]}] in #{value_008}.");
          next;
        end
      end

      # Getting pubdate from 008.
      pubdate = Traject::Macros::Marc21Semantics.publication_date(marcrecord);
      out['pubdate'] = [{'008' => pubdate}];

      # Getting everything else in @@spec from the MARC::Record.
      @@spec.keys.each do |tag|
        # e.g. tag='035a', outtype='oclc', out['oclc'] = []
        outtype = @@spec[tag];
        out[outtype] ||= [];
        outvals = get_from_marc(marcrecord, tag);
        outvals.each do |val|
          # e.g. out['oclc'] << {'035a' => 555}
          out[outtype] << {tag => val};
        end
      end

      # If there is better data outside the marc record, use that instead.
      if self.class.to_s == 'MongoReader' then
        # issn
        if record.has_key?('issn_normalized') && record['issn_normalized'].size > 0 then
          @@logger.d("Overwriting issn #{out['issn'].join(', ')} with #{record['issn_normalized'].join(', ')}");
          out['issn'] = [];
          record['issn_normalized'].each do |issn|
            out['issn'] << {'999x' => issn}; 
          end
        end
        # lccn
        if record.has_key?('lccn_normalized') && record['lccn_normalized'].size > 0 then
          @@logger.d("Overwriting lccn #{out['lccn'].join(', ')} with #{record['lccn_normalized'].join(', ')}");
          out['lccn'] = [];
          record['lccn_normalized'].each do |lccn|
            out['lccn'] << {'999x' => lccn}; 
          end
        end
        # oclc
        if record.has_key?('oclc_resolved') && record['oclc_resolved'].size > 0 then
          @@logger.d("Overwriting oclc #{out['oclc'].join(', ')} with #{record['oclc_resolved'].join(', ')}");
          out['oclc'] = [];
          record['oclc_resolved'].each do |oclc|
            out['oclc'] << {'999x' => oclc}; 
          end
        end
      end

      # if there were e.g. no lccns, remove empty lccn array from output.
      out.delete_if {|k,v| v.class == [].class && v.empty?};

      if !out.keys.empty? then

        # Special filter for the textier elements, trim punctuation.
        %w[publisher title].each do |label|
          if out.has_key?(label) then
            out[label] = out[label].map do |h|
              k = h.keys.first;
              begin
                {k => @@text_filter.transliterate(h[k]).gsub(/\p{Punct}/, '')}
              rescue Exception => e
                @@logger.f(e.message);
                @@logger.f(e.backtrace.inspect);
                @@logger.f("Problem filtering the text <#{k}> : <#{h[k]}>, mongo source_id #{mongo_id}");
                exit;
              end
            end
          end
        end

        # Output with holdings, if any.
        if out.has_key?('item_id') && out.has_key?('enumc') then
          # Output once per item_id-enumc pair.
          item_ids = out.delete('item_id');
          enumcs   = out.delete('enumc');
          while enumcs.size > 0 do
            enumc   = enumcs.shift;
            item_id = item_ids.shift || nil;
            out['item_id'] = [item_id];
            out['enumc']   = [enumc];
            puts out.to_json;
          end
        else
          # Or just output if no holdings.
          puts out.to_json;
        end

      end
    end
  end

  # Takes a MARC::Record and tag like 500x
  # returns array of all values for the tag found in the record.
  def get_from_marc (rec, tag)
    raise "Bad input <#{tag}>" if tag !~ /^\d{3}[0-9a-z]?$/;
    # Split tag into field and subfield.
    # if tag="500"  then field="500", subfield="".
    # if tag="500x" then field="500", subfield="x".
    field, subfield = tag[0,3], tag[3,4];
    acc = [];
    rec.fields(field).each do |f|
      if f.class == MARC::ControlField then
        acc << f.value;
      else
        # Get all subfields if subfield is empty, otherwise only subfields matching subfield.
        f.find_all{ |sf| (subfield.empty? || sf.code == subfield) }.map(&:value).each{ |v| acc << v }
      end
    end
    return acc.map{|v| strip_val(v)};
  end

  def strip_val (str)
    str.strip.gsub(/ +/, ' ');
  end
end

# There are 3 different subclasses of HathiMarcReader (below);
# JsonReader, AlephReader, MongoReader.
# These determine how and where to read from.
# The parent class HathiMarcReader (above) does the parsing, interpreting and outputting.

# Default file reader
class JsonReader < HathiMarcReader
  def initialize (stream)
    puts "JsonReader using stream #{stream}";
    @infile = stream;
    @reader = Traject::NDJReader.new(handle_gz(stream), {:logger => @@logger});
    return self;
  end
end

# Aleph reader, used if the string 'aleph' was in ARGV.
class AlephReader < HathiMarcReader
  def initialize (stream)
    @infile = stream;
    @reader = Traject::AlephSequentialReader.new(handle_gz(stream), {});
    return self;
  end
end

# Mongo reader, used if the string 'mongo' was in ARGV.
class MongoReader < HathiMarcReader
  include Enumerable; # Gives us each_with_index for gratis
  Mongo::Logger.logger.level = ::Logger::WARN;
  def initialize (conn, collection_name, query, infile)
    @infile = infile;
    @reader = self;
    @cursor = conn[collection_name].find(query);
    return self;
  end

  def each
    # Yield each doc to calling block.
    @cursor.each do |doc|
      yield doc;
    end
  end
end

# Make it so the file readers can handle gzipped files.
def handle_gz (file_path)
  if file_path =~ /\.gz$/ then
    return Zlib::GzipReader.open(file_path);
  end
  return File.new(file_path);
end

# Reads the provided marc_profile, or the default profile if none given.
def load_profile (profile)
  profile.gsub!(/^profile=/, '');
  @@logger.d("Loading @@spec with contents of #{profile}...");
  HTPH::Hathidata.read(profile) do |line|
    # Assume tag<tab>value on each line.
    # Like, 035a<tab>oclc
    if line =~ /.+\t.+/ then
      (tag,label) = line.strip.split("\t");
      if tag =~ /^\d{3}[0-9a-z]?$/ then
        # Like, @@spec['035a'] = 'oclc';
        @@spec[tag] = label;
        # Keep track of these especially.
        if label == 'item_id' then
          @@item_id_tag = tag;
        elsif label == 'enumc' then
          @@enumc_tag = tag;
        end

      end
    end
  end
  # Log the loaded profile.
  @@logger.d("@@spec:");
  @@spec.keys.sort.each do |k|
    @@logger.d("#{k}\t#{@@spec[k]}");
  end
end

if __FILE__ == $0 then
  hmr        = nil;
  aleph      = false;
  mongo      = false;
  mongo_path = nil;

  # Use aleph reader if 'aleph' in ARGV.
  if ARGV.include?('aleph') then
    ARGV.delete('aleph');
    aleph = true;
  end

  # Use mongo reader if 'mongo' in ARGV.
  if ARGV.include?('mongo') then
    ARGV.delete('mongo');
    mongo = true;
  end

  # If ARGV has 'require_fu', then set @@require_fu to true.
  # This modifies the behavior to reject records that don't have f and u in the proper positions in the 008.
  if ARGV.include?('require_fu') then
    ARGV.delete('require_fu');
    @@require_fu = true;
  end

  # Get a marc profile (which field has title, which has enumc, etc).
  # If none given, use data/marc_profiles/default.tsv
  # Populates @@spec
  profile = ARGV.find{|x| x =~ /profile=.+/} || 'marc_profiles/default.tsv';
  load_profile(profile);
  ARGV.delete(profile);

  # At this point we know which reader to use.
  # For the remaing args, read file(s) and output data.
  ARGV.each do |arg|
    if aleph then
      hmr = AlephReader.new(arg);
    elsif mongo then
      mongo = HTPH::Hathimongo::Db.new();
      hmr   = MongoReader.new(mongo.conn, 'source_records', {'file_path' => arg}, arg);
    else
      hmr = JsonReader.new(arg);
    end
    # Read.
    hmr.main();
  end
end
