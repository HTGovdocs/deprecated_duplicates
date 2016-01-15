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

# Call thusly:
#   bundle exec ruby general_marcreader.rb CIC.ndj profile=/path/to/marc/profile.tsv
# Add 'aleph' as commandline argument if you want to use Traject::AlephSequentialReader
# Will transparently handle .gz extension.
# Use 'mongo' and file_path to read from mongodb instead of file on disk:
#  bundle exec ruby general_marcreader.rb mongo <mongo_file_path> profile=/path/to/marc/profile.tsv
# If no profile is given, use default profile.

@@spec = {}; # Store marc profile, i.e. what to look for in each marc record and where to look.
@@item_id_tag = nil;
@@enumc_tag   = nil;
@@require_fu  = false;
@@logger      = HTPH::Hathilog::Log.new();

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
      field_008 = marcrecord.fields("008");
      value_008 = nil;
      if field_008.class == [].class then
        if field_008.size == 1 then
          value_008 = field_008.first.value;
        else
          @@logger.d("Record #{i} has a weird number of 008s (#{field_008.size}), skipping.");
        end
      else
        @@logger.d("Record #{i} has no 008, skipping.");
      end

      if @@require_fu then
        # Throw out ones that aren't strictly us fed docs according to the 008.
        if value_008[17] != 'u' || value_008[28] != 'f' then
          @@logger.d("Record #{i} has a 008 that doesn't look like a us fed doc: [#{value_008[17]}#{value_008[28]}] in #{value_008}.");
        end
      end

      # Getting pubdate from 008.
      pubdate = Traject::Macros::Marc21Semantics.publication_date(marcrecord);
      out['pubdate'] = [{'008' => pubdate}];

      marcrecord.fields.each do |f|
        # Loop over each MARC::DataField
        if f.class == MARC::DataField then
          f.subfields.each do |subfield| # subfield is a MARC::SubField
            tagsub = f.tag; # e.g. 260, use if in @@spec, otherwise add subfield.
            # If @@spec has plain 260, do not use subfielded 260 as tagsub.
            if !@@spec.has_key?(tagsub) then
              tagsub = f.tag + subfield.code; # e.g. 260c
            end
            if (tagsub == @@item_id_tag || tagsub == @@enumc_tag) then
              # Special case: holdings. Always look for no matter what @@spec says.
              holding[tagsub] = strip_val(subfield.value);
              # Add to holdings if we have both enumc and item_id.
              if holding.has_key?(@@item_id_tag) && holding.has_key?(@@enumc_tag) then
                holdings << holding;
                holding = {};
              end
            elsif @@spec.has_key?(tagsub) then
              name        = @@spec[tagsub];
              out[name] ||= [];
              val         = strip_val(subfield.value);
              out[name]  << {tagsub => val};
            end
          end
        elsif f.class == MARC::ControlField then
          # MARC::ControlField doesn't have subfields.
          if @@spec.has_key?(f.tag) then
            name        = @@spec[f.tag];
            out[name] ||= [];
            val         = strip_val(f.value);
            out[name]  << {f.tag => val};
          end
        end
      end
      # Output json, if any.
      if !out.keys.empty? then
        # Special for OCLC
        if out.has_key?('oclc') then
          out['oclc'] = out['oclc'].map do |h|
            k = h.keys.first;
            v = h[k];
            o = Traject::Macros::Marc21Semantics.oclcnum_extract(v);
            o.nil? ? nil : {k => o};
          end.compact;
        end

        # Special filter for the textier elements, trim punctuation.
        %w[publisher title].each do |label|
          if out.has_key?(label) then
            out[label] = out[label].map do |h|
              k = h.keys.first;
              {k => Traject::Macros::Marc21.trim_punctuation(h[k])}
            end
          end
        end

        # Output with holdings, if any.
        if holdings.empty? then
          puts out.to_json;
        else
          # Repeat output with different holding, for each holding.
          # So if holdings = {{@@item_id_tag=>1, @@enumc_tag=>'v.12'}, {@@item_id_tag=>2, @@enumc_tag=>'v.34'}}
          # ... then we output 2 json strings, same except for item_id and enumc.
          holdings.each do |h|
            out['item_id'] = {@@item_id_tag => h[@@item_id_tag]};
            out['enumc']   = {@@enumc_tag   => h[@@enumc_tag]};
            puts out.to_json;
          end
        end
      end
    end
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
    if line =~ /.+\t.+/ then
      (tag,label) = line.strip.split("\t");
      if tag =~ /^\d{3}[0-9a-z]?$/ then
        # Item_id and enumc stored outside @@spec, for reasons I can't remember.
        if label == 'item_id' then
          @@item_id_tag = tag;
        elsif label == 'enumc' then
          @@enumc_tag = tag;
        else
          @@spec[tag] = label;
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
  # Populates @@spec, @@item_id_tag, @@enumc_tag.
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
