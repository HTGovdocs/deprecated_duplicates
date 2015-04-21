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
#   bundle exec ruby general_marcreader.rb CIC.ndj
# Add 'aleph' as commandline argument if you want to use Traject::AlephSequentialReader
# Will transparently handle .gz extension.

@@spec = {};
@@item_id_tag = nil;
@@enumc_tag   = nil;

# Without this we can't parse zephir_full_* files, which have a FMT in them.
MARC::ControlField.control_tags.delete('FMT');

class HathiMarcReader
  def main
    @reader.each_with_index do |marcrecord,i| # Marc::Record
      out = {'infile' => @infile, 'lineno' => i}; # 1 json line per marc record
      holding  = {}; # A single holding, goes into holdings ...
      holdings = []; # ... the holdings (pairs of item_id and enumc), if any.
      marcrecord.fields.each do |f| # MARC::DataField
        if f.class == MARC::DataField then
          f.subfields.each do |subfield| # MARC::SubField
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
        # Special filter for OCLC, because there are other, non-OCLC values, in 035a.
        if out.has_key?('oclc') then
          out['oclc'] = out['oclc'].map do |h|
            k = h.keys.first;
            v = h[k];
            if v =~ /^\d+$/ then
              # oclcnum_extract returns nil if there are no prefixes, so we have to let the pure numbers go first.
              {k => v};
            else
              o = Traject::Macros::Marc21Semantics.oclcnum_extract(v);
              o.nil? ? nil : {k => o};
            end
          end.compact;
        end

        # Special filter for the textier elements
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

class JsonReader < HathiMarcReader
  def initialize (stream)
    logger = Logger.new(STDERR);
    logger.formatter = proc do |severity, datetime, progname, msg|
      fileLine = caller(2)[4].split(':')[0,2].join(':');
      "#{datetime} | #{fileLine} | #{severity} | #{msg}\n";
    end
    @infile = stream;
    @reader = Traject::NDJReader.new(check_gz(stream), {:logger => logger});
    return self;
  end
end

class AlephReader < HathiMarcReader
  def initialize (stream)
    @infile = stream;
    @reader = Traject::AlephSequentialReader.new(check_gz(stream), {});
    return self;
  end
end

def check_gz (file_path)
  if file_path =~ /\.gz$/ then
    return Zlib::GzipReader.open(file_path);
  end
  return File.new(file_path);
end

def load_profile (profile)
  profile.gsub!(/^profile=/, '');
  STDERR.puts "Loading @@spec with contents of #{profile}...";
  HTPH::Hathidata.read(profile) do |line|
    if line =~ /.+\t.+/ then
      (tag,label) = line.strip.split("\t");
      if tag =~ /^\d{3}[a-z]?$/ then
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
  STDERR.puts "@@spec:";
  @@spec.keys.sort.each do |k|
    STDERR.puts "#{k}\t#{@@spec[k]}";
  end
end

if __FILE__ == $0 then
  hmr = nil;
  aleph = false;
  if ARGV.include?('aleph') then
    ARGV.delete('aleph');
    aleph = true;
  end

  # Get a marc profile (which field has title, which has enumc, etc).
  # If none given, use data/marc_profiles/default.tsv
  # Populates @@spec, @@item_id_tag, @@enumc_tag.
  profile = ARGV.find{|x| x =~ /profile=.+/} || 'marc_profiles/default.tsv';
  load_profile(profile);
  ARGV.delete(profile);

  ARGV.each do |arg|
    if aleph then
      hmr = AlephReader.new(arg);
    else
      hmr = JsonReader.new(arg);
    end
    hmr.main();
  end
end
