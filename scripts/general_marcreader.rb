require 'traject';
require 'traject/marc_reader';
require 'traject/macros/marc21';
require 'traject/macros/marc21_semantics';
require 'traject/alephsequential_reader';
require 'traject/ndj_reader';
require 'json';

@@spec = {
  '001'  => 'record_id',
  '010a' => 'lccn',
  '022a' => 'issn',
  '035a' => 'oclc',
  '086'  => 'sudoc',
  '110'  => 'publisher',
  '245a' => 'title',
  '245b' => 'title',
  '246a' => 'title',
  '246b' => 'title',
  '260b' => 'publisher',
  '260c' => 'pubdate',
  '264'  => 'publisher',
  '710'  => 'publisher',
};

class HathiMarcReader
  # Call thusly-like:
  #   zcat CIC.ndj.gz | bundle exec ruby general_marcreader.rb
  # or:
  #   bundle exec ruby general_marcreader.rb CIC.ndj
  # Add 'aleph' as commandline argument if you want to use Traject::AlephSequentialReader

  def main
    @reader.each do |marcrecord| # Marc::Record
      out = {}; # 1 json line per marc record
      holdings = []; # The holdings (pairs of 974u and z), if any.
      marcrecord.fields.each do |f| # MARC::DataField
        holding = {};
        if f.class == MARC::DataField then
          f.subfields.each do |subfield| # MARC::SubField
            tagsub = f.tag; # e.g. 260, use if in @@spec, otherwise add subfield.
            # If @@spec has plain 260, do not use subfielded 260 as tagsub.
            if !@@spec.has_key?(tagsub) then
              tagsub = f.tag + subfield.code; # e.g. 260c
            end
            if tagsub =~ /^974[uz]$/ then
              # Special case: holdings. Always look for no matter what @@spec says.
              holding[tagsub] = strip_val(subfield.value);
              # Add to holdings if we have both u and z.
              if holding.has_key?('974u') && holding.has_key?('974z') then
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
            o = Traject::Macros::Marc21Semantics.oclcnum_extract(v);
            o.nil? ? nil : {k => o};
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
            out['item_id'] = {'974u' => h['974u']};
            out['enumc']   = {'974z' => h['974z']};
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
  def initialize ()
    @reader = Traject::NDJReader.new(ARGF, {});
    return self;
  end
end

class AlephReader < HathiMarcReader
  def initialize
    @reader = Traject::AlephSequentialReader.new(ARGF, {});
    return self;
  end
end

if __FILE__ == $0 then
  hmr = nil;
  if ARGV.include?('aleph') then
    ARGV.delete('aleph');
    hmr = AlephReader.new();
  else
    hmr = JsonReader.new();
  end
  hmr.main();
end
