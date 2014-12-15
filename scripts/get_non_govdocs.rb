# Read marc-in-json lines piped in 
#   ::: zcat zephir_full_20140930_vufind.json.gz | bundle exec ruby scripts/get_non_govdocs.rb
# Convert to JSON and get 110, 260b and 710 + volume_id
# Output to file.

require 'json';

$ac = [].class;
$hc = {}.class;

def run 
  wanted_fields = ["008", "110", "260_b", "710", "HOL_p"];

  ARGF.lines do |line|
    j = JSON.parse(line);
    mini_hash     = {};
    if j.has_key?("fields") then
      if j["fields"].class == $ac then
        if j["fields"].size > 0 then
          j["fields"].each do |f|
            if f.class == $hc then
              wanted_fields.each do |w|
                (wanted_field, wanted_subfield) = w.split("_");
                if f.keys.include?(wanted_field) then
                  if wanted_subfield.nil? then
                    mini_hash[w] = extract_subfields(f[wanted_field]);
                  else
                    if f[wanted_field].class == $hc then
                      if f[wanted_field].has_key?("subfields") then
                        if f[wanted_field]["subfields"].class == $ac then
                          f[wanted_field]["subfields"].each do |sf|
                            if sf.class == $hc then
                              if sf.has_key?(wanted_subfield) then
                                mini_hash[w] = sf[wanted_subfield];
                              end
                            end
                          end
                        end
                      end
                    end
                  end
                end
              end
            end
          end
        end
      end
    end
    if mini_hash.has_key?("008") then
      # If the 28th position in the 008 is an 'f' then it is a gov doc.
      if mini_hash["008"][28] != 'f' then
        puts mini_hash.to_json;
      end
    end
  end
end

def extract_subfields field
  r = [];
  if field.class != $hc then
    return field;
  end
  field["subfields"].each do |x|
    x.keys.each do |y|
      if y =~ /^[a-z]$/ then
        r << x[y];
      end
    end
  end

  return r;
end

if $0 == __FILE__ then
  run();
end
