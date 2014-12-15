require 'traject/macros/marc21_semantics'
extend  Traject::Macros::Marc21Semantics

require 'traject/alephsequential_reader';
require 'traject/json_writer';

settings do
  store "reader_class_name", "Traject::AlephSequentialReader";
  provide "writer_class_name", "Traject::JsonWriter";
end
# Trying with JUST the 110
to_field 'agency',          extract_marc("110", :trim_punctuation => true);
# to_field 'agency',          extract_marc("110:710:260b", :trim_punctuation => true, :first => true);
to_field 'sudoc',           extract_marc("086",          :trim_punctuation => true);
