require 'traject/macros/marc21_semantics'
extend  Traject::Macros::Marc21Semantics

require 'traject/marc_reader';
require 'traject/json_writer';

settings do
  provide "reader_class_name", "Traject::MarcReader";
  provide "marc_source.type",  "json";
  provide "writer_class_name", "Traject::JsonWriter";
end

to_field 'agency',          extract_marc("110:260b:710", :trim_punctuation => true);
to_field 'enum_chron',      extract_marc("974z",         :trim_punctuation => true);
to_field 'issn',            extract_marc("022",          :trim_punctuation => true);
to_field 'item_dimensions', extract_marc("300c",         :trim_punctuation => true);
to_field 'oclc',            oclcnum;
to_field 'pagination',      extract_marc("300a",         :trim_punctuation => true);
to_field 'personal_author', extract_marc("100a:700a",    :trim_punctuation => true);
to_field 'pub_date',        extract_marc("260c",         :trim_punctuation => true);
to_field 'pub_place',       extract_marc("260a",         :trim_punctuation => true);
to_field 'series_title',    extract_marc("130a:490a:830a", :trim_punctuation => true); # removed 245a & 246a
to_field 'sudoc',           extract_marc("086",          :trim_punctuation => true);
to_field 'title',           extract_marc("245ab:246ab",  :trim_punctuation => true);
to_field 'lccn',            extract_marc("010a",         :trim_punctuation => true);

to_field 'who' do |rec, acc| 
  acc << 'minn' 
end
