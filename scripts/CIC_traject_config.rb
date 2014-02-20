require 'traject/macros/marc21_semantics'
extend  Traject::Macros::Marc21Semantics

require 'traject/marc_reader'
require 'traject/json_writer'

settings do
  provide "reader_class_name",  "Traject::MarcReader"
  provide "marc_source.type",   "json"
  provide "writer_class_name",  "Traject::JsonWriter"
end

# Want to trim them all of whitespaces.
# Want to output empty elements too, for purposes of making a mysql loadfile?

# Various ids
to_field "id",               extract_marc("001", :first => true);
to_field "lccn",             extract_marc("010a", :trim_punctuation => true)
to_field "issn",             extract_marc("022a:022l:022y:773x:774x:776x", :separator => nil)
to_field "oclcnum",          oclcnum

# Title info
to_field "title",            extract_marc("245", :trim_punctuation => true)
to_field "title_series",     extract_marc("440a:490a:800abcdt:400abcd:810abcdt:410abcd:811acdeft:411acdef:830adfgklmnoprst:760ast:762ast", :trim_punctuation => true)

# Author info
to_field "author",           extract_marc("100abcdq:110abnp:111:130a", :trim_punctuation => true)

# Publication date and place
to_field "published",        extract_marc("260a", :trim_punctuation => true)
to_field "pub_date",         marc_publication_date

# Agency (need to filter out gov't printing office)
to_field "agency",           extract_marc("110ab:260b:710", :trim_punctuation => true)

# Lang
to_field "language_code",    marc_languages
