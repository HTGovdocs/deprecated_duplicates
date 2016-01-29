require 'htph';
mongo = HTPH::Hathimongo::Db.new();
Mongo::Logger.logger.level = ::Logger::WARN;
hdout  = HTPH::Hathidata::Data.new('distinct_mongo_file_path.txt').open('w');
cursor = mongo.conn['source_records'].distinct('file_path');
cursor.each do |row|
  hdout.file.puts row;
end
hdout.close();
