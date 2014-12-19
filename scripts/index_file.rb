require 'digest';
require 'htph';
require 'json';

@str_mem      = {};
@max_str_mem  = 10000;
@str_mem_hit  = 0;
@str_mem_miss = 0;
@sha_digester = nil;
# Covers: 1973, 1973-1974, 1973-74, 1973-, -1973, 1974 or 1975, 1975 i.e 1976
# Any can end in a '?', begin with a 'c'
@pubdate_rx   = /c?-?\d{4}\??(\s*(-|i.\s*e.|or)?\s*c?\d{0,4})\??/;

# The German version of Array.compact, does both '' and nil, plus flatten and uniq.
class Array
  def kompakt
    self.flatten.select { |x| 
      !x.nil? && x != ''
    }.uniq
  end
end

def setup ()
  db     = HTPH::Hathidb::Db.new();
  @conn  = db.get_conn();
  @bench = HTPH::Hathibench::Benchmark.new();
  last_id_sql         = "SELECT LAST_INSERT_ID() AS id";
  str_exist_sql       = "SELECT id, str FROM hathi_str WHERE str = ?";
  str_insert_sql      = "INSERT INTO hathi_str (str) VALUES (?)";
  hathi_gd_insert_sql = "INSERT INTO hathi_gd (gov_doc, file_id, hashsum, record_id) VALUES (?, ?, ?, ?)";

  @last_id_q          = @conn.prepare(last_id_sql);
  @str_exist_q        = @conn.prepare(str_exist_sql);
  @str_insert_q       = @conn.prepare(str_insert_sql);
  @hathi_gd_insert_q  = @conn.prepare(hathi_gd_insert_sql);

  @sha_digester = Digest::SHA256.new();
  @log          = HTPH::Hathilog::Log.new({:file_name => 'hathi_indexing.log'});

  @loadfiles = {}; # Write tab-delim data, and when all is done, load into table.
  %w[isbn issn lccn oclc title enumc pubdate publisher sudoc].each do |suffix|
    @loadfiles[suffix] = HTPH::Hathidata::Data.new("#{suffix}.dat").open('w');
  end
end

def prep_infile (hdin)
  checksum = %x{md5sum #{hdin.path}}.split(" ")[0];
  puts "checksum is <#{checksum}>";

  input_select_sql = "SELECT id, date_read FROM hathi_input_file WHERE checksum = ?";
  input_insert_sql = "INSERT INTO hathi_input_file (file_path, checksum, date_read) VALUES (?, ?, SYSDATE())";
  input_select_q   = @conn.prepare(input_select_sql);
  input_insert_q   = @conn.prepare(input_insert_sql);

  @file_id = nil;
  input_select_q.enumerate(checksum) do |row|
    puts "We've seen #{hdin.path} before, at #{row[:date_read]}";
    @file_id = row[:id];
  end

  if @file_id.nil? then
    puts "We haven't seen #{hdin.path} before, inserting...";
    puts input_insert_sql;
    input_insert_q.execute(hdin.path, checksum);
    @last_id_q.enumerate do |row|
      @file_id = row[:id];
    end
  end

  if @file_id.nil? then
    raise "Could not get a file id.";
  else
    puts "file_id is #{@file_id}";
  end
end

def delete
  %w[
    hathi_isbn
    hathi_issn
    hathi_lccn
    hathi_oclc
    hathi_title
    hathi_enumc
    hathi_pubdate
    hathi_publisher
    hathi_sudoc
    hathi_gd
  ].each do |tablename|
    sql = "DELETE FROM #{tablename}";
    q   = @conn.prepare(sql);
    puts sql;
    q.execute();
  end
end

def run (hdin)
  i    = 0;
  dups = 0;
  hdin.open('r').file.each_line do |line|
    i += 1;

    if i == 1 then
      next;
#    elsif i > 20000 then
#      puts "ok we are done here";
#      break;
    elsif i % 1000 == 0 then
      puts "#{i} ...";
    end

    gd_id     = nil;
    hashsum   = @sha_digester.hexdigest(line);
    line_json = JSON.parse(line);
    rec_id    = line_json['record_id'].first;

    if rec_id.nil? then
      puts "bad line, no rec_id:\n#{line}";
      next;
    end

    # Get an ID.
    begin
      @log.d("inserting #{[1, hashsum]} into hathi_gd_insert_q");
      @hathi_gd_insert_q.execute(1, @file_id, hashsum, rec_id);
      @last_id_q.query() do |row|
        gd_id = row[:id];
      end
    rescue Java::ComMysqlJdbcExceptionsJdbc4::MySQLIntegrityConstraintViolationException => e
      if (e.to_s =~ /Duplicate entry.+for key 'hashsum'/) == 0 then
        dups += 1;
        next;
      else
        puts e;
        puts line;
      end
    end
    # If we got an ID, proceed to insert the rest.
    insert_line(line_json, gd_id);
  end
  hdin.close();

  # Use the loadfiles for their intended purpose.
  @loadfiles.keys.each do |suffix|
    loadfile = @loadfiles[suffix];
    loadfile.close();
    sql = "LOAD DATA LOCAL INFILE ? INTO TABLE hathi_#{suffix} (gd_id, str_id)";
    puts sql;
    query = @conn.prepare(sql);
    query.execute(loadfile.path);
    loadfile.delete();
  end

  puts @bench.prettyprint();
  puts "#{dups} dups";
end

def get_from_json (j, k, splitstr = ',; ')
  return j[k].map { |x| 
    x.to_s.strip.split(/[#{splitstr}]/)
  }.kompakt;
end

def insert_line (json, gd_id)
  # Actually writes to several .dat files that are LOADed into db at the end.
  json.default = [];
  oclcs    = get_from_json(json, 'oclc');
  isbns    = get_from_json(json, 'isbn');
  issns    = get_from_json(json, 'issn');
  lccns    = get_from_json(json, 'lccn');
  titles   = get_from_json(json, 'title', ':;').map{|x| HTPH::Hathinormalize.title(x)}.kompakt;
  enumcs   = json['enumc'].map{|x| HTPH::Hathinormalize.enumc(x)}.kompakt;
  imprints = json['imprint'].map{|x| x.split(/[:;]/).map{|y| y.strip.gsub(/[\[\]<>\(\)]/, '')}}.kompakt;
  pubdates = get_from_json(json, 'pubdate');

  oclcs.each do |oclc|
    @loadfiles['oclc'].file.puts("#{gd_id}\t#{get_str_id(oclc)}");
  end

  isbns.uniq.each do |isbn|
    @loadfiles['isbn'].file.puts("#{gd_id}\t#{get_str_id(isbn)}");
  end

  issns.uniq.each do |issn|
    @loadfiles['issn'].file.puts("#{gd_id}\t#{get_str_id(issn)}");
  end

  lccns.uniq.each do |lccn|
    @loadfiles['lccn'].file.puts("#{gd_id}\t#{get_str_id(lccn)}");
  end

  # Title is the only one that can be real long. So we gotta truncate at 750.
  titles.uniq.each do |title|
    @loadfiles['title'].file.puts("#{gd_id}\t#{get_str_id(title[0..749])}");
  end

  enumcs.uniq.each do |enumc|
    @loadfiles['enumc'].file.puts("#{gd_id}\t#{get_str_id(enumc)}");
  end

  if imprints.size > 0 then
    if !imprints.last.nil? && !imprints.last.match(@pubdate_rx).nil? then
      imp_date = imprints.last.match(@pubdate_rx)[0];
      imprints.last.gsub!(imp_date, '');
    end
  end

  imprints.map{ |x| HTPH::Hathinormalize.agency(x) }.uniq.compact.each do |publisher|
    if publisher != '' then
      @loadfiles['publisher'].file.puts("#{gd_id}\t#{get_str_id(publisher)}");
    end
  end

  pubdates.uniq.each do |pubdate|
    @loadfiles['pubdate'].file.puts("#{gd_id}\t#{get_str_id(pubdate)}");
  end
end

def get_str_id (str)
  str_id = nil;
  str = str.gsub(/ +/, ' ');
  str = str.sub(/^ /, '');
  str = str.sub(/ $/, '');

  if str == '' then
    @log.w("Failing on #{str}");
    return str_id;
  end

  if @str_mem.has_key?(str) then
    @str_mem_hit += 1;
    return @str_mem[str];
  end
  @str_mem_miss += 1;

  @str_exist_q.enumerate(str) do |res|
    str_id = res[:id];
  end

  if str_id.nil? then
    @bench.time('insert_str') do
      @str_insert_q.execute(str);
    end
    @last_id_q.enumerate do |res|
      str_id = res[:id];
    end
  end

  if @str_mem.keys.size >= @max_str_mem then
    @bench.time('str_mem') do
      # Mem hash is full, make some room, delete first 10% of keys.
      @str_mem.keys[0 .. (@str_mem.keys.size / 10)].each do |k|
        @str_mem.delete(k);
      end
    end
  end

  @str_mem[str] = str_id.to_i;

  if str_id.nil? then
    @log.w("Failing on #{str}");
  end

  return str_id.to_i;
end

if __FILE__ == $0 then
  @do_delete = false;
  if ARGV[0] == '--delete' then
    @do_delete = true;
    ARGV.shift;
  end

  infile = ARGV.shift;
  if infile.nil? then
    raise "Need infile as 1st arg.";
  end
  hdin   = HTPH::Hathidata::Data.new(infile);
  if !hdin.exists? then
    raise "Cannot find infile #{hdin.path}.";
  end
  setup();
  prep_infile(hdin)
  delete() if @do_delete;
  run(hdin);
end
