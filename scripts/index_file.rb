require 'digest';
require 'htph';
require 'json';

@str_mem      = {};
@max_str_mem  = 10000;
@max_str_len  = 749;
@str_mem_hit  = 0;
@str_mem_miss = 0;
@sha_digester = nil;
@reject_pubdate_rx = Regexp.new(/^[^0-9ivxclmd.-]+$/i);

def setup ()
  db     = HTPH::Hathidb::Db.new();
  @conn  = db.get_conn();
  @bench = HTPH::Hathibench::Benchmark.new();
  last_id_sql         = "SELECT LAST_INSERT_ID() AS id";
  str_exist_sql       = "SELECT id, str FROM hathi_str WHERE str = ?";
  str_insert_sql      = "INSERT INTO hathi_str (str) VALUES (?)";
  hathi_gd_insert_sql = "INSERT INTO hathi_gd (gov_doc, file_id, hashsum, record_id, item_id) VALUES (?, ?, ?, ?, ?)";

  @last_id_q         = @conn.prepare(last_id_sql);
  @str_exist_q       = @conn.prepare(str_exist_sql);
  @str_insert_q      = @conn.prepare(str_insert_sql);
  @hathi_gd_insert_q = @conn.prepare(hathi_gd_insert_sql);

  @sha_digester = Digest::SHA256.new();
  @log          = HTPH::Hathilog::Log.new({:file_name => 'hathi_indexing.log'});

  @loadfiles = {}; # Write tab-delim data, and when all is done, load into table.
  %w[isbn issn lccn oclc title enumc pubdate publisher sudoc].each do |suffix|
    @loadfiles[suffix] = HTPH::Hathidata::Data.new("#{suffix}.dat");
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
    hathi_related
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

  @loadfiles.values.each do |hdout|
    hdout.open('w');
  end

  hdin.open('r').file.each_line do |line|
    i += 1;

    if i == 1 then
      next;
#    elsif i > 2000 then
#      puts "ok we are done here";
#      break;
    elsif i % 1000 == 0 then
      puts "#{i} ...";
    end

    gd_id     = nil;
    hashsum   = @sha_digester.hexdigest(line);
    line_json = JSON.parse(line);
    rec_id    = line_json['record_id'].first.values.first;
    item_id   = nil;
    if !line_json['item_id'].nil? then
      item_id = line_json['item_id'].values.first;
    end

    if rec_id.nil? then
      puts "bad line, no rec_id:\n#{line}";
      next;
    end

    # Get an ID.
    begin
      @log.d("inserting #{[1, hashsum]} into hathi_gd_insert_q");
      @hathi_gd_insert_q.execute(1, @file_id, hashsum, rec_id, item_id);
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
    sql = "LOAD DATA LOCAL INFILE ? INTO TABLE hathi_#{suffix} (gd_id, str_id, marc_field)";
    puts sql;
    query = @conn.prepare(sql);
    query.execute(loadfile.path);
    # loadfile.delete();
  end

  puts @bench.prettyprint();
  puts "#{dups} dups";
end

def insert_line (json, gd_id)
  # Actually writes to several .dat files that are LOADed into db at the end.
  json.default = [];

  json['oclc'].each do |oclc|
    marc_field = oclc.keys.first;
    val        = HTPH::Hathinormalize.oclc(oclc[marc_field]);
    next if val.nil?;
    next if val.empty?;
    str_id     = get_str_id(val);
    @loadfiles['oclc'].file.puts("#{gd_id}\t#{str_id}\t#{marc_field}");
  end

  json['sudoc'].each do |sudoc|
    marc_field = sudoc.keys.first;
    val        = sudoc[marc_field];
    next if val.nil?;
    val.gsub!(/ +/, '');
    next if val.empty?;
    val.upcase!;
    str_id     = get_str_id(val);
    @loadfiles['sudoc'].file.puts("#{gd_id}\t#{str_id}\t#{marc_field}");
  end

  json['isbn'].each do |isbn|
    marc_field = isbn.keys.first;
    val        = isbn[marc_field];
    next if val.nil?;
    next if val.empty?;
    str_id     = get_str_id(val);
    @loadfiles['isbn'].file.puts("#{gd_id}\t#{str_id}\t#{marc_field}");
  end

  json['issn'].each do |issn|
    marc_field = issn.keys.first;
    val        = issn[marc_field];
    next if val.nil?;
    next if val.empty?;
    next if val == '1';

    # Sometimes an issn is catalogued as "0149-2195 (Print)",
    # so remove all parentheticals.
    val.gsub!(/ \(.+?\)/, '')

    str_id = get_str_id(val);
    @loadfiles['issn'].file.puts("#{gd_id}\t#{str_id}\t#{marc_field}");
  end

  json['lccn'].each do |lccn|
    marc_field = lccn.keys.first;
    val        = lccn[marc_field]
    next if val.nil?;
    val.tr_s!(' ', '');
    val.tr_s!('^', '');
    next if val.empty?;
    val.upcase!;
    str_id     = get_str_id(val);
    @loadfiles['lccn'].file.puts("#{gd_id}\t#{str_id}\t#{marc_field}");
  end

  json['title'].each do |title|
    marc_field = title.keys.first;
    val        = HTPH::Hathinormalize.title(title[marc_field]);
    next if val.nil?;
    next if val.empty?;
    str_id = get_str_id(val);
    @loadfiles['title'].file.puts("#{gd_id}\t#{str_id}\t#{marc_field}");
  end

  json['enumc'].each do |enumc|
    marc_field = enumc[0];
    val        = HTPH::Hathinormalize.enumc(enumc[1]);
    next if val.nil?;
    next if val.empty?;
    str_id = get_str_id(val);
    @loadfiles['enumc'].file.puts("#{gd_id}\t#{str_id}\t#{marc_field}");
  end

  json['pubdate'].each do |pubdate|
    marc_field = pubdate.keys.first;
    val        = pubdate[marc_field];
    # Date normalization?
    next if val.nil?
    next if val.empty?;
    str_id     = get_str_id(val);
    @loadfiles['pubdate'].file.puts("#{gd_id}\t#{str_id}\t#{marc_field}");
  end

  json['publisher'].each do |publisher|
    marc_field = publisher.keys.first;
    val        = HTPH::Hathinormalize.agency(publisher[marc_field]);
    next if val.nil?;
    next if val.empty?;
    str_id = get_str_id(val);
    @loadfiles['publisher'].file.puts("#{gd_id}\t#{str_id}\t#{marc_field}");
  end

end

def get_str_id (str)
  str_id = nil;
  str = str.gsub(/ +/, ' ');
  str = str.sub(/^ /, '');
  str = str.sub(/ $/, '');
  str = str[0..@max_str_len];

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
  setup();

  ARGV.map!{|arg|
    (arg != '--delete' && arg) || delete() && nil;
  }.compact!

  if ARGV.size <= 0 then
    raise "Need infile as 1st arg.";
  end

  while  ARGV.size > 0 do
    infile = ARGV.shift;
    hdin   = HTPH::Hathidata::Data.new(infile);
    if !hdin.exists? then
      raise "Cannot find infile #{hdin.path}.";
    end

    prep_infile(hdin);
    run(hdin);
  end
end
