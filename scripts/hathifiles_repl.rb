require 'htph';

# This is a very strict query interpreter for the hathi_* govdoc tables. It only does exact matches on single values.
# A complex query is broken down into several smaller queries, where the results of the previous query is required to
# be included in successful results of the next.

db         = HTPH::Hathidb::Db.new();
@conn      = db.get_conn();
@hundred_q = (['?'] * 100).join(',')

# Allowed hathi_ suffixes.
@table_suffix = {
  'enumc'     => true,
  'isbn'      => true,
  'issn'      => true,
  'lccn'      => true,
  'oclc'      => true,
  'pubdate'   => true,
  'publisher' => true,
  'sudoc'     => true,
  'title'     => true,
};

# First query is always this.
@main_sql         = "SELECT DISTINCT hx.gd_id FROM table_x AS hx JOIN hathi_str AS hs ON (hx.str_id = hs.id) AND hs.str = ? ";
@prep_query_cache = {};
@str_cache_hash   = {};
@str_cache_max    = 5000;
@filter           = [];

def get_table (suffix)
  if @table_suffix.has_key?(suffix) then
    return "hathi_#{suffix}";
  end
  raise "What the hell kind of table is hathi_#{suffix} ??";
end

def read_input
  # Terminate statement with ; or empty line. One WHERE X AND-clause per line.
  input  = [];
  @filter = [];
  while true do
    print ">> ";
    str = gets;
    (str.nil? && exit(0)) || str.strip!;
    if str == '' then
      return run_query(input);
    elsif str.match(/;/) then
      str.gsub!(';', '');
      input << str;
      return run_query(input);
    elsif str.match(/!(\w+)/) then
      @filter << $1;
    else
      input << str;
    end
  end
end

def run_query (input)
  output    = [];
  input.each_with_index do |cond,i|
    if (cond =~ /.+=.+/) then
      puts "...";
      conds = cond.split('=');
      attr  = conds.shift.strip;
      val   = conds.join('=').strip;
      sql   = @main_sql.sub('table_x', get_table(attr));
      puts attr;
      if i == 0 then
        # First query run without IN()-clause.
        q = @conn.prepare(sql);
        puts sql;
        puts "?= #{val}";
        q.enumerate(val) do |row|
          output << row[:gd_id];
        end
      else
        # All subsequent queries with IN()-clause, covering the
        # IDs in the running result.
        ids    = output;
        # Clear output.
        output = [];
        sql   += "AND gd_id IN (#{@hundred_q})\n";
        puts sql;
        q      = @conn.prepare(sql);
        # Do 100 ids at a time.
        while ids.size > 0 do
          p_ids = [];
          1.upto(100).each do |i|
            if ids.size > 0 then
              p_ids << ids.shift;
            else
              p_ids << 0;
            end
          end
          q.enumerate(val, *p_ids) do |row|
            output << row[:gd_id];
          end
        end
      end
    end
  end
  puts "";
  return output;
end

def prep_cache (q)
  if !@prep_query_cache.has_key?(q) then
    @prep_query_cache[q] = @conn.prepare(q);
  end

  return @prep_query_cache[q];
end

def str_cache (id)
  # Reduce hash to 75% by removing the oldest %25 when full.
  if @str_cache_hash.size > @str_cache_max then
    @str_cache_hash.keys[0 .. (@str_cache_max / 4)].each do |k|
      @str_cache_hash.delete(k);
    end
  end

  if !@str_cache_hash.key?(id) then
    q = prep_cache("SELECT str FROM hathi_str WHERE id = ?");
    q.enumerate(id) do |row|
      @str_cache_hash[id] = row[:str];
    end
  end

  return @str_cache_hash[id];
end

def format_record (id)
  record = {'id' => id};
  @table_suffix.keys.each do |attr|
    if !@filter.empty? && !@filter.include?(attr) then
      next;
    end
    q = prep_cache("SELECT str_id FROM #{get_table(attr)} WHERE gd_id = ?");
    record[attr] = [];
    q.enumerate(id) do |row|
      record[attr] << str_cache(row.str_id);
    end
  end

  return record;
end

if __FILE__ == $0 then
  while true do # Main outer loop.
    # Read input, get output ids.
    # Look up output ids for full records.
    # Display output.
    ids      = read_input();
    out_hash = {};
    ids.each do |id|
      puts format_record(id);
    end
    puts "[#{ids.size} records]";
  end
end
