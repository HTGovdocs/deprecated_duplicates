require 'htph';

# List all the @attrs for all the solo clusters in @file 
# where @attr occurs at least @threshold times.
# This should shine some light on what the dup detection does wrong:
# for instance, there *should* be few (if any) solo clusters with the same oclc.
#
# So do:
#   bundle exec ruby scripts/inspect_solos.rb detected_duplicates.tsv oclc 2
# ... and inspect the output.
#
# Up the value of @threshold for fewer but bigger clusters.

@file      = ARGV.shift;
@attr      = ARGV.shift;
@threshold = (ARGV.shift || 1).to_i;

@max  = 1000;
db    = HTPH::Hathidb::Db.new();
@conn = db.get_conn();
qmark = ['?'] * @max;

sql = %W{
    SELECT hx.gd_id, hs.str
    FROM hathi_str AS hs
    JOIN hathi_#{@attr} AS hx ON (hs.id = hx.str_id)
    WHERE hx.gd_id IN (#{qmark.join(',')})
}.join(' ');
puts sql;
@q = @conn.prepare(sql);

# key: attr, value: array of ids.
@attr_to_ids = {};

def main
  hdin = HTPH::Hathidata::Data.new(@file).open('r');
  ids  = [];
  hdin.file.each_line do |line|
    line.strip!;
    if line =~ /^solo\t(\d+)/ then
      gd_id = $1;
      ids << gd_id;
      if (ids.size >= @max || hdin.file.eof?) then
        process_ids(ids);
        ids = [];
      end
    end
  end
  hdin.close();

  @attr_to_ids.keys.sort.each do |attr|
    if @attr_to_ids[attr].size >= @threshold then
      puts "#{attr}\t#{@attr_to_ids[attr].join(',')}";
    end
  end

end

def process_ids (ids)
  ids = [ids, ([nil] * (@max - ids.size))].flatten;
  @q.enumerate(*ids) do |row|
    id  = row[:gd_id];
    str = row[:str];
    # puts "#{id}\t#{str}";
    @attr_to_ids[str] ||= [];
    @attr_to_ids[str] << id;
  end
end

main() if $0 == __FILE__;
