require 'htph';

db    = HTPH::Hathidb::Db.new();
@conn = db.get_conn();

@id_pair_buffer = [];

def run
  seen  = {}; # Keep track of which pairs you've seen.

  # Get all publishers and pubdates that cooccur more than once.
  sql_pubdate_publisher  = %w[
    SELECT COUNT(d.str_id) AS c, d.str_id AS date_id, p.str_id AS publisher_id
    FROM hathi_pubdate   AS d
    JOIN hathi_publisher AS p ON (p.gd_id = d.gd_id)
    GROUP BY d.str_id, p.str_id
    HAVING c > 1
  ].join(" ");
  q_pubdate_publisher = @conn.prepare(sql_pubdate_publisher);

  # Given a publisher and a pubdate, get all ids that have those in common.
  sql_get_ids = %w[
    SELECT p.gd_id 
    FROM hathi_pubdate   AS d 
    JOIN hathi_publisher AS p ON (p.gd_id = d.gd_id)
    WHERE d.str_id = ? AND p.str_id = ?
    ORDER BY p.gd_id ASC
  ].join(" ");
  q_get_ids = @conn.prepare(sql_get_ids);

  
  # Go through each pair of pubdates and publishers.
  q_pubdate_publisher.enumerate() do |row|
    puts [:c, :date_id, :publisher_id].map{|x| row[x]}.join("\t");
    ids = [];
    # Get the ids.
    q_get_ids.enumerate(row[:date_id], row[:publisher_id]) do |id_row|
      puts "\t#{id_row[:gd_id]}";
      ids << id_row[:gd_id];
    end
    ids.each do |x|
      ids.each do |y|
        # Only compare 2 ids once.
        next if x >= y;
        next if seen.has_key?("#{x} #{y}");
        puts "compare #{x} #{y}";
        seen["#{x} #{y}"] = 1;
        @id_pair_buffer << [x, y];
        
        if @id_pair_buffer.size >= 1000 then
          process_buffer();
        end

      end
    end
  end
  puts seen.keys.size;
end

def process_buffer
  @id_pair_buffer = [];
end

if $0 == __FILE__ then
  run();
end
