require 'htph';
require 'set';

db    = HTPH::Hathidb::Db.new();
@conn = db.get_conn();

# The default is 1024 chars, which is too little.
@conn.execute("SET SESSION group_concat_max_len = 10000");

def run
  # Get all publishers and pubdates that cooccur more than once,
  # and all the other things they cooccur with. Not actually using
  # any of the selected values other than the group_concat, but have
  # to SELECT fields or we can't GROUP BY them.
  sql_get_ids = %w[
    SELECT
      d.str_id AS date_id,
      p.str_id AS publisher_id,
      s.str_id AS sudoc_id,
      o.str_id AS oclc_id,
      t.str_id AS title_id,
      l.str_id AS lccn_id,
      i.str_id AS issn_id,
      GROUP_CONCAT(h.id ORDER BY h.id) AS ids
    FROM
      hathi_gd                  AS h
      LEFT JOIN hathi_pubdate   AS d ON (h.id = d.gd_id)
      LEFT JOIN hathi_publisher AS p ON (h.id = p.gd_id)
      LEFT JOIN hathi_sudoc     AS s ON (h.id = s.gd_id)
      LEFT JOIN hathi_oclc      AS o ON (h.id = o.gd_id)
      LEFT JOIN hathi_title     AS t ON (h.id = t.gd_id)
      LEFT JOIN hathi_lccn      AS l ON (h.id = l.gd_id)
      LEFT JOIN hathi_issn      AS i ON (h.id = i.gd_id)
    GROUP BY
      date_id,
      publisher_id,
      sudoc_id,
      oclc_id,
      title_id,
      lccn_id,
      issn_id
    HAVING
      COUNT(h.id) >= 2
  ].join(" ");
  q_get_ids = @conn.prepare(sql_get_ids);

  # Similar query to get the actual records.
  qmarks_a = ['?'] * 50;
  qmarks   = qmarks_a.join(',');
  sql_get_values = %W[
    SELECT
      h.id,
      d.str_id AS date_id,
      p.str_id AS publisher_id,
      s.str_id AS sudoc_id,
      o.str_id AS oclc_id,
      t.str_id AS title_id,
      l.str_id AS lccn_id,
      i.str_id AS issn_id,
      e.str_id AS enumc_id
    FROM
      hathi_gd                  AS h
      LEFT JOIN hathi_pubdate   AS d ON (h.id = d.gd_id)
      LEFT JOIN hathi_publisher AS p ON (h.id = p.gd_id)
      LEFT JOIN hathi_sudoc     AS s ON (h.id = s.gd_id)
      LEFT JOIN hathi_oclc      AS o ON (h.id = o.gd_id)
      LEFT JOIN hathi_title     AS t ON (h.id = t.gd_id)
      LEFT JOIN hathi_lccn      AS l ON (h.id = l.gd_id)
      LEFT JOIN hathi_issn      AS i ON (h.id = i.gd_id)
      LEFT JOIN hathi_enumc     AS e ON (h.id = e.gd_id)
    WHERE
      h.id IN (#{qmarks})
  ].join(' ');
  q_get_values = @conn.prepare(sql_get_values);

  seen = Set.new();

  q_get_ids.enumerate() do |row|
    ids = row[:ids].split(',');
    # break if seen.size >= 500;
    next if seen.include?(row[:ids]);
    seen << row[:ids];

    count_ids    = ids.size;
    chunk_max_size = 50;
    enumc_id_map = {};

    # For a cluster, get a hash of all values for sudoc, oclc, etc., 
    # and how many of each. {:sudoc_id=>{nil=>5}, :oclc_id=>{498486=>5}, ... }
    uniq_attr_set  = {
      :sudoc_id => {},
      :oclc_id  => {},
      :title_id => {},
      :lccn_id  => {},
      :issn_id  => {},
      :enumc_id => {},
    };

    # In case there are more than 50 ids.
    while ids.size > 0 do
      ids_chunk = [];
      1.upto(50).each do
        if ids.size > 0 then
          ids_chunk << ids.shift;
        end
      end
      padding = [nil] * (qmarks_a.size - ids_chunk.size);
      q_args  = [ids_chunk, padding].flatten;
      q_get_values.enumerate(*q_args) do |vals|
        [:sudoc_id, :oclc_id, :title_id, :lccn_id, :issn_id, :enumc_id].each do |x|
          uniq_attr_set[x][vals[x]] = 1;
        end
        # Keep track of which enumc goes with which id(s).
        enumc_id_map[vals[:enumc_id]] ||= Set.new();
        enumc_id_map[vals[:enumc_id]] << vals[:id];
      end
    end

    rel = "";
    if uniq_attr_set[:enumc_id].keys == [nil] then
      # None of the docs have enumchrons.
      rel = "duplicates";
    elsif uniq_attr_set[:enumc_id].keys.size == 1 then
      # All of the docs have the same enumchron.
      rel = "duplicates";
    elsif uniq_attr_set[:enumc_id].keys.size == count_ids then
      # There are as many enumchrons as there are documents.
      rel = "related";
    else
      # Look for duplicates inside the cluster.
      # If so, then mark duplicates with asterisk,
      # and the cluster as a whole with asterisk.
      subclusters = false;
      enumc_id_map.keys.each do |enumc| 
        if enumc_id_map[enumc].size > 1 then
          subclusters = true;
          puts "duplicates*\t#{enumc_id_map[enumc].sort.join(',')}";
        end
      end
      if subclusters == true then
        rel = "related*";
      else
        rel = "unclear";
      end
    end
    puts "#{rel}\t#{row[:ids]}";
  end
end

run();
