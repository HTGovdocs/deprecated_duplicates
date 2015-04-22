require 'htph';
require 'set';

# Deal with the ones deemed too heavy for analyze_cluster, which it will have put in a "huge_$ymd.tsv" file.
# Nota Bene, output of this here script should be passed through `sort -u`.

db    = HTPH::Hathidb::Db.new();
@log  = HTPH::Hathilog::Log.new(); 
@conn = db.get_conn();
@qmarks_a = ['?'] * 2500;
@qmarks   = @qmarks_a.join(',');
@infile   = ARGV.shift;

def run
  # Clean the table of related ids.
  sql_delete_related = "DELETE FROM hathi_related";
  q_delete_related   = @conn.prepare(sql_delete_related);
  @log.d(sql_delete_related);
  q_delete_related.execute();

  # Get related ids and the checksum of the things that make them related.
  # Enum-chrons are NOT included in this pass.
  # -- dropped title.
  sql_get_related = %W[
    SELECT
      h.id AS gd_id,
      MD5(
        CONCAT_WS(
          ',',
          COALESCE(d.str_id, ''),
          COALESCE(s.str_id, ''),
          COALESCE(o.str_id, ''),
          COALESCE(l.str_id, ''),
          COALESCE(i.str_id, '')
        )
      ) AS checksum
    FROM
      hathi_gd                  AS h
      LEFT JOIN hathi_pubdate   AS d ON (h.id = d.gd_id)
      LEFT JOIN hathi_sudoc     AS s ON (h.id = s.gd_id)
      LEFT JOIN hathi_oclc      AS o ON (h.id = o.gd_id)
      LEFT JOIN hathi_lccn      AS l ON (h.id = l.gd_id)
      LEFT JOIN hathi_issn      AS i ON (h.id = i.gd_id)
    WHERE h.id IN (#{@qmarks})
  ].join(" ");
  q_get_related = @conn.prepare(sql_get_related);

  # Put related ids into a table.
  sql_load_related = "LOAD DATA LOCAL INFILE ? INTO TABLE hathi_related (gd_id, checksum)";
  q_load_related   = @conn.prepare(sql_load_related);

  sql_get_checksums = "SELECT DISTINCT checksum FROM hathi_related";
  q_get_checksums   = @conn.prepare(sql_get_checksums);

  sql_get_ids = %w[SELECT gd_id FROM hathi_related WHERE checksum = ?].join(' ');
  q_get_ids   = @conn.prepare(sql_get_ids);

  # Similar query to get the actual records.
  sql_get_values = %W[
    SELECT
      h.id,
      d.str_id AS date_id,
      s.str_id AS sudoc_id,
      o.str_id AS oclc_id,
      l.str_id AS lccn_id,
      i.str_id AS issn_id,
      t.str_id AS title_id,
      p.str_id AS publisher_id,
      e.str_id AS enumc_id
    FROM
      hathi_gd                  AS h
      LEFT JOIN hathi_pubdate   AS d ON (h.id = d.gd_id)
      LEFT JOIN hathi_sudoc     AS s ON (h.id = s.gd_id)
      LEFT JOIN hathi_oclc      AS o ON (h.id = o.gd_id)
      LEFT JOIN hathi_lccn      AS l ON (h.id = l.gd_id)
      LEFT JOIN hathi_issn      AS i ON (h.id = i.gd_id)
      LEFT JOIN hathi_title     AS t ON (h.id = t.gd_id)
      LEFT JOIN hathi_publisher AS p ON (h.id = p.gd_id)
      LEFT JOIN hathi_enumc     AS e ON (h.id = e.gd_id)
    WHERE
      h.id IN (#{@qmarks})
  ].join(' ');
  q_get_values = @conn.prepare(sql_get_values);

  seen  = {};
  # Assuming 1 id per line
  hdin  = HTPH::Hathidata::Data.new(@infile).open('r');
  hdout = HTPH::Hathidata::Data.new('related_ids_by_hash.dat').open('w');
  hdin_chunk = [];
  hdin.file.each_line do |line|
    line.strip!;
    hdin_chunk << line;
    if (hdin_chunk.size >= @qmarks_a.size) || hdin.file.eof? then
      if hdin_chunk.size < @qmarks_a.size then
        # Pad with nils so we get @qmarks_a.size elements.
        hdin_chunk = [nil] * (@qmarks_a.size - hdin_chunk.size) + hdin_chunk;
      end
      q_get_related.enumerate(*hdin_chunk) do |row|
        hdout.file.puts("#{row[:gd_id]}\t#{row[:checksum]}");
      end
      hdin_chunk = [];
    end
  end
  hdout.close();
  hdin.close();

  @log.d(sql_load_related);
  q_load_related.execute(hdout.path);

  # Loop over checksums that are known to have 2+ ids.
  q_get_checksums.enumerate() do |ch_row|
    checksum = ch_row[:checksum];

    ids = [];
    # Get ids connected to checksum.
    q_get_ids.enumerate(checksum) do |gi_row|
      ids << gi_row[:gd_id];
    end

    ids.sort!;
    ids_clone = ids.clone;
    # Lets see how bad it gets without seen.
    # next if seen.has_key?(ids);
    # seen[ids] = 1;
    count_ids = ids.size;

    if count_ids == 1 then
      # If there is only one ID, then why bother?
      puts "solo\t#{ids[0]}";
      next;
    end

    chunk_max_size = 50;
    enumc_id_map = {};
    @doc_attr_vals = {};

    # For a cluster, get a hash of all values for sudoc, oclc, etc.,
    # and how many of each. {:sudoc_id=>{nil=>5}, :oclc_id=>{498486=>5}, ... }
    uniq_attr_set  = {
      :sudoc_id => {},
      :oclc_id  => {},
      :lccn_id  => {},
      :issn_id  => {},
      :title_id => {},
      :publisher_id => {},
      :enumc_id => {},    
    };

    # In case there are more than @qmarks_a.size ids.
    while ids.size > 0 do
      ids_chunk = [];
      1.upto(chunk_max_size).each do
        if ids.size > 0 then
          ids_chunk << ids.shift;
        end
      end
      padding = [nil] * (@qmarks_a.size - ids_chunk.size);
      q_args  = [ids_chunk, padding].flatten;
      q_get_values.enumerate(*q_args) do |vals|
          uniq_attr_set.keys.each do |x|
          uniq_attr_set[x][vals[x]] = 1;
          if x != :enumc_id && !vals[x].nil? then
            @doc_attr_vals[vals[:id]] ||= {};
            @doc_attr_vals[vals[:id]][x] = vals[x];
          end
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
          puts "duplicates*\t#{score(enumc_id_map[enumc])}\t#{enumc_id_map[enumc].sort.join(',')}";
        end
      end
      if subclusters == true then
        rel = "related*";
      else
        rel = "unclear";
      end
    end
    puts "#{rel}\t#{score(ids_clone)}\t#{ids_clone.join(',')}";
  end
  @log.d("Done");
end

# Copied from analyze_cluster. They should be the same.
def score (ids)
  count_vals = {};
  ids.each do |doc|
    @doc_attr_vals[doc].keys.each do |attr|
      val = @doc_attr_vals[doc][attr];
      count_vals[val] ||= 0;
      count_vals[val]  += 1;
    end
  end
  val_counts = count_vals.values;
  sum_vals   = val_counts.inject(:+).to_f;
  tot        = 0.0;

  val_counts.each do |vc|
    score = 1 - ((ids.size - vc) / sum_vals);
    # puts "#{score} = 1 - ((#{ids.size} - #{vc}) / #{sum_vals})";
    # puts "#{tot} += #{score}";
    tot += score;
  end

  # puts "(#{tot} / #{val_counts.size}) ** 3";
  return (tot / val_counts.size) ** 3;
end

run if __FILE__ == $0;
