$LOAD_PATH << './../lib/'
require 'gddb';

db    = Gddb::Db.new();
@conn = db.get_interactive();
@schema = 'mwarin_ht';

# Assuming a cluster has 2 members or more, but no more than 25.
min_cluster_size = 2;
max_cluster_size = 25;
get_vals_sql = %W<
    SELECT gp.prop, gp.val, COUNT(gp.val) AS cv 
      FROM #{@schema}.gd_prop AS gp JOIN #{@schema}.gd_str AS gs ON (gp.prop = gs.gd_str_id) 
     WHERE gs.str = ? 
  GROUP BY gp.prop, gp.val 
    HAVING cv BETWEEN #{min_cluster_size} AND #{max_cluster_size}
>.join(' ');
@get_vals_q  = @conn.prepare(get_vals_sql);

get_by_val_sql = "SELECT gd_item_id FROM #{@schema}.gd_prop WHERE prop = ? AND val = ?";
@get_by_val_q  = @conn.prepare(get_by_val_sql);

insert_cluster_sql = "INSERT INTO #{@schema}.gd_cluster () VALUES ()";
@insert_cluster_q  = @conn.prepare(insert_cluster_sql);

insert_cluster_item_sql = "INSERT IGNORE INTO #{@schema}.gd_item_cluster (gd_item_id, gd_cluster_id) VALUES (?, ?)";
@insert_cluster_item_q  = @conn.prepare(insert_cluster_item_sql);

last_id_sql = "SELECT LAST_INSERT_ID() AS id";
@last_id_q  = @conn.prepare(last_id_sql);

[
 "DELETE FROM #{@schema}.gd_item_cluster", 
 "DELETE FROM #{@schema}.gd_cluster"
].each do |dq|
  puts dq;
  @conn.update(dq);
end

@get_subvals_sql = "SELECT gp.val, COUNT(gp.val) AS cv FROM #{@schema}.gd_prop AS gp JOIN #{@schema}.v_gd_prop_str AS vp ON (gp.prop = vp.prop) WHERE gp.gd_item_id IN (__?__) AND vp.str = ? GROUP BY gp.val HAVING cv > 1";

@get_intersect_items_sql = "SELECT gd_item_id FROM #{@schema}.gd_prop WHERE gd_item_id IN (__?__) AND val = ?";

# Keep prepared statements with lists of bind params of variable length
@preps = {}; 

# Display a cluster:
# select gic.gd_cluster_id, gp.gd_item_id, gp.prop, gp.val from gd_item_cluster as gic, gd_prop gp where gic.gd_item_id = gp.gd_item_id and gic.gd_cluster_id = ? order by gic.gd_cluster_id, gp.gd_item_id, prop;

# Takes a list of ids (at least 2) and creates a cluster for them
# and assigns them to that cluster.
def make_cluster (item_id_list)
  @insert_cluster_q.execute();
  gd_cluster_id = nil;
  @last_id_q.enumerate() do |row|
    gd_cluster_id = row[:id];
  end
  puts "Making cluster #{gd_cluster_id}";
  item_id_list.each do |item_id|
    @insert_cluster_item_q.execute(item_id, gd_cluster_id);
  end
end

# Gets items with the same something.
# Have these return lists rather than making clusters on the spot,
# that way we can perform joins / filters on said lists.
def same (prop_name)
  clusters = [];
  puts "Finding clusters based on #{prop_name}"
  @get_vals_q.enumerate(prop_name) do |row1|
    puts "#{prop_name} cluster, #{row1[:cv]} #{row1[:val]}:";
    ids = [];
    @get_by_val_q.enumerate(row1[:prop], row1[:val]) do |row2|
      ids << row2[:gd_item_id];
    end
    puts ids.join(', ');
    clusters << ids.uniq;
  end
  return clusters; # [[1,5], [3,7,9], ...]
end

def intersect (clusters, prop_name)
  subclusters = [];
  clusters.each do |cluster|
    # These guys already share 1+ property-value pair.
    # Let's see if there are sub-clusters on prop_name.
    ids = [];
    get_subvals_p = prepare_qmarks(@get_subvals_sql, cluster);
    get_subvals_p.enumerate(*cluster, prop_name) do |row1|
      get_intersect_items_p = prepare_qmarks(@get_intersect_items_sql, cluster);
      get_intersect_items_p.enumerate(*cluster, row1[:val]) do |row2|
        ids << row2[:gd_item_id];
      end
      if ids.size > 0 then
        puts "Got #{ids}";
        subclusters << ids.uniq;
      end
    end
  end
  return subclusters;
end

# A cache for prepared queries with variable number of bind-params.
# First time you ask for "SELECT __?__" and ['a','b','c']
# Get out a prepared statement of "SELECT ?, ?, ?"
# Next time you ask for "SELECT __?__" and ['a','b','c'] you will get the cached prepared statement.
def prepare_qmarks (sql, bindparams)
  s = bindparams.size;
  if !@preps.has_key?(sql) then
    @preps[sql] = {};
  end

  if @preps[sql].has_key?(s) then
    # Cache hit.
    return @preps[sql][s];
  end

  # Cache miss. Generate prepared statement and cache it.
  q = sql.sub('__?__', (['?'] * s).join(','));
  p = @conn.prepare(q);
  @preps[sql][s] = p;

  return p;
end

if $0 == __FILE__ then
  # same('oclcnum').each do |cluster|
  #   make_cluster(cluster);
  # end
  # same('lccn').each do |cluster|
  #   make_cluster(cluster);
  # end
  # same('title').each do |cluster|
  #   make_cluster(cluster);
  # end
  # same('title_series').each do |cluster|
  #   make_cluster(cluster);
  # end

  intersect(intersect(same('title_series'), 'agency'), 'pub_date').each do |cluster|
    make_cluster(cluster);
  end

  @conn.close();
end
