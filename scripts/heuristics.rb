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
 "DELETE FROM #{@schema}.gd_cluster_weights",
 "DELETE FROM #{@schema}.gd_item_cluster",
 "DELETE FROM #{@schema}.gd_cluster"
].each do |dq|
  puts dq;
  @conn.update(dq);
end

# Not prepared here, but by prepare_qmarks as needed.
@get_subvals_sql = "SELECT val, COUNT(val) AS cv FROM #{@schema}.gd_prop WHERE gd_item_id IN (__?__) AND prop = ? GROUP BY val HAVING cv > 1";
@get_intersect_items_sql = "SELECT gd_item_id FROM #{@schema}.gd_prop WHERE gd_item_id IN (__?__) AND val = ?";

insert_weight_sql = "INSERT INTO #{@schema}.gd_cluster_weights (gd_cluster_id, prop, weight) VALUES (?, ?, ?)"
@insert_weight_q   = @conn.prepare(insert_weight_sql);

# Keep prepared statements with lists of bind params of variable length
@preps     = {};

# If we use this dict of props we don't need to join against gd_str to get human-readable props.
@prop_dict = {};
# Assuming it isn't going to be huge and that there will be no overlap between keys and values.
@conn.query("SELECT prop, str FROM #{@schema}.v_gd_prop_str").each do |row|
  @prop_dict[row[:prop].to_i] = row[:str];
  @prop_dict[row[:str]]       = row[:prop].to_i;
end

# Takes a list of ids (at least 2) and creates a cluster for them
# and assigns them to that cluster.
def make_cluster (item_id_list, prop_names)
  @insert_cluster_q.execute();
  gd_cluster_id = nil;
  @last_id_q.enumerate() do |row|
    gd_cluster_id = row[:id];
  end
  puts "Making cluster #{gd_cluster_id} for items #{item_id_list.join(', ')}";
  item_id_list.each do |item_id|
    @insert_cluster_item_q.execute(item_id, gd_cluster_id);
  end

  make_cluster_weights(gd_cluster_id, prop_names);
end

def make_cluster_weights (gd_cluster_id, prop_names)
  prop_names.each do |pn|
    @insert_weight_q.execute(gd_cluster_id, @prop_dict[pn], 2);
  end
end

def find_clusters (prop_names = [])
  if prop_names.class != [].class then
    prop_names = [prop_names];
  end
  puts "Looking for clusters based on #{prop_names.join(' & ')}";
  clusters = [];
  prop_names.each_with_index do |pn,i|
    puts "Looking for level #{i} clusters based on #{pn} ...";
    if i == 0 then
      clusters = same(pn).uniq;
      puts "got #{clusters.size}";
    else
      clusters = intersect(clusters, pn).uniq;
    end
    puts "There are now #{clusters.size} clusters based on #{prop_names[0..i].join(' & ')}";
  end

  clusters.each do |cluster|
    make_cluster(cluster, prop_names);
  end
end

# Gets items with the same property.
# Returns an array of arrays, where each sub-array has the same
# value for the property.
def same (prop_name)
  clusters = [];
  @get_vals_q.enumerate(prop_name) do |row1|
    ids = [];
    @get_by_val_q.enumerate(row1[:prop], row1[:val]) do |row2|
      ids << row2[:gd_item_id];
    end
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
    get_subvals_p.enumerate(*cluster, @prop_dict[prop_name]) do |row1|
      get_intersect_items_p = prepare_qmarks(@get_intersect_items_sql, cluster);
      get_intersect_items_p.enumerate(*cluster, row1[:val]) do |row2|
        ids << row2[:gd_item_id];
      end
      if ids.size > 0 then
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

def public_static_void_main
  find_clusters(%w{title_series agency});
  find_clusters(%w{author title published});
  find_clusters('lccn');
  find_clusters('issn');
  # Is not going to find much until we do data from >1 sources.
  find_clusters('oclcnum');

  @conn.close();
end

if $0 == __FILE__ then
  public_static_void_main()
end
