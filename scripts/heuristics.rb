$LOAD_PATH << './lib/'
require 'gddb';

db    = Gddb::Db.new();
@conn = db.get_interactive();

get_vals_sql   = "SELECT gp.prop, gp.val, COUNT(gp.val) AS cv FROM mwarin_ht.gd_prop AS gp, mwarin_ht.gd_str AS gs WHERE gp.prop = gs.gd_str_id AND gs.str = ? GROUP BY gp.prop, gp.val HAVING cv >= 2";
@get_vals_q    = @conn.prepare(get_vals_sql);

get_by_val_sql = "SELECT gd_item_id FROM mwarin_ht.gd_prop WHERE prop = ? AND val = ?"
@get_by_val_q  = @conn.prepare(get_by_val_sql);

insert_cluster_sql = "INSERT INTO mwarin_ht.gd_cluster () VALUES ()";
@insert_cluster_q  = @conn.prepare(insert_cluster_sql);

insert_cluster_item_sql = "INSERT INTO mwarin_ht.gd_item_cluster (gd_item_id, gd_cluster_id) VALUES (?, ?)";
@insert_cluster_item_q  = @conn.prepare(insert_cluster_item_sql);

last_id_sql    = "SELECT LAST_INSERT_ID() AS id";
@last_id_q     = @conn.prepare(last_id_sql);

["DELETE FROM mwarin_ht.gd_item_cluster", "DELETE FROM mwarin_ht.gd_cluster"].each do |dq|
  puts dq;
  @conn.update(dq);
end

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
    clusters << ids;
  end
  return clusters;
end

if $0 == __FILE__ then
  same('oclcnum').each do |cluster|
    make_cluster(cluster);
  end
  same('lccn').each do |cluster|
    make_cluster(cluster);
  end
  same('title').each do |cluster|
    make_cluster(cluster);
  end

  @conn.close();
end
