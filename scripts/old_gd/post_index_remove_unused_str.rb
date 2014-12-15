# IF we don't truncate gd_str before re-indexing (and let's assume we don't, which should save some time),
# then we might end up with unused, out-of-date rows in gd_str. They are now removed in a post-processing
# step where we do a left join against gd_prop and remove all gd_str rows that are not used there.

require 'htph';

db   = HTPH::Hathidb::Db.new();
conn = db.get_conn();

# Can't do:
# ON (gp.val = gs.gd_str_id OR gp.prop = gs.gd_str_id)
# because the indexes are not so good, maybe? Not sure
# but that query took forever and had a scary looking
# explain-plan. Same same if slapping on a second 
# LEFT JOIN on mwarin_ht.gd_prop.

get_sql = %w[
    SELECT gs.gd_str_id 
    FROM      mwarin_ht.gd_str  AS gs 
    LEFT JOIN mwarin_ht.gd_prop AS gp 
    ON (gp.val = gs.gd_str_id) 
    WHERE gp.val IS NULL
].join(' ');

del_sql = "DELETE FROM mwarin_ht.gd_str WHERE gd_str_id = ?";
del_q   = conn.prepare(del_sql);

i = 0;
puts get_sql;
conn.query(get_sql) do |row|
  if i % 1000 == 0 then
    puts i;
  end
  i += 1;

  str_id = row[:gd_str_id];
  begin
    del_q.execute(str_id);
  rescue StandardError => e
    # str_id is probably used as a prop.
    puts "Exception when deleting #{str_id}";
    puts e;
    sleep 1;
  end
end
