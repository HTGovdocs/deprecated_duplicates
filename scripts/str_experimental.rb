$LOAD_PATH << './lib/'
require 'gddb';

db            = Gddb::Db.new();
conn          = db.get_interactive();
@str_exist_q  = conn.prepare("SELECT gd_str_id, str FROM mwarin_ht.gd_str WHERE str = ?");
@str_insert_q = conn.prepare("INSERT INTO mwarin_ht.gd_str (str) VALUES (?)");
@last_id_q    = conn.prepare("SELECT LAST_INSERT_ID() AS id");

def insert_prop (prop, val)
  ret = {
    :prop_id => prop,
    :val_id  => val
  };

  ret.keys.each do |k|
    str_id = nil;
    @str_exist_q.enumerate(ret[k]) do |res|
      str_id = res[:gd_str_id];
    end

    if str_id.nil? then
      @str_insert_q.execute(ret[k]);
      ret[k] = nil;
      @last_id_q.enumerate do |res|
        ret[k] = res[:id];
      end
    else
      ret[k] = str_id;
    end
  end

  return ret;

end

ret = insert_prop('published', 'Washington');

puts ret;
