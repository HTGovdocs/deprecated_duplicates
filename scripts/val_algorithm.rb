require 'java';
require 'dotenv';
Dotenv.load();
require 'htph';

jdbc = HTPH::Hathijdbc::Jdbc.new();
conn = jdbc.get_conn();

def duplicates (ri, rj)
  puts "#{ri} and #{rj} are duplicates";
end

def related (ri, rj)
  puts "#{ri} and #{rj} are related";
end

def veq (ri, rj, k) # Verbose equality
  puts "Does #{k} match for [#{ri[k]}] and [#{rj[k]}]?";
  tf = false;
  if ri.has_key?(k) && ri[k] == rj[k] then
    tf = true;
  end
  puts tf;
  return tf;
end

records = [];
q = "SELECT vp.str AS prop, gp.val FROM gd_prop AS gp JOIN v_gd_prop_str AS vp ON (gp.prop = vp.prop) WHERE gd_item_id = ?";
ARGV.each do |arg|
  puts "Getting #{arg}";
  record = {:id => arg};
  conn.prepared_select(q, [arg]) do |row|
    p = row.get_object('prop').to_sym;
    v = row.get_object('val');
    record[p] ||= [];
    record[p] << v;
  end
  puts record;
  records << record;
end

records.each_with_index do |ri,i|
  records.each_with_index do |rj,j|
    next if j <= i;
    puts "comparing #{ri[:id]} =?= #{rj[:id]}";
    if veq(ri, rj, :oclc) then
      if veq(ri, rj, :enum_chron) then
        duplicates(ri, rj);
      else
        related(ri, rj);
      end
    elsif veq(ri, rj, :lccn) then
      if veq(ri, rj, :enum_chron) then
        duplicates(ri, rj);
      end
    elsif veq(ri, rj, :issn) then
      if veq(ri, rj, :enum_chron) then
        duplicates(ri, rj);
      else
        related(ri, rj);
      end
    elsif veq(ri, rj, :sudoc) then
      if veq(ri, rj, :title) then
        if veq(ri, rj, :enum_chron) then
          duplicates(ri, rj);
        end
      end
    elsif veq(ri, rj, :title) then
      if veq(ri, rj, :agency) then
        if veq(ri, rj, :pub_date) then
          if veq(ri, rj, :enum_chron) then
            duplicates(ri, rj);
          end
        end
      end
    end
    puts '---------';
  end
end
