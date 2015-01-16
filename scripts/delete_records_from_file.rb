# Take a file id and delete all hathi_x attribute records, and then all hathi_gd records associated with that file id.
require 'htph';
require 'set';

file_id = ARGV.shift;

if file_id.nil? then
  raise "Need a file_id as 1st arg.";
end

db = HTPH::Hathidb::Db.new();
conn = db.get_conn();

attribute_table_select_sql = "SELECT DISTINCT x.gd_id FROM hathi_XXX AS x JOIN hathi_gd AS h ON (h.id = x.gd_id) JOIN hathi_input_file AS hif ON (hif.id = h.file_id) WHERE hif.id = ?";
attribute_table_suffix = %w[enumc isbn issn lccn oclc pubdate publisher sudoc title related];

delete_chunk_size = 100;
qmarks_a = ['?'] * delete_chunk_size;
qmarks   = qmarks_a.join(',');
attribute_table_delete_sql = "DELETE FROM hathi_XXX WHERE gd_id IN (#{qmarks})";
main_table_delete_sql      = "DELETE FROM hathi_gd  WHERE id    IN (#{qmarks})";

all_gds = Set.new();
# For each table, remove records that belong to file.
# Remember each id.
attribute_table_suffix.each do |suffix|
  table_gds = Set.new();
  table_specific_select_sql = attribute_table_select_sql.gsub('XXX', suffix);
  puts table_specific_select_sql;
  attribute_table_select_q = conn.prepare(table_specific_select_sql);
  attribute_table_select_q.enumerate(file_id) do |row|
    table_gds << row[:gd_id];
    all_gds   << row[:gd_id];
  end
  puts "Delete #{table_gds.size} records from hathi_#{suffix} ...";
  puts attribute_table_delete_sql.gsub('XXX', suffix);
  attribute_table_delete_q = conn.prepare(attribute_table_delete_sql.gsub('XXX', suffix));
  puts "Delete from hathi_#{suffix} ...:";
  table_gds.each_slice(delete_chunk_size) do |chunk|
    padding = [nil] * (qmarks_a.size - chunk.size);
    q_args  = [chunk, padding].flatten;
    attribute_table_delete_q.execute(*q_args);
  end
end

# In hathi_gd, go through all seen ids and delete.
main_table_delete_q = conn.prepare(main_table_delete_sql);
puts "Delete #{all_gds.size} records from hathi_gd ...";
all_gds.each_slice(delete_chunk_size) do |chunk|
  padding = [nil] * (qmarks_a.size - chunk.size);
  q_args  = [chunk, padding].flatten;
  main_table_delete_q.execute(*q_args);
end
