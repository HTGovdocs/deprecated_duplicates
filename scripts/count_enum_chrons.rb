require 'json';

infile = ARGV.shift;
has_ec = 0;
no_ec  = 0;
num_ec = 0;

File.open(infile) do |f|
  f.each_line do |line|
    j = JSON.parse(line);
    if j.has_key?('enum_chron') then
      has_ec += 1;
      num_ec += j['enum_chron'].size;
    else
      no_ec += 1;
    end
  end
end

puts "has_ec: #{has_ec}";
puts "no_ec : #{no_ec}";
puts "num_ec: #{num_ec}";

puts "avg ec/doc : #{num_ec / (has_ec + no_ec * 1.0)}";
