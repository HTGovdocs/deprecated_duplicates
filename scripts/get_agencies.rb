require 'htph';
require 'json';

# Reads input file line by line and calls deeper methods.
def index_file (infile, agencies)
  puts "Started #{Time.new()}";
  c = 0;
  File.open(infile) do |f|
    f.each_line do |line|
      c += 1;
      if c % 25000 == 0 then
        puts "#{Time.new()} | #{c} records";
      end
      line.strip!;
      j = JSON.parse(line);
      if j.has_key?('agency') then
        j['agency'].each do |agency|
          agencies[agency] ||= 0;
          agencies[agency] +=  1;
        end
      end
    end
  end
end

if $0 == __FILE__ then
  if ARGV.size == 0 then
    raise "Need infile";
  end
  agencies = {};
  ARGV.each do |infile|
    if !File.exists?(infile) then
      raise "Need infile that actually exists.";
    end
    index_file(infile, agencies);
  end
  HTPH::Hathidata.write('agencies_raw_count.tsv') do |hdout|
    agencies.keys.each do |k|
      hdout.file.puts "#{agencies[k]}\t#{k}";
    end
  end
end
