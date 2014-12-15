require 'htph';

b = HTPH::Hathibench::Benchmark.new();
b.time("foo") do
  puts "eep";
end
b.time("bar") do
  puts "oop";
end
b.time("foo") do
  sleep 1;
  puts "eep";
end

1.upto(10) do |x|
  b.time() do
    puts x;
  end
end

puts b.prettyprint();
