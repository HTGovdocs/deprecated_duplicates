require 'htph';

class Array
  def slurp(filename)
    HTPH::Hathidata.read(filename) do |line|
      self << line.strip;
    end
    return self;
  end
end


a = [].slurp('states.txt');
b = [].slurp('gov_agency_acronyms.txt');

puts a;
puts b;
