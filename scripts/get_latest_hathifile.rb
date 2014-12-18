require 'htph';
require 'net/http';
require 'open-uri';

def run (fn)
  root_url = 'http://www.hathitrust.org/hathifiles';
  puts "Grabbing #{fn.nil? ? 'latest file' : fn} from #{root_url}";
  files = get_HT_filenames(root_url);
  retrieve_HT_file(files, fn);
  puts 'Done.';
end

def get_HT_filenames (url)
  # Look for matching urls in the root url, return list of them.
  body = open(url).read;
  hits = [];
  if !body.empty? then
    hits = body.scan(/\"(http:.+hathi_full_\d+\.txt\.gz)\"/).flatten.sort;
  end

  return hits;
end

def retrieve_HT_file (urls, targetfile)
  # Go through list of urls and look for the target file.
  # If found, download and unzip.
  success = 0;

  if !targetfile.nil? then
    # If filename given, discard all that do not match filename.
    urls.keep_if do |x|
      x.include?(targetfile);
    end
  end

  if urls.size > 0 then
    # Get last of matching files.
    url   = urls.last;
    bits  = url.split('/');
    filen = bits[-1];
    puts filen;
    
    hd = HTPH::Hathidata::Data.new('hathi_full_$ymd.txt.gz');
    if !hd.exists? then
      hd.open('wb');
      puts "Saving #{url} to #{hd.path}";
      Net::HTTP.start("www.hathitrust.org") do |http|
        begin
          http.request_get(url) do |response|
            response.read_body do |segment|
              hd.file.write(segment)
            end
          end
        end
      end
      hd.close();
    end
    hd.inflate();
    success += 1;    
  end

  if success < 1
    puts "Did not find the specified file (#{targetfile.nil? ? 'latest' : targetfile})";
    exit(1);
  end
end

# Get things started, make sure there is a filename coming in.
if __FILE__== $0
  fn = ARGV.shift;
  if !fn.nil? && !fn.strip.empty? then
    fn.strip!;
    run(fn);
  else
    # Get the latest one.
    run(nil);
  end
end
