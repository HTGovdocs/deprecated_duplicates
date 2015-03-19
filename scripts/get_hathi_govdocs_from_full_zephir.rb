# zcat zephir_full_YYYYMMDD_vufind.json.gz | ruby get_hathi_govdocs_from_full_zephir.rb > zephir_full_YYYYMMDD_only_govdocs.json

leader_rx = Regexp.new(/"008":"([^"]+)"/);
us_rx     = Regexp.new(/^[a-z]{2}u$/);

ARGF.each_line do |line|
  if line =~ leader_rx then    
    leader = $1;
    if leader[15..17] =~ us_rx &&  leader[28] == 'f' then
      puts line;
    end
  end
end
