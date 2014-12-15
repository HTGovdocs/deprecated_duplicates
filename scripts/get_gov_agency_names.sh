# Gets data from a yaml that looks like:
# agency:
#   agency: Advisory Council on Historic Preservation
#   acronym: achp
# agency:
#   agency: African Development Foundation
#   acronym: adf
# agency:
#   ...

curl "https://raw.githubusercontent.com/unitedstates/acronym/gh-pages/_data/agencies.yml" | 
egrep ' +agency:(.+)' | 
sed -r 's/agency: +//' | 
sed -e 's/^[ \t]*//g;s/[ \t]*$//g' | # trim
egrep -v '^$' |
tr '[a-z]' '[A-Z]' |
sort -u;

# Results in a list of uppercased acronyms:
# ACF
# ACHP
# ACP
# ...
# WB
# WHD
# WHS
