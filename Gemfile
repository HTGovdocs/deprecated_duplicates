source 'https://rubygems.org';

gem 'dotenv-rails';
gem 'marc';
gem 'pry';
gem 'traject', '~> 1.1.0';
gem 'htph', :git => 'https://github.com/HTGovdocs/HTPH-rubygem.git';

# Needed for the UoM files.
gem 'traject_alephsequential_reader', :github => 'traject-project/traject_alephsequential_reader';

# Has to be done by anyone using HTPH.
# Not sure if this is abominable? But it works... 
# ... IF you are in the same dir as the gemfile when calling bundle.
require 'dotenv';
Dotenv.load();
