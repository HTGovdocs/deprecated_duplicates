#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

# Get path to /data/
DATADIR=`readlink -e $SCRIPTPATH/../data`;

# Drop everything and rebuild empty tables.
echo "Refreshing database, gonna need mysql password for user `whoami`";
mysql --verbose --show-warnings -h mysql-htprep -p -D gd_test < $SCRIPTPATH/../sql/hathi_gd.sql;

# Get distinct values for 'file_path' in the test mongo, to file.
bundle exec ruby $SCRIPTPATH/get_distinct_mongo_file_path.rb;

# Count lines in output from get_distinct_mongo_file_path.rb
# Make sure we got as many as expected.
wcl_mongo_file_path=`wc -l $DATADIR/distinct_mongo_file_path.txt | awk '{print $1}'`;
wcl_mongo_file_path_expected=41;
if [ $wcl_mongo_file_path = $wcl_mongo_file_path_expected ]; then
    echo "yay $wcl_mongo_file_path = $wcl_mongo_file_path_expected, continue";
else
    echo "Expected $wcl_mongo_file_path_expected, got $wcl_mongo_file_path";
    exit 1;
fi

# Read data from mongo into file that can be read into mysql.
echo '' > $DATADIR/test_mongo_output.ndj;
cat $DATADIR/distinct_mongo_file_path.txt | sort | while read file_path
do :
    marc_profile=`grep $file_path $SCRIPTPATH/../sql/marc_profiles_for_input_files.sql | grep -Po '[A-Za-z._/]+\.tsv'`;
    echo "Read $file_path from mongo using marc profile $marc_profile ...";
    bundle exec ruby $SCRIPTPATH/../scripts/general_marcreader.rb mongo $file_path profile=$marc_profile >> $DATADIR/test_mongo_output.ndj;
done

# Read file into mysql.
bundle exec ruby $SCRIPTPATH/../scripts/index_file.rb $DATADIR/test_mongo_output.ndj;