#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

dbhost=`grep -Po '^db_host\s+=\s+.+' $SCRIPTPATH/../.env | awk '{print $3}'`;

# Drop everything and rebuild empty tables.
echo "Refreshing database, gonna need mysql password for user `whoami`";
mysql --verbose --show-warnings -h mysql-htprep -p -D gd_test < $SCRIPTPATH/../sql/hathi_gd.sql;
echo "$0 done.";