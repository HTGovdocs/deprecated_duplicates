#!/bin/bash

# Get abs path to this dir.
pushd `dirname $0` > /dev/null;
SCRIPTPATH=`pwd`;
popd > /dev/null;

LOGDIR=`readlink -e $SCRIPTPATH/../log`;

bash $SCRIPTPATH/clean_db.sh;

echo "Nohupping $SCRIPTPATH/rebuild_db.sh and logging to $LOGDIR/rebuild_db.log";
nohup bash $SCRIPTPATH/rebuild_db.sh > $LOGDIR/rebuild_db.log & 
echo "$0 done.";