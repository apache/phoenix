#!/bin/sh
# Downloads the specified HBase version sources into PWD, extracts it, 
# then rebuilds and installs the artifacts with -Dhadoop.profile=3.0

if [ $# -ne 1 ]
  then
    echo "Supply the Hbase version as paramater i.e.: rebuild_hbase.sh 2.2.6 "
fi

wget https://downloads.apache.org/hbase/$1/hbase-$1-src.tar.gz
tar xfvz hbase-$1-src.tar.gz

STARTDIR=$PWD
cd hbase-$1
mvn clean install -Dhadoop.profile=3.0 -DskipTests
cd $STARTDIR