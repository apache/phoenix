#!/usr/bin/env bash
# The name of the Apache Hbase source file
HBASE_SOURCE_NAME="hbase-$1-src.tar.gz"
# The relative path on the ASF mirrors for the Hbase source file
HBASE_SOURCE_MIRROR_NAME="hbase/$1/$HBASE_SOURCE_NAME"

# Downloads the specified HBase version source, extracts it,
# then rebuilds and installs the maven artifacts locally with -Dhadoop.profile=3.0

if [ $# -ne 1 ]
  then
    echo "Supply the Hbase version as paramater i.e.: rebuild_hbase.sh 2.2.6 "
fi

DEV_SUPPORT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ARTIFACTS_DIR="$DEV_SUPPORT/artifacts"
WORK_DIR="$DEV_SUPPORT/work"

mkdir "$ARTIFACTS_DIR"
mkdir "$WORK_DIR"

$DEV_SUPPORT/cache-apache-project-artifact.sh --keys https://downloads.apache.org/hbase/KEYS \
    --working-dir "$WORK_DIR" "$ARTIFACTS_DIR/$HBASE_SOURCE_NAME" "$HBASE_SOURCE_MIRROR_NAME"

STARTDIR=$PWD
cd $ARTIFACTS_DIR
tar xfz hbase-$1-src.tar.gz
cd hbase-$1
mvn clean install -Dhadoop.profile=3.0 -DskipTests -B
cd ${STARTDIR}
