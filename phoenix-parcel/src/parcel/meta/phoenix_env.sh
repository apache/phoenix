#!/bin/bash
set -ex


#The following is written to aid local testing
if [ -z $PARCELS_ROOT ] ; then
    export MYDIR=`dirname "${BASH_SOURCE[0]}"`
    PARCELS_ROOT=`cd $MYDIR/../.. &&  pwd`
fi
PARCEL_DIRNAME=${PARCEL_DIRNAME-PHOENIX}

MYLIBDIR=${PARCELS_ROOT}/${PARCEL_DIRNAME}/lib/phoenix

[ -d $MYLIBDIR ] || {
    echo "Could not find phoenix parcel lib dir, exiting" >&2
    exit 1
}

APPENDSTRING=`echo ${MYLIBDIR}/*.jar | sed 's/ /:/g'`
echo "appending '$APPENDSTRING' to HBASE_CLASSPATH"
if [ -z $HBASE_CLASSPATH ] ; then
    export HBASE_CLASSPATH=$APPENDSTRING
else
    export HBASE_CLASSPATH="$HBASE_CLASSPATH:$APPENDSTRING"
fi
echo "Set HBASE_CLASSPATH to '$HBASE_CLASSPATH'"
echo "phoenix_env.sh successfully executed at `date`"
