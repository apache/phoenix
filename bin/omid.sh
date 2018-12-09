#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;

# Load Omid environment variables
source omid-env.sh

# Configure classpath...
CLASSPATH=./:../conf:${HBASE_CONF_DIR}:${HADOOP_CONF_DIR}

# ...for source release and...
for j in ../target/omid-tso*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

# and for binary release
for j in ../omid-tso*.jar; do
    CLASSPATH=$CLASSPATH:$j
done
for j in ../lib/*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

tso() {
    exec java $JVM_FLAGS -cp $CLASSPATH org.apache.omid.tso.TSOServer $@
}

tsoRelauncher() {
    until ./omid.sh tso $@; do
        echo "TSO Server crashed with exit code $?.  Re-launching..." >&2
        sleep 1
    done
}

createHBaseCommitTable() {
    java -cp $CLASSPATH org.apache.omid.tools.hbase.OmidTableManager commit-table $@
}

createHBaseTimestampTable() {
    java -cp $CLASSPATH org.apache.omid.tools.hbase.OmidTableManager timestamp-table $@
}

usage() {
    echo "Usage: omid.sh <command> <options>"
    echo "where <command> is one of:"
    echo "  tso                           Starts The Status Oracle server (TSO)"
    echo "  tso-relauncher                Starts The Status Oracle server (TSO) re-launching it if the process exits"
    echo "  create-hbase-commit-table     Creates the hbase commit table."
    echo "  create-hbase-timestamp-table  Creates the hbase timestamp table."
}

# if no args specified, show usage
if [ $# = 0 ]; then
    usage;
    exit 1
fi

COMMAND=$1
shift

if [ "$COMMAND" = "tso" ]; then
    createHBaseTimestampTable $@;
    createHBaseCommitTable $@;
    tso $@;
elif [ "$COMMAND" = "tso-relauncher" ]; then
    tsoRelauncher $@;
elif [ "$COMMAND" = "create-hbase-commit-table" ]; then
    createHBaseCommitTable $@;
elif [ "$COMMAND" = "create-hbase-timestamp-table" ]; then
    createHBaseTimestampTable $@;
else
    exec java -cp $CLASSPATH $COMMAND $@
fi


