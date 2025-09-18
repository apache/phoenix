#!/bin/bash
###########################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
###########################################################################

# This script is intended to run the sql queries in a file with the given client version

zk_url=$1
artifact_count=$2

artifacts=()
param_index=3
for ((i=0; i<artifact_count; i++)); do
  group_id=${!param_index}
  ((param_index++))
  artifact_id=${!param_index}
  ((param_index++))
  version=${!param_index}
  ((param_index++))
  artifacts+=("$group_id:$artifact_id:$version")
done

sqlfile=${!param_index}
((param_index++))
resultfile=${!param_index}
((param_index++))
tmp_dir=${!param_index}
((param_index++))
maven_home=${!param_index}

if [ -n $maven_home ]; then
   export PATH=$maven_home/bin:$PATH
fi

java -Djava.security.manager=allow -version &> /dev/null || error_code=$?
if [ -z ${error_code+x} ]; then
  security_manager='-Djava.security.manager=allow'
fi

echo "Downloading ${#artifacts[@]} artifacts..."
for artifact in "${artifacts[@]}"; do
  echo "Downloading $artifact"
  mvn -B dependency:get -Dartifact=$artifact
  if [ $? -ne 0 ]; then
    echo "Failed to download artifact $artifact"
    exit 1
  fi

  mvn -B dependency:copy -Dartifact=$artifact -DoutputDirectory=$tmp_dir
  if [ $? -ne 0 ]; then
    echo "Failed to copy artifact $artifact"
    exit 1
  fi
done

classpath="."
for jar in $tmp_dir/*.jar; do
  classpath="$classpath:$jar"
done

echo "Using classpath: $classpath"

phoenix_props="-Dphoenix.query.timeoutMs=60000 -Dphoenix.query.keepAliveMs=60000"

phoenix_props="$phoenix_props -Dlog4j.logger.org.apache.phoenix=DEBUG"
phoenix_props="$phoenix_props -Dlog4j.logger.org.apache.hadoop.hbase=DEBUG"
phoenix_props="$phoenix_props -Dlog4j.logger.sqlline=DEBUG"
phoenix_props="$phoenix_props -Dlog4j.logger.org.apache.zookeeper=INFO"

echo "Attempting to connect to Phoenix with URL: jdbc:phoenix:$zk_url"
echo "Using classpath: $classpath"
echo "Phoenix properties: $phoenix_props"

java -cp "$classpath" $security_manager $phoenix_props sqlline.SqlLine \
-d org.apache.phoenix.jdbc.PhoenixDriver -u jdbc:phoenix:$zk_url -n none -p none \
--color=false --fastConnect=true --outputformat=csv \
--silent=true --verbose=false --isolation=TRANSACTION_READ_COMMITTED --run=$sqlfile &> $resultfile
