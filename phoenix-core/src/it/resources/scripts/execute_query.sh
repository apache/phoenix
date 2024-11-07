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
client_group_id=$2
client_artifact_id=$3
client_version=$4
sqlfile=$5
resultfile=$6
tmp_dir=$7
maven_home=$8

if [ -n $maven_home ]; then
   export PATH=$maven_home/bin:$PATH
fi

java -Djava.security.manager=allow -version &> /dev/null || error_code=$?
if [ -z ${error_code+x} ]; then
  security_manager='-Djava.security.manager=allow'
fi

mvn -B dependency:get -Dartifact=${client_group_id}:${client_artifact_id}:${client_version}
mvn -B dependency:copy -Dartifact=${client_group_id}:${client_artifact_id}:${client_version} \
-DoutputDirectory=$tmp_dir

phoenix_client_jar=$tmp_dir/${client_artifact_id}-${client_version}.jar
java -cp ".:$phoenix_client_jar" $security_manager sqlline.SqlLine \
-d org.apache.phoenix.jdbc.PhoenixDriver -u jdbc:phoenix:$zk_url -n none -p none \
--color=false --fastConnect=true --outputformat=csv \
--silent=true --verbose=false --isolation=TRANSACTION_READ_COMMITTED --run=$sqlfile &> $resultfile
