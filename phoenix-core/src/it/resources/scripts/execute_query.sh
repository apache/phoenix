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
client_version=$2
sqlfile=$3
resultfile=$4
tmp_dir=$5
maven_home=$6

if [ -n $maven_home ]; then
   export PATH=$maven_home/bin:$PATH
fi

mvn -B dependency:get -Dartifact=org.apache.phoenix:phoenix-client:$client_version
mvn -B dependency:copy -Dartifact=org.apache.phoenix:phoenix-client:$client_version \
-DoutputDirectory=$tmp_dir

phoenix_client_jar=$tmp_dir/phoenix-client-$client_version.jar
java -cp ".:$phoenix_client_jar" sqlline.SqlLine -d org.apache.phoenix.jdbc.PhoenixDriver \
-u jdbc:phoenix:$zk_url -n none -p none --color=false --fastConnect=true --outputformat=csv \
--silent=true --verbose=false --isolation=TRANSACTION_READ_COMMITTED --run=$sqlfile &> $resultfile
