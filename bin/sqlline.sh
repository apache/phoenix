#!/bin/bash
############################################################################
#
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
#
############################################################################

# Phoenix client jar. To generate new jars: $ mvn package -DskipTests
current_dir=$(cd $(dirname $0);pwd)
phoenix_jar_path="$current_dir/../phoenix-assembly/target"
phoenix_client_jar=$(find $phoenix_jar_path/phoenix-*-client.jar)


if [ -z "$1" ] 
  then echo -e "Zookeeper not specified. \nUsage: sqlline.sh <zookeeper> <optional_sql_file> \nExample: \n 1. sqlline.sh localhost \n 2. sqlline.sh localhost ../examples/stock_symbol.sql";
  exit;
fi

if [ "$2" ] 
  then sqlfile="--run=$2";
fi

java -cp ".:$phoenix_client_jar" -Dlog4j.configuration=file:$current_dir/log4j.properties sqlline.SqlLine -d org.apache.phoenix.jdbc.PhoenixDriver -u jdbc:phoenix:$1 -n none -p none --color=true --fastConnect=false --verbose=true --isolation=TRANSACTION_READ_COMMITTED $sqlfile
