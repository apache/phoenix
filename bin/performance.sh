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

# Note: This script is tested on Linux environment only. It should work on any Unix platform but is not tested.


# command line arguments
zookeeper=$1
rowcount=$2
table="performance_$2"

# helper variable and functions
ddl="ddl.sql"
data="data.csv"
qry="query.sql"
statements=""

# Phoenix client jar. To generate new jars: $ mvn package -DskipTests
current_dir=$(cd $(dirname $0);pwd)
phoenix_jar_path="$current_dir/../phoenix-assembly/target"
phoenix_client_jar=$(find $phoenix_jar_path/phoenix-*-client.jar)
testjar="$current_dir/../phoenix-core/target/phoenix-*-tests.jar"

# HBase configuration folder path (where hbase-site.xml reside) for HBase/Phoenix client side property override
hbase_config_path="$current_dir"

execute="java -cp "$hbase_config_path:$phoenix_client_jar" -Dlog4j.configuration=file:$current_dir/log4j.properties org.apache.phoenix.util.PhoenixRuntime -t $table $zookeeper "
function usage {
	echo "Performance script arguments not specified. Usage: performance.sh <zookeeper> <row count>"
	echo "Example: performance.sh localhost 100000"
	exit
}

function queryex {
	echo "Query # $1 - $2"
        statements="$statements$2"
}
function cleartempfiles {
	delfile $ddl
	delfile $data
	delfile $qry
}
function delfile {
	if [ -f $1 ]; then rm $1 ;fi;
}

# Create Table DDL
createtable="CREATE TABLE IF NOT EXISTS $table (HOST CHAR(2) NOT NULL,DOMAIN VARCHAR NOT NULL,
FEATURE VARCHAR NOT NULL,DATE DATE NOT NULL,USAGE.CORE BIGINT,USAGE.DB BIGINT,STATS.ACTIVE_VISITOR 
INTEGER CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, DATE)) 
SPLIT ON ('CSGoogle','CSSalesforce','EUApple','EUGoogle','EUSalesforce','NAApple','NAGoogle','NASalesforce');"

# generate and upsert data
clear
echo "Phoenix Performance Evaluation Script 1.0";echo "-----------------------------------------"
if [ -z "$2" ] 
then usage; fi;
echo ""; echo "Creating performance table..."
echo $createtable > $ddl; $execute "$ddl"

# Write real,user,sys time on console for the following queries
queryex "1 - Count" "SELECT COUNT(1) FROM $table;"
queryex "2 - Group By First PK" "SELECT HOST FROM $table GROUP BY HOST;"
queryex "3 - Group By Second PK" "SELECT DOMAIN FROM $table GROUP BY DOMAIN;"
queryex "4 - Truncate + Group By" "SELECT TRUNC(DATE,'DAY') DAY FROM $table GROUP BY TRUNC(DATE,'DAY');"
queryex "5 - Filter + Count" "SELECT COUNT(1) FROM $table WHERE CORE<10;"

echo ""; echo "Generating and upserting data..."
java -jar $testjar $rowcount
echo ""; 
echo $statements>$qry
$execute $data $qry

# clear temporary files
cleartempfiles
