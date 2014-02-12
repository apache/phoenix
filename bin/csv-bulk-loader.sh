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

# commandline-options
# -i                             CSV data file path in hdfs (mandatory)
# -s                             Phoenix schema name (mandatory if table is created without default phoenix schema name)
# -t                             Phoenix table name (mandatory)
# -sql                           Phoenix create table ddl path (mandatory)
# -zk                            Zookeeper IP:<port> (mandatory)
# -hd                            HDFS NameNode IP:<port> (mandatory)
# -mr                            MapReduce Job Tracker IP:<port> (mandatory)
# -o                             Output directory path in hdfs (optional)
# -idx                           Phoenix index table name (optional, index support is yet to be added)
# -error                         Ignore error while reading rows from CSV ? (1 - YES | 0 - NO, defaults to 1) (optional)
# -help                          Print all options (optional)

phoenix_client_jar=$(find ../phoenix-assembly/target/phoenix-*-client.jar)
java -cp "$phoenix_client_jar" org.apache.phoenix.map.reduce.CSVBulkLoader "$@"
