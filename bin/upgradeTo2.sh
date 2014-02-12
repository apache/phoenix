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
phoenix_jar_path="$current_dir/../target"
phoenix_client_jar=$(find $phoenix_jar_path/phoenix-*-client.jar)

# HBase configuration folder path (where hbase-site.xml reside) for HBase/Phoenix client side property override
hbase_config_path="$current_dir"

java -cp "$hbase_config_path:$phoenix_client_jar" -Dlog4j.configuration=file:$current_dir/log4j.properties org.apache.phoenix.util.PhoenixRuntime -u "$@"
