#!/bin/bash
#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
#

current_dir=$(cd $(dirname $0);pwd)
phoenix_jar_path="$current_dir/../phoenix-core/target/*"

# check envvars which might override default args
if [ "${PHOENIX_LIB_DIR}" != "" ]; then
  phoenix_jar_path="${PHOENIX_LIB_DIR}"
fi

# HBase configuration folder path (where hbase-site.xml reside) for HBase/Phoenix client side property override
hbase_config_path="$current_dir"
# check envvars which might override default args
if [ "${HBASE_CONF_DIR}" != "" ]; then
  hbase_config_path="${HBASE_CONF_DIR}"
fi

hbase_library_path="$current_dir"
# check envvars which might override default args
if [ "${HBASE_LIBRARY_DIR}" != "" ]; then
  #Sample class path would be: /usr/lib/hbase-0.94.15/lib/*:/usr/lib/hbase-0.94.15/*
  hbase_library_path="${HBASE_LIBRARY_DIR}"
fi

echo "Current ClassPath="$hbase_config_path:$hbase_library_path:$phoenix_jar_path

java -cp "$hbase_config_path:$phoenix_jar_path:$hbase_library_path" org.apache.phoenix.end2end.End2EndTestDriver "$@"
