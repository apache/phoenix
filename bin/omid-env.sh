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

# Set the flags to pass to the jvm when running omid
# export JVM_FLAGS=-Xmx8096m
# ---------------------------------------------------------------------------------------------------------------------
# Check if HADOOP_CONF_DIR and HBASE_CONF_DIR are set
# ---------------------------------------------------------------------------------------------------------------------

if [ -z ${HADOOP_CONF_DIR+x} ]; then echo "WARNING: HADOOP_CONF_DIR is unset"; else echo "HADOOP_CONF_DIR is set to '$HADOOP_CONF_DIR'"; fi
if [ -z ${HBASE_CONF_DIR+x} ]; then echo "WARNING: HBASE_CONF_DIR is unset"; else echo "HBASE_CONF_DIR is set to '$HBASE_CONF_DIR'"; fi

