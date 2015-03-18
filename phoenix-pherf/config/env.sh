#!/bin/sh

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Required variable to point to Java installation
JAVA_HOME=

# Absolute path the the unzipped root directory of the HBase installation
# This is required if you build using the default or cluster profile
# Cluster profile assumes you want to pick up dependencies from HBase classpath
# Not required in standalone.
HBASE_ROOT=

# Add a space seperated list of -D environment args. "-Dkey1-val1 -Dkey2=val2"
ENV_PROPS=""

# Uncomment if you would like to remotely debug
#REMOTE_DEBUG="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=6666"
