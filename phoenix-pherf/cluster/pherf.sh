#!/bin/bash

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

source config/env.sh
HBASE_CLASSPATH=`$HBASE_ROOT/hbase/hbase/bin/hbase classpath`


PHERF_HOME=$(cd "`dirname $0`" && pwd)
CLASSPATH=${HBASE_CLASSPATH}
CLASSPATH=${PHERF_HOME}/config:${CLASSPATH}

for f in $PHERF_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

CMD="time $}JAVA_HOME}/bin/java ${REMOTE_DEBUG} -Dapp.home=${PHERF_HOME} ${ENV_PROPS} -Xms512m -Xmx3072m -cp ${CLASSPATH} org.apache.phoenix.pherf.Pherf ${@}"

eval $CMD