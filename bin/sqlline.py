#!/usr/bin/env python
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

import os
import subprocess
import sys
import phoenix_utils

current_dir = os.path.dirname(os.path.abspath(__file__))
phoenix_jar_path = os.getenv('PHOENIX_LIB_DIR',
                             os.path.join(current_dir, "..", "phoenix-assembly",
                                "target"))
phoenix_client_jar = phoenix_utils.find("phoenix-*-client.jar", phoenix_jar_path)

if len(sys.argv) < 2:
    print "Zookeeper not specified. \nUsage: sqlline.sh <zookeeper> \
<optional_sql_file> \nExample: \n 1. sqlline.sh localhost \n 2. sqlline.sh \
localhost ../examples/stock_symbol.sql"
    sys.exit()

sqlfile = ""

if len(sys.argv) > 2:
    sqlfile = "--run=" + sys.argv[2]

java_cmd = 'java -cp ".' + os.pathsep + phoenix_client_jar + \
    '" -Dlog4j.configuration=file:' + \
    os.path.join(current_dir, "log4j.properties") + \
    " sqlline.SqlLine -d org.apache.phoenix.jdbc.PhoenixDriver \
-u jdbc:phoenix:" + sys.argv[1] + \
    " -n none -p none --color=true --fastConnect=false --verbose=true \
--isolation=TRANSACTION_READ_COMMITTED " + sqlfile

subprocess.call(java_cmd, shell=True)
