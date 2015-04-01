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

phoenix_utils.setPath()

if os.name == 'nt':
    args = subprocess.list2cmdline(sys.argv[1:])
else:
    import pipes    # pipes module isn't available on Windows
    args = " ".join([pipes.quote(v) for v in sys.argv[1:]])

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
java_cmd = 'java -cp "' + phoenix_utils.hbase_conf_path + os.pathsep + phoenix_utils.phoenix_client_jar + \
    '" -Dlog4j.configuration=file:' + \
    os.path.join(phoenix_utils.current_dir, "log4j.properties") + \
    " org.apache.phoenix.util.PhoenixRuntime " + args 

exitcode = subprocess.call(java_cmd, shell=True)
sys.exit(exitcode)
