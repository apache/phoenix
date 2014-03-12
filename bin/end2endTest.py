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

current_dir = os.path.dirname(os.path.abspath(__file__))
phoenix_jar_path = os.getenv('PHOENIX_LIB_DIR',
                             os.path.join(current_dir, "..", "phoenix-core",
                                          "target", "*"))

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = os.getenv('HBASE_CONF_DIR', current_dir)
hbase_library_path = os.getenv('HBASE_LIBRARY_DIR', current_dir)

print "Current ClassPath=%s:%s:%s" % (hbase_config_path, phoenix_jar_path,
                                      hbase_library_path)

java_cmd = "java -cp " + hbase_config_path + os.pathsep + phoenix_jar_path + os.pathsep + \
    hbase_library_path + " org.apache.phoenix.end2end.End2EndTestDriver " + \
    ' '.join(sys.argv[1:])

subprocess.call(java_cmd, shell=True)
