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

# !!!  PLEASE READ !!!
# !!!  Do NOT run the script against a prodcution cluster because it wipes out
# !!!  existing data of the cluster 

import os
import subprocess
import sys
import phoenix_utils

phoenix_utils.setPath()

phoenix_jar_path = os.getenv(phoenix_utils.phoenix_class_path, phoenix_utils.phoenix_test_jar_path)

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = os.getenv('HBASE_CONF_DIR', phoenix_utils.hbase_conf_path)
hbase_library_path = os.getenv('HBASE_LIBRARY_DIR', '')

print "Current ClassPath=%s:%s:%s" % (hbase_config_path, phoenix_jar_path,
                                      hbase_library_path)

java_cmd = "java -cp " + hbase_config_path + os.pathsep + phoenix_jar_path + os.pathsep + \
    hbase_library_path + " org.apache.phoenix.end2end.End2EndTestDriver " + \
    ' '.join(sys.argv[1:])

exitcode = subprocess.call(java_cmd, shell=True)
sys.exit(exitcode)
