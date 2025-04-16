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

from __future__ import print_function
import os
import subprocess
import sys
import phoenix_utils

phoenix_utils.setPath()

# Get the cached classpath
base_dir = os.path.join(phoenix_utils.current_dir, '..')
phoenix_target_dir = os.path.join(base_dir, 'phoenix-core', 'target')

cp_file_path = os.path.join(phoenix_target_dir, 'cached_classpath.txt')
cp_components = []
with open(cp_file_path, 'r') as cp_file:
    cp_components.append(cp_file.read())
cached_classpath = ":".join(cp_components)

# Get the hbase classpath from HBASE_HOME
hbase_classpath = ''
if os.getenv('HBASE_HOME'):
    hbase_bin_dir = os.path.join(os.getenv('HBASE_HOME'), "bin")
    hbase_classpath = subprocess.check_output(
        [os.path.join(hbase_bin_dir, "hbase"), "--internal-classpath", "classpath"]).decode().strip()
else:
    print("HBASE_HOME is not set, using HBase jars from cached_classpath.txt")
    # Set whatever config dir phoenix_utlils has decided instead
    hbase_classpath = phoenix_utils.hbase_conf_dir

# The internal classpath returned by `hbase classpath`, and the cached classpath have a lot of the
# same files. If HBASE_HOME is set, then the jars from it come before the cached jars, so those
# take precedence. Otherwise we use the HBase jars from the cached classpath.
# If HBASE_HOME is specified, then the cached classpath would only be used for Junit.

print("Current ClassPath=%s:%s:%s" %
      (hbase_classpath, phoenix_utils.phoenix_test_jar_path, cached_classpath))

java_cmd = "java -cp " + hbase_classpath + os.pathsep + phoenix_utils.phoenix_test_jar_path \
     + os.pathsep + cached_classpath + " org.apache.phoenix.end2end.End2EndTestDriver " + \
    ' '.join(sys.argv[1:])

os.execl("/bin/sh", "/bin/sh", "-c", java_cmd)
