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

# This script is intended for use where HBase/Phoenix is loaded from HBase classpath
# therefore HBASE_DIR environment variable needs to be configured for this script to execute

import os
import subprocess
import sys
import phoenix_utils

phoenix_utils.setPath()

args = phoenix_utils.shell_quote(sys.argv[1:])

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = os.getenv('HBASE_CONF_DIR', phoenix_utils.current_dir)
hbase_path = os.getenv('HBASE_DIR')

java_home = os.getenv('JAVA_HOME')

# load hbase-env.??? to extract JAVA_HOME, HBASE_PID_DIR, HBASE_LOG_DIR
hbase_env_path = None
hbase_env_cmd  = None
if os.name == 'posix':
    hbase_env_path = os.path.join(hbase_config_path, 'hbase-env.sh')
    hbase_env_cmd = ['bash', '-c', 'source %s && env' % hbase_env_path]
elif os.name == 'nt':
    hbase_env_path = os.path.join(hbase_config_path, 'hbase-env.cmd')
    hbase_env_cmd = ['cmd.exe', '/c', 'call %s & set' % hbase_env_path]
if not hbase_env_path or not hbase_env_cmd:
    print >> sys.stderr, "hbase-env file unknown on platform %s" % os.name
    sys.exit(-1)

hbase_env = {}
if os.path.isfile(hbase_env_path):
    p = subprocess.Popen(hbase_env_cmd, stdout = subprocess.PIPE)
    for x in p.stdout:
        (k, _, v) = x.partition('=')
        hbase_env[k.strip()] = v.strip()

if hbase_env.has_key('JAVA_HOME'):
    java_home = hbase_env['JAVA_HOME']

if java_home:
    java = os.path.join(java_home, 'bin', 'java')
else:
    java = 'java'

print "HBASE_DIR environment variable is currently set to: " + hbase_path

# Get the HBase classpath
hbasecp, stderr = subprocess.Popen(hbase_path + "/bin/hbase classpath",
                                  shell=True,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE).communicate()

java_cmd = java +' -cp "' + hbasecp + os.pathsep + phoenix_utils.pherf_conf_path + os.pathsep + phoenix_utils.hbase_conf_dir + os.pathsep + phoenix_utils.phoenix_pherf_jar + \
    '" -Dlog4j.configuration=file:' + \
    os.path.join(phoenix_utils.current_dir, "log4j.properties") + \
    " org.apache.phoenix.pherf.Pherf " + args 

os.execl("/bin/sh", "/bin/sh", "-c", java_cmd)
