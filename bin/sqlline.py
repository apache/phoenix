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

from __future__ import print_function
from phoenix_utils import tryDecode
import os
import subprocess
import sys
import phoenix_utils
import atexit

# import argparse
try:
    import argparse
except ImportError:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(os.path.join(current_dir, 'argparse-1.4.0'))
    import argparse

global childProc
childProc = None
def kill_child():
    if childProc is not None:
        childProc.terminate()
        childProc.kill()
        if os.name != 'nt':
            os.system("reset")
atexit.register(kill_child)

phoenix_utils.setPath()

parser = argparse.ArgumentParser(description='Launches the Apache Phoenix Client.')
# Positional argument 'zookeepers' is optional. The PhoenixDriver will automatically populate
# this if it's not provided by the user (so, we want to leave a default value of empty)
parser.add_argument('zookeepers', nargs='?', help='The ZooKeeper quorum string', default='')
# Positional argument 'sqlfile' is optional
parser.add_argument('sqlfile', nargs='?', help='A file of SQL commands to execute', default='')
# Common arguments across sqlline.py and sqlline-thin.py
phoenix_utils.common_sqlline_args(parser)
# Parse the args
args=parser.parse_args()

zookeeper = tryDecode(args.zookeepers)
sqlfile = tryDecode(args.sqlfile)

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = phoenix_utils.hbase_conf_dir

if sqlfile and not os.path.isfile(sqlfile):
    parser.print_help()
    sys.exit(-1)

if sqlfile:
    sqlfile = "--run=" + phoenix_utils.shell_quote([sqlfile])

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
    sys.stderr.write("hbase-env file unknown on platform {}{}".format(os.name, os.linesep))
    sys.exit(-1)

hbase_env = {}
if os.path.isfile(hbase_env_path):
    p = subprocess.Popen(hbase_env_cmd, stdout = subprocess.PIPE)
    for x in p.stdout:
        (k, _, v) = x.decode().partition('=')
        hbase_env[k.strip()] = v.strip()

if 'JAVA_HOME' in hbase_env:
    java_home = hbase_env['JAVA_HOME']

if java_home:
    java = os.path.join(java_home, 'bin', 'java')
else:
    java = 'java'

colorSetting = tryDecode(args.color)
# disable color setting for windows OS
if os.name == 'nt':
    colorSetting = "false"

java_cmd = java + ' $PHOENIX_OPTS ' + \
    ' -cp "' + phoenix_utils.hbase_conf_dir + os.pathsep + phoenix_utils.hadoop_conf + os.pathsep + \
    phoenix_utils.sqlline_with_deps_jar + os.pathsep + phoenix_utils.slf4j_backend_jar + os.pathsep + \
    phoenix_utils.phoenix_client_embedded_jar + \
    '" -Dlog4j.configuration=file:' + os.path.join(phoenix_utils.current_dir, "log4j.properties") + \
    " sqlline.SqlLine -d org.apache.phoenix.jdbc.PhoenixDriver" + \
    " -u jdbc:phoenix:" + phoenix_utils.shell_quote([zookeeper]) + \
    " -n none -p none --color=" + colorSetting + " --fastConnect=" + tryDecode(args.fastconnect) + \
    " --verbose=" + tryDecode(args.verbose) + " --incremental=false --isolation=TRANSACTION_READ_COMMITTED " + sqlfile

os.execl("/bin/sh", "/bin/sh", "-c", java_cmd)
