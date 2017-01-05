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
import atexit
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
# Positional argument 'zookeepers' is optional
parser.add_argument('zookeepers', nargs='?', help='The ZooKeeper quorum string', default='localhost:2181:/hbase')
# Positional argument 'sqlfile' is optional
parser.add_argument('sqlfile', nargs='?', help='A file of SQL commands to execute', default='')
# Common arguments across sqlline.py and sqlline-thin.py
phoenix_utils.common_sqlline_args(parser)
# Parse the args
args=parser.parse_args()

zookeeper = args.zookeepers
sqlfile = args.sqlfile

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = os.getenv('HBASE_CONF_DIR', phoenix_utils.current_dir)

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

colorSetting = args.color
# disable color setting for windows OS
if os.name == 'nt':
    colorSetting = "false"

java_cmd = java + ' $PHOENIX_OPTS ' + \
    ' -cp "' + hbase_config_path + os.pathsep + phoenix_utils.hbase_conf_dir + os.pathsep + phoenix_utils.phoenix_client_jar + \
    os.pathsep + phoenix_utils.hadoop_common_jar + os.pathsep + phoenix_utils.hadoop_hdfs_jar + \
    os.pathsep + phoenix_utils.hadoop_conf + os.pathsep + phoenix_utils.hadoop_classpath + '" -Dlog4j.configuration=file:' + \
    os.path.join(phoenix_utils.current_dir, "log4j.properties") + \
    " sqlline.SqlLine -d org.apache.phoenix.jdbc.PhoenixDriver" + \
    " -u jdbc:phoenix:" + phoenix_utils.shell_quote([zookeeper]) + \
    " -n none -p none --color=" + colorSetting + " --fastConnect=" + args.fastconnect + \
    " --verbose=" + args.verbose + " --incremental=false --isolation=TRANSACTION_READ_COMMITTED " + sqlfile

childProc = subprocess.Popen(java_cmd, shell=True)
#Wait for child process exit
(output, error) = childProc.communicate()
returncode = childProc.returncode
childProc = None
# Propagate Java return code to this script
if returncode is not None:
    sys.exit(returncode)
