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
import urlparse
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

parser = argparse.ArgumentParser(description='Launches the Apache Phoenix Thin Client.')
# Positional argument "url" is optional
parser.add_argument('url', nargs='?', help='The URL to the Phoenix Query Server.', default='http://localhost:8765')
# Positional argument "sqlfile" is optional
parser.add_argument('sqlfile', nargs='?', help='A file of SQL commands to execute.', default='')
# Avatica wire authentication
parser.add_argument('-a', '--authentication', help='Mechanism for HTTP authentication.', choices=('SPNEGO', 'BASIC', 'DIGEST', 'NONE'), default='')
# Avatica wire serialization
parser.add_argument('-s', '--serialization', help='Serialization type for HTTP API.', choices=('PROTOBUF', 'JSON'), default=None)
# Avatica authentication
parser.add_argument('-au', '--auth-user', help='Username for HTTP authentication.')
parser.add_argument('-ap', '--auth-password', help='Password for HTTP authentication.')
# Common arguments across sqlline.py and sqlline-thin.py
phoenix_utils.common_sqlline_args(parser)
# Parse the args
args=parser.parse_args()

phoenix_utils.setPath()

url = args.url
sqlfile = args.sqlfile
serialization_key = 'phoenix.queryserver.serialization'

def cleanup_url(url):
    parsed = urlparse.urlparse(url)
    if parsed.scheme == "":
        url = "http://" + url
        parsed = urlparse.urlparse(url)
    if ":" not in parsed.netloc:
        url = url + ":8765"
    return url

def get_serialization():
    default_serialization='PROTOBUF'
    env=os.environ.copy()
    if os.name == 'posix':
      hbase_exec_name = 'hbase'
    elif os.name == 'nt':
      hbase_exec_name = 'hbase.cmd'
    else:
      print 'Unknown platform "%s", defaulting to HBase executable of "hbase"' % os.name
      hbase_exec_name = 'hbase'

    hbase_cmd = phoenix_utils.which(hbase_exec_name)
    if hbase_cmd is None:
        print 'Failed to find hbase executable on PATH, defaulting serialization to %s.' % default_serialization
        return default_serialization

    env['HBASE_CONF_DIR'] = phoenix_utils.hbase_conf_dir
    proc = subprocess.Popen([hbase_cmd, 'org.apache.hadoop.hbase.util.HBaseConfTool', serialization_key],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = proc.communicate()
    if proc.returncode != 0:
        print 'Failed to extract serialization from hbase-site.xml, defaulting to %s.' % default_serialization
        return default_serialization
    # Don't expect this to happen, but give a default value just in case
    if stdout is None:
        return default_serialization

    stdout = stdout.strip()
    if stdout == 'null':
        return default_serialization
    return stdout

url = cleanup_url(url)

if sqlfile != "":
    sqlfile = "--run=" + sqlfile

colorSetting = args.color
# disable color setting for windows OS
if os.name == 'nt':
    colorSetting = "false"

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = os.getenv('HBASE_CONF_DIR', phoenix_utils.current_dir)

serialization = args.serialization if args.serialization else get_serialization()

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

jdbc_url = 'jdbc:phoenix:thin:url=' + url + ';serialization=' + serialization
if args.authentication:
    jdbc_url += ';authentication=' + args.authentication
if args.auth_user:
    jdbc_url += ';avatica_user=' + args.auth_user
if args.auth_password:
    jdbc_url += ';avatica_password=' + args.auth_password

java_cmd = java + ' $PHOENIX_OPTS ' + \
    ' -cp "' + phoenix_utils.hbase_conf_dir + os.pathsep + phoenix_utils.phoenix_thin_client_jar + \
    os.pathsep + phoenix_utils.hadoop_conf + os.pathsep + phoenix_utils.hadoop_classpath + '" -Dlog4j.configuration=file:' + \
    os.path.join(phoenix_utils.current_dir, "log4j.properties") + \
    " org.apache.phoenix.queryserver.client.SqllineWrapper -d org.apache.phoenix.queryserver.client.Driver " + \
    ' -u "' + jdbc_url + '"' + " -n none -p none " + \
    " --color=" + colorSetting + " --fastConnect=" + args.fastconnect + " --verbose=" + args.verbose + \
    " --incremental=false --isolation=TRANSACTION_READ_COMMITTED " + sqlfile

exitcode = subprocess.call(java_cmd, shell=True)
sys.exit(exitcode)
