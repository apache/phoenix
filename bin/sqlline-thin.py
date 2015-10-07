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

url = "localhost:8765"
sqlfile = ""

def usage_and_exit():
    sys.exit("usage: sqlline-thin.py [host[:port]] [sql_file]")

def cleanup_url(url):
    parsed = urlparse.urlparse(url)
    if parsed.scheme == "":
        url = "http://" + url
        parsed = urlparse.urlparse(url)
    if ":" not in parsed.netloc:
        url = url + ":8765"
    return url


if len(sys.argv) == 1:
    pass
elif len(sys.argv) == 2:
    if os.path.isfile(sys.argv[1]):
        sqlfile = sys.argv[1]
    else:
        url = sys.argv[1]
elif len(sys.argv) == 3:
    url = sys.argv[1]
    sqlfile = sys.argv[2]
else:
    usage_and_exit()

url = cleanup_url(url)

if sqlfile != "":
    sqlfile = "--run=" + sqlfile

colorSetting = "true"
# disable color setting for windows OS
if os.name == 'nt':
    colorSetting = "false"

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = os.getenv('HBASE_CONF_DIR', phoenix_utils.current_dir)

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

java_cmd = java + ' -cp "' + phoenix_utils.hbase_conf_dir + os.pathsep + phoenix_utils.phoenix_thin_client_jar + \
    '" -Dlog4j.configuration=file:' + \
    os.path.join(phoenix_utils.current_dir, "log4j.properties") + \
    " sqlline.SqlLine -d org.apache.phoenix.queryserver.client.Driver " + \
    " -u jdbc:phoenix:thin:url=" + url + \
    " -n none -p none --color=" + colorSetting + " --fastConnect=false --verbose=true " + \
    " --isolation=TRANSACTION_READ_COMMITTED " + sqlfile

exitcode = subprocess.call(java_cmd, shell=True)
sys.exit(exitcode)
