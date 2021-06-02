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

#
# Script to handle launching the trace server process.
#
# usage: traceserver.py [start|stop]
#

from __future__ import print_function
from phoenix_utils import tryDecode
import datetime
import getpass
import os
import os.path
import signal
import subprocess
import sys
import tempfile

try:
    import daemon
    daemon_supported = True
except ImportError:
    # daemon script not supported on some platforms (windows?)
    daemon_supported = False

import phoenix_utils

phoenix_utils.setPath()

command = None
args = sys.argv

if len(args) > 1:
    if tryDecode(args[1]) == 'start':
        command = 'start'
    elif tryDecode(args[1]) == 'stop':
        command = 'stop'
if command:
    args = args[2:]

if os.name == 'nt':
    args = subprocess.list2cmdline(args[1:])
else:
    import pipes    # pipes module isn't available on Windows
    args = " ".join([pipes.quote(tryDecode(v)) for v in args[1:]])

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = phoenix_utils.hbase_conf_dir

# default paths ## TODO: add windows support
java_home = os.getenv('JAVA_HOME')
hbase_pid_dir = os.path.join(tempfile.gettempdir(), 'phoenix')
phoenix_log_dir = os.path.join(tempfile.gettempdir(), 'phoenix')
phoenix_file_basename = 'phoenix-%s-traceserver' % getpass.getuser()
phoenix_log_file = '%s.log' % phoenix_file_basename
phoenix_out_file = '%s.out' % phoenix_file_basename
phoenix_pid_file = '%s.pid' % phoenix_file_basename
opts = os.getenv('PHOENIX_TRACESERVER_OPTS', '')

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
        (k, _, v) = tryDecode(x).partition('=')
        hbase_env[k.strip()] = v.strip()

if 'JAVA_HOME' in hbase_env:
    java_home = hbase_env['JAVA_HOME']
if 'HBASE_PID_DIR' in hbase_env:
    hbase_pid_dir = hbase_env['HBASE_PID_DIR']
if 'HBASE_LOG_DIR' in hbase_env:
    phoenix_log_dir = hbase_env['HBASE_LOG_DIR']
if 'PHOENIX_TRACESERVER_OPTS' in hbase_env:
    opts = hbase_env['PHOENIX_TRACESERVER_OPTS']

log_file_path = os.path.join(phoenix_log_dir, phoenix_log_file)
out_file_path = os.path.join(phoenix_log_dir, phoenix_out_file)
pid_file_path = os.path.join(hbase_pid_dir, phoenix_pid_file)

if java_home:
    java = os.path.join(java_home, 'bin', 'java')
else:
    java = 'java'

#    " -Xdebug -Xrunjdwp:transport=dt_socket,address=5005,server=y,suspend=n " + \
#    " -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true" + \
java_cmd = '%(java)s  ' + \
    '-cp ' + hbase_config_path + os.pathsep + phoenix_utils.hadoop_conf + os.pathsep + \
    phoenix_utils.phoenix_traceserver_jar + os.pathsep + phoenix_utils.slf4j_backend_jar + os.pathsep + \
    phoenix_utils.phoenix_client_embedded_jar + os.pathsep + phoenix_utils.phoenix_queryserver_jar + \
    " -Dproc_phoenixtraceserver" + \
    " -Dlog4j.configuration=file:" + os.path.join(phoenix_utils.current_dir, "log4j.properties") + \
    " -Dpsql.root.logger=%(root_logger)s" + \
    " -Dpsql.log.dir=%(log_dir)s" + \
    " -Dpsql.log.file=%(log_file)s" + \
    " " + opts + \
    " org.apache.phoenix.tracingwebapp.http.Main " + args

if command == 'start':
    if not daemon_supported:
        sys.stderr.write("daemon mode not supported on this platform{}".format(os.linesep))
        sys.exit(-1)

    # run in the background
    d = os.path.dirname(out_file_path)
    if not os.path.exists(d):
        os.makedirs(d)
    with open(out_file_path, 'a+') as out:
        context = daemon.DaemonContext(
            pidfile = daemon.PidFile(pid_file_path, 'Trace Server already running, PID file found: %s' % pid_file_path),
            stdout = out,
            stderr = out,
        )
        print('starting Trace Server, logging to %s' % log_file_path)
        with context:
            # this block is the main() for the forked daemon process
            child = None
            cmd = java_cmd % {'java': java, 'root_logger': 'INFO,DRFA', 'log_dir': phoenix_log_dir, 'log_file': phoenix_log_file}

            # notify the child when we're killed
            def handler(signum, frame):
                if child:
                    child.send_signal(signum)
                sys.exit(0)
            signal.signal(signal.SIGTERM, handler)

            print('%s launching %s' % (datetime.datetime.now(), cmd))
            child = subprocess.Popen(cmd.split())
            sys.exit(child.wait())

elif command == 'stop':
    if not daemon_supported:
        sys.stderr.write("daemon mode not supported on this platform{}".format(os.linesep))
        sys.exit(-1)

    if not os.path.exists(pid_file_path):
        sys.stderr.write("no Trace Server to stop because PID file not found, {}{}"
                         .format(pid_file_path, os.linesep))
        sys.exit(0)

    if not os.path.isfile(pid_file_path):
        sys.stderr.write("PID path exists but is not a file! {}{}"
                         .format(pid_file_path, os.linesep))
        sys.exit(1)

    pid = None
    with open(pid_file_path, 'r') as p:
        pid = int(p.read())
    if not pid:
        sys.exit("cannot read PID file, %s" % pid_file_path)

    print("stopping Trace Server pid %s" % pid)
    with open(out_file_path, 'a+') as out:
        out.write("%s terminating Trace Server%s" % (datetime.datetime.now(), os.linesep))
    os.kill(pid, signal.SIGTERM)

else:
    # run in the foreground using defaults from log4j.properties
    cmd = java_cmd % {'java': java, 'root_logger': 'INFO,console', 'log_dir': '.', 'log_file': 'psql.log'}
    splitcmd = cmd.split()
    os.execvp(splitcmd[0], splitcmd)
