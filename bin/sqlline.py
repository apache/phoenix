#!/usr/bin/env python
"""sqlline.py"""
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
import os
import phoenix_utils
import subprocess
import signal
import sys

phoenix_utils.setPath()


def print_usage():
    print('''Usage: sqlline.py [zookeeper] [optional_sql_file]
Example:
    1. sqlline.py
    2. sqlline.py localhost:2181:/hbase
    3. sqlline.py localhost:2181:/hbase ../examples/stock_symbol.sql
    4. sqlline.py ../examples/stock_symbol.sql
''')
    sys.exit(-1)


def main():
    sqlfile = ""
    zookeeper = ""

    if len(sys.argv) > 3:
        print_usage()

    if len(sys.argv) == 2:
        if os.path.isfile(sys.argv[1]):
            sqlfile = sys.argv[1]
        else:
            zookeeper = sys.argv[1]

    if len(sys.argv) == 3:
        if os.path.isfile(sys.argv[1]):
            print_usage()
        else:
            zookeeper = sys.argv[1]
            sqlfile = sys.argv[2]

    if sqlfile:
        sqlfile = "--run=" + phoenix_utils.shell_quote([sqlfile])

    java_home = os.getenv('JAVA_HOME')
    # HBase configuration folder path (where hbase-site.xml reside) for
    # HBase/Phoenix client side property override
    hbase_config_path = os.getenv('HBASE_CONF_DIR', phoenix_utils.current_dir)
    # load hbase-env.??? to extract JAVA_HOME, HBASE_PID_DIR, HBASE_LOG_DIR
    hbase_env_path = None
    hbase_env_cmd = None
    if os.name == 'posix':
        hbase_env_path = os.path.join(hbase_config_path, 'hbase-env.sh')
        hbase_env_cmd = ['bash', '-c', 'source %s && env' % hbase_env_path]
    elif os.name == 'nt':
        hbase_env_path = os.path.join(hbase_config_path, 'hbase-env.cmd')
        hbase_env_cmd = ['cmd.exe', '/c', 'call %s & set' % hbase_env_path]
    if not hbase_env_path or not hbase_env_cmd:
        print("hbase-env script unknown on platform %s" % (os.name, ),
              file=sys.stderr)
        sys.exit(-1)

    hbase_env = {}
    if os.path.isfile(hbase_env_path):
        p = subprocess.Popen(hbase_env_cmd, stdout=subprocess.PIPE)
        for x in p.stdout:
            (k, _, v) = x.partition('=')
            hbase_env[k.strip()] = v.strip()

    if 'JAVA_HOME' in hbase_env:
        java_home = hbase_env['JAVA_HOME']

    java = os.path.join(java_home, 'bin', 'java') if java_home else 'java'
    # disable color setting for windows OS
    color_setting = 'false' if os.name == 'nt' else 'true'
    java = os.path.join(java_home, 'bin', 'java') if java_home else 'java'
    conf_dir = os.path.join(phoenix_utils.hbase_conf_dir,
                            phoenix_utils.phoenix_thin_client_jar)
    class_paths = os.pathsep.join(
        [hbase_config_path, conf_dir, phoenix_utils.hadoop_conf,
         phoenix_utils.hadoop_classpath])
    log4j_props = os.path.join(phoenix_utils.current_dir, "log4j.properties")
    sqlopt = '--run=' + sqlfile if sqlfile else sqlfile

    java_cmd = """{java} $PHOENIX_OPTS \
-cp "{cp}" \
-Dlog4j.configuration=file:{l4j} \
sqlline.SqlLine \
-d org.apache.phoenix.jdbc.PhoenixDriver \
-u jdbc:phoenix:{zk} \
-n none \
-p none \
--color={color} \
--fastConnect=false \
--verbose=true \
--incremental=false \
--isolation=TRANSACTION_READ_COMMITTED {sql}
""".format(color=color_setting,
           cp=class_paths,
           java=java,
           l4j=log4j_props,
           sql=sqlopt,
           zk=phoenix_utils.shell_quote([zookeeper]))
    proc = subprocess.Popen(java_cmd, stdout=subprocess.PIPE,
                            shell=True, preexec_fn=os.setsid)
    (output, error) = proc.communicate()
    returncode = proc.returncode
    if returncode != 0:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    sys.exit(returncode)


if __name__ == '__main__':
    main()
