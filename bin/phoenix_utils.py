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
import os
import fnmatch
import re
import subprocess

def find(pattern, classPaths):
    paths = classPaths.split(os.pathsep)

    # for each class path
    for path in paths:
        # remove * if it's at the end of path
        if ((path is not None) and (len(path) > 0) and (path[-1] == '*')) :
            path = path[:-1]
    
        for root, dirs, files in os.walk(path):
            # sort the file names so *-client always precedes *-thin-client
            files.sort()
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    return os.path.join(root, name)
                
    return ""

def tryDecode(input):
    """ Python 2/3 compatibility hack
    """
    try:
        return input.decode()
    except:
        return input

def findFileInPathWithoutRecursion(pattern, path):
    if not os.path.exists(path):
        return ""
    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f))]
    # sort the file names so *-client always precedes *-thin-client
    files.sort()
    for name in files:
        if fnmatch.fnmatch(name, pattern):
            return os.path.join(path, name)

    return ""

def which(command):
    for path in os.environ["PATH"].split(os.pathsep):
        if os.path.exists(os.path.join(path, command)):
            return os.path.join(path, command)
    return None

def findClasspath(command_name):
    command_path = which(command_name)
    if command_path is None:
        # We don't have this command, so we can't get its classpath
        return ''
    command = "%s%s" %(command_path, ' classpath')
    return tryDecode(subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read())

def setPath():
    PHOENIX_CLIENT_EMBEDDED_JAR_PATTERN = "phoenix-client-embedded-hbase-*[!s].jar"
    PHOENIX_TRACESERVER_JAR_PATTERN = "phoenix-tracing-webapp-*-runnable.jar"
    PHOENIX_TESTS_JAR_PATTERN = "phoenix-core-*-tests*.jar"
    PHOENIX_PHERF_JAR_PATTERN = "phoenix-pherf-*[!s].jar"
    SLF4J_BACKEND_JAR_PATTERN = "log4j-slf4j*.jar"
    LOGGING_JAR_PATTERN = "log4j-core*.jar"
    LOGGING_JAR_PATTERN2 = "log4j-api*.jar"
    LOGGING_JAR_PATTERN3 = "log4j-1.2-api*.jar"
    SQLLINE_WITH_DEPS_PATTERN = "sqlline-*-jar-with-dependencies.jar"


    OVERRIDE_SLF4J_BACKEND = "OVERRIDE_SLF4J_BACKEND_JAR_LOCATION"
    OVERRIDE_LOGGING = "OVERRIDE_LOGGING_JAR_LOCATION"
    OVERRIDE_SQLLINE = "OVERRIDE_SQLLINE_JAR_LOCATION"

    # Backward support old env variable PHOENIX_LIB_DIR replaced by PHOENIX_CLASS_PATH
    global phoenix_class_path
    phoenix_class_path = os.getenv('PHOENIX_LIB_DIR','')
    if phoenix_class_path == "":
        phoenix_class_path = os.getenv('PHOENIX_CLASS_PATH','')

    global hbase_conf_dir
    # if HBASE_CONF_DIR set explicitly, use that
    hbase_conf_dir = os.getenv('HBASE_CONF_DIR', os.getenv('HBASE_CONF_PATH'))
    if not hbase_conf_dir:
        # else fall back to HBASE_HOME
        if os.getenv('HBASE_HOME'):
            hbase_conf_dir = os.path.join(os.getenv('HBASE_HOME'), "conf")
        elif os.name == 'posix':
            # default to the bigtop configuration dir
            hbase_conf_dir = '/etc/hbase/conf'
        else:
            # Try to provide something valid
            hbase_conf_dir = '.'

    global current_dir
    current_dir = os.path.dirname(os.path.abspath(__file__))

    global pherf_conf_path
    pherf_conf_path = os.path.join(current_dir, "config")
    pherf_properties_file = find("pherf.properties", pherf_conf_path)
    if pherf_properties_file == "":
        pherf_conf_path = os.path.join(current_dir, "..", "phoenix-pherf", "config")

    global phoenix_embedded_jar_path
    phoenix_embedded_jar_path = os.path.join(current_dir, "..", "phoenix-client-parent" , "phoenix-client-embedded", "target","*")

    global phoenix_client_embedded_jar
    phoenix_client_embedded_jar = find(PHOENIX_CLIENT_EMBEDDED_JAR_PATTERN, phoenix_embedded_jar_path)
    if phoenix_client_embedded_jar == "":
        phoenix_client_embedded_jar = findFileInPathWithoutRecursion(PHOENIX_CLIENT_EMBEDDED_JAR_PATTERN, os.path.join(current_dir, ".."))
    if phoenix_client_embedded_jar == "":
        phoenix_client_embedded_jar = find(PHOENIX_CLIENT_EMBEDDED_JAR_PATTERN, phoenix_class_path)

    global phoenix_test_jar_path
    phoenix_test_jar_path = os.path.join(current_dir, "..", "phoenix-core", "target","*")

    global hadoop_conf
    hadoop_conf = os.getenv('HADOOP_CONF_DIR', None)
    if not hadoop_conf:
        if os.name == 'posix':
            # Try to provide a sane configuration directory for Hadoop if not otherwise provided.
            # If there's no jaas file specified by the caller, this is necessary when Kerberos is enabled.
            hadoop_conf = '/etc/hadoop/conf'
        else:
            # Try to provide something valid..
            hadoop_conf = '.'

    global hadoop_classpath
    if (os.name != 'nt'):
        hadoop_classpath = findClasspath('hadoop').rstrip()
    else:
        hadoop_classpath = os.getenv('HADOOP_CLASSPATH', '').rstrip()

    global testjar
    testjar = find(PHOENIX_TESTS_JAR_PATTERN, phoenix_test_jar_path)
    if testjar == "":
        testjar = findFileInPathWithoutRecursion(PHOENIX_TESTS_JAR_PATTERN, os.path.join(current_dir, "..", 'lib'))
    if testjar == "":
        testjar = find(PHOENIX_TESTS_JAR_PATTERN, phoenix_class_path)

    global phoenix_traceserver_jar
    phoenix_traceserver_jar = find(PHOENIX_TRACESERVER_JAR_PATTERN, os.path.join(current_dir, "..", "phoenix-tracing-webapp", "target", "*"))
    if phoenix_traceserver_jar == "":
        phoenix_traceserver_jar = findFileInPathWithoutRecursion(PHOENIX_TRACESERVER_JAR_PATTERN, os.path.join(current_dir, "..", "lib"))
    if phoenix_traceserver_jar == "":
        phoenix_traceserver_jar = findFileInPathWithoutRecursion(PHOENIX_TRACESERVER_JAR_PATTERN, os.path.join(current_dir, ".."))

    global phoenix_pherf_jar
    phoenix_pherf_jar = find(PHOENIX_PHERF_JAR_PATTERN, os.path.join(current_dir, "..", "phoenix-pherf", "target", "*"))
    if phoenix_pherf_jar == "":
        phoenix_pherf_jar = findFileInPathWithoutRecursion(PHOENIX_PHERF_JAR_PATTERN, os.path.join(current_dir, "..", "lib"))
    if phoenix_pherf_jar == "":
        phoenix_pherf_jar = findFileInPathWithoutRecursion(PHOENIX_PHERF_JAR_PATTERN, os.path.join(current_dir, ".."))

    global sqlline_with_deps_jar
    sqlline_with_deps_jar = os.environ.get(OVERRIDE_SQLLINE)
    if sqlline_with_deps_jar is None or sqlline_with_deps_jar == "":
        sqlline_with_deps_jar = findFileInPathWithoutRecursion(SQLLINE_WITH_DEPS_PATTERN, os.path.join(current_dir, "..","lib"))

    global slf4j_backend_jar
    slf4j_backend_jar = os.environ.get(OVERRIDE_SLF4J_BACKEND)
    if slf4j_backend_jar is None or slf4j_backend_jar == "":
        slf4j_backend_jar = findFileInPathWithoutRecursion(SLF4J_BACKEND_JAR_PATTERN, os.path.join(current_dir, "..","lib"))

    global logging_jar
    logging_jar = os.environ.get(OVERRIDE_LOGGING)
    if logging_jar is None or logging_jar == "":
        logging_jar = findFileInPathWithoutRecursion(LOGGING_JAR_PATTERN, os.path.join(current_dir, "..","lib"))
        logging_jar += ":"+findFileInPathWithoutRecursion(LOGGING_JAR_PATTERN2, os.path.join(current_dir, "..","lib"))
        logging_jar += ":"+findFileInPathWithoutRecursion(LOGGING_JAR_PATTERN3, os.path.join(current_dir, "..","lib"))

    __set_java_home()
    __set_jvm_flags()
    return ""

def shell_quote(args):
    """
    Return the platform specific shell quoted string. Handles Windows and *nix platforms.

    :param args: array of shell arguments
    :return: shell quoted string
    """
    if os.name == 'nt':
        import subprocess
        return subprocess.list2cmdline(args)
    else:
        # pipes module isn't available on Windows
        import pipes
        return " ".join([pipes.quote(tryDecode(v)) for v in args])


def __set_java_home():
    global java_home
    global java
    java_home = os.getenv('JAVA_HOME')
    java = 'java'

    # HBase configuration folder path (where hbase-site.xml reside) for
    # HBase/Phoenix client side property override
    hbase_config_path = hbase_conf_dir

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

    if java_home:
        java = os.path.join(java_home, 'bin', 'java')

    return ""


def __set_jvm_flags():
    global jvm_module_flags
    jvm_module_flags = ""
    # This should be ASCII
    version_output = subprocess.check_output([java, "-version"], stderr=subprocess.STDOUT).decode()
    version_output = tryDecode(version_output)
    m = re.search(r'version\s"(\d+)\.(\d+)', version_output)
    if (m is None):
        # Could not find version
        return ""
    major = m.group(1)
    minor = m.group(2)
    if (major is None or minor is None):
        #Could not identify version
        return ""
    if (minor == "1"):
        major = minor
    if (int(major) >= 11):
        # Copied from hbase startup script
        jvm_module_flags = "-Dorg.apache.hbase.thirdparty.io.netty.tryReflectionSetAccessible=true \
--add-modules jdk.unsupported \
--add-opens java.base/java.nio=ALL-UNNAMED \
--add-opens java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens java.base/java.lang=ALL-UNNAMED \
--add-opens java.base/jdk.internal.ref=ALL-UNNAMED \
--add-opens java.base/java.lang.reflect=ALL-UNNAMED \
--add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
--add-exports java.security.jgss/sun.security.krb5=ALL-UNNAMED \
--add-exports java.base/sun.net.dns=ALL-UNNAMED \
--add-exports java.base/sun.net.util=ALL-UNNAMED"
    return ""

def common_sqlline_args(parser):
    parser.add_argument('-v', '--verbose', help='Verbosity on sqlline.', action="store_true")
    parser.add_argument('-c', '--color', help='Color setting for sqlline.', action="store_true")
    parser.add_argument('-fc', '--fastconnect',
                        help='Fetch all schemas on initial connection',
                        action="store_true")

if __name__ == "__main__":
    setPath()
    print("phoenix_class_path:", phoenix_class_path)
    print("hbase_conf_dir:", hbase_conf_dir)
    print("current_dir:", current_dir)
    print("phoenix_embedded_jar_path:", phoenix_embedded_jar_path)
    print("phoenix_client_embedded_jar:", phoenix_client_embedded_jar)
    print("phoenix_test_jar_path:", phoenix_test_jar_path)
    print("testjar:", testjar)
    print("hadoop_classpath:", hadoop_classpath)
    print("sqlline_with_deps_jar:", sqlline_with_deps_jar)
    print("slf4j_backend_jar:", slf4j_backend_jar)
    print("logging_jar:", logging_jar)
    print("java_home:", java_home)
    print("java:", java)
    print("jvm_module_flags:", jvm_module_flags)
