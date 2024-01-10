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

phoenix_utils.setPath()

args = phoenix_utils.shell_quote(sys.argv[1:])

java_cmd = phoenix_utils.java + ' ' + phoenix_utils.jvm_module_flags + ' $PHOENIX_OPTS ' + \
    ' -cp "' + phoenix_utils.hbase_conf_dir + os.pathsep + phoenix_utils.hadoop_conf + \
    os.pathsep + phoenix_utils.slf4j_backend_jar + \
    os.pathsep + phoenix_utils.logging_jar + \
    os.pathsep + phoenix_utils.phoenix_client_embedded_jar + '" -Dlog4j2.configurationFile=file:' + \
    os.path.join(phoenix_utils.current_dir, "log4j2.properties") + \
    " org.apache.phoenix.util.PhoenixRuntime " + args 

print(java_cmd)

os.execl("/bin/sh", "/bin/sh", "-c", java_cmd)
