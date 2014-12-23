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

phoenix_utils.setPath()

base_dir = os.path.join(phoenix_utils.current_dir, '..')
phoenix_target_dir = os.path.join(base_dir, 'phoenix-core', 'target')

cp_file_path = os.path.join(phoenix_target_dir, 'cached_classpath.txt')


if not os.path.exists(cp_file_path):
    sys.err.write("cached_classpath.txt is not present under "
                + "phoenix-core/target, please rebuild the project first")
    sys.exit(1)

logging_config = os.path.join(base_dir, 'bin', 'sandbox-log4j.properties')

cp_components = [phoenix_target_dir + "/*"]
with open(cp_file_path, 'rb') as cp_file:
    cp_components.append(cp_file.read())

java_cmd = ("java -Dlog4j.configuration=file:%s " +
                "-cp %s org.apache.phoenix.Sandbox") % (
                            logging_config, ":".join(cp_components))

proc = subprocess.Popen(java_cmd, shell=True)
try:
    proc.wait()
except KeyboardInterrupt:
    print "Shutting down sandbox..."
    proc.terminate()

proc.wait()

print "Sandbox is stopped"
