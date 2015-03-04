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
import sys
import fnmatch

def find(pattern, classPaths):
    paths = classPaths.split(os.pathsep)

    # for each class path
    for path in paths:
        # remove * if it's at the end of path
        if ((path is not None) and (len(path) > 0) and (path[-1] == '*')) :
            path = path[:-1]
    
        for root, dirs, files in os.walk(path):
            for name in files:
                if fnmatch.fnmatch(name, pattern):
                    return os.path.join(root, name)
                
    return ""

def findFileInPathWithoutRecursion(pattern, path):

    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f))]
    for name in files:
        if fnmatch.fnmatch(name, pattern):
            return os.path.join(path, name)

    return ""

def setPath():
 PHOENIX_CLIENT_JAR_PATTERN = "phoenix-*-client*.jar"
 PHOENIX_TESTS_JAR_PATTERN = "phoenix-core-*-tests*.jar"
 global current_dir
 current_dir = os.path.dirname(os.path.abspath(__file__))
 global phoenix_jar_path
 phoenix_jar_path = os.path.join(current_dir, "..", "phoenix-assembly", "target","*")
 global phoenix_client_jar
 phoenix_client_jar = find("phoenix-*-client.jar", phoenix_jar_path)
 global phoenix_test_jar_path
 phoenix_test_jar_path = os.path.join(current_dir, "..", "phoenix-core", "target","*")
 global hbase_conf_path
 hbase_conf_path = os.getenv('HBASE_CONF_PATH','.')
 global testjar
 testjar = find(PHOENIX_TESTS_JAR_PATTERN, phoenix_test_jar_path)

 if phoenix_client_jar == "":
     phoenix_client_jar = findFileInPathWithoutRecursion(PHOENIX_CLIENT_JAR_PATTERN, os.path.join(current_dir, ".."))

 if testjar == "":
     testjar = findFileInPathWithoutRecursion(PHOENIX_TESTS_JAR_PATTERN, os.path.join(current_dir, ".."))

 # Backward support old env variable PHOENIX_LIB_DIR replaced by PHOENIX_CLASS_PATH
 global phoenix_class_path
 phoenix_class_path = os.getenv('PHOENIX_LIB_DIR','')
 if phoenix_class_path == "":
     phoenix_class_path = os.getenv('PHOENIX_CLASS_PATH','')

 if phoenix_client_jar == "":
     phoenix_client_jar = find(PHOENIX_CLIENT_JAR_PATTERN, phoenix_class_path)

 if testjar == "":
     testjar = find(PHOENIX_TESTS_JAR_PATTERN, phoenix_class_path)

 return ""
