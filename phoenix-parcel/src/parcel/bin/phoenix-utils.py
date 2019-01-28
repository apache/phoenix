#!/bin/bash
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

  # Reference: http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
  SOURCE="${BASH_SOURCE[0]}"
  BIN_DIR="$( dirname "$SOURCE" )"
  while [ -h "$SOURCE" ]
  do
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  done
  BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  LIB_DIR=$BIN_DIR/../lib


# Autodetect JAVA_HOME if not defined
. $LIB_DIR/../../CDH/lib/bigtop-utils/bigtop-detect-javahome

export PATH=$JAVA_HOME/jre/bin:$PATH
exec $LIB_DIR/phoenix/bin/phoenix_utils.py "$@"
