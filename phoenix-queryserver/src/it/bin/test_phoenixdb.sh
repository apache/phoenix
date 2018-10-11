#!/usr/bin/env bash
#
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

set -u
set -x
set -e

function cleanup {
    # Capture last command status
    RCODE=$?
    set +e
    set +u
    kdestroy
    rm -rf $PY_ENV_PATH
    exit $RCODE
}

trap cleanup EXIT

echo "LAUNCHING SCRIPT"

LOCAL_PY=$1
PRINC=$2
KEYTAB_LOC=$3
KRB5_CFG_FILE=$4
PQS_PORT=$5
PYTHON_SCRIPT=$6

PY_ENV_PATH=$( mktemp -d )

virtualenv $PY_ENV_PATH

pushd ${PY_ENV_PATH}/bin

# conda activate does stuff with unbound variables :(
set +u
. activate ""

popd

set -u
echo "INSTALLING COMPONENTS"
pip install -e file:///${LOCAL_PY}/requests-kerberos
pip install -e file:///${LOCAL_PY}/phoenixdb

export KRB5_CONFIG=$KRB5_CFG_FILE
cat $KRB5_CONFIG
export KRB5_TRACE=/dev/stdout

echo "RUNNING KINIT"
kinit -kt $KEYTAB_LOC $PRINC
klist

unset http_proxy
unset https_proxy

echo "Working Directory is ${PWD}"

echo "RUN PYTHON TEST on port $PQS_PORT"
python $PYTHON_SCRIPT $PQS_PORT
