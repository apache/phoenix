#/usr/bin/env bash

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
pip install -e file:///${LOCAL_PY}/python-phoenixdb

export KRB5_CONFIG=$KRB5_CFG_FILE
cat $KRB5_CONFIG
export KRB5_TRACE=/dev/stdout

#echo "RUNNING KINIT"
kinit -kt $KEYTAB_LOC $PRINC
klist

unset http_proxy
unset https_proxy

echo "Working Directory is ${PWD}"

echo "RUN PYTHON TEST on port $PQS_PORT"
python $PYTHON_SCRIPT $PQS_PORT
