#/usr/bin/env bash

set -u
set -x
set -e

function cleanup {
    set +e
    set +u
    kdestroy
    rm $KRB5_TMP
    rm -rf $PY_ENV_PATH
}

trap cleanup EXIT

echo "LAUNCHING SCRIPT"

LOCAL_PY=$1
PRINC=$2
KEYTAB_LOC=$3
KRB5_CFG_FILE=$4
PQS_PORT=$5

PY_ENV_PATH=$( mktemp -d )
conda create -y -p $PY_ENV_PATH
cd ${PY_ENV_PATH}/bin

# conda activate does stuff with unbound variables :(
set +u
. activate ""

set -u
echo "INSTALLING COMPONENTS"
pip install -e file:///${LOCAL_PY}/requests-kerberos
pip install -e file:///${LOCAL_PY}/phoenixdb-module

export KRB5_CONFIG=$KRB5_CFG_FILE
cat $KRB5_CONFIG
export KRB5_TRACE=/dev/stdout

echo "RUNNING KINIT"
kinit -kt $KEYTAB_LOC $PRINC
klist

unset http_proxy
unset https_proxy

echo "RUN PYTHON TEST"
python /Users/lbronshtein/DEV/phoenix/phoenix-queryserver/src/it/bin/test_phoenixdb.py $PQS_PORT

