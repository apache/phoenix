#/usr/bin/env bash

set -u
set -x
set -e

function cleanup {
    set +e
    kdestroy
    rm $KRB5_CONFIG
    rm -rf $PY_ENV_PATH
}

trap cleanup EXIT

echo "LAUNCHING SCRIPT"

LOCAL_PY=$1
TABLE_NAME=$2
PRINC=$3
KEYTAB_LOC=$4

#export http_proxy=http://proxy.bloomberg.com:81
#export https_proxy=http://proxy.bloomberg.com:81
#export no_proxy=localhost,127.*,[::1],repo.dev.bloomberg.com,artifactory.bdns.bloomberg.com,artprod.dev.bloomberg.com

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

export KRB5_CONFIG=/Users/lbronshtein/DEV/phoenix/phoenix-queryserver/src/it/resources/krb5.conf

echo "RUNNING KINIT"
kinit -kt $KEYTAB_LOC $PRINC

echo "RUN PYTHON TEST"
python /Users/lbronshtein/DEV/phoenix/phoenix-queryserver/src/it/bin/test_phoenixdb.py

