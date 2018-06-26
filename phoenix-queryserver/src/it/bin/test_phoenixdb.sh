#/usr/bin/env bash

set -e
set -u
set +x

PY_ENV_PATH=$1
LOCAL_PY=$2
PQS_URL=$3
TABLE_NAME=$4
PRINC=$5
KEYTAB_LOC=$6

conda create -p $PY_ENV_PATH
cd ${PY_ENV_PATH}/bin
. activate
pip install -e file:///${LOCAL_PY}/requests-kerberos
pip install -e file:///${LOCAL_PY}/phoenixdb-module

$KRB5_CONF_FILE=$( mktemp )

cat << KRB5C > ${KRB5_CONF_FILE}
[libdefaults]
 default_realm = EXAMPLE.COM
 ticket_lifetime = 86400
 forwardable = true
 renew_lifetime = 604800

[domain_realm]
 .example.com = EXAMPLE.COM

[relams]
 EXAMPLE.COM = {
  kdc = localhost
 }

KRB5C

export KRB5_CONFIG=$KRB5_CONF_FILE


kinit -kt $KEYTAB_LOC $PRINC


python <<TEST_SCRIPT

import phoenixdb
import phoenixdb.cursor

database_url = 'http://${PQS_URL}/'
conn = phoenixdb.connect(database_url, autocommit=True, auth="SPNEGO")
cursor = conn.cursor()
cursor.execute("CREATE TABLE " + ${TABLE_NAME} + "(pk integer not null primary key)")
cursor.execute("UPSERT INTO " + ${TABLE_NAME} + " values(" + i + ")")
cursor.execute("SELECT * FROM " + ${TABLE_NAME})
print(cursor.fetchall())

TEST_SCRIPT

kdestroy
rm $KRB5_CONF_FILE
rm -rf $PY_ENV_PATH