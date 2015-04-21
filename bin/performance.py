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

def queryex(description, statement):
    global statements
    print "Query # %s - %s" % (description, statement)
    statements = statements + statement

def delfile(filename):
    if os.path.exists(filename):
        os.remove(filename)

def usage():
    print "Performance script arguments not specified. Usage: performance.sh \
<zookeeper> <row count>"
    print "Example: performance.sh localhost 100000"


def createFileWithContent(filename, content):
    fo = open(filename, "w+")
    fo.write(content)
    fo.close()

if len(sys.argv) < 3:
    usage()
    sys.exit()

# command line arguments
zookeeper = sys.argv[1]
rowcount = sys.argv[2]
table = "PERFORMANCE_" + sys.argv[2]

# helper variable and functions
ddl = "ddl.sql"
data = "data.csv"
qry = "query.sql"
statements = ""

phoenix_utils.setPath()

# HBase configuration folder path (where hbase-site.xml reside) for
# HBase/Phoenix client side property override
hbase_config_path = os.getenv('HBASE_CONF_DIR', phoenix_utils.current_dir)

execute = ('java -cp "%s%s%s" -Dlog4j.configuration=file:' +
           os.path.join(phoenix_utils.current_dir, "log4j.properties") +
           ' org.apache.phoenix.util.PhoenixRuntime -t %s %s ') % \
    (hbase_config_path, os.pathsep, phoenix_utils.phoenix_client_jar, table, zookeeper)

# Create Table DDL
createtable = "CREATE TABLE IF NOT EXISTS %s (HOST CHAR(2) NOT NULL,\
DOMAIN VARCHAR NOT NULL, FEATURE VARCHAR NOT NULL,DATE DATE NOT NULL,\
USAGE.CORE BIGINT,USAGE.DB BIGINT,STATS.ACTIVE_VISITOR  \
INTEGER CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, DATE))  \
SPLIT ON ('CSGoogle','CSSalesforce','EUApple','EUGoogle','EUSalesforce',\
'NAApple','NAGoogle','NASalesforce');" % (table)

# generate and upsert data
print "Phoenix Performance Evaluation Script 1.0"
print "-----------------------------------------"

print "\nCreating performance table..."
createFileWithContent(ddl, createtable)

exitcode = subprocess.call(execute + ddl, shell=True)
if exitcode != 0:
    sys.exit(exitcode)

# Write real,user,sys time on console for the following queries
queryex("1 - Count", "SELECT COUNT(1) FROM %s;" % (table))
queryex("2 - Group By First PK", "SELECT HOST FROM %s GROUP BY HOST;" % (table))
queryex("3 - Group By Second PK", "SELECT DOMAIN FROM %s GROUP BY DOMAIN;" % (table))
queryex("4 - Truncate + Group By", "SELECT TRUNC(DATE,'DAY') DAY FROM %s GROUP BY TRUNC(DATE,'DAY');" % (table))
queryex("5 - Filter + Count", "SELECT COUNT(1) FROM %s WHERE CORE<10;" % (table))

print "\nGenerating and upserting data..."
exitcode = subprocess.call('java -jar %s %s' % (phoenix_utils.testjar, rowcount), shell=True)
if exitcode != 0:
    sys.exit(exitcode)

print "\n"
createFileWithContent(qry, statements)

exitcode = subprocess.call(execute + data + ' ' + qry, shell=True)
if exitcode != 0:
    sys.exit(exitcode)

# clear temporary files
delfile(ddl)
delfile(data)
delfile(qry)
