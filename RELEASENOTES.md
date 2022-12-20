
<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
# PHOENIX  5.1.3 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [PHOENIX-6655](https://issues.apache.org/jira/browse/PHOENIX-6655) | *Major* | **SYSTEM.SEQUENCE should have CACHE\_DATA\_ON\_WRITE set to true**

Phoenix now sets the CACHE\_DATA\_ON\_WRITE property on the SYSTEM.SEQUENCE table to improve performance.


---

* [PHOENIX-6837](https://issues.apache.org/jira/browse/PHOENIX-6837) | *Major* | **Switch to Using the -hadoop3 HBase Artifacts Where Available**

Phoenix now uses the public Hadoop 3 compatible HBase artifacts where available.
At the time of writing, public Hadoop 3 compatible artifacts are only available for HBase 2.5.2 and later.


---

* [PHOENIX-6823](https://issues.apache.org/jira/browse/PHOENIX-6823) | *Major* | **calling Joda-based round() function on temporal PK field causes division by zero error**

Fixed division by zero errors, and incorrect results when using round(), ceiling(), floor() functions in the where clause on date/time fields that are part of the primary key.


---

* [PHOENIX-6834](https://issues.apache.org/jira/browse/PHOENIX-6834) | *Major* | **Use Pooled HConnection for Server Side Upsert Select**

Server Side upsert selects now cache the HBase connection to the target table on the region server.


---

* [PHOENIX-6751](https://issues.apache.org/jira/browse/PHOENIX-6751) | *Critical* | **Force using range scan vs skip scan when using the IN operator and large number of RVC elements**

Adds a new config parameter, phoenix.max.inList.skipScan.size, which controls the size of an IN clause before it will be automatically converted from a skip scan to a range scan.


---

* [PHOENIX-6692](https://issues.apache.org/jira/browse/PHOENIX-6692) | *Major* | **Add HBase 2.5 support**

Phoenix now supports HBase 2.5.
In Phoenix 5.1.x, Tephra support is not available with HBase 2.5.
(In 5.2.x Tephra support is fully removed)


---

* [PHOENIX-6686](https://issues.apache.org/jira/browse/PHOENIX-6686) | *Major* | **Update Jackson to 2.12.6.1**

The Jackson dependency in Phoenix has been updated to 2.12.6.1


---

* [PHOENIX-6665](https://issues.apache.org/jira/browse/PHOENIX-6665) | *Major* | **PreparedStatement#getMetaData() fails on parametrized "select next ? values for SEQ"**

PreparedStatement#getMetaData() no longer fails on parametrized "select next ? values" sequence operations.
This also fixes a problem where parametrized sequence operations didn't work via Phoenix Query Server.


---

* [PHOENIX-6645](https://issues.apache.org/jira/browse/PHOENIX-6645) | *Minor* | **Remove unneccessary SCN related properties from SYSTEM tables on upgrade**

The KEEP\_DELETED\_CELLS property is removed, and the VERSIONS property is set to 1 for the SYSTEM.STATS and SYSTEM.LOG tables on upgrade now.


---

* [PHOENIX-6586](https://issues.apache.org/jira/browse/PHOENIX-6586) | *Critical* | **Set NORMALIZATION\_ENABLED to false on salted tables**

Phoenix now automatically sets NORMALIZATION\_ENABLED=false when creating salted tables.


---

* [PHOENIX-6551](https://issues.apache.org/jira/browse/PHOENIX-6551) | *Major* | **Bump HBase version to 2.4.6 and 2.2.7**

Phoenix now builds with HBase 2.4.6 and 2.2.7 for the respective hbase profiles.



# PHOENIX  5.1.2 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [PHOENIX-6357](https://issues.apache.org/jira/browse/PHOENIX-6357) | *Major* | **Change all command line tools to use the fixed commons-cli constructor**

Fixed command line parsing of double quoted identifiers.
The previous workaround of using double doublequotes as a workaround is no longer supported.



# PHOENIX  5.1.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [PHOENIX-6359](https://issues.apache.org/jira/browse/PHOENIX-6359) | *Major* | **New module to support HBase 2.4.1+ releases**

Added phoenix-hbase-compat-2.4.1 to support latest patch versions of HBase 2.4 release line.


---

* [PHOENIX-6343](https://issues.apache.org/jira/browse/PHOENIX-6343) | *Major* | **Phoenix allows duplicate column names when one of them is a primary key**

Although user provided CF can have same column name as one of primary keys, default CF is no longer supported to have same column name as primary key columns.


---

* [PHOENIX-5250](https://issues.apache.org/jira/browse/PHOENIX-5250) | *Blocker* | **The accumulated wal files can't be cleaned**

When updating from  an earlier version, and using HBase 2.1.x, HBase 2.2.6 or earlier, HBase 2.3.3 or earlier, or HBase 2.4.0, HBase is unable to clean up WAL files that include data for global indexed tables with the old implementation.

To avoid running out of disk space, use IndexUpgradeTool to upgrade all tables with global indexes, and then restart all region servers as soon as possible.



# PHOENIX  5.1.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [PHOENIX-6349](https://issues.apache.org/jira/browse/PHOENIX-6349) | *Major* | **Add and use commons-cli to phoenix-thirdparty**

Added phoenix-shaded-commons-cli to phoenix-thirdparty.
This helps avoid classpath conflicts and includes a patch on top of commons-cli 1.4.0 that fixes CLI-254.


---

* [PHOENIX-6273](https://issues.apache.org/jira/browse/PHOENIX-6273) | *Major* | **Add support to handle MR Snapshot restore externally**

Adds mapreduce configuration param "phoenix.mapreduce.external.snapshot.restore" which when set to true indicates that snapshot-based MapReduce jobs shouldn't try to restore the snapshot themselves, but assume an external application has already done so.


---

* [PHOENIX-6323](https://issues.apache.org/jira/browse/PHOENIX-6323) | *Blocker* | **isRawFilterSupported() returns true for HBase 2.2 compat module**

When used with HBase 2.2, Phoenix now only support HBase 2.2.5 and later versions. (i.e HBase 2.2.0-2.2.4 are not supported)


---

* [PHOENIX-6326](https://issues.apache.org/jira/browse/PHOENIX-6326) | *Critical* | **Phoenix doesn't work with Java version 11.0.9.1 , due to Jetty problem**

There is bug in the Jetty version used by HBase 2.3 and earlier, that causes incompatilbity with Java releases that have a four number version string like  "11.0.9.1"
As Phoenix builds on top of HBase, Phoenix is also incompatible with these releases, when built with an hbase.profile older than 2.4.


---

* [PHOENIX-6307](https://issues.apache.org/jira/browse/PHOENIX-6307) | *Major* | **Build and release official binary distributions with each HBase profile**

The phoenix-client and phoenix-server JARs have been renamed to include the supported HBase version.

Instead of phoenix-client-\<phoenix.version\>.jar, the client is now phoenix-client-hbase-\<hbase-major.minor\>-\<phoenix.version\>.jar
I.e. The Phoenix 4.16 client for Hbase 1.5 is now called phoenix-client-hbase-1.5-5.1.0.jar

The maven coordinates also have also changed to "org.apache.phoenix:phoenix-client-hbase-\<hbase-major.minor\>:\<phoenix.version\>".
I.e the Phoenix 4.15.0 client for Hbase 1.5 is  "org.apache.phoenix:phoenix-client:4.15.0-HBase-1.5", but the phoenix client for Phoenix 4.16 is "org.apache.phoenix:phoenix-client-hbase-1.5:4.16.0"


---

* [PHOENIX-5265](https://issues.apache.org/jira/browse/PHOENIX-5265) | *Major* | **[UMBRELLA] Phoenix Test should use object based Plan for result comparison instead of using hard-corded comparison**

New API for Explain plan queries that can be used for comparison of individual plan attributes.


---

* [PHOENIX-6280](https://issues.apache.org/jira/browse/PHOENIX-6280) | *Major* | **Support HBase 2.4**

HBase 2.4 is now supported. (Tested with HBase 2.4.0)


---

* [PHOENIX-6282](https://issues.apache.org/jira/browse/PHOENIX-6282) | *Major* | **Generate PB files inline with build and remove checked in files**

We no longer have generated protobuf Java files available in source code. These files are expected to be generated inline with mvn build. We have also used an optimization with the plugin to ensure protoc is not invoked with mvn build if no .proto file is updated between two consecutive builds.


---

* [PHOENIX-6086](https://issues.apache.org/jira/browse/PHOENIX-6086) | *Critical* | **Take a snapshot of all SYSTEM tables before attempting to upgrade them**

While upgrading System tables, all system tables where we perform some significant DDL operations, we start taking snapshots of them:
 
1. SYSTEM.CATALOG (was already covered before this Jira)
2. SYSTEM.CHILD\_LINK
3. SYSTEM.SEQUENCE
4. SYSTEM.STATS
5. SYSTEM.TASK

If the upgrade doesn't complete successfully, we should get warning log providing all snapshots taken so far, which can be used to restore some snapshots if required.


A sample Warning log:

Failed upgrading System tables. Snapshots for system tables created so far: {SYSTEM:STATS=SNAPSHOT\_SYSTEM.STATS\_4.15.x\_TO\_4.16.0\_20201202114411, SYSTEM:CATALOG=SNAPSHOT\_SYSTEM.CATALOG\_4.15.x\_TO\_4.16.0\_20201202114258, SYSTEM:CHILD\_LINK=SNAPSHOT\_SYSTEM.CHILD\_LINK\_4.15.x\_TO\_4.16.0\_20201202114405, SYSTEM:SEQUENCE=SNAPSHOT\_SYSTEM.SEQUENCE\_4.15.x\_TO\_4.16.0\_20201202114407, SYSTEM:TASK=SNAPSHOT\_SYSTEM.TASK\_4.15.x\_TO\_4.16.0\_20201202114413}


---

* [PHOENIX-4412](https://issues.apache.org/jira/browse/PHOENIX-4412) | *Critical* | **Tephra transaction context visibility level returns null instead of SNAPSHOT\_ALL**

**WARNING: No release note provided for this change.**


---

* [PHOENIX-6155](https://issues.apache.org/jira/browse/PHOENIX-6155) | *Major* | **Prevent doing direct upserts into SYSTEM.TASK from the client**

A new coprocessor endpoint to avoid direct upserts into SYSTEM.TASK from the client.


---

* [PHOENIX-6186](https://issues.apache.org/jira/browse/PHOENIX-6186) | *Major* | **Store table metadata last modified timestamp in PTable / System.Catalog**

Introduces a new field in System.Catalog, LAST\_DDL\_TIMESTAMP, which is the epoch timestamp at which a table or view is created, or last had a column added or dropped. Child views inherit the max ddl timestamp of their ancestors.


---

* [PHOENIX-6125](https://issues.apache.org/jira/browse/PHOENIX-6125) | *Major* | **Make sure SYSTEM.TASK does not split**

We have new split policy introduced for SYSTEM.TASK which for now is just extending DisabledRegionSplitPolicy. As part of an upgrade to 4.16/5.1, updating split policy will be taken care of unless it was already updated manually.

Hence, before 4.16/5.1 upgrade, if operator has already manually updated split policy of SYSTEM.TASK, an exception will be thrown during upgrade to 4.16/5.1 which would mandate an operator intervention to perform:

1. Merging SYSTEM.TASK regions into one single region (if multiple regions were already available before 4.16/5.1 upgrade)
2. Remove split policy of the table manually.


---

* [PHOENIX-5446](https://issues.apache.org/jira/browse/PHOENIX-5446) | *Major* | **Support Protobuf shaded clients (thin + thick)**

**WARNING: No release note provided for this change.**


---

* [PHOENIX-6016](https://issues.apache.org/jira/browse/PHOENIX-6016) | *Major* | **Support HBase 2.3.0**

Phoenix now supports HBase 2.3


---

* [PHOENIX-5716](https://issues.apache.org/jira/browse/PHOENIX-5716) | *Major* | **Remove or update HBase 2.0 support**

Support for HBase 2.0 has been dropped from Phoenix.


---

* [PHOENIX-4866](https://issues.apache.org/jira/browse/PHOENIX-4866) | *Blocker* | **UDFs get error: org.apache.phoenix.schema.FunctionNotFoundException: ERROR 6001 (42F01): Function undefined**

**WARNING: No release note provided for this change.**


---

* [PHOENIX-5268](https://issues.apache.org/jira/browse/PHOENIX-5268) | *Blocker* | **HBase 2.1/2.2 compatibility**

Phoenix now supports HBase 2.1  (HBase 2.1.6 and later) and HBase 2.2 (HBase 2.2.1 and later)


---

* [PHOENIX-3655](https://issues.apache.org/jira/browse/PHOENIX-3655) | *Major* | **Global Phoenix Client Metrics for PQS**

**WARNING: No release note provided for this change.**



