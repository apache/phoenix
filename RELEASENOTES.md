
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
# PHOENIX  5.3.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [PHOENIX-7696](https://issues.apache.org/jira/browse/PHOENIX-7696) | *Major* | **Update Hadoop 3.4 version to 3.4.2**

Phoenix is not built with Hadoop 3.4.2 by default.


---

* [PHOENIX-7600](https://issues.apache.org/jira/browse/PHOENIX-7600) | *Major* | **Replace commons-logging with slf4j in Phoenix related repos**

Phoenix now uses org.slf4j:jcl-over-slf4j instead of commons-logging.

commons-logging has been removed from the client uberjars.
If commons-logging or jcl-over-slf4j is not already present on the classpath,
you will need to add jcl-over-slf4j to the application's classpath.


---

* [PHOENIX-7606](https://issues.apache.org/jira/browse/PHOENIX-7606) | *Major* | **Remove HBase 2.4 support from master branch**

Phoenix 5.3 no longer supports HBase 2.4.


---

* [PHOENIX-7681](https://issues.apache.org/jira/browse/PHOENIX-7681) | *Major* | **Update HBase 2.5 profile default version to 2.5.12**

Phoenix now uses HBase 2.5.12 by default when built with the HBase 2.5 profile.


---

* [PHOENIX-7668](https://issues.apache.org/jira/browse/PHOENIX-7668) | *Blocker* | **Update HBase 2.6 profile default version to 2.6.3**

Phoenix now uses Hbase 2.6.3 when built with the HBase 2.6 profile.


---

* [PHOENIX-7630](https://issues.apache.org/jira/browse/PHOENIX-7630) | *Major* | **Standard JDBC support for UPSERT returning ResultSet**

When executing an UPSERT query with ON DUPLICATE KEY, the JDBC statement's upsert() and executeUpsert() APIs will result in a ResultSet to retrieve the new or old (when used with IGNORE) state of the row. This will facilitate an atomic update followed by a read.


---

* [PHOENIX-7550](https://issues.apache.org/jira/browse/PHOENIX-7550) | *Major* | **Update OWASP plugin to 12.1.0**

Phoenix now uses the 12.1.0 version of WASP plugin, which requires Java 11.

This Java requirement only applies when running the site plugin, and does not affect the normal build.


---

* [PHOENIX-7539](https://issues.apache.org/jira/browse/PHOENIX-7539) | *Major* | **Update default HBase 2.5 version to 2.5.11**

Phoenix with the HBase 2.5 profile is no built with Hbase 2.5.11 and Hadoop 3.4.1.

To build Phoenix with Hbase 2.5.10 or earlier the hbase.properties system properties to the corresponding value.


---

* [PHOENIX-7532](https://issues.apache.org/jira/browse/PHOENIX-7532) | *Major* | **Update default Hbase 2.6 version to 2.6.2**

Phoenix with the HBase 2.6 profile is now built with Hbase 2.6.2 and Hadoop 3.4.1.

To build Phoenix with Hbase 2.6.0 or 2.6.1, use the HBase 2.6.0 profile, which uses Hadoop 3.3.6.


---

* [PHOENIX-7520](https://issues.apache.org/jira/browse/PHOENIX-7520) | *Major* | **Use HBASE\_OPTS from hbase-env.sh in startup scripts**

The Phoenix startup scripts (sqlline.py, psql.py, performance.py) now try to parse and apply environment variables set in hbase-env.sh / hbase-env.cmd.
Also, the contents of the HBASE\_OPTS enviroment variable is now added to the java command when starting the above scripts
This new behaviour can be disabled by setting the SKIP\_HBASE\_ENV environment variable to any value.


---

* [PHOENIX-7180](https://issues.apache.org/jira/browse/PHOENIX-7180) | *Minor* | **Use phoenix-client-lite in sqlline script**

sqlline.py now uses the phoenix-client-lite JAR instead of the phoenix-client-embedded JAR.


---

* [PHOENIX-7404](https://issues.apache.org/jira/browse/PHOENIX-7404) | *Major* | **Build the HBase 2.5+ profiles with Hadoop 3.3.6**

Phoenix is now built with Hadoop 3.3.6 for the HBase 2.5 and 2.6 profiles.


---

* [PHOENIX-7363](https://issues.apache.org/jira/browse/PHOENIX-7363) | *Blocker* | **Protect server side metadata cache updates for the given PTable**

PHOENIX-6066 introduces a way for us to take HBase read level row-lock while retrieving the PTable object as part of the getTable() RPC call, by default. Before PHOENIX-6066, only write level row-lock was used, which hurts the performance even if the server side metadata cache has latest data, requiring no lookup from SYSTEM.CATALOG table.

PHOENIX-7363 allows to protect the metadata cache update at the server side with Phoenix write level row-lock. As part of getTable() call, we already must be holding HBase read level row-lock. Hence, PHOENIX-7363 provides protection for server side metadata cache updates.

PHOENIX-6066 and PHOENIX-7363 must be combined.


---

* [PHOENIX-7001](https://issues.apache.org/jira/browse/PHOENIX-7001) | *Major* | **Change Data Capture leveraging Max Lookback and Uncovered Indexes**

Change Data Capture (CDC) is a feature designed to capture changes to tables or updatable views in near real-time. This new functionality supports various use cases, including:
\* Real-Time Change Retrieval: Capture and retrieve changes as they happen or with minimal delay.
\* Flexible Time Range Queries: Perform queries based on specific time ranges, typically short periods such as the last few minutes, hours, or the last few days.
\* Comprehensive Change Tracking: Track all types of changes including insertions, updates, and deletions. Note that CDC does not differentiate between inserts and updates due to Phoenix’s handling of new versus existing rows.

Key features of the CDC include:
\* Ordered Change Delivery: Changes are delivered in the order they arrive, ensuring the sequence of events is maintained.
\* Streamlined Integration: Changes can be visualized and delivered to applications similarly to how Phoenix query results are retrieved, but with enhancements to support multiple results for each row and inclusion of deleted rows.
\* Detailed Change Information: Optionally capture pre and post-change images of rows to provide a complete picture of modifications.

This enhancement empowers applications to maintain an accurate and timely reflection of database changes, supporting a wide array of real-time data processing and monitoring scenarios.


---

* [PHOENIX-628](https://issues.apache.org/jira/browse/PHOENIX-628) | *Blocker* | **Support native JSON data type**

Initial support for JSON datatype in Phoenix. More follow-up work is expected in future.



