
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
# PHOENIX Changelog

## Release 5.3.0 - Unreleased (as of 2025-09-14)



### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7651](https://issues.apache.org/jira/browse/PHOENIX-7651) | Support RETURNING \* with UPSERT and DELETE |  Major | . |
| [PHOENIX-7684](https://issues.apache.org/jira/browse/PHOENIX-7684) | Introduce Segment Scan |  Major | . |
| [PHOENIX-7662](https://issues.apache.org/jira/browse/PHOENIX-7662) | BSON Condition Function contains() |  Major | . |
| [PHOENIX-7585](https://issues.apache.org/jira/browse/PHOENIX-7585) | New BSON Condition Function begins\_with() |  Major | . |
| [PHOENIX-7653](https://issues.apache.org/jira/browse/PHOENIX-7653) | New CDC Event for TTL expired rows |  Major | . |
| [PHOENIX-7648](https://issues.apache.org/jira/browse/PHOENIX-7648) | Introduce new Atomic Operation - ON DUPLICATE KEY UPDATE\_ONLY |  Major | . |
| [PHOENIX-7630](https://issues.apache.org/jira/browse/PHOENIX-7630) | Standard JDBC support for UPSERT returning ResultSet |  Major | . |
| [PHOENIX-7170](https://issues.apache.org/jira/browse/PHOENIX-7170) | Conditional TTL |  Major | . |
| [PHOENIX-7434](https://issues.apache.org/jira/browse/PHOENIX-7434) | Extend atomic support to single row delete with condition on non-pk columns |  Major | . |
| [PHOENIX-7424](https://issues.apache.org/jira/browse/PHOENIX-7424) | Support toStringBinary/toBytesBinary conversion |  Major | core |
| [PHOENIX-5117](https://issues.apache.org/jira/browse/PHOENIX-5117) | Return the count of rows scanned in HBase |  Major | . |
| [PHOENIX-7411](https://issues.apache.org/jira/browse/PHOENIX-7411) | Atomic Delete: PhoenixStatement API to return row if single row is atomically deleted |  Major | . |
| [PHOENIX-7357](https://issues.apache.org/jira/browse/PHOENIX-7357) | New variable length binary data type: VARBINARY\_ENCODED |  Major | . |
| [PHOENIX-7398](https://issues.apache.org/jira/browse/PHOENIX-7398) | New PhoenixStatement API to return row for Atomic/Conditional Upserts |  Major | . |
| [PHOENIX-7396](https://issues.apache.org/jira/browse/PHOENIX-7396) | BSON\_VALUE function to retrieve BSON field value with given data type |  Major | . |
| [PHOENIX-7330](https://issues.apache.org/jira/browse/PHOENIX-7330) | Introducing Binary JSON (BSON) with Complex Document structures in Phoenix |  Major | . |
| [PHOENIX-628](https://issues.apache.org/jira/browse/PHOENIX-628) | Support native JSON data type |  Blocker | . |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7700](https://issues.apache.org/jira/browse/PHOENIX-7700) | Update default values for schema change retry policy |  Major | . |
| [PHOENIX-7697](https://issues.apache.org/jira/browse/PHOENIX-7697) | Update Gson to 2.13.1 |  Major | . |
| [PHOENIX-7698](https://issues.apache.org/jira/browse/PHOENIX-7698) | Update Netty to 4.1.126 |  Major | . |
| [PHOENIX-7699](https://issues.apache.org/jira/browse/PHOENIX-7699) | Update Jetty to 9.4.58.v20250814 |  Major | . |
| [PHOENIX-7696](https://issues.apache.org/jira/browse/PHOENIX-7696) | Update Hadoop 3.4 version to 3.4.2 |  Major | core |
| [PHOENIX-6883](https://issues.apache.org/jira/browse/PHOENIX-6883) | Phoenix metadata caching redesign |  Major | . |
| [PHOENIX-7507](https://issues.apache.org/jira/browse/PHOENIX-7507) | Update ROW\_KEY\_MATCHER column type to be VARBINARY\_ENCODED |  Major | . |
| [PHOENIX-7376](https://issues.apache.org/jira/browse/PHOENIX-7376) | ViewUtil#findAllDescendantViews should provide two versions to differentiate CQSI initiated by clients and servers |  Major | . |
| [PHOENIX-7493](https://issues.apache.org/jira/browse/PHOENIX-7493) | Graceful Failover with Phoenix HA |  Major | . |
| [PHOENIX-7634](https://issues.apache.org/jira/browse/PHOENIX-7634) | CDC Stream name format should allow separator that is not allowed in table name |  Major | . |
| [PHOENIX-7535](https://issues.apache.org/jira/browse/PHOENIX-7535) | Capture time spent in various stages of exceuteQuery call |  Minor | . |
| [PHOENIX-7690](https://issues.apache.org/jira/browse/PHOENIX-7690) | Add a config to enable using bloom filters for multi-key point lookups |  Major | . |
| [PHOENIX-7692](https://issues.apache.org/jira/browse/PHOENIX-7692) | Path validations for bson update expression |  Major | . |
| [PHOENIX-7670](https://issues.apache.org/jira/browse/PHOENIX-7670) | Region level thread pool for uncovered index to scan data table rows |  Major | . |
| [PHOENIX-7682](https://issues.apache.org/jira/browse/PHOENIX-7682) | Review and simplify compatibility modules |  Major | core |
| [PHOENIX-6851](https://issues.apache.org/jira/browse/PHOENIX-6851) | Use spotless to format code in phoenix |  Major | . |
| [PHOENIX-7659](https://issues.apache.org/jira/browse/PHOENIX-7659) | Leverage = ANY() instead of big IN list to do huge number of point lookups in a single query |  Minor | . |
| [PHOENIX-7677](https://issues.apache.org/jira/browse/PHOENIX-7677) | TTL\_DELETE CDC event to use batch mutation |  Major | . |
| [PHOENIX-7625](https://issues.apache.org/jira/browse/PHOENIX-7625) | Adding query plan information to ConnectionActivityLogger |  Major | . |
| [PHOENIX-7683](https://issues.apache.org/jira/browse/PHOENIX-7683) | Bump Apache Commons Lang 3 to 3.18.0 due to CVE-2025-48924 |  Major | . |
| [PHOENIX-7606](https://issues.apache.org/jira/browse/PHOENIX-7606) | Remove HBase 2.4 support from master branch |  Major | core, phoenix |
| [PHOENIX-7681](https://issues.apache.org/jira/browse/PHOENIX-7681) | Update HBase 2.5 profile default version to 2.5.12 |  Major | core |
| [PHOENIX-7668](https://issues.apache.org/jira/browse/PHOENIX-7668) | Update HBase 2.6 profile default version to 2.6.3 |  Blocker | core |
| [PHOENIX-7551](https://issues.apache.org/jira/browse/PHOENIX-7551) | BSON\_VALUE\_TYPE function to return the data type of BSON field value |  Major | . |
| [PHOENIX-7661](https://issues.apache.org/jira/browse/PHOENIX-7661) | Clean up old framework for TTL masking and expiration |  Major | . |
| [PHOENIX-7536](https://issues.apache.org/jira/browse/PHOENIX-7536) | Capture Query parsing time for Read/Write path |  Major | . |
| [PHOENIX-7673](https://issues.apache.org/jira/browse/PHOENIX-7673) | BSON Condition Function size() |  Major | . |
| [PHOENIX-7663](https://issues.apache.org/jira/browse/PHOENIX-7663) | BSON Condition Function field\_type() |  Major | . |
| [PHOENIX-7671](https://issues.apache.org/jira/browse/PHOENIX-7671) | Sync maxLookback from data table to indexes |  Major | . |
| [PHOENIX-7667](https://issues.apache.org/jira/browse/PHOENIX-7667) | Strict vs Relaxed TTL |  Major | . |
| [PHOENIX-7545](https://issues.apache.org/jira/browse/PHOENIX-7545) | BSON\_VALUE() to support returning sub-document |  Major | . |
| [PHOENIX-7584](https://issues.apache.org/jira/browse/PHOENIX-7584) | Conditional TTL on SYSTEM.CDC\_STREAM |  Major | . |
| [PHOENIX-7652](https://issues.apache.org/jira/browse/PHOENIX-7652) | Clear CDC Stream metadata when a table with CDC enabled is dropped |  Major | . |
| [PHOENIX-7650](https://issues.apache.org/jira/browse/PHOENIX-7650) | Default value support in BSON\_VALUE() for all supported types |  Major | . |
| [PHOENIX-7626](https://issues.apache.org/jira/browse/PHOENIX-7626) | Add metrics to capture HTable thread pool utilization and contention |  Minor | . |
| [PHOENIX-7646](https://issues.apache.org/jira/browse/PHOENIX-7646) | New PhoenixStatement API to return old row state in Atomic Updates |  Major | . |
| [PHOENIX-7647](https://issues.apache.org/jira/browse/PHOENIX-7647) | BSON\_UPDATE\_EXPRESSION() set value if field key does not exist |  Major | . |
| [PHOENIX-7644](https://issues.apache.org/jira/browse/PHOENIX-7644) | Remove CDC Object from SYSTEM.CATALOG when table is dropped |  Major | . |
| [PHOENIX-7643](https://issues.apache.org/jira/browse/PHOENIX-7643) | Add parent partition's start time to SYSTEM.CDC\_STREAM table |  Major | . |
| [PHOENIX-7642](https://issues.apache.org/jira/browse/PHOENIX-7642) | Add CDC Stream creation datetime in stream name |  Major | . |
| [PHOENIX-7641](https://issues.apache.org/jira/browse/PHOENIX-7641) | Support placeholder for document field keys in BSON condition expression |  Major | . |
| [PHOENIX-7639](https://issues.apache.org/jira/browse/PHOENIX-7639) | Improve error handling in PhoenixMasterObserver |  Major | . |
| [PHOENIX-7633](https://issues.apache.org/jira/browse/PHOENIX-7633) | Tuple API to provide serialized bytes size for the given Tuple |  Major | . |
| [PHOENIX-7631](https://issues.apache.org/jira/browse/PHOENIX-7631) | BSON\_VALUE() to support returning binary value with VARBINARY\_ENCODED data type encoding |  Major | . |
| [PHOENIX-7591](https://issues.apache.org/jira/browse/PHOENIX-7591) | Concurrent updates to tables with indexes can cause data inconsistency |  Major | . |
| [PHOENIX-7605](https://issues.apache.org/jira/browse/PHOENIX-7605) | Allow the threadpool used by HBase client to be managed at the CQSI level |  Major | . |
| [PHOENIX-7586](https://issues.apache.org/jira/browse/PHOENIX-7586) | Handle Role transitions for ActiveToStanby role in Failover HAPolicy |  Major | . |
| [PHOENIX-7604](https://issues.apache.org/jira/browse/PHOENIX-7604) | Log exceptions in GlobalClientMetrics static initializer |  Major | . |
| [PHOENIX-7517](https://issues.apache.org/jira/browse/PHOENIX-7517) | Allow BSON as primary key column |  Major | . |
| [PHOENIX-7515](https://issues.apache.org/jira/browse/PHOENIX-7515) | Add metric for count of Phoenix client batches used by a commit call |  Minor | . |
| [PHOENIX-7449](https://issues.apache.org/jira/browse/PHOENIX-7449) | Bypass disable table during drop if table is already disabled in HBase |  Minor | core |
| [PHOENIX-7588](https://issues.apache.org/jira/browse/PHOENIX-7588) | SUBBINARY() - Substring like search for Binary data types |  Major | . |
| [PHOENIX-7587](https://issues.apache.org/jira/browse/PHOENIX-7587) | Revert configuring Table level MaxLookBack in syscat |  Blocker | . |
| [PHOENIX-7107](https://issues.apache.org/jira/browse/PHOENIX-7107) | Add support for indexing on SYSTEM.CATALOG table |  Major | . |
| [PHOENIX-7576](https://issues.apache.org/jira/browse/PHOENIX-7576) | Update Jetty to 9.4.57.v20241219 |  Minor | core, queryserver |
| [PHOENIX-7463](https://issues.apache.org/jira/browse/PHOENIX-7463) | New ANTLR grammar to evaluate BSON's SQL style expressions |  Major | . |
| [PHOENIX-7186](https://issues.apache.org/jira/browse/PHOENIX-7186) | Support Square Brackets Notation for IPv6 in JDBC URL |  Minor | core |
| [PHOENIX-7578](https://issues.apache.org/jira/browse/PHOENIX-7578) | Fix flapping tests in CDCStreamIT with region merges |  Major | . |
| [PHOENIX-7575](https://issues.apache.org/jira/browse/PHOENIX-7575) | Update Netty to 4.1.119 |  Blocker | phoenix |
| [PHOENIX-7563](https://issues.apache.org/jira/browse/PHOENIX-7563) | Use HBASE\_OPTS as a fallback for PHOENIX\_OPTS |  Major | . |
| [PHOENIX-7572](https://issues.apache.org/jira/browse/PHOENIX-7572) | Update OMID to 1.1.3 |  Critical | . |
| [PHOENIX-7553](https://issues.apache.org/jira/browse/PHOENIX-7553) | Support Python 3.13 |  Minor | python |
| [PHOENIX-7502](https://issues.apache.org/jira/browse/PHOENIX-7502) | Decouple principal from HAGroupInfo |  Major | . |
| [PHOENIX-7546](https://issues.apache.org/jira/browse/PHOENIX-7546) | When hbase client metrics is enabled set hbase client metrics scope |  Minor | . |
| [PHOENIX-7495](https://issues.apache.org/jira/browse/PHOENIX-7495) | Support for HBase Registry Implementations in Pheonix HA Connections |  Major | . |
| [PHOENIX-7552](https://issues.apache.org/jira/browse/PHOENIX-7552) | Escape bloomfilter value and column family in SchemaTool synthesis |  Major | . |
| [PHOENIX-7547](https://issues.apache.org/jira/browse/PHOENIX-7547) | Upgrade BSON version to 5.3.1 |  Major | . |
| [PHOENIX-7540](https://issues.apache.org/jira/browse/PHOENIX-7540) | Fix PhoenixTestDriverIT#testDifferentCQSForServerConnection test failure |  Major | . |
| [PHOENIX-7529](https://issues.apache.org/jira/browse/PHOENIX-7529) | Optimize exception log printing in CSVCommonsLoader |  Major | . |
| [PHOENIX-7520](https://issues.apache.org/jira/browse/PHOENIX-7520) | Use HBASE\_OPTS from hbase-env.sh in startup scripts |  Major | core |
| [PHOENIX-7453](https://issues.apache.org/jira/browse/PHOENIX-7453) | Fix Phoenix to compile with commons-cli 1.9 |  Major | . |
| [PHOENIX-7462](https://issues.apache.org/jira/browse/PHOENIX-7462) | Single row Atomic Upsert/Delete returning result should return ResultSet |  Major | . |
| [PHOENIX-7473](https://issues.apache.org/jira/browse/PHOENIX-7473) | Eliminating index maintenance for CDC index |  Major | . |
| [PHOENIX-7469](https://issues.apache.org/jira/browse/PHOENIX-7469) | Partitioned CDC Index for partitioned tables |  Major | . |
| [PHOENIX-7425](https://issues.apache.org/jira/browse/PHOENIX-7425) | Partitioned CDC Index for eliminating salting |  Major | . |
| [PHOENIX-7428](https://issues.apache.org/jira/browse/PHOENIX-7428) | Add usable error message in BackwardCompatibilityIT |  Minor | core, test |
| [PHOENIX-7432](https://issues.apache.org/jira/browse/PHOENIX-7432) | getTable for PHYSICAL\_TABLE link should use common utility |  Critical | . |
| [PHOENIX-7416](https://issues.apache.org/jira/browse/PHOENIX-7416) | Bump Avro dependency version to 1.11.4 |  Major | . |
| [PHOENIX-6982](https://issues.apache.org/jira/browse/PHOENIX-6982) | Exclude Maven descriptors from shaded JARs |  Major | . |
| [PHOENIX-7381](https://issues.apache.org/jira/browse/PHOENIX-7381) | Client should not validate LAST\_DDL\_TIMESTAMP for a table with non-zero UPDATE\_CACHE\_FREQUENCY if the client's cache entry is not old enough |  Major | . |
| [PHOENIX-7180](https://issues.apache.org/jira/browse/PHOENIX-7180) | Use phoenix-client-lite in sqlline script |  Minor | core |
| [PHOENIX-7382](https://issues.apache.org/jira/browse/PHOENIX-7382) | Eliminating index building and treating max lookback as TTL for CDC Index |  Major | . |
| [PHOENIX-7395](https://issues.apache.org/jira/browse/PHOENIX-7395) | Metadata Cache metrics at server and client side |  Major | . |
| [PHOENIX-6870](https://issues.apache.org/jira/browse/PHOENIX-6870) | Add cluster-wide metadata upgrade block |  Major | core |
| [PHOENIX-7397](https://issues.apache.org/jira/browse/PHOENIX-7397) | Optimize ClientAggregatePlan/ClientScanPlan when inner query is UnionPlan |  Major | core |
| [PHOENIX-7404](https://issues.apache.org/jira/browse/PHOENIX-7404) | Build the HBase 2.5+ profiles with Hadoop 3.3.6 |  Major | . |
| [PHOENIX-7394](https://issues.apache.org/jira/browse/PHOENIX-7394) | MaxPhoenixColumnSizeExceededException should not print rowkey |  Major | . |
| [PHOENIX-7393](https://issues.apache.org/jira/browse/PHOENIX-7393) | Update transitive dependency of woodstox-core to 5.4.0 |  Major | . |
| [PHOENIX-7375](https://issues.apache.org/jira/browse/PHOENIX-7375) | CQSI connection init from regionserver hosting SYSTEM.CATALOG does not require RPC calls to system tables |  Major | . |
| [PHOENIX-7386](https://issues.apache.org/jira/browse/PHOENIX-7386) | Override UPDATE\_CACHE\_FREQUENCY if table has disabled indexes |  Major | . |
| [PHOENIX-7385](https://issues.apache.org/jira/browse/PHOENIX-7385) | Fix MetadataGetTableReadLockIT flapper |  Major | . |
| [PHOENIX-7379](https://issues.apache.org/jira/browse/PHOENIX-7379) | Improve handling of concurrent index mutations with the same timestamp |  Major | . |
| [PHOENIX-7333](https://issues.apache.org/jira/browse/PHOENIX-7333) | Add HBase 2.6 profile to multibranch Jenkins job |  Minor | core |
| [PHOENIX-7309](https://issues.apache.org/jira/browse/PHOENIX-7309) | Support specifying splits.txt file while creating a table. |  Major | . |
| [PHOENIX-7352](https://issues.apache.org/jira/browse/PHOENIX-7352) | Improve OrderPreservingTracker to support extracting partial ordering columns for TupleProjectionPlan |  Major | core |
| [PHOENIX-6066](https://issues.apache.org/jira/browse/PHOENIX-6066) | MetaDataEndpointImpl.doGetTable should acquire a readLock instead of an exclusive writeLock on the table header row |  Major | . |
| [PHOENIX-6978](https://issues.apache.org/jira/browse/PHOENIX-6978) | Redesign Phoenix TTL for Views |  Major | . |
| [PHOENIX-7356](https://issues.apache.org/jira/browse/PHOENIX-7356) | Centralize and update versions for exclude-only dependencies |  Minor | core |
| [PHOENIX-7354](https://issues.apache.org/jira/browse/PHOENIX-7354) | Disable TransformToolIT.testInvalidRowsWithTableLevelMaxLookback() on HBase 2.6+ |  Major | core |
| [PHOENIX-7287](https://issues.apache.org/jira/browse/PHOENIX-7287) | Leverage bloom filters for multi-key point lookups |  Major | . |
| [PHOENIX-6714](https://issues.apache.org/jira/browse/PHOENIX-6714) | Return update status from Conditional Upserts |  Major | . |
| [PHOENIX-7303](https://issues.apache.org/jira/browse/PHOENIX-7303) | fix CVE-2024-29025 in netty package |  Major | phoenix |
| [PHOENIX-7130](https://issues.apache.org/jira/browse/PHOENIX-7130) | Support skipping of shade sources jar creation |  Minor | phoenix |
| [PHOENIX-7172](https://issues.apache.org/jira/browse/PHOENIX-7172) | Support HBase 2.6 |  Major | core |
| [PHOENIX-7001](https://issues.apache.org/jira/browse/PHOENIX-7001) | Change Data Capture leveraging Max Lookback and Uncovered Indexes |  Major | . |
| [PHOENIX-7326](https://issues.apache.org/jira/browse/PHOENIX-7326) | Simplify LockManager and make it more efficient |  Major | . |
| [PHOENIX-7314](https://issues.apache.org/jira/browse/PHOENIX-7314) | Enable CompactionScanner for flushes and minor compaction |  Major | . |
| [PHOENIX-7320](https://issues.apache.org/jira/browse/PHOENIX-7320) | Upgrade HBase 2.4 to 2.4.18 |  Major | core |
| [PHOENIX-7319](https://issues.apache.org/jira/browse/PHOENIX-7319) | Leverage Bloom Filters to improve performance on write path |  Major | . |
| [PHOENIX-7306](https://issues.apache.org/jira/browse/PHOENIX-7306) | Metadata lookup should be permitted only within query timeout |  Major | . |
| [PHOENIX-7248](https://issues.apache.org/jira/browse/PHOENIX-7248) | Add logging excludes to hadoop-mapreduce-client-app and hadoop-mapreduce-client-jobclient |  Major | test |
| [PHOENIX-7006](https://issues.apache.org/jira/browse/PHOENIX-7006) | Configure maxLookbackAge at table level |  Major | . |
| [PHOENIX-7236](https://issues.apache.org/jira/browse/PHOENIX-7236) | Fix release scripts and Update version to 5.3.0 |  Major | . |
| [PHOENIX-7229](https://issues.apache.org/jira/browse/PHOENIX-7229) | Leverage bloom filters for single key point lookups |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7239](https://issues.apache.org/jira/browse/PHOENIX-7239) | When an uncovered index has different number of salt buckets than the data table, query returns no data |  Major | . |
| [PHOENIX-7391](https://issues.apache.org/jira/browse/PHOENIX-7391) | Do not return tenant\_id column when getting columns list using PhoenixRuntime.generateColumnInfo |  Minor | core |
| [PHOENIX-7638](https://issues.apache.org/jira/browse/PHOENIX-7638) | Creating a large number of views leads to OS thread exhaustion |  Major | . |
| [PHOENIX-7666](https://issues.apache.org/jira/browse/PHOENIX-7666) | Index query failure with SkipScanFilter |  Major | . |
| [PHOENIX-7691](https://issues.apache.org/jira/browse/PHOENIX-7691) | Bson update expression function evaluation should not exit early when the expression is an empty bson doc |  Major | . |
| [PHOENIX-7687](https://issues.apache.org/jira/browse/PHOENIX-7687) | Wrong Hadoop version used for Hbase 2.5.0 profile |  Major | . |
| [PHOENIX-7628](https://issues.apache.org/jira/browse/PHOENIX-7628) | Don't add Apache Snapshot Maven Repo by Default |  Major | . |
| [PHOENIX-7674](https://issues.apache.org/jira/browse/PHOENIX-7674) | Fix Bson3IT for HBase 2.6 |  Critical | core, test |
| [PHOENIX-7664](https://issues.apache.org/jira/browse/PHOENIX-7664) | Remove EmptyColumnOnlyFilter and FirstKeyOnlyFilter for CDC scanners |  Major | . |
| [PHOENIX-7660](https://issues.apache.org/jira/browse/PHOENIX-7660) | traceserver.py has a blank line that causes a syntax error |  Minor | python |
| [PHOENIX-7654](https://issues.apache.org/jira/browse/PHOENIX-7654) | Restrict BSON datatype in composite PK as last part of the pk column |  Major | . |
| [PHOENIX-7645](https://issues.apache.org/jira/browse/PHOENIX-7645) | HighAvailabilityGroup can leak zookeeper connections |  Critical | . |
| [PHOENIX-7637](https://issues.apache.org/jira/browse/PHOENIX-7637) | Add jcl-over-slf4j dependency to phoenix-pherf pom |  Major | . |
| [PHOENIX-7615](https://issues.apache.org/jira/browse/PHOENIX-7615) | NPE when NULL is bound to a CASE inside ON DUPLICATE KEY |  Major | . |
| [PHOENIX-7636](https://issues.apache.org/jira/browse/PHOENIX-7636) | CDC on table with case-sensitive pk columns fails to read change records |  Major | . |
| [PHOENIX-7619](https://issues.apache.org/jira/browse/PHOENIX-7619) | Excess HFiles are being read to look for more than required column versions |  Major | . |
| [PHOENIX-7629](https://issues.apache.org/jira/browse/PHOENIX-7629) | Invalidate already closed CQSI in case of URL change for Phoenix HA Connections |  Major | . |
| [PHOENIX-7468](https://issues.apache.org/jira/browse/PHOENIX-7468) | NPE in SchemaExtractionTool on salted table |  Major | . |
| [PHOENIX-7627](https://issues.apache.org/jira/browse/PHOENIX-7627) | Atomic Delete return row fails for case-sensitive schema and table names |  Major | . |
| [PHOENIX-7358](https://issues.apache.org/jira/browse/PHOENIX-7358) | Upsert select result wrong when use order by in query |  Major | core |
| [PHOENIX-7618](https://issues.apache.org/jira/browse/PHOENIX-7618) | Missing jul-to-slf4j.jar in the lib folder of the assembled tar file |  Major | core |
| [PHOENIX-7616](https://issues.apache.org/jira/browse/PHOENIX-7616) | NPE when there are conditional expressions on indexed columns |  Major | . |
| [PHOENIX-7617](https://issues.apache.org/jira/browse/PHOENIX-7617) | BSON serialization should retain ByteBuffer offset |  Major | . |
| [PHOENIX-7599](https://issues.apache.org/jira/browse/PHOENIX-7599) | Count of rows scanned metric does not work for Uncovered Indexes |  Major | . |
| [PHOENIX-7610](https://issues.apache.org/jira/browse/PHOENIX-7610) | Using CAST() on pk columns always result in full table scan |  Critical | . |
| [PHOENIX-7614](https://issues.apache.org/jira/browse/PHOENIX-7614) | Atomic upsert return result fails when table name is case sensitive |  Major | . |
| [PHOENIX-7609](https://issues.apache.org/jira/browse/PHOENIX-7609) | CDC creation fails when data table has case sensitive name |  Major | . |
| [PHOENIX-7608](https://issues.apache.org/jira/browse/PHOENIX-7608) | Partial index creation fails when data table has case sensitive name |  Major | . |
| [PHOENIX-7534](https://issues.apache.org/jira/browse/PHOENIX-7534) | ConnectionIT.testRPCConnections  is very flakey |  Major | core |
| [PHOENIX-7574](https://issues.apache.org/jira/browse/PHOENIX-7574) | Phoenix Compaction doesn't correctly handle DeleteFamilyVersion markers |  Major | . |
| [PHOENIX-7533](https://issues.apache.org/jira/browse/PHOENIX-7533) | Fix broken compatibility for Zookeeper based ConnectionInfo |  Major | . |
| [PHOENIX-7580](https://issues.apache.org/jira/browse/PHOENIX-7580) | Data in last salt bucket is not being scanned for range scan |  Blocker | . |
| [PHOENIX-7543](https://issues.apache.org/jira/browse/PHOENIX-7543) | Wrong result returned when query is served by index and some columns are null |  Major | . |
| [PHOENIX-7577](https://issues.apache.org/jira/browse/PHOENIX-7577) | Update commons-lang3 to 3.17.0 |  Major | connectors, core |
| [PHOENIX-7558](https://issues.apache.org/jira/browse/PHOENIX-7558) | Release script docker image cannot be built |  Blocker | . |
| [PHOENIX-7527](https://issues.apache.org/jira/browse/PHOENIX-7527) | NPE thrown when extract table schema using sqlline. |  Major | . |
| [PHOENIX-7550](https://issues.apache.org/jira/browse/PHOENIX-7550) | Update OWASP plugin to 12.1.0 |  Major | . |
| [PHOENIX-7470](https://issues.apache.org/jira/browse/PHOENIX-7470) | Simplify Kerberos login in ConnectionInfo |  Major | core |
| [PHOENIX-7479](https://issues.apache.org/jira/browse/PHOENIX-7479) | Minor logging fix for ParallelPhoenixConnection exception report |  Trivial | phoenix |
| [PHOENIX-7501](https://issues.apache.org/jira/browse/PHOENIX-7501) | GC issues in TTLRegionScanner when gap is more than TTL |  Major | . |
| [PHOENIX-7516](https://issues.apache.org/jira/browse/PHOENIX-7516) | TableNotFoundException thrown when not specify schema in CsvBulkloadTool |  Major | . |
| [PHOENIX-7519](https://issues.apache.org/jira/browse/PHOENIX-7519) | Ancestor Last ddl timestamp map is empty for local indexes |  Major | . |
| [PHOENIX-7518](https://issues.apache.org/jira/browse/PHOENIX-7518) | Flapper test in CDCQueryIT |  Major | . |
| [PHOENIX-7509](https://issues.apache.org/jira/browse/PHOENIX-7509) | Metadata Cache should handle tables which have LAST\_DDL\_TIMESTAMP column null in syscat |  Major | . |
| [PHOENIX-7510](https://issues.apache.org/jira/browse/PHOENIX-7510) | VARBINARY\_ENCODED should support using hex format in WHERE clause |  Major | . |
| [PHOENIX-7477](https://issues.apache.org/jira/browse/PHOENIX-7477) | Set java.util.logging.config.class in scripts |  Major | core, queryserver |
| [PHOENIX-7492](https://issues.apache.org/jira/browse/PHOENIX-7492) | Subquery with join and union not working together when one side of union finds no result |  Major | core |
| [PHOENIX-7494](https://issues.apache.org/jira/browse/PHOENIX-7494) | NPE thrown when enable applyTimeZone |  Major | core |
| [PHOENIX-7464](https://issues.apache.org/jira/browse/PHOENIX-7464) | Performance Regression in Connection Initialization due to Configuration Handling in ConnInfo |  Major | . |
| [PHOENIX-7497](https://issues.apache.org/jira/browse/PHOENIX-7497) | ExplainPlanV2 regionserver location is not updated for less than max regions |  Major | . |
| [PHOENIX-7488](https://issues.apache.org/jira/browse/PHOENIX-7488) | Use central repo, not repository.apache.org |  Major | connectors, omid, phoenix, thirdparty |
| [PHOENIX-7491](https://issues.apache.org/jira/browse/PHOENIX-7491) | Mixed-cased alias doesn't work in select statement of “INNER JOIN” |  Major | phoenix |
| [PHOENIX-7268](https://issues.apache.org/jira/browse/PHOENIX-7268) | Namespace mapped system catalog table not snapshotted before upgrade |  Major | core |
| [PHOENIX-7448](https://issues.apache.org/jira/browse/PHOENIX-7448) | Phoenix Compaction can miss retaining some cells when there is a gap more than TTL |  Major | . |
| [PHOENIX-7447](https://issues.apache.org/jira/browse/PHOENIX-7447) | Update maven-shade-plugin to 3.6.0 |  Blocker | connectors, core, queryserver |
| [PHOENIX-7422](https://issues.apache.org/jira/browse/PHOENIX-7422) | Flapper TableTTLIT#testMinorCompactionShouldNotRetainCellsWhenMaxLookbackIsDisabled |  Major | . |
| [PHOENIX-7440](https://issues.apache.org/jira/browse/PHOENIX-7440) | TableSnapshotReadsMapReduceIT fails with HBase 2.6.1 |  Major | core |
| [PHOENIX-7410](https://issues.apache.org/jira/browse/PHOENIX-7410) | Escape metadata attribute values/property names in SchemaTool synthesis |  Major | . |
| [PHOENIX-7282](https://issues.apache.org/jira/browse/PHOENIX-7282) | Incorrect data in index column for corresponding BIGINT type column in data table |  Major | . |
| [PHOENIX-7427](https://issues.apache.org/jira/browse/PHOENIX-7427) | PHOENIX-7418 breaks backwards compatibility tests |  Critical | core |
| [PHOENIX-7418](https://issues.apache.org/jira/browse/PHOENIX-7418) | SystemExitRule errors out because of SecurityManager deprecation / removal |  Critical | core, test |
| [PHOENIX-7431](https://issues.apache.org/jira/browse/PHOENIX-7431) | Duplicate dependency on jackson in phoenix-core |  Major | . |
| [PHOENIX-7429](https://issues.apache.org/jira/browse/PHOENIX-7429) | End2EndTestDriver should not extend AbstractHBaseTool |  Critical | core, test |
| [PHOENIX-7421](https://issues.apache.org/jira/browse/PHOENIX-7421) | Checkstyle plugin fails in phoenix-client-embedded module |  Minor | test |
| [PHOENIX-7420](https://issues.apache.org/jira/browse/PHOENIX-7420) | Bump commons-io:commons-io from 2.11.0 to 2.14.0 |  Major | core, queryserver |
| [PHOENIX-7081](https://issues.apache.org/jira/browse/PHOENIX-7081) | Replace /tmp with {java.io.tmpdir} in tests |  Minor | core |
| [PHOENIX-7402](https://issues.apache.org/jira/browse/PHOENIX-7402) | Even if a row is updated within TTL its getting expired partially |  Critical | . |
| [PHOENIX-7406](https://issues.apache.org/jira/browse/PHOENIX-7406) | Index creation fails when creating a partial index on a table which was created with column names in double quotes |  Major | . |
| [PHOENIX-7405](https://issues.apache.org/jira/browse/PHOENIX-7405) | Update Jetty to 9.4.56.v20240826 |  Major | . |
| [PHOENIX-7401](https://issues.apache.org/jira/browse/PHOENIX-7401) | Fix GitHub project homepage in .asf.yaml |  Critical | core |
| [PHOENIX-7387](https://issues.apache.org/jira/browse/PHOENIX-7387) | SnapshotScanner's next method is ignoring the boolean value from hbase's nextRaw method |  Major | core |
| [PHOENIX-7367](https://issues.apache.org/jira/browse/PHOENIX-7367) | Snapshot based mapreduce jobs fails after HBASE-28401 |  Major | . |
| [PHOENIX-7363](https://issues.apache.org/jira/browse/PHOENIX-7363) | Protect server side metadata cache updates for the given PTable |  Blocker | . |
| [PHOENIX-7369](https://issues.apache.org/jira/browse/PHOENIX-7369) | Avoid redundant recursive getTable() RPC calls |  Blocker | . |
| [PHOENIX-7368](https://issues.apache.org/jira/browse/PHOENIX-7368) | Rename commons-lang.version maven property to commons-lang3.version |  Trivial | . |
| [PHOENIX-7373](https://issues.apache.org/jira/browse/PHOENIX-7373) | Fix test hangs with HBase 2.6 caused by PHOENIX-6978 |  Critical | core |
| [PHOENIX-7359](https://issues.apache.org/jira/browse/PHOENIX-7359) | BackwardCompatibilityIT throws NPE with Hbase 2.6 profile |  Major | core |
| [PHOENIX-7353](https://issues.apache.org/jira/browse/PHOENIX-7353) | Disable remote procedure delay in TransformToolIT |  Major | core |
| [PHOENIX-7348](https://issues.apache.org/jira/browse/PHOENIX-7348) | Default INCLUDE scopes given in CREATE CDC are not getting recognized |  Minor | . |
| [PHOENIX-7316](https://issues.apache.org/jira/browse/PHOENIX-7316) | Need close more Statements |  Major | . |
| [PHOENIX-7336](https://issues.apache.org/jira/browse/PHOENIX-7336) | Upgrade org.iq80.snappy:snappy version to 0.5 |  Major | . |
| [PHOENIX-7337](https://issues.apache.org/jira/browse/PHOENIX-7337) | Centralize and upgrade com.jayway.jsonpath:json-path version from 2.6.0 to 2.9.0 |  Major | core |
| [PHOENIX-7331](https://issues.apache.org/jira/browse/PHOENIX-7331) | Fix incompatibilities with HBASE-28644 |  Critical | core |
| [PHOENIX-7328](https://issues.apache.org/jira/browse/PHOENIX-7328) | Fix flapping ConcurrentMutationsExtendedIT#testConcurrentUpserts |  Major | . |
| [PHOENIX-7192](https://issues.apache.org/jira/browse/PHOENIX-7192) | IDE shows errors on JSON comment |  Minor | core |
| [PHOENIX-7313](https://issues.apache.org/jira/browse/PHOENIX-7313) | All cell versions should not be retained during flushes and minor compaction when maxlookback is disabled |  Major | . |
| [PHOENIX-7250](https://issues.apache.org/jira/browse/PHOENIX-7250) | Fix HBase log level in tests |  Major | core |
| [PHOENIX-7245](https://issues.apache.org/jira/browse/PHOENIX-7245) | NPE in Phoenix Coproc leading to Region Server crash |  Major | phoenix |
| [PHOENIX-7290](https://issues.apache.org/jira/browse/PHOENIX-7290) | Cannot load or instantiate class org.apache.phoenix.query.DefaultGuidePostsCacheFactory from SquirrelSQL |  Major | core |
| [PHOENIX-7302](https://issues.apache.org/jira/browse/PHOENIX-7302) | Server Paging doesn't work on scans with limit |  Major | . |
| [PHOENIX-7299](https://issues.apache.org/jira/browse/PHOENIX-7299) | ScanningResultIterator should not time out a query after receiving a valid result |  Major | . |
| [PHOENIX-7255](https://issues.apache.org/jira/browse/PHOENIX-7255) | Non-existent artifacts referred in compatible\_client\_versions.json |  Major | core |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7655](https://issues.apache.org/jira/browse/PHOENIX-7655) | Remove WALRecoveryRegionPostOpenIT |  Major | . |
| [PHOENIX-7339](https://issues.apache.org/jira/browse/PHOENIX-7339) | HBase flushes with custom clock needs to disable remote procedure delay |  Major | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7409](https://issues.apache.org/jira/browse/PHOENIX-7409) | Tests in CDCQueryIT are flapping when using a different number of salt buckets for CDC and data table |  Major | . |
| [PHOENIX-7598](https://issues.apache.org/jira/browse/PHOENIX-7598) | Add ITs for client write behavior when they encounter ATS state. |  Major | . |
| [PHOENIX-7612](https://issues.apache.org/jira/browse/PHOENIX-7612) | Fix Cell references in IndexRegionObserver |  Major | . |
| [PHOENIX-7573](https://issues.apache.org/jira/browse/PHOENIX-7573) | Backward Compatibility Issues in Phoenix TTL |  Blocker | . |
| [PHOENIX-7443](https://issues.apache.org/jira/browse/PHOENIX-7443) | Add Spotless to the pre-commit checks |  Major | phoenix |
| [PHOENIX-7442](https://issues.apache.org/jira/browse/PHOENIX-7442) | Apply Spotless to reformat the entire codebase |  Major | phoenix |
| [PHOENIX-7675](https://issues.apache.org/jira/browse/PHOENIX-7675) | Update spotless plugin to also format IT classes |  Major | . |
| [PHOENIX-7676](https://issues.apache.org/jira/browse/PHOENIX-7676) |  Update checkstyle checker.xml based on spotless rules |  Major | . |
| [PHOENIX-7658](https://issues.apache.org/jira/browse/PHOENIX-7658) | CDC event for TTL\_DELETE to exclude pre-image if PRE scope is not selected |  Major | . |
| [PHOENIX-7476](https://issues.apache.org/jira/browse/PHOENIX-7476) | HBase 3 compatibility changes for Filters, ByteStringer, and Paging |  Major | phoenix |
| [PHOENIX-7481](https://issues.apache.org/jira/browse/PHOENIX-7481) | HBase 3 compatibility changes: Cleanup deprecated APIs, HTable and HTableDescriptor |  Major | phoenix |
| [PHOENIX-7478](https://issues.apache.org/jira/browse/PHOENIX-7478) | HBase 3 compatibility changes: Replace ClusterConnection with Connection API |  Major | phoenix |
| [PHOENIX-7441](https://issues.apache.org/jira/browse/PHOENIX-7441) | Integrate the Spotless plugin and update the code template |  Major | phoenix |
| [PHOENIX-7383](https://issues.apache.org/jira/browse/PHOENIX-7383) | Unify in-memory representation of Conditional TTL expressions and Literal TTL values |  Major | . |
| [PHOENIX-7499](https://issues.apache.org/jira/browse/PHOENIX-7499) | Update stream metadata when data table regions merge |  Major | . |
| [PHOENIX-7505](https://issues.apache.org/jira/browse/PHOENIX-7505) | HBase 3 compatibility changes: Update zookeeper handling |  Major | phoenix |
| [PHOENIX-7500](https://issues.apache.org/jira/browse/PHOENIX-7500) | Add PARENT\_PARTITION\_ID to SYSTEM.CDC\_STREAM table's composite pk |  Major | . |
| [PHOENIX-7460](https://issues.apache.org/jira/browse/PHOENIX-7460) | Update stream metadata when a data table region splits |  Major | . |
| [PHOENIX-7459](https://issues.apache.org/jira/browse/PHOENIX-7459) | Bootstrap stream metadata when CDC is enabled/created on a table |  Major | . |
| [PHOENIX-7458](https://issues.apache.org/jira/browse/PHOENIX-7458) | Create new SYSTEM tables for tracking CDC Stream metadata |  Major | . |
| [PHOENIX-7343](https://issues.apache.org/jira/browse/PHOENIX-7343) | Support for complex types in CDC |  Major | . |
| [PHOENIX-4555](https://issues.apache.org/jira/browse/PHOENIX-4555) | Only mark view as updatable if rows cannot overlap with other updatable views |  Major | . |
| [PHOENIX-7329](https://issues.apache.org/jira/browse/PHOENIX-7329) | Change TTL column type to VARCHAR in syscat |  Major | . |
| [PHOENIX-7318](https://issues.apache.org/jira/browse/PHOENIX-7318) | Support JSON\_MODIFY in Upserts |  Major | . |
| [PHOENIX-7072](https://issues.apache.org/jira/browse/PHOENIX-7072) | Implement json\_modify function on the json object as Atomic Upserts |  Major | . |
| [PHOENIX-7243](https://issues.apache.org/jira/browse/PHOENIX-7243) | Add connectionType property to ConnectionInfo class. |  Major | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7600](https://issues.apache.org/jira/browse/PHOENIX-7600) | Replace commons-logging with slf4j in Phoenix related repos |  Major | queryserver |
| [PHOENIX-7539](https://issues.apache.org/jira/browse/PHOENIX-7539) | Update default HBase 2.5 version to 2.5.11 |  Major | core |
| [PHOENIX-7532](https://issues.apache.org/jira/browse/PHOENIX-7532) | Update default Hbase 2.6 version to 2.6.2 |  Major | core |
| [PHOENIX-7475](https://issues.apache.org/jira/browse/PHOENIX-7475) |  Bump commons-io:commons-io from 2.14.0 to 2.18.0 |  Major | core, queryserver |
| [PHOENIX-7304](https://issues.apache.org/jira/browse/PHOENIX-7304) | Add support for getting list of View Index IDs for any View Index Physical Table |   | . |
| [PHOENIX-7417](https://issues.apache.org/jira/browse/PHOENIX-7417) | Remove commons-collections dependency |  Major | core |
| [PHOENIX-7446](https://issues.apache.org/jira/browse/PHOENIX-7446) | Document GPG passphrase handling in release process |  Major | . |
| [PHOENIX-7439](https://issues.apache.org/jira/browse/PHOENIX-7439) | Bump default HBase 2.6 version to 2.6.1 |  Major | . |
| [PHOENIX-7362](https://issues.apache.org/jira/browse/PHOENIX-7362) | Update owasp plugin to 10.0.2 |  Major | connectors, core, queryserver |
| [PHOENIX-7371](https://issues.apache.org/jira/browse/PHOENIX-7371) | Update Hbase 2.5 version to 2.5.10 |  Major | . |
| [PHOENIX-7365](https://issues.apache.org/jira/browse/PHOENIX-7365) | ExplainPlanV2 should get trimmed list for regionserver location |  Major | . |
