
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

## Release 5.2.1 - Unreleased (as of 2024-10-22)



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7428](https://issues.apache.org/jira/browse/PHOENIX-7428) | Add usable error message in BackwardCompatibilityIT |  Minor | core, test |
| [PHOENIX-7432](https://issues.apache.org/jira/browse/PHOENIX-7432) | getTable for PHYSICAL\_TABLE link should use common utility |  Critical | . |
| [PHOENIX-7416](https://issues.apache.org/jira/browse/PHOENIX-7416) | Bump Avro dependency version to 1.11.4 |  Major | . |
| [PHOENIX-6982](https://issues.apache.org/jira/browse/PHOENIX-6982) | Exclude Maven descriptors from shaded JARs |  Major | . |
| [PHOENIX-7395](https://issues.apache.org/jira/browse/PHOENIX-7395) | Metadata Cache metrics at server and client side |  Major | . |
| [PHOENIX-7397](https://issues.apache.org/jira/browse/PHOENIX-7397) | Optimize ClientAggregatePlan/ClientScanPlan when inner query is UnionPlan |  Major | core |
| [PHOENIX-7404](https://issues.apache.org/jira/browse/PHOENIX-7404) | Build the HBase 2.5+ profiles with Hadoop 3.3.6 |  Major | . |
| [PHOENIX-7394](https://issues.apache.org/jira/browse/PHOENIX-7394) | MaxPhoenixColumnSizeExceededException should not print rowkey |  Major | . |
| [PHOENIX-7393](https://issues.apache.org/jira/browse/PHOENIX-7393) | Update transitive dependency of woodstox-core to 5.4.0 |  Major | . |
| [PHOENIX-7386](https://issues.apache.org/jira/browse/PHOENIX-7386) | Override UPDATE\_CACHE\_FREQUENCY if table has disabled indexes |  Major | . |
| [PHOENIX-7385](https://issues.apache.org/jira/browse/PHOENIX-7385) | Fix MetadataGetTableReadLockIT flapper |  Major | . |
| [PHOENIX-7379](https://issues.apache.org/jira/browse/PHOENIX-7379) | Improve handling of concurrent index mutations with the same timestamp |  Major | . |
| [PHOENIX-7333](https://issues.apache.org/jira/browse/PHOENIX-7333) | Add HBase 2.6 profile to multibranch Jenkins job |  Minor | core |
| [PHOENIX-7309](https://issues.apache.org/jira/browse/PHOENIX-7309) | Support specifying splits.txt file while creating a table. |  Major | . |
| [PHOENIX-7352](https://issues.apache.org/jira/browse/PHOENIX-7352) | Improve OrderPreservingTracker to support extracting partial ordering columns for TupleProjectionPlan |  Major | core |
| [PHOENIX-6066](https://issues.apache.org/jira/browse/PHOENIX-6066) | MetaDataEndpointImpl.doGetTable should acquire a readLock instead of an exclusive writeLock on the table header row |  Major | . |
| [PHOENIX-7356](https://issues.apache.org/jira/browse/PHOENIX-7356) | Centralize and update versions for exclude-only dependencies |  Minor | core |
| [PHOENIX-7287](https://issues.apache.org/jira/browse/PHOENIX-7287) | Leverage bloom filters for multi-key point lookups |  Major | . |
| [PHOENIX-6714](https://issues.apache.org/jira/browse/PHOENIX-6714) | Return update status from Conditional Upserts |  Major | . |
| [PHOENIX-7303](https://issues.apache.org/jira/browse/PHOENIX-7303) | fix CVE-2024-29025 in netty package |  Major | phoenix |
| [PHOENIX-7130](https://issues.apache.org/jira/browse/PHOENIX-7130) | Support skipping of shade sources jar creation |  Minor | phoenix |
| [PHOENIX-7172](https://issues.apache.org/jira/browse/PHOENIX-7172) | Support HBase 2.6 |  Major | core |
| [PHOENIX-7326](https://issues.apache.org/jira/browse/PHOENIX-7326) | Simplify LockManager and make it more efficient |  Major | . |
| [PHOENIX-7314](https://issues.apache.org/jira/browse/PHOENIX-7314) | Enable CompactionScanner for flushes and minor compaction |  Major | . |
| [PHOENIX-7320](https://issues.apache.org/jira/browse/PHOENIX-7320) | Upgrade HBase 2.4 to 2.4.18 |  Major | core |
| [PHOENIX-7319](https://issues.apache.org/jira/browse/PHOENIX-7319) | Leverage Bloom Filters to improve performance on write path |  Major | . |
| [PHOENIX-7306](https://issues.apache.org/jira/browse/PHOENIX-7306) | Metadata lookup should be permitted only within query timeout |  Major | . |
| [PHOENIX-7248](https://issues.apache.org/jira/browse/PHOENIX-7248) | Add logging excludes to hadoop-mapreduce-client-app and hadoop-mapreduce-client-jobclient |  Major | test |
| [PHOENIX-7229](https://issues.apache.org/jira/browse/PHOENIX-7229) | Leverage bloom filters for single key point lookups |  Major | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7440](https://issues.apache.org/jira/browse/PHOENIX-7440) | TableSnapshotReadsMapReduceIT fails with HBase 2.6.1 |  Major | core |
| [PHOENIX-7282](https://issues.apache.org/jira/browse/PHOENIX-7282) | Incorrect data in index column for corresponding BIGINT type column in data table |  Major | . |
| [PHOENIX-7427](https://issues.apache.org/jira/browse/PHOENIX-7427) | PHOENIX-7418 breaks backwards compatibility tests |  Critical | core |
| [PHOENIX-7418](https://issues.apache.org/jira/browse/PHOENIX-7418) | SystemExitRule errors out because of SecurityManager deprecation / removal |  Critical | core, test |
| [PHOENIX-7429](https://issues.apache.org/jira/browse/PHOENIX-7429) | End2EndTestDriver should not extend AbstractHBaseTool |  Critical | core, test |
| [PHOENIX-7421](https://issues.apache.org/jira/browse/PHOENIX-7421) | Checkstyle plugin fails in phoenix-client-embedded module |  Minor | test |
| [PHOENIX-7420](https://issues.apache.org/jira/browse/PHOENIX-7420) | Bump commons-io:commons-io from 2.11.0 to 2.14.0 |  Major | core, queryserver |
| [PHOENIX-7081](https://issues.apache.org/jira/browse/PHOENIX-7081) | Replace /tmp with {java.io.tmpdir} in tests |  Minor | core |
| [PHOENIX-7402](https://issues.apache.org/jira/browse/PHOENIX-7402) | Even if a row is updated within TTL its getting expired partially |  Critical | . |
| [PHOENIX-7406](https://issues.apache.org/jira/browse/PHOENIX-7406) | Index creation fails when creating a partial index on a table which was created with column names in double quotes |  Major | . |
| [PHOENIX-7405](https://issues.apache.org/jira/browse/PHOENIX-7405) | Update Jetty to 9.4.56.v20240826 |  Major | . |
| [PHOENIX-7387](https://issues.apache.org/jira/browse/PHOENIX-7387) | SnapshotScanner's next method is ignoring the boolean value from hbase's nextRaw method |  Major | core |
| [PHOENIX-7367](https://issues.apache.org/jira/browse/PHOENIX-7367) | Snapshot based mapreduce jobs fails after HBASE-28401 |  Major | . |
| [PHOENIX-7363](https://issues.apache.org/jira/browse/PHOENIX-7363) | Protect server side metadata cache updates for the given PTable |  Blocker | . |
| [PHOENIX-7369](https://issues.apache.org/jira/browse/PHOENIX-7369) | Avoid redundant recursive getTable() RPC calls |  Blocker | . |
| [PHOENIX-7368](https://issues.apache.org/jira/browse/PHOENIX-7368) | Rename commons-lang.version maven property to commons-lang3.version |  Trivial | . |
| [PHOENIX-7359](https://issues.apache.org/jira/browse/PHOENIX-7359) | BackwardCompatibilityIT throws NPE with Hbase 2.6 profile |  Major | core |
| [PHOENIX-7353](https://issues.apache.org/jira/browse/PHOENIX-7353) | Disable remote procedure delay in TransformToolIT |  Major | core |
| [PHOENIX-7316](https://issues.apache.org/jira/browse/PHOENIX-7316) | Need close more Statements |  Major | . |
| [PHOENIX-7336](https://issues.apache.org/jira/browse/PHOENIX-7336) | Upgrade org.iq80.snappy:snappy version to 0.5 |  Major | . |
| [PHOENIX-7331](https://issues.apache.org/jira/browse/PHOENIX-7331) | Fix incompatibilities with HBASE-28644 |  Critical | core |
| [PHOENIX-7328](https://issues.apache.org/jira/browse/PHOENIX-7328) | Fix flapping ConcurrentMutationsExtendedIT#testConcurrentUpserts |  Major | . |
| [PHOENIX-7313](https://issues.apache.org/jira/browse/PHOENIX-7313) | All cell versions should not be retained during flushes and minor compaction when maxlookback is disabled |  Major | . |
| [PHOENIX-7250](https://issues.apache.org/jira/browse/PHOENIX-7250) | Fix HBase log level in tests |  Major | core |
| [PHOENIX-7245](https://issues.apache.org/jira/browse/PHOENIX-7245) | NPE in Phoenix Coproc leading to Region Server crash |  Major | phoenix |
| [PHOENIX-7290](https://issues.apache.org/jira/browse/PHOENIX-7290) | Cannot load or instantiate class org.apache.phoenix.query.DefaultGuidePostsCacheFactory from SquirrelSQL |  Major | core |
| [PHOENIX-7302](https://issues.apache.org/jira/browse/PHOENIX-7302) | Server Paging doesn't work on scans with limit |  Major | . |
| [PHOENIX-7299](https://issues.apache.org/jira/browse/PHOENIX-7299) | ScanningResultIterator should not time out a query after receiving a valid result |  Major | . |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7339](https://issues.apache.org/jira/browse/PHOENIX-7339) | HBase flushes with custom clock needs to disable remote procedure delay |  Major | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7041](https://issues.apache.org/jira/browse/PHOENIX-7041) | Populate ROW\_KEY\_PREFIX column when creating views |  Major | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7439](https://issues.apache.org/jira/browse/PHOENIX-7439) | Bump default HBase 2.6 version to 2.6.1 |  Major | . |
| [PHOENIX-7362](https://issues.apache.org/jira/browse/PHOENIX-7362) | Update owasp plugin to 10.0.2 |  Major | connectors, core, queryserver |
| [PHOENIX-7371](https://issues.apache.org/jira/browse/PHOENIX-7371) | Update Hbase 2.5 version to 2.5.10 |  Major | . |
| [PHOENIX-7365](https://issues.apache.org/jira/browse/PHOENIX-7365) | ExplainPlanV2 should get trimmed list for regionserver location |  Major | . |
| [PHOENIX-7335](https://issues.apache.org/jira/browse/PHOENIX-7335) |  Bump Phoenix version to 5.2.1-SNAPSHOT |  Major | . |



## Release 5.2.0 - Unreleased (as of 2024-04-06)



### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7038](https://issues.apache.org/jira/browse/PHOENIX-7038) | Implement Connection Query Service Metrics |  Major | . |
| [PHOENIX-6973](https://issues.apache.org/jira/browse/PHOENIX-6973) | Add option to CREATE TABLE to skip verification of HBase table |  Major | core |
| [PHOENIX-6491](https://issues.apache.org/jira/browse/PHOENIX-6491) | Phoenix High Availability |  Major | core |
| [PHOENIX-6692](https://issues.apache.org/jira/browse/PHOENIX-6692) | Add HBase 2.5 support |  Major | core |
| [PHOENIX-6681](https://issues.apache.org/jira/browse/PHOENIX-6681) | Enable new indexes to be optionally created in CREATE\_DISABLED state |  Major | . |
| [PHOENIX-6413](https://issues.apache.org/jira/browse/PHOENIX-6413) | Having cannot resolve alias |  Major | . |
| [PHOENIX-6405](https://issues.apache.org/jira/browse/PHOENIX-6405) | Disallow bulk loading into non-empty tables with global secondary indexes |  Major | core |
| [PHOENIX-6457](https://issues.apache.org/jira/browse/PHOENIX-6457) | Optionally store schema version string in SYSTEM.CATALOG |  Major | . |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7253](https://issues.apache.org/jira/browse/PHOENIX-7253) | Metadata lookup performance improvement for range scan queries |  Critical | . |
| [PHOENIX-7275](https://issues.apache.org/jira/browse/PHOENIX-7275) | Update HBase 2.5 default version to 2.5.8 |  Minor | core |
| [PHOENIX-7258](https://issues.apache.org/jira/browse/PHOENIX-7258) | Query Optimizer should pick Index hint even for point lookup queries |  Major | . |
| [PHOENIX-7229](https://issues.apache.org/jira/browse/PHOENIX-7229) | Leverage bloom filters for single key point lookups |  Major | . |
| [PHOENIX-7230](https://issues.apache.org/jira/browse/PHOENIX-7230) | Optimize rpc call to master if all indexes are migrated to new coprocs |  Major | . |
| [PHOENIX-7216](https://issues.apache.org/jira/browse/PHOENIX-7216) | Bump Hadoop version to 3.2.4 for 2.5.x profile |  Major | core |
| [PHOENIX-7144](https://issues.apache.org/jira/browse/PHOENIX-7144) | TableLevel Phoenix Metrics returns base tableName when queried for Index Table. |  Major | . |
| [PHOENIX-7220](https://issues.apache.org/jira/browse/PHOENIX-7220) | Make HBase 2.5 profile the default |  Major | core |
| [PHOENIX-7218](https://issues.apache.org/jira/browse/PHOENIX-7218) | Drop HBase 2.4.0 support in 5.2 |  Major | core |
| [PHOENIX-7157](https://issues.apache.org/jira/browse/PHOENIX-7157) | Upgrade to phoenix-thirdparty 2.1.0 |  Major | core |
| [PHOENIX-7213](https://issues.apache.org/jira/browse/PHOENIX-7213) | Add option for unlimited phoenix.query.QueueSize |  Major | . |
| [PHOENIX-7188](https://issues.apache.org/jira/browse/PHOENIX-7188) | Remove Omid TTable.getTableDescriptor() calls |  Critical | core |
| [PHOENIX-7106](https://issues.apache.org/jira/browse/PHOENIX-7106) | Data Integrity issues due to invalid rowkeys returned by various coprocessors |  Blocker | . |
| [PHOENIX-7187](https://issues.apache.org/jira/browse/PHOENIX-7187) | Improvement of Integration test case with Explain Plan for Partial Index |  Major | . |
| [PHOENIX-7181](https://issues.apache.org/jira/browse/PHOENIX-7181) | Do not declare commons-configuration2 dependency |  Major | core |
| [PHOENIX-7043](https://issues.apache.org/jira/browse/PHOENIX-7043) | Split FailoverPhoenixConnectionIT |  Minor | core |
| [PHOENIX-7156](https://issues.apache.org/jira/browse/PHOENIX-7156) | Run integration tests based on @Category |  Major | . |
| [PHOENIX-7140](https://issues.apache.org/jira/browse/PHOENIX-7140) | Update Apache Parent and Maven Plugin Versions in Core |  Major | core |
| [PHOENIX-6053](https://issues.apache.org/jira/browse/PHOENIX-6053) | Split Server Side Code into a Separate Module |  Major | core |
| [PHOENIX-7101](https://issues.apache.org/jira/browse/PHOENIX-7101) | Explain plan to output local index name if it is used |  Major | . |
| [PHOENIX-7063](https://issues.apache.org/jira/browse/PHOENIX-7063) | Track and account garbage collected phoenix connections |  Major | . |
| [PHOENIX-7067](https://issues.apache.org/jira/browse/PHOENIX-7067) | View indexes should be created only on non overlapping updatable views |  Major | . |
| [PHOENIX-6523](https://issues.apache.org/jira/browse/PHOENIX-6523) | Support for HBase Registry Implementations through Phoenix connection URL |  Major | core |
| [PHOENIX-7066](https://issues.apache.org/jira/browse/PHOENIX-7066) | Specify -Xms for tests |  Major | core |
| [PHOENIX-7055](https://issues.apache.org/jira/browse/PHOENIX-7055) | Usage improvements for sqline.py |  Major | . |
| [PHOENIX-7051](https://issues.apache.org/jira/browse/PHOENIX-7051) | Remove direct dependency on Google Guice from phoenix-core |  Critical | core |
| [PHOENIX-6900](https://issues.apache.org/jira/browse/PHOENIX-6900) | Enable paging feature when the phoenix query is made on a table snapshot. |  Major | . |
| [PHOENIX-7036](https://issues.apache.org/jira/browse/PHOENIX-7036) | Copy the Java version specific profiles for JVM options from HBase |  Major | connectors, core, queryserver |
| [PHOENIX-6907](https://issues.apache.org/jira/browse/PHOENIX-6907) | Explain Plan should output region locations with servers |  Major | . |
| [PHOENIX-6995](https://issues.apache.org/jira/browse/PHOENIX-6995) | HA client connections ignore additional jdbc params in the jdbc string |  Major | . |
| [PHOENIX-6986](https://issues.apache.org/jira/browse/PHOENIX-6986) | Add property to disable server merges for hinted uncovered indexes |  Major | core |
| [PHOENIX-6983](https://issues.apache.org/jira/browse/PHOENIX-6983) | Add hint to disable server merges for uncovered index queries |  Major | core |
| [PHOENIX-6984](https://issues.apache.org/jira/browse/PHOENIX-6984) | Fix fallback to skip-join-merge for hinted global indexes |  Major | phoenix |
| [PHOENIX-6981](https://issues.apache.org/jira/browse/PHOENIX-6981) | Bump Jackson version to 2.14.1 |  Major | . |
| [PHOENIX-6944](https://issues.apache.org/jira/browse/PHOENIX-6944) | Randomize mapper task ordering for Index MR tools |  Major | core |
| [PHOENIX-6560](https://issues.apache.org/jira/browse/PHOENIX-6560) | Rewrite dynamic SQL queries to use Preparedstatement |  Major | core |
| [PHOENIX-6918](https://issues.apache.org/jira/browse/PHOENIX-6918) | ScanningResultIterator should not retry when the query times out |  Major | . |
| [PHOENIX-6832](https://issues.apache.org/jira/browse/PHOENIX-6832) | Uncovered Global Secondary Indexes |  Major | . |
| [PHOENIX-6821](https://issues.apache.org/jira/browse/PHOENIX-6821) | Batching with auto-commit connections |  Major | . |
| [PHOENIX-6914](https://issues.apache.org/jira/browse/PHOENIX-6914) | Refactor phoenix-pherf to use java.time instead of Joda-Time |  Major | core |
| [PHOENIX-6881](https://issues.apache.org/jira/browse/PHOENIX-6881) | Implement the applicable Date/Time features from JDBC 4.2 |  Major | core |
| [PHOENIX-6899](https://issues.apache.org/jira/browse/PHOENIX-6899) | Query limit not enforced in UncoveredIndexRegionScanner |  Major | . |
| [PHOENIX-6880](https://issues.apache.org/jira/browse/PHOENIX-6880) | Remove dynamicFilter from BaseQueryPlan |  Trivial | . |
| [PHOENIX-6877](https://issues.apache.org/jira/browse/PHOENIX-6877) | Remove HBase 2.3 support from 5.2 |  Major | core |
| [PHOENIX-6889](https://issues.apache.org/jira/browse/PHOENIX-6889) | Improve extraction of ENCODED\_QUALIFIERs |  Major | core |
| [PHOENIX-6776](https://issues.apache.org/jira/browse/PHOENIX-6776) | Abort scans of closed connections at ScanningResultIterator |  Major | . |
| [PHOENIX-6655](https://issues.apache.org/jira/browse/PHOENIX-6655) | SYSTEM.SEQUENCE should have CACHE\_DATA\_ON\_WRITE set to true |  Major | . |
| [PHOENIX-6837](https://issues.apache.org/jira/browse/PHOENIX-6837) | Switch to Using the -hadoop3 HBase Artifacts Where Available |  Major | core |
| [PHOENIX-6761](https://issues.apache.org/jira/browse/PHOENIX-6761) | Phoenix Client Side Metadata Caching Improvement |  Major | . |
| [PHOENIX-6834](https://issues.apache.org/jira/browse/PHOENIX-6834) | Use Pooled HConnection for Server Side Upsert Select |  Major | core |
| [PHOENIX-6818](https://issues.apache.org/jira/browse/PHOENIX-6818) | Remove dependency on the i18n-util library |  Major | core |
| [PHOENIX-6827](https://issues.apache.org/jira/browse/PHOENIX-6827) | Update hbase-version to 2.4.15 in phoenix master branch |  Major | . |
| [PHOENIX-6826](https://issues.apache.org/jira/browse/PHOENIX-6826) | Don't invalidate meta cache if CQSI#getTableRegionLocation encounters IOException. |  Major | core |
| [PHOENIX-6561](https://issues.apache.org/jira/browse/PHOENIX-6561) | Allow pherf to intake phoenix Connection properties as argument. |  Minor | . |
| [PHOENIX-6749](https://issues.apache.org/jira/browse/PHOENIX-6749) | Replace deprecated HBase 1.x API calls |  Major | connectors, core, queryserver |
| [PHOENIX-6767](https://issues.apache.org/jira/browse/PHOENIX-6767) | Traversing through all the guideposts to prepare parallel scans is not required for salted tables when the query is point lookup |  Major | . |
| [PHOENIX-6779](https://issues.apache.org/jira/browse/PHOENIX-6779) | Account for connection attempted & failure metrics in all paths |  Major | . |
| [PHOENIX-6707](https://issues.apache.org/jira/browse/PHOENIX-6707) | Use Configuration.getLongBytes where applicable |  Minor | core |
| [PHOENIX-5274](https://issues.apache.org/jira/browse/PHOENIX-5274) | ConnectionQueryServiceImpl#ensureNamespaceCreated and ensureTableCreated should use HBase APIs that do not require ADMIN permissions for existence checks |  Major | . |
| [PHOENIX-6554](https://issues.apache.org/jira/browse/PHOENIX-6554) | Pherf CLI option long/short option names do not follow conventions |  Minor | core |
| [PHOENIX-6703](https://issues.apache.org/jira/browse/PHOENIX-6703) | Exclude Jetty and servlet-api from phoenix-client |  Major | core |
| [PHOENIX-6690](https://issues.apache.org/jira/browse/PHOENIX-6690) | Bump HBase 2.4 version to 2.4.11 |  Major | core |
| [PHOENIX-6588](https://issues.apache.org/jira/browse/PHOENIX-6588) | Update to phoenix-thirdparty 2.0.0 |  Major | core |
| [PHOENIX-6663](https://issues.apache.org/jira/browse/PHOENIX-6663) | Use batching when joining data table rows with uncovered local index rows |  Major | . |
| [PHOENIX-6501](https://issues.apache.org/jira/browse/PHOENIX-6501) | Use batching when joining data table rows with uncovered global index rows |  Major | . |
| [PHOENIX-6458](https://issues.apache.org/jira/browse/PHOENIX-6458) | Using global indexes for queries with uncovered columns |  Major | . |
| [PHOENIX-6599](https://issues.apache.org/jira/browse/PHOENIX-6599) | Missing checked exceptions on SchemaRegistryRepository |  Major | . |
| [PHOENIX-6247](https://issues.apache.org/jira/browse/PHOENIX-6247) | Change SYSTEM.CATALOG to allow separation of physical name (Hbase name) from logical name (Phoenix name) |  Major | . |
| [PHOENIX-6556](https://issues.apache.org/jira/browse/PHOENIX-6556) | Log INPUT\_TABLE\_CONDITIONS for MR jobs |  Minor | core, spark-connector |
| [PHOENIX-6544](https://issues.apache.org/jira/browse/PHOENIX-6544) | Adding metadata inconsistency metric |  Minor | . |
| [PHOENIX-6387](https://issues.apache.org/jira/browse/PHOENIX-6387) | Conditional updates on tables with indexes |  Major | . |
| [PHOENIX-6450](https://issues.apache.org/jira/browse/PHOENIX-6450) | Checkstyle creating warnings for line length \> 80 but \< 100 |  Major | core |
| [PHOENIX-6500](https://issues.apache.org/jira/browse/PHOENIX-6500) | Allow 4.16 client to connect to 5.1 server |  Major | . |
| [PHOENIX-6495](https://issues.apache.org/jira/browse/PHOENIX-6495) | Include phoenix-tools jar in assembly |  Major | core |
| [PHOENIX-6497](https://issues.apache.org/jira/browse/PHOENIX-6497) | Remove embedded profile and always build phoenix-client-embedded |  Major | . |
| [PHOENIX-6454](https://issues.apache.org/jira/browse/PHOENIX-6454) | Add feature to SchemaTool to get the DDL in specification mode |  Major | . |
| [PHOENIX-6378](https://issues.apache.org/jira/browse/PHOENIX-6378) | Unbundle sqlline from phoenix-client-embedded, and use it in sqlline.py |  Major | core |
| [PHOENIX-6444](https://issues.apache.org/jira/browse/PHOENIX-6444) | Extend Cell Tags to Delete object for Indexer coproc |  Major | core |
| [PHOENIX-6357](https://issues.apache.org/jira/browse/PHOENIX-6357) | Change all command line tools to use the fixed commons-cli constructor |  Major | core |
| [PHOENIX-6422](https://issues.apache.org/jira/browse/PHOENIX-6422) | Remove CorrelatePlan |  Minor | core |
| [PHOENIX-6271](https://issues.apache.org/jira/browse/PHOENIX-6271) | Effective DDL generated by SchemaExtractionTool should maintain the order of PK and other columns |  Minor | . |
| [PHOENIX-6435](https://issues.apache.org/jira/browse/PHOENIX-6435) | Fix ViewTTLIT test flapper |  Blocker | . |
| [PHOENIX-6434](https://issues.apache.org/jira/browse/PHOENIX-6434) | Secondary Indexes on PHOENIX\_ROW\_TIMESTAMP() |  Major | . |
| [PHOENIX-6409](https://issues.apache.org/jira/browse/PHOENIX-6409) | Include local index uncovered columns merge in explain plan. |  Minor | . |
| [PHOENIX-6385](https://issues.apache.org/jira/browse/PHOENIX-6385) | Not to use Scan#setSmall for HBase 2.x versions |  Major | . |
| [PHOENIX-6402](https://issues.apache.org/jira/browse/PHOENIX-6402) | Allow using local indexes with uncovered columns in the WHERE clause |  Blocker | . |
| [PHOENIX-6388](https://issues.apache.org/jira/browse/PHOENIX-6388) | Add sampled logging for read repairs |  Minor | . |
| [PHOENIX-6396](https://issues.apache.org/jira/browse/PHOENIX-6396) | PChar illegal data exception should not contain value |  Major | . |
| [PHOENIX-6380](https://issues.apache.org/jira/browse/PHOENIX-6380) | phoenix-client-embedded depends on logging classes |  Major | core |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7299](https://issues.apache.org/jira/browse/PHOENIX-7299) | ScanningResultIterator should not time out a query after receiving a valid result |  Major | . |
| [PHOENIX-7295](https://issues.apache.org/jira/browse/PHOENIX-7295) | Fix getTableRegions failing due to overlap/inconsistencies on region |  Major | . |
| [PHOENIX-7291](https://issues.apache.org/jira/browse/PHOENIX-7291) | Bump up omid to 1.1.2 |  Major | . |
| [PHOENIX-7285](https://issues.apache.org/jira/browse/PHOENIX-7285) | Upgade Zookeeper to 3.8.4 |  Major | core |
| [PHOENIX-7234](https://issues.apache.org/jira/browse/PHOENIX-7234) | Bump org.apache.commons:commons-compress from 1.21 to 1.26.0 |  Major | core |
| [PHOENIX-7223](https://issues.apache.org/jira/browse/PHOENIX-7223) | Make Sure Tools Always Close HBase Connections on Exit |  Minor | core |
| [PHOENIX-7160](https://issues.apache.org/jira/browse/PHOENIX-7160) | Change the TSO default port to be compatible with Omid 1.1.1 |  Major | omid |
| [PHOENIX-7175](https://issues.apache.org/jira/browse/PHOENIX-7175) | Set java.io.tmpdir to the maven build directory for tests |  Minor | connectors, core, queryserver |
| [PHOENIX-7089](https://issues.apache.org/jira/browse/PHOENIX-7089) | TableMetricsManager#getSizeHistogramsForAllTables() is returning empty map |  Minor | . |
| [PHOENIX-7193](https://issues.apache.org/jira/browse/PHOENIX-7193) | Fix cluster override for mapreduce jobs for non-ZK registries |  Major | core |
| [PHOENIX-7165](https://issues.apache.org/jira/browse/PHOENIX-7165) | getTable should retrieve PTable from server if not cached |  Major | . |
| [PHOENIX-7191](https://issues.apache.org/jira/browse/PHOENIX-7191) | Connectionless CQSs don't work with non-ZK registries |  Blocker | core |
| [PHOENIX-7141](https://issues.apache.org/jira/browse/PHOENIX-7141) | Use relocate by default for phoenix-server shading |  Blocker | core |
| [PHOENIX-6769](https://issues.apache.org/jira/browse/PHOENIX-6769) | Align mockito version with Hadoop and HBase |  Major | . |
| [PHOENIX-7195](https://issues.apache.org/jira/browse/PHOENIX-7195) | Exclude reload4j dependencies from Hadoop and add explicit log4j2 test dependencies |  Blocker | core |
| [PHOENIX-7194](https://issues.apache.org/jira/browse/PHOENIX-7194) | Yetus does not run tests in phoenix-core if phoenix-core-client or phoenix-core-server changes |  Blocker | core |
| [PHOENIX-7171](https://issues.apache.org/jira/browse/PHOENIX-7171) | Update Zookeeper to 3.8.3 when building with HBase 2.4+ |  Major | core |
| [PHOENIX-7138](https://issues.apache.org/jira/browse/PHOENIX-7138) | Import netty-bom to make sure all netty 4.1 modules share the same version |  Major | core |
| [PHOENIX-7178](https://issues.apache.org/jira/browse/PHOENIX-7178) | Use skipITs instead of skip to disable failsafe tests |  Major | core |
| [PHOENIX-7176](https://issues.apache.org/jira/browse/PHOENIX-7176) | QueryTimeoutIT#testQueryTimeout fails with incorrect error message |  Major | phoenix |
| [PHOENIX-7139](https://issues.apache.org/jira/browse/PHOENIX-7139) | Create phoenix-mapreduce-byo-shaded-hbase artifact |  Major | . |
| [PHOENIX-7137](https://issues.apache.org/jira/browse/PHOENIX-7137) | Create phoenix-client-lite shaded JAR without server-side dependencies |  Major | core |
| [PHOENIX-7152](https://issues.apache.org/jira/browse/PHOENIX-7152) | SchemaExtractionProcessor package does not match directory |  Major | . |
| [PHOENIX-6721](https://issues.apache.org/jira/browse/PHOENIX-6721) | CSV bulkload tool fails with FileNotFoundException if --output points to the S3 location |  Major | core |
| [PHOENIX-7153](https://issues.apache.org/jira/browse/PHOENIX-7153) | Fix Warnings Flagged as Errors by Eclipse |  Trivial | core |
| [PHOENIX-7076](https://issues.apache.org/jira/browse/PHOENIX-7076) | MetaDataRegionObserver#postOpen hook improvements |  Major | . |
| [PHOENIX-7143](https://issues.apache.org/jira/browse/PHOENIX-7143) | Detect JVM version and add the necessary module flags in startup scripts |  Major | core, queryserver |
| [PHOENIX-6909](https://issues.apache.org/jira/browse/PHOENIX-6909) | Create client/server metrics for phoenix Paging feature. |  Major | core |
| [PHOENIX-7109](https://issues.apache.org/jira/browse/PHOENIX-7109) | Incorrect query results when using OFFSET |  Major | phoenix |
| [PHOENIX-7024](https://issues.apache.org/jira/browse/PHOENIX-7024) | Issues in ServerPaging |  Major | . |
| [PHOENIX-7121](https://issues.apache.org/jira/browse/PHOENIX-7121) | Do not exclude commons-beanutils from Omid dependencies |  Major | core |
| [PHOENIX-7095](https://issues.apache.org/jira/browse/PHOENIX-7095) | Implement Statement.closeOnCompletion() and fix related close() bugs |  Major | core |
| [PHOENIX-7102](https://issues.apache.org/jira/browse/PHOENIX-7102) | phoenix-connectors doesn't compile with core HEAD |  Major | connectors, core |
| [PHOENIX-7097](https://issues.apache.org/jira/browse/PHOENIX-7097) | Allow specifying full JDBC URL string in psql/PhoenixRuntime and sqllline.py |  Major | core |
| [PHOENIX-7090](https://issues.apache.org/jira/browse/PHOENIX-7090) | Hash join throw NullPointerException when subquery return null |  Major | core |
| [PHOENIX-7070](https://issues.apache.org/jira/browse/PHOENIX-7070) | Child View of ReadOnly View is marked as updatable View |  Major | . |
| [PHOENIX-5980](https://issues.apache.org/jira/browse/PHOENIX-5980) | MUTATION\_BATCH\_FAILED\_SIZE metric is incorrectly updated for failing delete mutations |  Major | . |
| [PHOENIX-7060](https://issues.apache.org/jira/browse/PHOENIX-7060) | Compilation fails on 5.1 with Hbase 2.1 or 2.2 |  Blocker | . |
| [PHOENIX-7046](https://issues.apache.org/jira/browse/PHOENIX-7046) | Query results return different values when PKs of view have DESC order |  Major | . |
| [PHOENIX-7035](https://issues.apache.org/jira/browse/PHOENIX-7035) | Index on data table with only pk columns result into invalid state |  Major | . |
| [PHOENIX-7062](https://issues.apache.org/jira/browse/PHOENIX-7062) | Stabilize testDeletingStatsShouldNotFailWithADEWhenTableDropped |  Critical | core |
| [PHOENIX-7034](https://issues.apache.org/jira/browse/PHOENIX-7034) | Disallow mapped view creation when the schema does not exists |  Major | . |
| [PHOENIX-7057](https://issues.apache.org/jira/browse/PHOENIX-7057) | Potential bug in MetadataEndpointImpl#updateIndexState. |  Major | . |
| [PHOENIX-7039](https://issues.apache.org/jira/browse/PHOENIX-7039) | Snapshot scanner should skip replay WAL and update seqid while opening region |  Major | . |
| [PHOENIX-7003](https://issues.apache.org/jira/browse/PHOENIX-7003) | Harden hbase region inconsistencies check in CQSI#getAllTableRegions method |  Major | core |
| [PHOENIX-7052](https://issues.apache.org/jira/browse/PHOENIX-7052) | phoenix-pherf ITs fail because they do not use the miniCluster ZK quorum |  Major | core |
| [PHOENIX-7045](https://issues.apache.org/jira/browse/PHOENIX-7045) | AlterTableWithViewsIT.testCreateViewWithPropsMaintainsOwnProps failing on 5.1 |  Major | core |
| [PHOENIX-7042](https://issues.apache.org/jira/browse/PHOENIX-7042) | Update Jetty to 9.4.52.v20230823 |  Major | core, queryserver |
| [PHOENIX-7002](https://issues.apache.org/jira/browse/PHOENIX-7002) | Insufficient logging in phoenix client when server throws StaleRegionBoundaryCacheException. |  Major | . |
| [PHOENIX-6960](https://issues.apache.org/jira/browse/PHOENIX-6960) | Scan range is wrong when query desc columns |  Critical | . |
| [PHOENIX-6920](https://issues.apache.org/jira/browse/PHOENIX-6920) | PhoenixResultSetMetaData doesn't distinguish between label and column\_name |  Major | core |
| [PHOENIX-6864](https://issues.apache.org/jira/browse/PHOENIX-6864) | create view throwing NPE when referring back to itself |  Minor | core |
| [PHOENIX-6942](https://issues.apache.org/jira/browse/PHOENIX-6942) | Some config properties do not have phoenix prefix |  Minor | . |
| [PHOENIX-7019](https://issues.apache.org/jira/browse/PHOENIX-7019) | ParallelPhoenixConnectionTest failures because of misconfigured EnvironmentEdgeManager |  Critical | core |
| [PHOENIX-6945](https://issues.apache.org/jira/browse/PHOENIX-6945) | Update Surefire plugin to 3.0.0 and switch to TCP forkNode implementation |  Major | connectors, core, queryserver |
| [PHOENIX-7009](https://issues.apache.org/jira/browse/PHOENIX-7009) | Bump commons-codec to 1.16.0 |  Major | core |
| [PHOENIX-7010](https://issues.apache.org/jira/browse/PHOENIX-7010) | Upgrade Jetty to 9.4.51.v20230217 |  Major | . |
| [PHOENIX-5833](https://issues.apache.org/jira/browse/PHOENIX-5833) | Incorrect results with RVCs and AND operator |  Critical | core |
| [PHOENIX-7005](https://issues.apache.org/jira/browse/PHOENIX-7005) | Spark connector tests cannot compile with latest Phoenix |  Major | connectors, core, spark-connector |
| [PHOENIX-6897](https://issues.apache.org/jira/browse/PHOENIX-6897) | Filters on unverified index rows return wrong result |  Major | . |
| [PHOENIX-7000](https://issues.apache.org/jira/browse/PHOENIX-7000) | Always schedule RenewLeaseTasks |  Major | . |
| [PHOENIX-6952](https://issues.apache.org/jira/browse/PHOENIX-6952) | Do not disable normalizer on salted tables |  Blocker | core |
| [PHOENIX-6999](https://issues.apache.org/jira/browse/PHOENIX-6999) | Point lookups fail with reverse scan |  Major | core |
| [PHOENIX-6910](https://issues.apache.org/jira/browse/PHOENIX-6910) | Scans created during query compilation and execution against salted tables need to be more resilient |  Major | . |
| [PHOENIX-6961](https://issues.apache.org/jira/browse/PHOENIX-6961) | Using a covered field in hinted non-covered indexed query fails |  Critical | core |
| [PHOENIX-6966](https://issues.apache.org/jira/browse/PHOENIX-6966) | UngroupedAggregateRegionScanner.insertEmptyKeyValue() writes wrong qualifier for encoded CQ tables |  Major | core |
| [PHOENIX-6974](https://issues.apache.org/jira/browse/PHOENIX-6974) | Move PBaseColumn to test |  Minor | core |
| [PHOENIX-6962](https://issues.apache.org/jira/browse/PHOENIX-6962) | Write javadoc for different mutation plans within DeleteCompiler. |  Minor | core |
| [PHOENIX-6969](https://issues.apache.org/jira/browse/PHOENIX-6969) | Projection bug in hinted uncovered index query with order by |  Major | . |
| [PHOENIX-6965](https://issues.apache.org/jira/browse/PHOENIX-6965) | UngroupedAggregateRegionScanner.insertEmptyKeyValue() generates too many cells |  Critical | core |
| [PHOENIX-6950](https://issues.apache.org/jira/browse/PHOENIX-6950) | PhoenixDriver APIs should unlock closeLock only if thread is able to take lock |  Major | . |
| [PHOENIX-6953](https://issues.apache.org/jira/browse/PHOENIX-6953) | Creating indexes on a table with old indexing leads to inconsistent co-processors |  Major | core |
| [PHOENIX-6954](https://issues.apache.org/jira/browse/PHOENIX-6954) | Fix Category for some index related ITs |  Minor | core |
| [PHOENIX-6951](https://issues.apache.org/jira/browse/PHOENIX-6951) | Missing information on Index Repair Region and Time Taken to Repair |  Major | . |
| [PHOENIX-6916](https://issues.apache.org/jira/browse/PHOENIX-6916) | Cannot handle ranges where start is a prefix of end for desc columns |  Critical | core |
| [PHOENIX-6929](https://issues.apache.org/jira/browse/PHOENIX-6929) | Explicitly depend on org.glassfish:javax.el |  Critical | core |
| [PHOENIX-6888](https://issues.apache.org/jira/browse/PHOENIX-6888) | Fixing TTL and Max Lookback Issues for Phoenix Tables |  Major | . |
| [PHOENIX-6903](https://issues.apache.org/jira/browse/PHOENIX-6903) | Shade or exclude javax.xml.bind:jaxb-api from the shaded client |  Major | . |
| [PHOENIX-6904](https://issues.apache.org/jira/browse/PHOENIX-6904) | Clean up some dependencies |  Major | core |
| [PHOENIX-6890](https://issues.apache.org/jira/browse/PHOENIX-6890) | Remove binds parameters from QueryCompiler |  Trivial | . |
| [PHOENIX-5066](https://issues.apache.org/jira/browse/PHOENIX-5066) | The TimeZone is incorrectly used during writing or reading data |  Critical | . |
| [PHOENIX-6893](https://issues.apache.org/jira/browse/PHOENIX-6893) | Remove explicit dependency declaration on hbase-thirdparty artifacts |  Major | core |
| [PHOENIX-6871](https://issues.apache.org/jira/browse/PHOENIX-6871) | Write threads blocked with deadlock |  Major | . |
| [PHOENIX-6873](https://issues.apache.org/jira/browse/PHOENIX-6873) | Use cached Connection in IndexHalfStoreFileReaderGenerator |  Major | core |
| [PHOENIX-6874](https://issues.apache.org/jira/browse/PHOENIX-6874) | Support older HBase versions with broken ShortCircuitConnection |  Major | core |
| [PHOENIX-6872](https://issues.apache.org/jira/browse/PHOENIX-6872) | Use ServerUtil.ConnectionFactory.getConnection() in UngroupedAggregateRegionScanner |  Minor | core |
| [PHOENIX-6395](https://issues.apache.org/jira/browse/PHOENIX-6395) | Reusing Connection instance object instead of creating everytime in PhoenixAccessController class. |  Major | . |
| [PHOENIX-6052](https://issues.apache.org/jira/browse/PHOENIX-6052) | Include syscat time in mutation time |  Major | core |
| [PHOENIX-6720](https://issues.apache.org/jira/browse/PHOENIX-6720) | CREATE TABLE can't recreate column encoded tables that had columns dropped |  Blocker | core |
| [PHOENIX-6855](https://issues.apache.org/jira/browse/PHOENIX-6855) | Upgrade from 4.7 to 5+ fails if any of the local indexes exist. |  Major | core |
| [PHOENIX-6848](https://issues.apache.org/jira/browse/PHOENIX-6848) | Fix grant and revoke command rule documentation |  Major | core |
| [PHOENIX-6843](https://issues.apache.org/jira/browse/PHOENIX-6843) | Flakey ViewTTLIT |  Major | . |
| [PHOENIX-6840](https://issues.apache.org/jira/browse/PHOENIX-6840) | Very flakey ParallelPhoenixConnectionFailureTest |  Critical | core |
| [PHOENIX-6841](https://issues.apache.org/jira/browse/PHOENIX-6841) | Depend on omid-codahale-metrics |  Blocker | core, omid |
| [PHOENIX-6823](https://issues.apache.org/jira/browse/PHOENIX-6823) | calling Joda-based round() function on temporal PK field causes division by zero error |  Major | core |
| [PHOENIX-6835](https://issues.apache.org/jira/browse/PHOENIX-6835) | Flakey RowTimestampIT.testAutomaticallySettingRowTimestampWithDate |  Major | . |
| [PHOENIX-6784](https://issues.apache.org/jira/browse/PHOENIX-6784) | PhantomJS fails on recent Linux distributions |  Blocker | . |
| [PHOENIX-6806](https://issues.apache.org/jira/browse/PHOENIX-6806) | Protobufs don't compile on ARM-based Macs (Apple Silicon) |  Major | . |
| [PHOENIX-6462](https://issues.apache.org/jira/browse/PHOENIX-6462) | Index build mapper that failed should not be logging into the PHOENIX\_INDEX\_TOOL\_RESULT table |  Major | . |
| [PHOENIX-6669](https://issues.apache.org/jira/browse/PHOENIX-6669) | RVC returns a wrong result |  Major | . |
| [PHOENIX-6798](https://issues.apache.org/jira/browse/PHOENIX-6798) | Eliminate unnecessary reversed scan for AggregatePlan |  Major | core |
| [PHOENIX-6751](https://issues.apache.org/jira/browse/PHOENIX-6751) | Force using range scan vs skip scan when using the IN operator and large number of RVC elements |  Critical | . |
| [PHOENIX-6773](https://issues.apache.org/jira/browse/PHOENIX-6773) | PhoenixDatabaseMetadata.getColumns() always returns null COLUMN\_DEF |  Minor | core |
| [PHOENIX-6789](https://issues.apache.org/jira/browse/PHOENIX-6789) | Remove com.sun.istack.NotNull usage |  Minor | core |
| [PHOENIX-6764](https://issues.apache.org/jira/browse/PHOENIX-6764) | Implement Binary and Hexadecimal literals |  Major | core |
| [PHOENIX-6687](https://issues.apache.org/jira/browse/PHOENIX-6687) | The region server hosting the SYSTEM.CATALOG fails to serve any metadata requests as default handler pool  threads are exhausted. |  Major | core |
| [PHOENIX-6771](https://issues.apache.org/jira/browse/PHOENIX-6771) | Allow only "squash and merge" from GitHub UI |  Major | . |
| [PHOENIX-6754](https://issues.apache.org/jira/browse/PHOENIX-6754) | Upgrades from pre 4.10 versions are broken again |  Blocker | core |
| [PHOENIX-6766](https://issues.apache.org/jira/browse/PHOENIX-6766) | Fix failure of sqlline due to conflicting jline dependency pulled from Hadoop 3.3 |  Major | . |
| [PHOENIX-6760](https://issues.apache.org/jira/browse/PHOENIX-6760) | update log4j2 to 2.18.0 |  Blocker | connectors, core |
| [PHOENIX-6756](https://issues.apache.org/jira/browse/PHOENIX-6756) | switch to using log4j2.properties instead of xml |  Major | core |
| [PHOENIX-6758](https://issues.apache.org/jira/browse/PHOENIX-6758) | During HBase 2 upgrade Phoenix Self healing task fails to create server side connection before reading SYSTEM.TASK |  Major | . |
| [PHOENIX-6755](https://issues.apache.org/jira/browse/PHOENIX-6755) | SystemCatalogRegionObserver extends BaseRegionObserver which doesn't exist in hbase-2.4 branch. |  Major | core |
| [PHOENIX-6746](https://issues.apache.org/jira/browse/PHOENIX-6746) | Test suite executions do not provide usable logs |  Blocker | core |
| [PHOENIX-6733](https://issues.apache.org/jira/browse/PHOENIX-6733) | Ref count leaked test failures |  Blocker | . |
| [PHOENIX-6530](https://issues.apache.org/jira/browse/PHOENIX-6530) | Fix tenantId generation for Sequential and Uniform load generators |  Major | . |
| [PHOENIX-6725](https://issues.apache.org/jira/browse/PHOENIX-6725) | ConcurrentMutationException when adding column to table/view |  Major | . |
| [PHOENIX-6734](https://issues.apache.org/jira/browse/PHOENIX-6734) | Revert default HBase version to 2.4.10 |  Major | core |
| [PHOENIX-5534](https://issues.apache.org/jira/browse/PHOENIX-5534) | Cursors With Request Metrics Enabled Throws Exception |  Major | . |
| [PHOENIX-6723](https://issues.apache.org/jira/browse/PHOENIX-6723) | Phoenix client unable to connect on JDK14+ |  Major | . |
| [PHOENIX-6636](https://issues.apache.org/jira/browse/PHOENIX-6636) | Replace bundled log4j libraries with reload4j |  Major | connectors, core |
| [PHOENIX-6498](https://issues.apache.org/jira/browse/PHOENIX-6498) | Fix incorrect Correlated Exists Subquery rewrite when Subquery is aggregate |  Major | . |
| [PHOENIX-6688](https://issues.apache.org/jira/browse/PHOENIX-6688) | Upgrade to phoenix 4.16 metadata upgrade fails when SYSCAT has large number of tenant views |  Major | core |
| [PHOENIX-6719](https://issues.apache.org/jira/browse/PHOENIX-6719) | Duplicate Salt Columns in Schema Registry after ALTER |  Major | . |
| [PHOENIX-6705](https://issues.apache.org/jira/browse/PHOENIX-6705) | PagedRegionScanner#next throws NPE if pagedFilter is not initialized. |  Major | core |
| [PHOENIX-6717](https://issues.apache.org/jira/browse/PHOENIX-6717) | Remove Hbase 2.1 and 2.2 from post-commit Jenkins job |  Major | core |
| [PHOENIX-6710](https://issues.apache.org/jira/browse/PHOENIX-6710) | Revert PHOENIX-3842 Turn on back default bloomFilter for Phoenix Tables |  Major | core |
| [PHOENIX-6699](https://issues.apache.org/jira/browse/PHOENIX-6699) | Phoenix metrics overwriting DefaultMetricsSystem in RegionServers |  Major | core |
| [PHOENIX-6708](https://issues.apache.org/jira/browse/PHOENIX-6708) | Bump junit from 4.13 to 4.13.1 |  Major | core |
| [PHOENIX-6682](https://issues.apache.org/jira/browse/PHOENIX-6682) | Jenkins tests are failing for Java 11.0.14.1 |  Major | . |
| [PHOENIX-6686](https://issues.apache.org/jira/browse/PHOENIX-6686) | Update Jackson to 2.12.6.1 |  Major | core |
| [PHOENIX-6679](https://issues.apache.org/jira/browse/PHOENIX-6679) | PHOENIX-6665 changed column name for CURRENT seqence values |  Minor | core |
| [PHOENIX-6616](https://issues.apache.org/jira/browse/PHOENIX-6616) | Alter table command can be used to set normalization\_enabled=true on salted tables |  Major | . |
| [PHOENIX-6675](https://issues.apache.org/jira/browse/PHOENIX-6675) | create-release script broken by upgrade to Yetus 0.13 |  Major | core |
| [PHOENIX-6665](https://issues.apache.org/jira/browse/PHOENIX-6665) | PreparedStatement#getMetaData() fails on parametrized "select next ? values for SEQ" |  Major | core |
| [PHOENIX-6658](https://issues.apache.org/jira/browse/PHOENIX-6658) | Replace HRegion.get() calls |  Major | . |
| [PHOENIX-6662](https://issues.apache.org/jira/browse/PHOENIX-6662) | Failed to delete rows when PK has one or more DESC column with IN clause |  Critical | . |
| [PHOENIX-6661](https://issues.apache.org/jira/browse/PHOENIX-6661) | Sqlline does not work on PowerPC linux |  Major | core, queryserver |
| [PHOENIX-6659](https://issues.apache.org/jira/browse/PHOENIX-6659) | RVC with AND clauses return incorrect result |  Critical | . |
| [PHOENIX-6656](https://issues.apache.org/jira/browse/PHOENIX-6656) | Reindent NonAggregateRegionScannerFactory |  Trivial | . |
| [PHOENIX-6646](https://issues.apache.org/jira/browse/PHOENIX-6646) | System tables are not upgraded after namespace migration |  Minor | core |
| [PHOENIX-6645](https://issues.apache.org/jira/browse/PHOENIX-6645) | Remove unneccessary SCN related properties from SYSTEM tables on upgrade |  Minor | core |
| [PHOENIX-5894](https://issues.apache.org/jira/browse/PHOENIX-5894) | Table versus Table Full Outer join on Salted tables not working |  Major | core |
| [PHOENIX-6576](https://issues.apache.org/jira/browse/PHOENIX-6576) | Do not use guava's Files.createTempDir() |  Major | . |
| [PHOENIX-6441](https://issues.apache.org/jira/browse/PHOENIX-6441) | Remove TSOMockModule reference from OmidTransactionProvider |  Major | core, omid |
| [PHOENIX-6638](https://issues.apache.org/jira/browse/PHOENIX-6638) | Test suite fails with -Dwithout.tephra |  Major | . |
| [PHOENIX-6591](https://issues.apache.org/jira/browse/PHOENIX-6591) | Update OWASP plugin to latest |  Major | connectors, core, queryserver |
| [PHOENIX-6579](https://issues.apache.org/jira/browse/PHOENIX-6579) | ACL check doesn't honor the namespace mapping for mapped views. |  Major | core |
| [PHOENIX-6596](https://issues.apache.org/jira/browse/PHOENIX-6596) | Schema extraction double quotes expressions, resulting in un-executabe create statements |  Major | core |
| [PHOENIX-5865](https://issues.apache.org/jira/browse/PHOENIX-5865) | column that has default value can not be correctly indexed |  Major | core |
| [PHOENIX-6615](https://issues.apache.org/jira/browse/PHOENIX-6615) | The Tephra transaction processor cannot be loaded anymore. |  Major | . |
| [PHOENIX-6611](https://issues.apache.org/jira/browse/PHOENIX-6611) | Fix IndexTool -snap option and set VERIFIED in PhoenixIndexImportDirectReducer |  Major | core |
| [PHOENIX-6618](https://issues.apache.org/jira/browse/PHOENIX-6618) | Yetus docker image cannot be built as openjdk 11.0.11 is no longer available |  Major | core |
| [PHOENIX-6604](https://issues.apache.org/jira/browse/PHOENIX-6604) | Allow using indexes for wildcard topN queries on salted tables |  Major | . |
| [PHOENIX-6528](https://issues.apache.org/jira/browse/PHOENIX-6528) | When view index pk has a variable length column, read repair doesn't work correctly |  Critical | . |
| [PHOENIX-6600](https://issues.apache.org/jira/browse/PHOENIX-6600) | Replace deprecated getCall with updated getRpcCall |  Major | . |
| [PHOENIX-6601](https://issues.apache.org/jira/browse/PHOENIX-6601) | Fix IndexTools bugs with namespace mapping |  Major | . |
| [PHOENIX-6594](https://issues.apache.org/jira/browse/PHOENIX-6594) | Clean up vararg warnings flagged as errors by Eclipse |  Minor | core |
| [PHOENIX-6592](https://issues.apache.org/jira/browse/PHOENIX-6592) | PhoenixStatsCacheLoader uses non-deamon threads |  Major | core |
| [PHOENIX-6586](https://issues.apache.org/jira/browse/PHOENIX-6586) | Set NORMALIZATION\_ENABLED to false on salted tables |  Critical | core |
| [PHOENIX-6583](https://issues.apache.org/jira/browse/PHOENIX-6583) | Inserting explicit Null into a (fixed length) binary field is stored as an array of zeroes |  Major | core |
| [PHOENIX-6555](https://issues.apache.org/jira/browse/PHOENIX-6555) | Wait for permissions to sync in Permission tests |  Major | core |
| [PHOENIX-6577](https://issues.apache.org/jira/browse/PHOENIX-6577) | phoenix\_sandbox.py incompatible with python3 |  Major | core |
| [PHOENIX-6578](https://issues.apache.org/jira/browse/PHOENIX-6578) | sqlline.py cannot be started from source tree |  Minor | core |
| [PHOENIX-6574](https://issues.apache.org/jira/browse/PHOENIX-6574) | Executing "DROP TABLE" drops all sequences |  Blocker | core |
| [PHOENIX-6568](https://issues.apache.org/jira/browse/PHOENIX-6568) | NullPointerException in  phoenix-queryserver-client  not in phoenix-client-hbase |  Major | core |
| [PHOENIX-6548](https://issues.apache.org/jira/browse/PHOENIX-6548) | Race condition when triggering index rebuilds as regionserver closes |  Minor | . |
| [PHOENIX-6563](https://issues.apache.org/jira/browse/PHOENIX-6563) | Unable to use 'UPPER'/'LOWER' together with 'IN' |  Major | core |
| [PHOENIX-6545](https://issues.apache.org/jira/browse/PHOENIX-6545) | IndexToolForNonTxGlobalIndexIT.testIndexToolFailedMapperNotRecordToResultTable() fails with HBase 2.1 |  Major | core |
| [PHOENIX-6546](https://issues.apache.org/jira/browse/PHOENIX-6546) | BackwardCompatibilityIT#testSystemTaskCreationWithIndexAsyncRebuild is flakey |  Major | core |
| [PHOENIX-6547](https://issues.apache.org/jira/browse/PHOENIX-6547) | BasePermissionsIT is still a bit flakey |  Major | core |
| [PHOENIX-6541](https://issues.apache.org/jira/browse/PHOENIX-6541) | Use ROW\_TIMESTAMP column value as timestamps for conditional upsert mutations |  Major | . |
| [PHOENIX-6543](https://issues.apache.org/jira/browse/PHOENIX-6543) | de-flake AuditLoggingIT |  Major | core |
| [PHOENIX-5072](https://issues.apache.org/jira/browse/PHOENIX-5072) | Cursor Query Loops Eternally with Local Index, Returns Fine Without It |  Major | . |
| [PHOENIX-6542](https://issues.apache.org/jira/browse/PHOENIX-6542) | WALRecoveryRegionPostOpenIT is flakey |  Major | core |
| [PHOENIX-6534](https://issues.apache.org/jira/browse/PHOENIX-6534) | Upgrades from pre 4.10 versions are broken |  Major | core |
| [PHOENIX-6486](https://issues.apache.org/jira/browse/PHOENIX-6486) | Phoenix uses inconsistent chronologies internally, breaking pre-Gregorian date handling |  Major | core |
| [PHOENIX-6472](https://issues.apache.org/jira/browse/PHOENIX-6472) | In case of region inconsistency phoenix should stop gracefully |  Major | . |
| [PHOENIX-6480](https://issues.apache.org/jira/browse/PHOENIX-6480) | SchemaExtractionProcessor doesn't add IMMUTABLE\_STORAGE\_SCHEME and COLUMN\_ENCODED\_BYTES to the generated sql |  Major | . |
| [PHOENIX-6506](https://issues.apache.org/jira/browse/PHOENIX-6506) | Tenant Connection is not able to access/validate Global Sequences |  Major | . |
| [PHOENIX-6476](https://issues.apache.org/jira/browse/PHOENIX-6476) | Index tool when verifying from index to data doesn't correctly split page into tasks |  Major | . |
| [PHOENIX-6515](https://issues.apache.org/jira/browse/PHOENIX-6515) | Phoenix uses hbase-testing-util but does not list it as a dependency |  Major | . |
| [PHOENIX-6507](https://issues.apache.org/jira/browse/PHOENIX-6507) | DistinctAggregatingResultIterator should keep original tuple order of the AggregatingResultIterator |  Major | . |
| [PHOENIX-6510](https://issues.apache.org/jira/browse/PHOENIX-6510) | Double-Checked Locking field must be volatile |  Major | . |
| [PHOENIX-6514](https://issues.apache.org/jira/browse/PHOENIX-6514) | Exception should be thrown |  Trivial | . |
| [PHOENIX-6509](https://issues.apache.org/jira/browse/PHOENIX-6509) | Forward port PHOENIX-4424 Allow users to create "DEFAULT" and "HBASE" Schema (Uppercase Schema Names) |  Major | core |
| [PHOENIX-6493](https://issues.apache.org/jira/browse/PHOENIX-6493) | MetaData schemaPattern handling errors |  Major | core |
| [PHOENIX-6453](https://issues.apache.org/jira/browse/PHOENIX-6453) | Possible ArrayIndexOutOfBoundsException while preparing scan start key with multiple key range queries |  Blocker | . |
| [PHOENIX-6479](https://issues.apache.org/jira/browse/PHOENIX-6479) | Duplicate commons-io dependency in phoenix-pherf |  Trivial | . |
| [PHOENIX-6475](https://issues.apache.org/jira/browse/PHOENIX-6475) | Build failure on Linux ARM64 |  Major | core |
| [PHOENIX-6420](https://issues.apache.org/jira/browse/PHOENIX-6420) | Wrong result when conditional and regular upserts are passed in the same commit batch |  Major | . |
| [PHOENIX-6471](https://issues.apache.org/jira/browse/PHOENIX-6471) | Revert PHOENIX-5387 to remove unneeded CPL 1.0 license |  Blocker | . |
| [PHOENIX-6442](https://issues.apache.org/jira/browse/PHOENIX-6442) | Phoenix should depend on the appropriate tephra-hbase-compat-x.y module |  Major | core, tephra |
| [PHOENIX-6437](https://issues.apache.org/jira/browse/PHOENIX-6437) | Delete marker for parent-child rows does not get replicated via SystemCatalogWALEntryFilter |  Major | core |
| [PHOENIX-6351](https://issues.apache.org/jira/browse/PHOENIX-6351) | PhoenixMRJobUtil getActiveResourceManagerAddress logic fails on pseudodistributed cluster |  Minor | core |
| [PHOENIX-6447](https://issues.apache.org/jira/browse/PHOENIX-6447) | Add support for SYSTEM.CHILD\_LINK table in systemcatalogwalentryfilter |  Major | core |
| [PHOENIX-6452](https://issues.apache.org/jira/browse/PHOENIX-6452) | cache-apache-client-artifact.sh stopped working |  Critical | core |
| [PHOENIX-6427](https://issues.apache.org/jira/browse/PHOENIX-6427) | Create sequence fails in lowercase schema |  Major | core |
| [PHOENIX-6424](https://issues.apache.org/jira/browse/PHOENIX-6424) | SELECT cf1.\* FAILS with a WHERE clause including cf2. |  Major | . |
| [PHOENIX-6421](https://issues.apache.org/jira/browse/PHOENIX-6421) | Selecting an indexed array value from an uncovered column with local index returns NULL |  Major | . |
| [PHOENIX-6423](https://issues.apache.org/jira/browse/PHOENIX-6423) | Wildcard queries fail with mixed default and explicit column families. |  Critical | . |
| [PHOENIX-6419](https://issues.apache.org/jira/browse/PHOENIX-6419) | Unused getResolverForQuery() in QueryCompiler.verifySCN() |  Trivial | core |
| [PHOENIX-6400](https://issues.apache.org/jira/browse/PHOENIX-6400) | Do no use local index with uncovered columns in the WHERE clause. |  Blocker | . |
| [PHOENIX-6386](https://issues.apache.org/jira/browse/PHOENIX-6386) | Bulkload generates unverified index rows |  Major | core |
| [PHOENIX-6343](https://issues.apache.org/jira/browse/PHOENIX-6343) | Phoenix allows duplicate column names when one of them is a primary key |  Major | core |
| [PHOENIX-6382](https://issues.apache.org/jira/browse/PHOENIX-6382) | Shaded artifact names and descriptions have unresolved ${hbase.profile} strings |  Major | core |
| [PHOENIX-6377](https://issues.apache.org/jira/browse/PHOENIX-6377) | phoenix-client has erronous maven dependecies |  Critical | core |
| [PHOENIX-6350](https://issues.apache.org/jira/browse/PHOENIX-6350) | Build failure on phoenix master branch |  Major | . |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-6653](https://issues.apache.org/jira/browse/PHOENIX-6653) | Add upgrade tests based on HBase snapshots |  Major | core |
| [PHOENIX-6483](https://issues.apache.org/jira/browse/PHOENIX-6483) | Flakes in BasePermissionsIT and AuditLoggingIT |  Major | . |
| [PHOENIX-6482](https://issues.apache.org/jira/browse/PHOENIX-6482) | PherfMainIT#testPherfMain is consistently failing |  Major | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-7028](https://issues.apache.org/jira/browse/PHOENIX-7028) | Annotate client initiated Scan with metadata attributes |  Major | . |
| [PHOENIX-7029](https://issues.apache.org/jira/browse/PHOENIX-7029) | Add support for multiple query services in PhoenixTestDriver. |  Major | . |
| [PHOENIX-6989](https://issues.apache.org/jira/browse/PHOENIX-6989) | Expose server side metrics for DDL operations |  Major | . |
| [PHOENIX-6985](https://issues.apache.org/jira/browse/PHOENIX-6985) | Setting server-side masking flag default to false |  Major | . |
| [PHOENIX-6963](https://issues.apache.org/jira/browse/PHOENIX-6963) | Check if LAST\_DDL\_TIMESTAMP is set for tables and indexes created before PHOENIX-6186 |  Major | core |
| [PHOENIX-5521](https://issues.apache.org/jira/browse/PHOENIX-5521) | Phoenix-level HBase Replication sink (Endpoint coproc) |  Major | . |
| [PHOENIX-6932](https://issues.apache.org/jira/browse/PHOENIX-6932) | Update LAST\_DDL\_TIMESTAMP for index table when add/drop/alter indexes. |  Major | . |
| [PHOENIX-6930](https://issues.apache.org/jira/browse/PHOENIX-6930) | Update LAST\_DDL\_TIMESTAMP for table/view when we alter properties on table/view |  Major | core |
| [PHOENIX-6649](https://issues.apache.org/jira/browse/PHOENIX-6649) | TransformTool should transform the tenant view content as well |  Major | . |
| [PHOENIX-6790](https://issues.apache.org/jira/browse/PHOENIX-6790) | Phoenix 5.2 HBase 2.5 profile to include missing zookeeper dependency |  Major | . |
| [PHOENIX-6627](https://issues.apache.org/jira/browse/PHOENIX-6627) | Remove all references to Tephra from 4.x and master |  Major | 4.x, tephra |
| [PHOENIX-5419](https://issues.apache.org/jira/browse/PHOENIX-5419) | Cleanup anonymous class in TracingQueryPlan |  Minor | . |
| [PHOENIX-6711](https://issues.apache.org/jira/browse/PHOENIX-6711) | Add support of skipping the system tables existence check during connection initialisation |  Major | . |
| [PHOENIX-6685](https://issues.apache.org/jira/browse/PHOENIX-6685) | Change Data Capture - Populate Table / Topic Mappings |  Major | . |
| [PHOENIX-6626](https://issues.apache.org/jira/browse/PHOENIX-6626) | Make the without.tephra profile behaviour the default in 4.x and master, and remove the profile |  Major | core, tephra |
| [PHOENIX-6639](https://issues.apache.org/jira/browse/PHOENIX-6639) | Read repair of a table after cutover (transform is complete and table is switched) |  Major | . |
| [PHOENIX-6622](https://issues.apache.org/jira/browse/PHOENIX-6622) | TransformMonitor should orchestrate transform and do retries |  Major | . |
| [PHOENIX-6620](https://issues.apache.org/jira/browse/PHOENIX-6620) | TransformTool should fix the unverified rows and do validation |  Major | . |
| [PHOENIX-6617](https://issues.apache.org/jira/browse/PHOENIX-6617) | IndexRegionObserver should create mutations for the transforming table |  Major | . |
| [PHOENIX-6612](https://issues.apache.org/jira/browse/PHOENIX-6612) | Add TransformTool |  Major | . |
| [PHOENIX-6603](https://issues.apache.org/jira/browse/PHOENIX-6603) | Create SYSTEM.TRANSFORM table |  Major | . |
| [PHOENIX-6589](https://issues.apache.org/jira/browse/PHOENIX-6589) | Add metrics for schema registry export |  Major | . |
| [PHOENIX-6227](https://issues.apache.org/jira/browse/PHOENIX-6227) | Option for DDL changes to export to external schema repository |  Major | . |
| [PHOENIX-6417](https://issues.apache.org/jira/browse/PHOENIX-6417) | Fix PHERF ITs that are failing in the local builds |  Minor | . |
| [PHOENIX-6118](https://issues.apache.org/jira/browse/PHOENIX-6118) | Multi Tenant Workloads using PHERF |  Major | . |
| [PHOENIX-6429](https://issues.apache.org/jira/browse/PHOENIX-6429) | Add support for global connections and sequential data generators |  Major | . |
| [PHOENIX-6430](https://issues.apache.org/jira/browse/PHOENIX-6430) | Add support for full row update for tables when no columns specfied in scenario |  Major | . |
| [PHOENIX-6431](https://issues.apache.org/jira/browse/PHOENIX-6431) | Add support for auto assigning pmfs |  Major | . |
| [PHOENIX-6432](https://issues.apache.org/jira/browse/PHOENIX-6432) | Add support for additional load generators |  Major | . |
| [PHOENIX-6397](https://issues.apache.org/jira/browse/PHOENIX-6397) | Implement TableMetricsManager class and its associated functions for select. upsert and Delete Queries |  Major | . |
| [PHOENIX-6408](https://issues.apache.org/jira/browse/PHOENIX-6408) | LIMIT on local index query with uncovered columns in the WHERE returns wrong result. |  Major | . |
| [PHOENIX-6379](https://issues.apache.org/jira/browse/PHOENIX-6379) | Implement a new Metric Type which will be used for TableMetrics |  Major | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-6436](https://issues.apache.org/jira/browse/PHOENIX-6436) | OrderedResultIterator overestimates memory requirements. |  Major | . |
| [PHOENIX-7189](https://issues.apache.org/jira/browse/PHOENIX-7189) | Update Omid to 1.1.1 |  Blocker | core |
| [PHOENIX-7179](https://issues.apache.org/jira/browse/PHOENIX-7179) | Remove HBase 2.3 leftover files |  Major | . |
| [PHOENIX-7173](https://issues.apache.org/jira/browse/PHOENIX-7173) | Update default HBase versions to 2.4.17 and 2.5.7 respectively |  Major | core |
| [PHOENIX-7082](https://issues.apache.org/jira/browse/PHOENIX-7082) | Upgrade Jetty to 9.4.53.v20231009 |  Major | core, queryserver |
| [PHOENIX-7027](https://issues.apache.org/jira/browse/PHOENIX-7027) | Add compatibility module for Hbase 2.5.4 and upgrade Hbase version to 2.5.5 |  Major | . |
| [PHOENIX-6866](https://issues.apache.org/jira/browse/PHOENIX-6866) | Upgrade hbase 2.4 and 2.5 versions to 2.4.16 and 2.5.3-hadoop3 respectively |  Major | . |
| [PHOENIX-6816](https://issues.apache.org/jira/browse/PHOENIX-6816) | Update Jetty to 9.4.49.v20220914 |  Major | . |
| [PHOENIX-6815](https://issues.apache.org/jira/browse/PHOENIX-6815) | Update Gson version to 2.9.1 |  Major | . |
| [PHOENIX-6715](https://issues.apache.org/jira/browse/PHOENIX-6715) | Update Omid to 1.1.0 |  Major | core |
| [PHOENIX-6805](https://issues.apache.org/jira/browse/PHOENIX-6805) | Update release scripts for Omid 1.1.0 |  Major | . |
| [PHOENIX-6485](https://issues.apache.org/jira/browse/PHOENIX-6485) | Clean up classpath in .py scripts |  Major | . |
| [PHOENIX-6753](https://issues.apache.org/jira/browse/PHOENIX-6753) | Update default HBase 2.4 version to 2.4.13 |  Major | . |
| [PHOENIX-6695](https://issues.apache.org/jira/browse/PHOENIX-6695) | Switch default logging backend to log4j2 |  Major | core |
| [PHOENIX-6693](https://issues.apache.org/jira/browse/PHOENIX-6693) | Remove Hbase 2.1 and Hbase 2.2 support from Phoenix |  Major | core |
| [PHOENIX-6696](https://issues.apache.org/jira/browse/PHOENIX-6696) | Drop legacy phoenix-client jar |  Major | core |
| [PHOENIX-6697](https://issues.apache.org/jira/browse/PHOENIX-6697) | log4j-reload4j is missing from phoenix-assembly |  Major | . |
| [PHOENIX-6582](https://issues.apache.org/jira/browse/PHOENIX-6582) | Bump default HBase version to 2.3.7 and 2.4.8 |  Major | . |
| [PHOENIX-6558](https://issues.apache.org/jira/browse/PHOENIX-6558) | Update SpotBugs |  Major | core |
| [PHOENIX-6557](https://issues.apache.org/jira/browse/PHOENIX-6557) | Fix code problems flagged by SpotBugs as High priority |  Major | core |
| [PHOENIX-6537](https://issues.apache.org/jira/browse/PHOENIX-6537) | Fix CI pipeline and upgrade Yetus |  Major | . |
| [PHOENIX-6551](https://issues.apache.org/jira/browse/PHOENIX-6551) | Bump HBase version to 2.4.6 and 2.2.7 |  Major | core |
| [PHOENIX-6550](https://issues.apache.org/jira/browse/PHOENIX-6550) | Upgrade jetty, jackson and commons-io |  Major | core |
| [PHOENIX-6526](https://issues.apache.org/jira/browse/PHOENIX-6526) | Bump default HBase version on 2.3 profile to 2.3.6 |  Major | . |
| [PHOENIX-6519](https://issues.apache.org/jira/browse/PHOENIX-6519) | Make SchemaTool work with lower case table and column names |  Major | core |
| [PHOENIX-6518](https://issues.apache.org/jira/browse/PHOENIX-6518) | Implement SHOW CREATE TABLE SQL command |  Major | core |
| [PHOENIX-6502](https://issues.apache.org/jira/browse/PHOENIX-6502) | Bump default HBase version on 2.4 profile to 2.4.4 |  Major | . |
| [PHOENIX-6456](https://issues.apache.org/jira/browse/PHOENIX-6456) | Support query logging for DDL and DML |  Major | core |
| [PHOENIX-6451](https://issues.apache.org/jira/browse/PHOENIX-6451) | Update joni and jcodings versions |  Major | . |
| [PHOENIX-6446](https://issues.apache.org/jira/browse/PHOENIX-6446) | Bump default HBase version on 2.3 profile to 2.3.5 |  Major | . |
| [PHOENIX-6418](https://issues.apache.org/jira/browse/PHOENIX-6418) | Bump default HBase version on 2.4 profile to 2.4.2 |  Major | . |
| [PHOENIX-6376](https://issues.apache.org/jira/browse/PHOENIX-6376) | Update MetaDataProtocol.java for Phoenix 5.2 |  Blocker | core |
| [PHOENIX-6394](https://issues.apache.org/jira/browse/PHOENIX-6394) | PostCommit Jenkins job detects incorrect HBase profile to rebuild with 2.3 and 2.4 |  Major | . |
| [PHOENIX-6359](https://issues.apache.org/jira/browse/PHOENIX-6359) | New module to support HBase 2.4.1+ releases |  Major | . |
| [PHOENIX-6371](https://issues.apache.org/jira/browse/PHOENIX-6371) | Script to verify release candidate |  Major | . |



