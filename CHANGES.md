
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

## Release 4.16.1 - Unreleased (as of 2021-03-26)



### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-6409](https://issues.apache.org/jira/browse/PHOENIX-6409) | Include local index uncovered columns merge in explain plan. |  Minor | . |
| [PHOENIX-6385](https://issues.apache.org/jira/browse/PHOENIX-6385) | Not to use Scan#setSmall for HBase 2.x versions |  Major | . |
| [PHOENIX-6402](https://issues.apache.org/jira/browse/PHOENIX-6402) | Allow using local indexes with uncovered columns in the WHERE clause |  Blocker | . |
| [PHOENIX-6388](https://issues.apache.org/jira/browse/PHOENIX-6388) | Add sampled logging for read repairs |  Minor | . |
| [PHOENIX-6396](https://issues.apache.org/jira/browse/PHOENIX-6396) | PChar illegal data exception should not contain value |  Major | . |
| [PHOENIX-6182](https://issues.apache.org/jira/browse/PHOENIX-6182) | IndexTool to verify and repair every index row |  Major | . |
| [PHOENIX-5543](https://issues.apache.org/jira/browse/PHOENIX-5543) | Implement show schemas / show tables SQL commands |  Minor | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-6424](https://issues.apache.org/jira/browse/PHOENIX-6424) | SELECT cf1.\* FAILS with a WHERE clause including cf2. |  Major | . |
| [PHOENIX-6421](https://issues.apache.org/jira/browse/PHOENIX-6421) | Selecting an indexed array value from an uncovered column with local index returns NULL |  Major | . |
| [PHOENIX-6423](https://issues.apache.org/jira/browse/PHOENIX-6423) | Wildcard queries fail with mixed default and explicit column families. |  Critical | . |
| [PHOENIX-6400](https://issues.apache.org/jira/browse/PHOENIX-6400) | Do no use local index with uncovered columns in the WHERE clause. |  Blocker | . |
| [PHOENIX-6370](https://issues.apache.org/jira/browse/PHOENIX-6370) | 4.x branch still includes the phoenix-pig example files |  Trivial | . |
| [PHOENIX-6386](https://issues.apache.org/jira/browse/PHOENIX-6386) | Bulkload generates unverified index rows |  Major | core |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-6408](https://issues.apache.org/jira/browse/PHOENIX-6408) | LIMIT on local index query with uncovered columns in the WHERE returns wrong result. |  Major | . |



## Release 4.16.0 - Unreleased (as of 2021-02-16)



### IMPORTANT ISSUES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-5265](https://issues.apache.org/jira/browse/PHOENIX-5265) | [UMBRELLA] Phoenix Test should use object based Plan for result comparison instead of using hard-corded comparison |  Major | . |
| [PHOENIX-4412](https://issues.apache.org/jira/browse/PHOENIX-4412) | Tephra transaction context visibility level returns null instead of SNAPSHOT\_ALL |  Critical | . |
| [PHOENIX-5446](https://issues.apache.org/jira/browse/PHOENIX-5446) | Support Protobuf shaded clients (thin + thick) |  Major | . |
| [PHOENIX-4866](https://issues.apache.org/jira/browse/PHOENIX-4866) | UDFs get error: org.apache.phoenix.schema.FunctionNotFoundException: ERROR 6001 (42F01): Function undefined |  Blocker | . |


### NEW FEATURES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-6213](https://issues.apache.org/jira/browse/PHOENIX-6213) | Extend Cell Tags to Delete object. |  Major | . |
| [PHOENIX-6186](https://issues.apache.org/jira/browse/PHOENIX-6186) | Store table metadata last modified timestamp in PTable / System.Catalog |  Major | . |
| [PHOENIX-5628](https://issues.apache.org/jira/browse/PHOENIX-5628) | Phoenix Function to Return HBase Row Key of Column Cell |  Major | . |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-6380](https://issues.apache.org/jira/browse/PHOENIX-6380) | phoenix-client-embedded depends on logging classes |  Major | core |
| [PHOENIX-6114](https://issues.apache.org/jira/browse/PHOENIX-6114) | Create shaded phoenix-pherf and remove lib dir from assembly |  Major | core |
| [PHOENIX-6182](https://issues.apache.org/jira/browse/PHOENIX-6182) | IndexTool to verify and repair every index row |  Major | . |
| [PHOENIX-6250](https://issues.apache.org/jira/browse/PHOENIX-6250) | Fix ViewMetadataIT test flapper |  Major | . |
| [PHOENIX-6327](https://issues.apache.org/jira/browse/PHOENIX-6327) | Consolidate Junit test categories |  Minor | core |
| [PHOENIX-6276](https://issues.apache.org/jira/browse/PHOENIX-6276) | Log when hconnection is getting closed in ConnectionQueryServicesImpl |  Major | core |
| [PHOENIX-5841](https://issues.apache.org/jira/browse/PHOENIX-5841) | When data columns get TTLed, we need inline index validation to publish a metric for this |  Major | . |
| [PHOENIX-6307](https://issues.apache.org/jira/browse/PHOENIX-6307) | Build and release official binary distributions with each HBase profile |  Major | core |
| [PHOENIX-6311](https://issues.apache.org/jira/browse/PHOENIX-6311) | Using phoenix-level table exists check in ViewUtil.getSystemTableForChildLinks |  Major | . |
| [PHOENIX-6211](https://issues.apache.org/jira/browse/PHOENIX-6211) | Paged scan filters |  Critical | . |
| [PHOENIX-6275](https://issues.apache.org/jira/browse/PHOENIX-6275) | Handle JVM exit gracefully in unit tests |  Major | . |
| [PHOENIX-6265](https://issues.apache.org/jira/browse/PHOENIX-6265) | Fix GlobalIndexOptimizationIT test flapper |  Major | . |
| [PHOENIX-6255](https://issues.apache.org/jira/browse/PHOENIX-6255) | Fix IndexScrutinyIT test flapper |  Major | . |
| [PHOENIX-6256](https://issues.apache.org/jira/browse/PHOENIX-6256) | Fix MaxConcurrentConnectionsIT test flapper |  Major | . |
| [PHOENIX-6259](https://issues.apache.org/jira/browse/PHOENIX-6259) | Fix IndexExtendedIT test flapper |  Major | . |
| [PHOENIX-6258](https://issues.apache.org/jira/browse/PHOENIX-6258) | Fix IndexScrutinyToolIT test flapper |  Major | . |
| [PHOENIX-6252](https://issues.apache.org/jira/browse/PHOENIX-6252) | Fix BackwardCompatibilityIT test flapper |  Major | . |
| [PHOENIX-6251](https://issues.apache.org/jira/browse/PHOENIX-6251) | Fix ConcurrentMutationsExtendedIT.testConcurrentUpsertsWithNoIndexedColumns timout issue |  Major | . |
| [PHOENIX-6245](https://issues.apache.org/jira/browse/PHOENIX-6245) | Update tephra dependency version to 0.16.0 |  Major | core |
| [PHOENIX-6082](https://issues.apache.org/jira/browse/PHOENIX-6082) | No need to do checkAndPut when altering properties for a table or view with column-encoding enabled |  Major | . |
| [PHOENIX-6242](https://issues.apache.org/jira/browse/PHOENIX-6242) | Remove remaining  commons-logging references |  Minor | core |
| [PHOENIX-6086](https://issues.apache.org/jira/browse/PHOENIX-6086) | Take a snapshot of all SYSTEM tables before attempting to upgrade them |  Critical | . |
| [PHOENIX-6237](https://issues.apache.org/jira/browse/PHOENIX-6237) | Relocate javax. classes in phoenix-client |  Major | core |
| [PHOENIX-6231](https://issues.apache.org/jira/browse/PHOENIX-6231) | Update Omid to 1.0.2 |  Major | core |
| [PHOENIX-5895](https://issues.apache.org/jira/browse/PHOENIX-5895) | Leverage WALCellFilter in the SystemCatalogWALEntryFilter to replicate system catalog table |  Minor | core |
| [PHOENIX-6155](https://issues.apache.org/jira/browse/PHOENIX-6155) | Prevent doing direct upserts into SYSTEM.TASK from the client |  Major | . |
| [PHOENIX-6207](https://issues.apache.org/jira/browse/PHOENIX-6207) | Paged server side grouped aggregate operations |  Major | . |
| [PHOENIX-5998](https://issues.apache.org/jira/browse/PHOENIX-5998) | Paged server side ungrouped aggregate operations |  Major | . |
| [PHOENIX-6212](https://issues.apache.org/jira/browse/PHOENIX-6212) | Improve SystemCatalogIT.testSystemTableSplit() to ensure no splitting occurs when splitting is disabled |  Major | . |
| [PHOENIX-6184](https://issues.apache.org/jira/browse/PHOENIX-6184) | Emit ageOfUnverifiedRow metric during read repairs |  Minor | core |
| [PHOENIX-6209](https://issues.apache.org/jira/browse/PHOENIX-6209) | Remove unused estimateParallelLevel() |  Minor | core |
| [PHOENIX-6208](https://issues.apache.org/jira/browse/PHOENIX-6208) | Backport the assembly changes in PHOENIX-6178 to 4.x |  Major | core |
| [PHOENIX-6126](https://issues.apache.org/jira/browse/PHOENIX-6126) | All createViewAddChildLink requests will go to the same region of SYSTEM.CHILD\_LINK |  Minor | . |
| [PHOENIX-6167](https://issues.apache.org/jira/browse/PHOENIX-6167) | Adding maxMutationCellSizeBytes config and exception |  Trivial | . |
| [PHOENIX-6181](https://issues.apache.org/jira/browse/PHOENIX-6181) | IndexRepairRegionScanner to verify and repair every global index row |  Major | . |
| [PHOENIX-6129](https://issues.apache.org/jira/browse/PHOENIX-6129) | Optimize tableExists() call while retrieving correct MUTEX table |  Major | . |
| [PHOENIX-6202](https://issues.apache.org/jira/browse/PHOENIX-6202) | New column in index gets added as PK with CASCADE INDEX |  Major | . |
| [PHOENIX-6172](https://issues.apache.org/jira/browse/PHOENIX-6172) | Updating VIEW\_INDEX\_ID column type and ts in Syscat with a 4.16 upgrade script |  Major | . |
| [PHOENIX-6189](https://issues.apache.org/jira/browse/PHOENIX-6189) | DATA\_EXCEEDS\_MAX\_CAPACITY exception error string should contain column name instead of actual value |  Trivial | . |
| [PHOENIX-6125](https://issues.apache.org/jira/browse/PHOENIX-6125) | Make sure SYSTEM.TASK does not split |  Major | . |
| [PHOENIX-6151](https://issues.apache.org/jira/browse/PHOENIX-6151) | Switch phoenix-client to shade-by-default mode |  Major | core |
| [PHOENIX-6185](https://issues.apache.org/jira/browse/PHOENIX-6185) | OPERATION\_TIMED\_OUT#newException method swallows the exception message and root cause exception. |  Major | core |
| [PHOENIX-6160](https://issues.apache.org/jira/browse/PHOENIX-6160) | Simplifying concurrent mutation handling for global Indexes |  Major | . |
| [PHOENIX-6173](https://issues.apache.org/jira/browse/PHOENIX-6173) | Archive test artifacts in Jenkins multibranch postcommit job |  Major | core |
| [PHOENIX-6128](https://issues.apache.org/jira/browse/PHOENIX-6128) | Remove unused getAdmin() call inside CQSI.init() |  Minor | . |
| [PHOENIX-6055](https://issues.apache.org/jira/browse/PHOENIX-6055) | Improve error reporting for index validation when there are "Not matching index rows" |  Major | . |
| [PHOENIX-5909](https://issues.apache.org/jira/browse/PHOENIX-5909) | Table and index-level metrics for indexing coprocs |  Major | . |
| [PHOENIX-6112](https://issues.apache.org/jira/browse/PHOENIX-6112) | Coupling of two classes only use logger |  Minor | core |
| [PHOENIX-5957](https://issues.apache.org/jira/browse/PHOENIX-5957) | Index rebuild should remove old index rows with higher timestamp |  Major | . |
| [PHOENIX-5896](https://issues.apache.org/jira/browse/PHOENIX-5896) | Implement incremental rebuild along the failed regions in IndexTool |  Major | . |
| [PHOENIX-6093](https://issues.apache.org/jira/browse/PHOENIX-6093) | adding hashcode to phoenix pherf Column class |  Minor | . |
| [PHOENIX-6102](https://issues.apache.org/jira/browse/PHOENIX-6102) | Better isolation for CI jobs an ASF Jenkins |  Major | . |
| [PHOENIX-6034](https://issues.apache.org/jira/browse/PHOENIX-6034) | Optimize InListIT |  Major | core |
| [PHOENIX-6059](https://issues.apache.org/jira/browse/PHOENIX-6059) | Adding more pagination tests |  Minor | . |
| [PHOENIX-6063](https://issues.apache.org/jira/browse/PHOENIX-6063) | Remove bugus jline and sqlline dependency from phoenix-core |  Major | core |
| [PHOENIX-5928](https://issues.apache.org/jira/browse/PHOENIX-5928) | Index rebuilds without replaying data table mutations |  Major | . |
| [PHOENIX-6037](https://issues.apache.org/jira/browse/PHOENIX-6037) | Change default HBase profile to 1.3 in 4.x |  Major | core |
| [PHOENIX-5760](https://issues.apache.org/jira/browse/PHOENIX-5760) | Pherf Support Sequential Datatypes for INTEGER type fields and have fixed row distribution |  Minor | . |
| [PHOENIX-5883](https://issues.apache.org/jira/browse/PHOENIX-5883) | Add HBase 1.6 compatibility module to 4.x branch |  Major | . |
| [PHOENIX-5789](https://issues.apache.org/jira/browse/PHOENIX-5789) | try to standardize on a JSON library |  Minor | core |
| [PHOENIX-5975](https://issues.apache.org/jira/browse/PHOENIX-5975) | Index rebuild/verification page size should be configurable from IndexTool |  Major | . |
| [PHOENIX-5951](https://issues.apache.org/jira/browse/PHOENIX-5951) | IndexTool output logging for past-max-lookback rows should be configurable |  Major | . |
| [PHOENIX-5897](https://issues.apache.org/jira/browse/PHOENIX-5897) | SingleKeyValueTuple.toString() returns unexpected result |  Minor | . |
| [PHOENIX-5793](https://issues.apache.org/jira/browse/PHOENIX-5793) | Support parallel init and fast null return for SortMergeJoinPlan. |  Minor | . |
| [PHOENIX-5956](https://issues.apache.org/jira/browse/PHOENIX-5956) | Optimize LeftSemiJoin For SortMergeJoin |  Major | . |
| [PHOENIX-5875](https://issues.apache.org/jira/browse/PHOENIX-5875) | Optional logging for IndexTool verification |  Major | . |
| [PHOENIX-5910](https://issues.apache.org/jira/browse/PHOENIX-5910) | IndexTool verification-only runs should have counters for unverified rows |  Major | . |
| [PHOENIX-5931](https://issues.apache.org/jira/browse/PHOENIX-5931) | PhoenixIndexFailurePolicy throws NPE if cause of IOE is null |  Minor | . |
| [PHOENIX-5256](https://issues.apache.org/jira/browse/PHOENIX-5256) | Remove queryserver related scripts/files as the former has its own repo |  Trivial | . |
| [PHOENIX-5899](https://issues.apache.org/jira/browse/PHOENIX-5899) | Index writes and verifications should contain information of underlying cause of failure |  Major | . |
| [PHOENIX-5892](https://issues.apache.org/jira/browse/PHOENIX-5892) | Add code coverage steps in build documentation |  Major | . |
| [PHOENIX-5891](https://issues.apache.org/jira/browse/PHOENIX-5891) | Ensure that code coverage does not drop with subsequent commits |  Major | . |
| [PHOENIX-5842](https://issues.apache.org/jira/browse/PHOENIX-5842) | Code Coverage tool for Phoenix |  Major | . |
| [PHOENIX-5878](https://issues.apache.org/jira/browse/PHOENIX-5878) | IndexTool should not fail jobs when the only errors are due to rows out of MaxLookback |  Major | . |
| [PHOENIX-5808](https://issues.apache.org/jira/browse/PHOENIX-5808) | Improve shaded artifact naming convetions |  Major | core |
| [PHOENIX-4521](https://issues.apache.org/jira/browse/PHOENIX-4521) | Allow Pherf scenario to define per query max allowed query execution duration after which thread is interrupted |  Major | . |
| [PHOENIX-5748](https://issues.apache.org/jira/browse/PHOENIX-5748) | Simplify index update generation code for consistent global indexes |  Major | . |
| [PHOENIX-5794](https://issues.apache.org/jira/browse/PHOENIX-5794) | Create a threshold for non async index creation, that can be modified in configs |  Major | . |
| [PHOENIX-5814](https://issues.apache.org/jira/browse/PHOENIX-5814) | disable trimStackTrace |  Major | connectors, core, omid, queryserver, tephra |
| [PHOENIX-5746](https://issues.apache.org/jira/browse/PHOENIX-5746) | Update release documentation to include versions information |  Major | . |
| [PHOENIX-5734](https://issues.apache.org/jira/browse/PHOENIX-5734) | IndexScrutinyTool should not report rows beyond maxLookBack age |  Major | . |
| [PHOENIX-5751](https://issues.apache.org/jira/browse/PHOENIX-5751) | Remove redundant IndexUtil#isGlobalIndexCheckEnabled() calls for immutable data tables |  Major | . |
| [PHOENIX-5641](https://issues.apache.org/jira/browse/PHOENIX-5641) | Decouple phoenix-queryserver from phoenix-core |  Major | . |
| [PHOENIX-5633](https://issues.apache.org/jira/browse/PHOENIX-5633) | Add table name info to scan logging |  Major | . |
| [PHOENIX-5720](https://issues.apache.org/jira/browse/PHOENIX-5720) | Multiple scans on the same table region cause incorrect IndexTool counters |  Major | . |
| [PHOENIX-5697](https://issues.apache.org/jira/browse/PHOENIX-5697) | Avoid resource leakage with try-with-resources |  Major | . |
| [PHOENIX-5703](https://issues.apache.org/jira/browse/PHOENIX-5703) | Add MAVEN\_HOME toPATH in jenkins build |  Major | . |
| [PHOENIX-5694](https://issues.apache.org/jira/browse/PHOENIX-5694) | Add MR job counters for IndexTool inline verification |  Major | . |
| [PHOENIX-5634](https://issues.apache.org/jira/browse/PHOENIX-5634) | Use 'phoenix.default.update.cache.frequency' from connection properties at query time |  Minor | . |
| [PHOENIX-5645](https://issues.apache.org/jira/browse/PHOENIX-5645) | BaseScannerRegionObserver should prevent compaction from purging very recently deleted cells |  Major | . |
| [PHOENIX-5674](https://issues.apache.org/jira/browse/PHOENIX-5674) | IndexTool to not write already correct index rows |  Major | . |
| [PHOENIX-5454](https://issues.apache.org/jira/browse/PHOENIX-5454) | Phoenix scripts start foreground java processes as child processes |  Minor | . |
| [PHOENIX-5658](https://issues.apache.org/jira/browse/PHOENIX-5658) | IndexTool to verify index rows inline |  Major | . |
| [PHOENIX-5630](https://issues.apache.org/jira/browse/PHOENIX-5630) | MAX\_MUTATION\_SIZE\_EXCEEDED and MAX\_MUTATION\_SIZE\_BYTES\_EXCEEDED SQLExceptions should print existing size |  Minor | . |
| [PHOENIX-5646](https://issues.apache.org/jira/browse/PHOENIX-5646) | Correct and update the release documentation |  Major | . |
| [PHOENIX-5614](https://issues.apache.org/jira/browse/PHOENIX-5614) | Remove unnecessary instances of ClassNotFoundException thrown stemming from various QueryUtil APIs |  Minor | . |


### BUG FIXES:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-6343](https://issues.apache.org/jira/browse/PHOENIX-6343) | Phoenix allows duplicate column names when one of them is a primary key |  Major | core |
| [PHOENIX-6382](https://issues.apache.org/jira/browse/PHOENIX-6382) | Shaded artifact names and descriptions have unresolved ${hbase.profile} strings |  Major | core |
| [PHOENIX-6377](https://issues.apache.org/jira/browse/PHOENIX-6377) | phoenix-client has erronous maven dependecies |  Critical | core |
| [PHOENIX-6360](https://issues.apache.org/jira/browse/PHOENIX-6360) | phoenix-core has compile dependency on phoenix-hbase-compat |  Blocker | core |
| [PHOENIX-5874](https://issues.apache.org/jira/browse/PHOENIX-5874) | IndexTool does not set TTL on its log tables correctly |  Major | . |
| [PHOENIX-6078](https://issues.apache.org/jira/browse/PHOENIX-6078) | Remove Internal Phoenix Connections from parent LinkedQueue when closed |  Major | . |
| [PHOENIX-5872](https://issues.apache.org/jira/browse/PHOENIX-5872) | Close Internal Phoenix Connections that were running during cancel |  Major | . |
| [PHOENIX-6339](https://issues.apache.org/jira/browse/PHOENIX-6339) | Older client using aggregate queries shows incorrect results. |  Blocker | . |
| [PHOENIX-6273](https://issues.apache.org/jira/browse/PHOENIX-6273) | Add support to handle MR Snapshot restore externally |  Major | core |
| [PHOENIX-6288](https://issues.apache.org/jira/browse/PHOENIX-6288) | Minicluster startup problems on Jenkins |  Critical | . |
| [PHOENIX-6330](https://issues.apache.org/jira/browse/PHOENIX-6330) | SystemCatalogRegionObserver isn't added when cluster is initialized with isNamespaceMappingEnabled=true |  Critical | core |
| [PHOENIX-6319](https://issues.apache.org/jira/browse/PHOENIX-6319) | remove cp python logic from the release script |  Major | . |
| [PHOENIX-3710](https://issues.apache.org/jira/browse/PHOENIX-3710) | Cannot use lowername data table name with indextool |  Minor | . |
| [PHOENIX-6298](https://issues.apache.org/jira/browse/PHOENIX-6298) | Use timestamp of PENDING\_DISABLE\_COUNT to calculate elapse time for PENDING\_DISABLE state |  Major | . |
| [PHOENIX-6148](https://issues.apache.org/jira/browse/PHOENIX-6148) | [SchemaExtractionTool]DDL parsing exception in Phoenix in view name |  Major | . |
| [PHOENIX-6295](https://issues.apache.org/jira/browse/PHOENIX-6295) | Fix non-static inner classes for better memory management |  Major | . |
| [PHOENIX-3499](https://issues.apache.org/jira/browse/PHOENIX-3499) | Enable null value for quote character for CSVBulkLoad tool |  Major | . |
| [PHOENIX-3633](https://issues.apache.org/jira/browse/PHOENIX-3633) | null pointer exception when subsquery for not exists returns empty result set |  Major | . |
| [PHOENIX-6293](https://issues.apache.org/jira/browse/PHOENIX-6293) | PHOENIX-6193 breaks projects depending on the phoenix-client artifact |  Blocker | core |
| [PHOENIX-6296](https://issues.apache.org/jira/browse/PHOENIX-6296) | Synchronize @Parameters, @BeforeClass and @AfterClass methods take 2 |  Critical | core |
| [PHOENIX-6286](https://issues.apache.org/jira/browse/PHOENIX-6286) | Extend syscat RegionSplitPolicies from the default Policy for the HBase branch |  Major | . |
| [PHOENIX-6287](https://issues.apache.org/jira/browse/PHOENIX-6287) | Fix incorrect log  in ParallelIterators.submitWork |  Major | . |
| [PHOENIX-6283](https://issues.apache.org/jira/browse/PHOENIX-6283) | MutableIndexExtendedIT#testCompactDisabledIndex consistently fails with HBase 2.3 |  Blocker | core |
| [PHOENIX-6267](https://issues.apache.org/jira/browse/PHOENIX-6267) | View Index PK Fixed Width Field Truncation |  Blocker | . |
| [PHOENIX-6274](https://issues.apache.org/jira/browse/PHOENIX-6274) | Flaky test TableSnapshotReadsMapReduceIT.testMapReduceSnapshotsMultiRegion |  Major | . |
| [PHOENIX-6263](https://issues.apache.org/jira/browse/PHOENIX-6263) | Few Table references are leaked |  Major | . |
| [PHOENIX-6218](https://issues.apache.org/jira/browse/PHOENIX-6218) | Rows deleted count for client side deletes is incorrect for immutable tables with indexes |  Major | . |
| [PHOENIX-6058](https://issues.apache.org/jira/browse/PHOENIX-6058) | When maxLookback is not enabled, IndexTool should not do deep verification |  Blocker | . |
| [PHOENIX-6241](https://issues.apache.org/jira/browse/PHOENIX-6241) | ViewIndexId sequences collide with ones created on the pre-4.15 version |  Blocker | . |
| [PHOENIX-5921](https://issues.apache.org/jira/browse/PHOENIX-5921) | Phoenix Index verification logging Exception with huge huge row |  Minor | . |
| [PHOENIX-5712](https://issues.apache.org/jira/browse/PHOENIX-5712) | Got SYSCAT  ILLEGAL\_DATA exception after created tenant index on view |  Blocker | . |
| [PHOENIX-6232](https://issues.apache.org/jira/browse/PHOENIX-6232) | Correlated subquery should not push to RegionServer as the probe side of the Hash join |  Major | . |
| [PHOENIX-6239](https://issues.apache.org/jira/browse/PHOENIX-6239) | NullPointerException when index table does not use COLUMN\_ENCODED\_BYTES |  Major | . |
| [PHOENIX-5860](https://issues.apache.org/jira/browse/PHOENIX-5860) | Throw exception which region is closing or splitting when delete data |  Major | core |
| [PHOENIX-5960](https://issues.apache.org/jira/browse/PHOENIX-5960) | Creating a view on a non-existent table throws the wrong exception |  Minor | . |
| [PHOENIX-6223](https://issues.apache.org/jira/browse/PHOENIX-6223) | could not find or load main class sqline.SqLine |  Minor | . |
| [PHOENIX-5920](https://issues.apache.org/jira/browse/PHOENIX-5920) | Skip SYSTEM TABLE checks while creating phoenix connection if client has set the DoNotUpgrade config |  Major | . |
| [PHOENIX-6233](https://issues.apache.org/jira/browse/PHOENIX-6233) | QueryTimeoutIT fails sometimes. |  Minor | . |
| [PHOENIX-6224](https://issues.apache.org/jira/browse/PHOENIX-6224) | Support  Correlated IN Subquery |  Major | . |
| [PHOENIX-6230](https://issues.apache.org/jira/browse/PHOENIX-6230) | IT suite hangs on ViewConcurrencyAndFailureIT |  Critical | core |
| [PHOENIX-6228](https://issues.apache.org/jira/browse/PHOENIX-6228) | Admin resources are not closed in some places |  Major | . |
| [PHOENIX-6191](https://issues.apache.org/jira/browse/PHOENIX-6191) | Creating a view which has its own new columns should also do checkAndPut checks on SYSTEM.MUTEX |  Critical | . |
| [PHOENIX-6221](https://issues.apache.org/jira/browse/PHOENIX-6221) | Getting CNF while creating transactional table with Omid |  Blocker | omid |
| [PHOENIX-6123](https://issues.apache.org/jira/browse/PHOENIX-6123) | Old clients cannot query a view if the parent has an index |  Blocker | . |
| [PHOENIX-5955](https://issues.apache.org/jira/browse/PHOENIX-5955) | OrphanViewToolIT is flapping |  Major | . |
| [PHOENIX-5472](https://issues.apache.org/jira/browse/PHOENIX-5472) | Typos in the docs description |  Minor | . |
| [PHOENIX-5669](https://issues.apache.org/jira/browse/PHOENIX-5669) | Remove hack for PHOENIX-3121 |  Major | . |
| [PHOENIX-6203](https://issues.apache.org/jira/browse/PHOENIX-6203) | CQS.getTable(byte[] tableName) does not throw TNFE even if table doesn't exist |  Major | . |
| [PHOENIX-5940](https://issues.apache.org/jira/browse/PHOENIX-5940) | Pre-4.15 client cannot connect to 4.15+ server after SYSTEM.CATALOG region has split |  Blocker | . |
| [PHOENIX-6091](https://issues.apache.org/jira/browse/PHOENIX-6091) | Calling MetaDataProtocol.getVersion() on a 4.16 timestamp gives version as 4.15.x |  Minor | . |
| [PHOENIX-6032](https://issues.apache.org/jira/browse/PHOENIX-6032) | When phoenix.allow.system.catalog.rollback=true, a view still sees data from a column that was dropped |  Blocker | . |
| [PHOENIX-5210](https://issues.apache.org/jira/browse/PHOENIX-5210) | NullPointerException when alter options of a table that is appendOnlySchema |  Major | . |
| [PHOENIX-6158](https://issues.apache.org/jira/browse/PHOENIX-6158) | create table/view should not update VIEW\_INDEX\_ID\_DATA\_TYPE column |  Major | . |
| [PHOENIX-6179](https://issues.apache.org/jira/browse/PHOENIX-6179) | Relax the MaxLookBack age checks during an upgrade |  Critical | . |
| [PHOENIX-6087](https://issues.apache.org/jira/browse/PHOENIX-6087) | Phoenix Connection leak in UpgradeUtil.addViewIndexToParentLinks() |  Major | . |
| [PHOENIX-6030](https://issues.apache.org/jira/browse/PHOENIX-6030) | When phoenix.allow.system.catalog.rollback=true, a view still sees data for columns that were dropped from its parent view |  Blocker | . |
| [PHOENIX-6002](https://issues.apache.org/jira/browse/PHOENIX-6002) | Fix connection leaks throughout instances where we use QueryUtil.getConnectionOnServer |  Major | . |
| [PHOENIX-6124](https://issues.apache.org/jira/browse/PHOENIX-6124) | Block adding/dropping a column on a parent view for clients \<4.15 and for clients that have phoenix.allow.system.catalog.rollback=true |  Blocker | . |
| [PHOENIX-6192](https://issues.apache.org/jira/browse/PHOENIX-6192) | UpgradeUtil.syncUpdateCacheFreqAllIndexes() does not use tenant-specific connection to resolve tenant views |  Major | . |
| [PHOENIX-6197](https://issues.apache.org/jira/browse/PHOENIX-6197) | AggregateIT and StoreNullsIT hangs |  Blocker | core |
| [PHOENIX-6142](https://issues.apache.org/jira/browse/PHOENIX-6142) | Make DDL operations resilient to orphan parent-\>child linking rows in SYSTEM.CHILD\_LINK |  Blocker | . |
| [PHOENIX-6188](https://issues.apache.org/jira/browse/PHOENIX-6188) | Jenkins job history uses too much storage |  Blocker | connectors, core |
| [PHOENIX-6193](https://issues.apache.org/jira/browse/PHOENIX-6193) | PHOENIX-6151 slows down shading |  Critical | core |
| [PHOENIX-6194](https://issues.apache.org/jira/browse/PHOENIX-6194) | Build failure due to missing slf4j dependency in phoenix-tools |  Major | . |
| [PHOENIX-5839](https://issues.apache.org/jira/browse/PHOENIX-5839) | CompatUtil#setStopRow semantics problem |  Major | core |
| [PHOENIX-6169](https://issues.apache.org/jira/browse/PHOENIX-6169) | IT suite never finishes on 4.x with HBase 1.3 or 1.4 |  Blocker | core |
| [PHOENIX-6159](https://issues.apache.org/jira/browse/PHOENIX-6159) | Phoenix-pherf writes the result file even disableRuntimeResult flag is true |  Major | . |
| [PHOENIX-6153](https://issues.apache.org/jira/browse/PHOENIX-6153) | Table Map Reduce job after a Snapshot based job fails with CorruptedSnapshotException |  Major | core |
| [PHOENIX-6168](https://issues.apache.org/jira/browse/PHOENIX-6168) | PHOENIX-6143 breaks tests on linux |  Major | core |
| [PHOENIX-6122](https://issues.apache.org/jira/browse/PHOENIX-6122) | Upgrade jQuery to 3.5.1 |  Major | core |
| [PHOENIX-6143](https://issues.apache.org/jira/browse/PHOENIX-6143) | Get Phoenix Tracing Webapp work |  Major | core |
| [PHOENIX-6075](https://issues.apache.org/jira/browse/PHOENIX-6075) | DDLs issued via a tenant-specific connection do not write SYSTEM.MUTEX cells |  Blocker | . |
| [PHOENIX-6136](https://issues.apache.org/jira/browse/PHOENIX-6136) | javax.servlet.UnavailableException thrown when using Spark connector |  Major | core, spark-connector |
| [PHOENIX-6072](https://issues.apache.org/jira/browse/PHOENIX-6072) | SYSTEM.MUTEX not created with a TTL on a fresh cluster connected to by a 4.15+ client |  Blocker | . |
| [PHOENIX-6130](https://issues.apache.org/jira/browse/PHOENIX-6130) | StatementContext.subqueryResults should be thread safe |  Major | . |
| [PHOENIX-6069](https://issues.apache.org/jira/browse/PHOENIX-6069) | We should check that the parent table key is in the region in the MetaDataEndpointImpl.dropTable code |  Major | . |
| [PHOENIX-5171](https://issues.apache.org/jira/browse/PHOENIX-5171) | SkipScan incorrectly filters composite primary key which the key range contains all values |  Blocker | . |
| [PHOENIX-6115](https://issues.apache.org/jira/browse/PHOENIX-6115) | Avoid scanning prior row state for uncovered local indexes on immutable tables. |  Major | . |
| [PHOENIX-6106](https://issues.apache.org/jira/browse/PHOENIX-6106) | Speed up ConcurrentMutationsExtendedIT |  Major | . |
| [PHOENIX-5986](https://issues.apache.org/jira/browse/PHOENIX-5986) | DropTableWithViewsIT.testDropTableWithChildViews is flapping again |  Major | . |
| [PHOENIX-6094](https://issues.apache.org/jira/browse/PHOENIX-6094) | Update jacoco plugin version to 0.8.5. |  Major | . |
| [PHOENIX-6090](https://issues.apache.org/jira/browse/PHOENIX-6090) | Local indexes get out of sync after changes for global consistent indexes |  Blocker | . |
| [PHOENIX-6073](https://issues.apache.org/jira/browse/PHOENIX-6073) | IndexTool IndexDisableLoggingType can't be set to NONE |  Minor | . |
| [PHOENIX-6077](https://issues.apache.org/jira/browse/PHOENIX-6077) | PHOENIX-5946 breaks mvn verify |  Blocker | core |
| [PHOENIX-5958](https://issues.apache.org/jira/browse/PHOENIX-5958) | Diverged view created via an older client still sees dropped column data |  Blocker | . |
| [PHOENIX-5969](https://issues.apache.org/jira/browse/PHOENIX-5969) | Read repair reduces the number of rows returned for LIMIT queries |  Major | . |
| [PHOENIX-6022](https://issues.apache.org/jira/browse/PHOENIX-6022) | RVC Offset does not handle trailing nulls properly |  Major | . |
| [PHOENIX-6045](https://issues.apache.org/jira/browse/PHOENIX-6045) | Delete that should qualify for index path does not use index when multiple indexes are available. |  Major | . |
| [PHOENIX-6023](https://issues.apache.org/jira/browse/PHOENIX-6023) | Wrong result when issuing query for an immutable table with multiple column families |  Major | . |
| [PHOENIX-6013](https://issues.apache.org/jira/browse/PHOENIX-6013) | RVC Offset does not handle coerced literal nulls properly. |  Major | . |
| [PHOENIX-6011](https://issues.apache.org/jira/browse/PHOENIX-6011) | ServerCacheClient throw NullPointerException |  Major | . |
| [PHOENIX-6026](https://issues.apache.org/jira/browse/PHOENIX-6026) | Fix BackwardCompatibilityIT so it can run locally |  Major | . |
| [PHOENIX-5976](https://issues.apache.org/jira/browse/PHOENIX-5976) | Cannot drop a column when the index view is involved |  Blocker | . |
| [PHOENIX-6017](https://issues.apache.org/jira/browse/PHOENIX-6017) | Hadoop QA Precommit build keeps failing with release audit warning for phoenix-server/dependency-reduced-pom.xml |  Major | . |
| [PHOENIX-5924](https://issues.apache.org/jira/browse/PHOENIX-5924) | RVC Offset does not handle variable length fields exclusive scan boundary correctly |  Major | . |
| [PHOENIX-5981](https://issues.apache.org/jira/browse/PHOENIX-5981) | Wrong multiple counting of resultSetTimeMs and wallclockTimeMs in OverallQueryMetrics |  Major | . |
| [PHOENIX-6000](https://issues.apache.org/jira/browse/PHOENIX-6000) | Client side DELETEs should use local indexes for filtering |  Major | . |
| [PHOENIX-5984](https://issues.apache.org/jira/browse/PHOENIX-5984) | Query timeout counter is not updated in all timeouts cases |  Major | . |
| [PHOENIX-5935](https://issues.apache.org/jira/browse/PHOENIX-5935) | Select with non primary keys and PHOENIX\_ROW\_TIMESTAMP() in where clause fails |  Major | . |
| [PHOENIX-5967](https://issues.apache.org/jira/browse/PHOENIX-5967) | phoenix-client transitively pulling in phoenix-core |  Critical | . |
| [PHOENIX-5996](https://issues.apache.org/jira/browse/PHOENIX-5996) | IndexRebuildRegionScanner.prepareIndexMutationsForRebuild may incorrectly delete index row when a delete and put mutation with the same timestamp |  Major | . |
| [PHOENIX-6001](https://issues.apache.org/jira/browse/PHOENIX-6001) | Incremental rebuild/verification can result in missing rows and false positives |  Critical | . |
| [PHOENIX-5997](https://issues.apache.org/jira/browse/PHOENIX-5997) | Phoenix Explain Plan for Deletes does not clearly differentiate between server side and client side paths. |  Minor | . |
| [PHOENIX-5995](https://issues.apache.org/jira/browse/PHOENIX-5995) | Index Rebuild page size is not honored in case of point deletes |  Major | . |
| [PHOENIX-5779](https://issues.apache.org/jira/browse/PHOENIX-5779) | SplitSystemCatalogIT tests fail with Multiple Regions error |  Major | . |
| [PHOENIX-5937](https://issues.apache.org/jira/browse/PHOENIX-5937) | Order by on nullable column sometimes filters rows |  Major | core |
| [PHOENIX-5898](https://issues.apache.org/jira/browse/PHOENIX-5898) | Phoenix function CURRENT\_TIME() returns wrong result when view indexes are used. |  Major | . |
| [PHOENIX-5970](https://issues.apache.org/jira/browse/PHOENIX-5970) | ViewUtil.dropChildViews may cause HConnection leak which may cause ITtests hange |  Major | . |
| [PHOENIX-5905](https://issues.apache.org/jira/browse/PHOENIX-5905) | Reset user to hbase by changing rpc context before getting user permissions on access controller service |  Major | . |
| [PHOENIX-5902](https://issues.apache.org/jira/browse/PHOENIX-5902) | Document or fix new compat jar behavior. |  Blocker | . |
| [PHOENIX-5942](https://issues.apache.org/jira/browse/PHOENIX-5942) | ParameterizedIndexUpgradeIT is too slow |  Minor | . |
| [PHOENIX-5932](https://issues.apache.org/jira/browse/PHOENIX-5932) | View Index rebuild results in surplus rows from other view indexes |  Major | . |
| [PHOENIX-5922](https://issues.apache.org/jira/browse/PHOENIX-5922) | IndexUpgradeTool should always re-enable tables on failure |  Major | . |
| [PHOENIX-5656](https://issues.apache.org/jira/browse/PHOENIX-5656) | Make Phoenix scripts work with Python 3 |  Critical | . |
| [PHOENIX-5929](https://issues.apache.org/jira/browse/PHOENIX-5929) | IndexToolForNonTxGlobalIndexIT.testDisableOutputLogging is flapping |  Major | . |
| [PHOENIX-5884](https://issues.apache.org/jira/browse/PHOENIX-5884) | Join query return empty result when filters for both the tables are present |  Major | . |
| [PHOENIX-5863](https://issues.apache.org/jira/browse/PHOENIX-5863) | Upsert into view against a table with index throws exception when 4.14.3 client connects to 4.16 server |  Blocker | . |
| [PHOENIX-4753](https://issues.apache.org/jira/browse/PHOENIX-4753) | Remove the need for users to have Write access to the Phoenix SYSTEM STATS TABLE to drop tables |  Major | . |
| [PHOENIX-5580](https://issues.apache.org/jira/browse/PHOENIX-5580) | Wrong values seen when updating a view for a table that has an index |  Major | . |
| [PHOENIX-5864](https://issues.apache.org/jira/browse/PHOENIX-5864) | RuleGeneratorTest unit test seem to be failing |  Major | . |
| [PHOENIX-5743](https://issues.apache.org/jira/browse/PHOENIX-5743) | Concurrent read repairs on the same index row should be idempotent |  Critical | . |
| [PHOENIX-5807](https://issues.apache.org/jira/browse/PHOENIX-5807) | Index rows without empty column should be treated as unverified |  Major | . |
| [PHOENIX-5799](https://issues.apache.org/jira/browse/PHOENIX-5799) | Inline Index Verification Output API |  Major | . |
| [PHOENIX-5810](https://issues.apache.org/jira/browse/PHOENIX-5810) | PhoenixMRJobSubmitter is not working on a cluster with a single yarn RM |  Major | . |
| [PHOENIX-5816](https://issues.apache.org/jira/browse/PHOENIX-5816) | IndexToolTimeRangeIT hangs forever |  Blocker | core |
| [PHOENIX-5802](https://issues.apache.org/jira/browse/PHOENIX-5802) | Connection leaks in UPSERT SELECT/DELETE paths due to MutatingParallelIteratorFactory iterator not being closed |  Major | . |
| [PHOENIX-5801](https://issues.apache.org/jira/browse/PHOENIX-5801) | Connection leak when creating a view with a where condition |  Major | . |
| [PHOENIX-5776](https://issues.apache.org/jira/browse/PHOENIX-5776) | Phoenix pherf unit tests failing |  Major | . |
| [PHOENIX-5698](https://issues.apache.org/jira/browse/PHOENIX-5698) | Phoenix Query with RVC IN list expression generates wrong scan with non-pk ordered pks |  Major | . |
| [PHOENIX-5797](https://issues.apache.org/jira/browse/PHOENIX-5797) | RVC Offset does not work with tenant views on global indexes |  Minor | . |
| [PHOENIX-5790](https://issues.apache.org/jira/browse/PHOENIX-5790) | Add Apache license header to compatible\_client\_versions.json |  Minor | . |
| [PHOENIX-5718](https://issues.apache.org/jira/browse/PHOENIX-5718) | GetTable builds a table excluding the given clientTimeStamp |  Major | . |
| [PHOENIX-5785](https://issues.apache.org/jira/browse/PHOENIX-5785) | Remove TTL check in QueryCompiler when doing an SCN / Lookback query |  Major | . |
| [PHOENIX-5673](https://issues.apache.org/jira/browse/PHOENIX-5673) | The mutation state is silently getting cleared on the execution of any DDL |  Critical | . |
| [PHOENIX-5065](https://issues.apache.org/jira/browse/PHOENIX-5065) | Inconsistent treatment of NULL and empty string |  Major | . |
| [PHOENIX-5753](https://issues.apache.org/jira/browse/PHOENIX-5753) | Fix erroneous query result when RVC is clipped with desc column |  Major | . |
| [PHOENIX-5768](https://issues.apache.org/jira/browse/PHOENIX-5768) | Supporting partial overwrites for immutable tables with indexes |  Critical | . |
| [PHOENIX-5731](https://issues.apache.org/jira/browse/PHOENIX-5731) | Loading bulkload hfiles should not be blocked if the upsert select happening for differet table. |  Major | . |
| [PHOENIX-5766](https://issues.apache.org/jira/browse/PHOENIX-5766) | PhoenixMetricsIT failure in 4.x for HBase 1.3 |  Major | . |
| [PHOENIX-5636](https://issues.apache.org/jira/browse/PHOENIX-5636) | Improve the error message when client connects to server with higher major version |  Minor | . |
| [PHOENIX-5745](https://issues.apache.org/jira/browse/PHOENIX-5745) | Fix QA false negatives |  Major | . |
| [PHOENIX-5737](https://issues.apache.org/jira/browse/PHOENIX-5737) | Hadoop QA run says no tests even though there are added IT tests |  Minor | . |
| [PHOENIX-5537](https://issues.apache.org/jira/browse/PHOENIX-5537) | Phoenix-4701 made hard coupling between phoenix.log.level and getting request metrics. |  Minor | . |
| [PHOENIX-5529](https://issues.apache.org/jira/browse/PHOENIX-5529) | Creating a grand-child view on a table with an index fails |  Major | . |
| [PHOENIX-5695](https://issues.apache.org/jira/browse/PHOENIX-5695) | Phoenix website build.sh should return when child script has errors |  Major | . |
| [PHOENIX-5724](https://issues.apache.org/jira/browse/PHOENIX-5724) | Use exec permission in Phoenix ACLs only when execute check enabled |  Major | . |
| [PHOENIX-5691](https://issues.apache.org/jira/browse/PHOENIX-5691) | create index is failing when phoenix acls enabled and ranger is enabled |  Major | . |
| [PHOENIX-5440](https://issues.apache.org/jira/browse/PHOENIX-5440) | multiple warnings when building phoenix |  Minor | . |
| [PHOENIX-5704](https://issues.apache.org/jira/browse/PHOENIX-5704) | Covered column updates are not generated for previously deleted data table row |  Critical | . |
| [PHOENIX-5708](https://issues.apache.org/jira/browse/PHOENIX-5708) | GlobalIndexChecker returns unverified index row cells |  Major | . |
| [PHOENIX-5706](https://issues.apache.org/jira/browse/PHOENIX-5706) | IndexTool verification reports failure when data row has no covered column values |  Major | . |
| [PHOENIX-5512](https://issues.apache.org/jira/browse/PHOENIX-5512) | IndexTool returns error after rebuilding a DISABLED index |  Major | . |
| [PHOENIX-5677](https://issues.apache.org/jira/browse/PHOENIX-5677) | Replace System.currentTimeMillis with EnvironmentEdgeManager in non-test code |  Major | . |
| [PHOENIX-5692](https://issues.apache.org/jira/browse/PHOENIX-5692) | SCN verification breaks with non-default column families |  Major | . |
| [PHOENIX-5676](https://issues.apache.org/jira/browse/PHOENIX-5676) | Inline-verification from IndexTool does not handle TTL/row-expiry |  Major | . |
| [PHOENIX-5666](https://issues.apache.org/jira/browse/PHOENIX-5666) | IndexRegionObserver incorrectly updates PostIndexUpdateFailure metric |  Major | . |
| [PHOENIX-5654](https://issues.apache.org/jira/browse/PHOENIX-5654) | String values (ALWAYS and NEVER) don't work for connection level config phoenix.default.update.cache.frequency |  Major | . |
| [PHOENIX-5650](https://issues.apache.org/jira/browse/PHOENIX-5650) | IndexUpgradeTool does not rebuild view indexes |  Major | . |
| [PHOENIX-5655](https://issues.apache.org/jira/browse/PHOENIX-5655) | ServerCache using table map is not correctly removed |  Major | . |
| [PHOENIX-5096](https://issues.apache.org/jira/browse/PHOENIX-5096) | Local index region pruning is not working as expected. |  Major | . |
| [PHOENIX-5637](https://issues.apache.org/jira/browse/PHOENIX-5637) | Queries with SCN return expired rows |  Major | . |
| [PHOENIX-5640](https://issues.apache.org/jira/browse/PHOENIX-5640) | Pending disable count should not be increased for rebuild write failures |  Major | . |
| [PHOENIX-4824](https://issues.apache.org/jira/browse/PHOENIX-4824) | Update BRANCH\_NAMES in dev/test-patch.properties |  Major | . |


### TESTS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-5719](https://issues.apache.org/jira/browse/PHOENIX-5719) | testIndexRebuildTask test is failing on pre-commit and master build |  Major | . |
| [PHOENIX-5296](https://issues.apache.org/jira/browse/PHOENIX-5296) | Ensure store file reader refcount is zero at end of relevant unit tests |  Major | . |
| [PHOENIX-6300](https://issues.apache.org/jira/browse/PHOENIX-6300) | Fix PartialIndexRebuilderIT.testIndexWriteFailureDisablingIndex test flapper |  Major | . |
| [PHOENIX-6302](https://issues.apache.org/jira/browse/PHOENIX-6302) | Fix ConcurrentUpsertsWithoutIndexedColsIT flapper |  Major | . |
| [PHOENIX-6297](https://issues.apache.org/jira/browse/PHOENIX-6297) | Fix IndexMetadataIT.testAsyncRebuildAll test flapper |  Major | . |
| [PHOENIX-6301](https://issues.apache.org/jira/browse/PHOENIX-6301) | Fix BackwardCompatibilityIT.testSystemTaskCreationWithIndexAsyncRebuild test flapper |  Major | . |
| [PHOENIX-6289](https://issues.apache.org/jira/browse/PHOENIX-6289) | Flaky test UpsertSelectIT.testUpsertSelectWithNoIndex |  Major | . |
| [PHOENIX-6284](https://issues.apache.org/jira/browse/PHOENIX-6284) | Flaky test UpgradeIT.testConcurrentUpgradeThrowsUpgradeInProgressException |  Major | . |
| [PHOENIX-6183](https://issues.apache.org/jira/browse/PHOENIX-6183) | Page size tests are not propagating test override values to server |  Major | . |
| [PHOENIX-5747](https://issues.apache.org/jira/browse/PHOENIX-5747) | Add upsert tests for immutable table indexes |  Minor | . |
| [PHOENIX-6246](https://issues.apache.org/jira/browse/PHOENIX-6246) | Flaky test PointInTimeQueryIT |  Major | . |
| [PHOENIX-5973](https://issues.apache.org/jira/browse/PHOENIX-5973) | IndexToolForNonTxGlobalIndexIT - Stabilize and speed up |  Major | . |
| [PHOENIX-5607](https://issues.apache.org/jira/browse/PHOENIX-5607) | Client-server backward compatibility tests |  Blocker | . |
| [PHOENIX-5671](https://issues.apache.org/jira/browse/PHOENIX-5671) | Add tests for ViewUtil |  Minor | . |
| [PHOENIX-5616](https://issues.apache.org/jira/browse/PHOENIX-5616) | Speed up ParameterizedIndexUpgradeToolIT |  Minor | . |
| [PHOENIX-5617](https://issues.apache.org/jira/browse/PHOENIX-5617) | Allow using the server side JDBC client in Phoenix Sandbox. |  Major | . |


### SUB-TASKS:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-6342](https://issues.apache.org/jira/browse/PHOENIX-6342) | RoundRobinResultIterator doesn't close itself when result size = fetchsize-1 |  Blocker | core |
| [PHOENIX-6329](https://issues.apache.org/jira/browse/PHOENIX-6329) | Eliminate or serialize minicluster restart operations in Integration tests |  Major | core |
| [PHOENIX-6220](https://issues.apache.org/jira/browse/PHOENIX-6220) | CREATE INDEX shouldn't ignore IMMUTABLE\_STORAGE\_SCHEME and COLUMN\_ENDCODED\_BYTES |  Major | . |
| [PHOENIX-6120](https://issues.apache.org/jira/browse/PHOENIX-6120) | Change IndexMaintainer for SINGLE\_CELL\_ARRAY\_WITH\_OFFSETS indexes. Currently it assumes data and index table having the same storage and encoding format. |  Major | . |
| [PHOENIX-6219](https://issues.apache.org/jira/browse/PHOENIX-6219) | GlobalIndexChecker should work for index:SINGLE\_CELL\_ARRAY\_WITH\_OFFSETS and data:ONE\_CELL\_PER\_COLUMN |  Major | . |
| [PHOENIX-6292](https://issues.apache.org/jira/browse/PHOENIX-6292) | Extend explain plan object based comparison to majority remaining tests |  Major | . |
| [PHOENIX-5435](https://issues.apache.org/jira/browse/PHOENIX-5435) | Annotate HBase WALs with Phoenix Metadata |  Major | . |
| [PHOENIX-6269](https://issues.apache.org/jira/browse/PHOENIX-6269) | Extend explain plan object based comparison to some more tests |  Major | . |
| [PHOENIX-5592](https://issues.apache.org/jira/browse/PHOENIX-5592) | MapReduce job to asynchronously delete rows where the VIEW\_TTL has expired. |  Major | . |
| [PHOENIX-5728](https://issues.apache.org/jira/browse/PHOENIX-5728) | ExplainPlan with plan as attributes object, use it for BaseStatsCollectorIT |  Major | . |
| [PHOENIX-5601](https://issues.apache.org/jira/browse/PHOENIX-5601) | PHOENIX-5601 Add a new coprocessor for PHOENIX\_TTL - PhoenixTTLRegionObserver |  Major | . |
| [PHOENIX-6171](https://issues.apache.org/jira/browse/PHOENIX-6171) | Child views should not be allowed to override the parent view PHOENIX\_TTL attribute. |  Major | . |
| [PHOENIX-6170](https://issues.apache.org/jira/browse/PHOENIX-6170) | PHOENIX\_TTL spec should be in seconds instead of milliseconds |  Major | . |
| [PHOENIX-6101](https://issues.apache.org/jira/browse/PHOENIX-6101) | Avoid duplicate work between local and global indexes |  Major | . |
| [PHOENIX-6097](https://issues.apache.org/jira/browse/PHOENIX-6097) | Improve LOCAL index consistency tests |  Minor | . |
| [PHOENIX-5933](https://issues.apache.org/jira/browse/PHOENIX-5933) | Rename VIEW\_TTL property to be PHOENIX\_TTL |  Major | . |
| [PHOENIX-5317](https://issues.apache.org/jira/browse/PHOENIX-5317) | Upserting rows into child views with pk fails when the base view has an index on it. |  Major | . |
| [PHOENIX-5678](https://issues.apache.org/jira/browse/PHOENIX-5678) | Cleanup anonymous inner classes used for BaseMutationPlan |  Major | . |
| [PHOENIX-5501](https://issues.apache.org/jira/browse/PHOENIX-5501) | Add support for VIEW\_TTL table property during DDL |  Major | . |


### OTHER:

| JIRA | Summary | Priority | Component |
|:---- |:---- | :--- |:---- |
| [PHOENIX-6147](https://issues.apache.org/jira/browse/PHOENIX-6147) | Copy Github PR discussions to JIRA |  Major | . |
| [PHOENIX-6107](https://issues.apache.org/jira/browse/PHOENIX-6107) | Discuss speed up of BaseQueryIT |  Major | . |
| [PHOENIX-6065](https://issues.apache.org/jira/browse/PHOENIX-6065) | Add OWASP dependency check, and update the flagged direct dependencies |  Major | connectors, core, queryserver |
| [PHOENIX-6309](https://issues.apache.org/jira/browse/PHOENIX-6309) | Use maven enforcer to ban imports |  Major | . |
| [PHOENIX-6282](https://issues.apache.org/jira/browse/PHOENIX-6282) | Generate PB files inline with build and remove checked in files |  Major | . |
| [PHOENIX-6261](https://issues.apache.org/jira/browse/PHOENIX-6261) | Reorganise project structure to make mvn versions:set work |  Major | connectors, core |
| [PHOENIX-6137](https://issues.apache.org/jira/browse/PHOENIX-6137) | Update Omid to 1.0.2 and Tephra to 0.16 in 4.x |  Major | core |
| [PHOENIX-6196](https://issues.apache.org/jira/browse/PHOENIX-6196) | Update phoenix.mutate.maxSizeBytes to accept long values |  Major | . |
| [PHOENIX-6146](https://issues.apache.org/jira/browse/PHOENIX-6146) | Run precommit checks on github PRs |  Major | core |
| [PHOENIX-5032](https://issues.apache.org/jira/browse/PHOENIX-5032) | add Apache Yetus to Phoenix |  Major | . |
| [PHOENIX-6056](https://issues.apache.org/jira/browse/PHOENIX-6056) | Migrate from builds.apache.org by August 15 |  Critical | . |
| [PHOENIX-5962](https://issues.apache.org/jira/browse/PHOENIX-5962) | Stabilize builds |  Major | . |
| [PHOENIX-5818](https://issues.apache.org/jira/browse/PHOENIX-5818) | Add documentation for query timeoutDuration attribute in Pherf scenarios |  Minor | . |
| [PHOENIX-5825](https://issues.apache.org/jira/browse/PHOENIX-5825) | Remove PhoenixCanaryTool and CanaryTestResult from phoenix repo |  Major | core, queryserver |
| [PHOENIX-5822](https://issues.apache.org/jira/browse/PHOENIX-5822) | Move Python driver to queryserver repo |  Major | core, queryserver |
| [PHOENIX-5762](https://issues.apache.org/jira/browse/PHOENIX-5762) | Update jackson |  Major | . |
| [PHOENIX-5767](https://issues.apache.org/jira/browse/PHOENIX-5767) | Add 4.x branch to the precommit script configuration |  Blocker | . |
| [PHOENIX-5721](https://issues.apache.org/jira/browse/PHOENIX-5721) | Unify 4.x branches |  Major | . |



