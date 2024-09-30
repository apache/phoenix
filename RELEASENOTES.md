
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
# PHOENIX  5.2.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [PHOENIX-7404](https://issues.apache.org/jira/browse/PHOENIX-7404) | *Major* | **Build the HBase 2.5+ profiles with Hadoop 3.3.6**

Phoenix is now built with Hadoop 3.3.6 for the HBase 2.5 and 2.6 profiles.


---

* [PHOENIX-7363](https://issues.apache.org/jira/browse/PHOENIX-7363) | *Blocker* | **Protect server side metadata cache updates for the given PTable**

PHOENIX-6066 introduces a way for us to take HBase read level row-lock while retrieving the PTable object as part of the getTable() RPC call, by default. Before PHOENIX-6066, only write level row-lock was used, which hurts the performance even if the server side metadata cache has latest data, requiring no lookup from SYSTEM.CATALOG table.

PHOENIX-7363 allows to protect the metadata cache update at the server side with Phoenix write level row-lock. As part of getTable() call, we already must be holding HBase read level row-lock. Hence, PHOENIX-7363 provides protection for server side metadata cache updates.

PHOENIX-6066 and PHOENIX-7363 must be combined.



# PHOENIX  5.2.0 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [PHOENIX-7285](https://issues.apache.org/jira/browse/PHOENIX-7285) | *Major* | **Upgade Zookeeper to 3.8.4**

Phoenix now includes Zookeeper 3.8.4 when build for HBase 2.4 and later.


---

* [PHOENIX-7230](https://issues.apache.org/jira/browse/PHOENIX-7230) | *Major* | **Optimize rpc call to master if all indexes are migrated to new coprocs**

Use "phoenix.index.region.observer.enabled.all.tables" as "false" only if new index coprocs (GlobalIndexChecker, IndexRegionObserver etc) are not in use by all tables. Default value "true" indicates that we will not perform extra RPC call to retrieve TableDescriptor for all Mutations.


---

* [PHOENIX-7220](https://issues.apache.org/jira/browse/PHOENIX-7220) | *Major* | **Make HBase 2.5 profile the default**

Phoenix now defaults to the HBase 2.5 profile when built from source.


---

* [PHOENIX-7218](https://issues.apache.org/jira/browse/PHOENIX-7218) | *Major* | **Drop HBase 2.4.0 support in 5.2**

Phoenix 5.2 does not support HBase 2.4.0.
Only HBase 2.4.1 and onwards are supported.


---

* [PHOENIX-7137](https://issues.apache.org/jira/browse/PHOENIX-7137) | *Major* | **Create phoenix-client-lite shaded JAR without server-side dependencies**

A new phoenix-client-lite shaded client JAR / artifact has been added.
This behaves mostly like phoenix-client-embdedded, but omits the Phoenix server side classes, and the HBase server dependencies.

It is recommended to use phoenix-client-lite as a JDBC driver.


---

* [PHOENIX-6721](https://issues.apache.org/jira/browse/PHOENIX-6721) | *Major* | **CSV bulkload tool fails with FileNotFoundException if --output points to the S3 location**

The Phoenix bulk load jobs now work correctly if S3 is used for --outputdir.


---

* [PHOENIX-6053](https://issues.apache.org/jira/browse/PHOENIX-6053) | *Major* | **Split Server Side Code into a Separate Module**

The previous phoenix-core maven module (and artifacts) have been split into phoenix-core-client for the client (JDBC driver) side code, and into the server (Hbase server classpath and mapreduce) modules.

The old phoenix-core module and aritfact now only includes tests.

As phoenix-core depends on both now artifacts, and acts as a backwards compatibility feature, it is expected that consumers of those maven artifacts will not be affected by this change. (But other changes in 5.2 are likely to to require changes)


---

* [PHOENIX-7143](https://issues.apache.org/jira/browse/PHOENIX-7143) | *Major* | **Detect JVM version and add the necessary module flags in startup scripts**

The Phoenix and Phoenix Query Server startup scripts now detect the JVM version used, and set the necessary JVM module flags automatically.


---

* [PHOENIX-7101](https://issues.apache.org/jira/browse/PHOENIX-7101) | *Major* | **Explain plan to output local index name if it is used**

Explain plan for the local index would output in the format of "${local\_index}(${physical\_table\_name})" instead of "${physical\_table\_name}". Because providing only "${physical\_table\_name}" in the output is confusing for user to understand whether local index is used by the plan.


---

* [PHOENIX-7095](https://issues.apache.org/jira/browse/PHOENIX-7095) | *Major* | **Implement Statement.closeOnCompletion() and fix related close() bugs**

Phoenix now implements the Statement.closeOnCompletion() method.
Phoenix now calls Statement.closeOnCompletion() when generating DatabaseMetaData resultSets.
Phoenix now correctly calls close() on all Statements when a Connection is closed().
Phoenix now closes the open ResultSet on a Statement when another statement is executed on the Statement object.

Note that Phoenix previously did not comply with the JDBC specification, and allowed for multiple open ResultSets on the same Statement object.
Application code that dependeded on this bug may need to be rewritten.


---

* [PHOENIX-7097](https://issues.apache.org/jira/browse/PHOENIX-7097) | *Major* | **Allow specifying full JDBC URL string in psql/PhoenixRuntime and sqllline.py**

Phoenix now also accepts a full JDBC URL in place of the ZK quorum in sqlline.py and psql.py


---

* [PHOENIX-6523](https://issues.apache.org/jira/browse/PHOENIX-6523) | *Major* | **Support for HBase Registry Implementations through Phoenix connection URL**

Add support for MasterRegistry and RPCConnectionRegistry to Phoenix.

Introduces the new URL protocol variants:
\* jdbc:phoenix+zk: Uses Zookeeper. This is the original registry supported since the inception of HBase and Phoenix.
\* jdbc:phoenix+rpc: Uses RPC to connecto to the specified HBase RS/Master nodes.
\* jdbc:phoenix+master: Uses RPC to connect to the specified HBase Master nodes

The syntax:
"jdbc:phoenix" : uses the default registry and and connection from hbase-site.xml

"jdbc:phoenix:param1:param2...": Protocol/Registry is determined from Hbase version and hbase-site.xml configuration, and parameters are interpreted accoring to the registry.

"jdbc:phoenix+zk:hosts:ports:zknode:principal:keytab;options..." : Behaves the same as  jdbc:phoenix... URL previously. Any missing parameters use defaults fom hbase-site.xml or the environment.

"jdbc:phoenix+rpc:hosts:ports::principal:keytab;options..." : Uses RPCConnectionRegistry. If more than two options are specified, then the third one (he unused zkNode paramater) must always be blank.

"jdbc:phoenix+master:hosts:ports::principal:keytab;options..." : Uses RPCMasterRegistry. If more than two options are specified, then the third one (he unused zkNode paramater) must always be blank.

Phoenix now also supports heterogenous ports defined in HBASE-12706 for every registry. When specifying the ports for each host separately the colon ":" character must be escaped with a backslash, i.e. "jdbc:phoenix+zk:host1\\:123,host2\\:345:/hbase:principal:keytab", or "jdbc:phoenix+rpc:host1\\:123,host2\\:345" You may need to add extra escapes to preserve the backslashes if defined in java code, etc.

Note that while the phoenix+zk URL handling code has heuristics that tries to handle some omitted parameters, the Master and ConnectionRPC registry code strictly maps the URL parameters to by their ordering.

Note that Phoenix now internally normalizes the URL. Whether you specify an explicit connection, or use the default "jdbc:phoenix" URL, Phoenix will internally normalize the connection, and set the  properties for the internal HBase Connection objects appropriately.  

Also note that for most non-HA use cases an explicit connection URL should NOT be used. The preferred way to specify the connection is to have an up-to-date hbase-site.xml with both Hbase and Phoenix client properties set correctly (with other Hadoop conficguration files as needed)  on the Phoenix application classpath , and using the default "jdbc:phoenix" URL.


---

* [PHOENIX-7034](https://issues.apache.org/jira/browse/PHOENIX-7034) | *Major* | **Disallow mapped view creation when the schema does not exists**

Phoenix no longer allows allow creating mapped views on Hbase tables whoose namespace is not defined as a Phoenix schema when namespace mapping is enabled.


---

* [PHOENIX-6973](https://issues.apache.org/jira/browse/PHOENIX-6973) | *Major* | **Add option to CREATE TABLE to skip verification of HBase table**

A new option NOVERIFY has been introduced for CREATE TABLE command that allows skipping the verification of columns with empty qualifier. This is useful when a Phoenix table is restored from HBase snapshot we can skip the lengthy validation process when executing CREATE TABLE.


---

* [PHOENIX-6920](https://issues.apache.org/jira/browse/PHOENIX-6920) | *Major* | **PhoenixResultSetMetaData doesn't distinguish between label and column\_name**

Phoenix now correctly returns the unialiased column name in ResultSetMetadata.getColumnName().


---

* [PHOENIX-6942](https://issues.apache.org/jira/browse/PHOENIX-6942) | *Minor* | **Some config properties do not have phoenix prefix**

Some properties that did not have the phoenix prefix were renamed:

index.verify.threads.max -\> phoenix.index.verify.threads.max
index.verify.row.count.per.task -\> phoenix.index.verify.row.count.per.task
index.threads.max -\> phoenix.index.threads.max
index.row.count.per.task -\> phoenix.index.row.count.per.task
index.writer.threads.max -\> phoenix.index.writer.threads.max
index.writer.threads.keepalivetime -\> phoenix.index.writer.threads.keepalivetime
phoneix.mapreduce.output.cluster.quorum -\> phoenix.mapreduce.output.cluster.quorum
index.writer.commiter.class -\> phoenix.index.writer.commiter.class
index.writer.failurepolicy.class -\> phoenix.index.writer.failurepolicy.class

The old property names still work via the Hadoop Configuration deprecation mechanism.


---

* [PHOENIX-6907](https://issues.apache.org/jira/browse/PHOENIX-6907) | *Major* | **Explain Plan should output region locations with servers**

"EXPLAIN {query}" has no change in the output.

Introduced new query syntax "EXPLAIN WITH REGIONS {query}" to add region locations in the output.

Config "phoenix.max.region.locations.size.explain.plan" can be used to limit num of region locations to be printed with the explain plan output. The default value is 5.


---

* [PHOENIX-6952](https://issues.apache.org/jira/browse/PHOENIX-6952) | *Blocker* | **Do not disable normalizer on salted tables**

Phoenix will no longer disable the normalizer on newly created salted tables, and will allow enabling the normalizer on salted tables.

It is highly recommended to review all salted Phoenix tables, and manually set NORMALIZATION\_ENABLED=true on them, to avoid regions growing uncontrollably.
It is not possible to determine whether NORMALIZATION\_ENABLED=false was automatically set by Phoenix, or was manually set by some other reason, i.e. because
merging/splitting is handled manually, so care must be taken not to revert normalization where it was set explcitly by the user.

Also consider setting MERGE\_ENABLED=false on salted Phoenix tables. While it is no longer necessary for correct operation, it may improve performance in some case.


---

* [PHOENIX-6910](https://issues.apache.org/jira/browse/PHOENIX-6910) | *Major* | **Scans created during query compilation and execution against salted tables need to be more resilient**

Queries on salted tables now work correctly even if rows belonging in different buckets are present in a single region.


---

* [PHOENIX-5521](https://issues.apache.org/jira/browse/PHOENIX-5521) | *Major* | **Phoenix-level HBase Replication sink (Endpoint coproc)**

The changes required to enable replication sink coproc, and allow it to attach phoenix metadata as Mutation attributes at the Sink cluster:

1. Add "org.apache.phoenix.coprocessor.ReplicationSinkEndpoint" to hbase.coprocessor.regionserver.classes config
2. phoenix.append.metadata.to.wal = true


---

* [PHOENIX-6944](https://issues.apache.org/jira/browse/PHOENIX-6944) | *Major* | **Randomize mapper task ordering for Index MR tools**

Phoenix can now randomize the mapper execution order for MapReduce tools.
This is enabled by default for IndexTool and IndexScrutinyTool.
The randomization can be enabled or disabled for all MR tools via the "phoenix.mapreduce.randomize.mapper.execution.order" boolean property.


---

* [PHOENIX-6916](https://issues.apache.org/jira/browse/PHOENIX-6916) | *Critical* | **Cannot handle ranges where start is a prefix of end for desc columns**

Phoenix now handles ranges where the start key is a prefix of the desc key for descending columns.


---

* [PHOENIX-6881](https://issues.apache.org/jira/browse/PHOENIX-6881) | *Major* | **Implement the applicable Date/Time features from JDBC 4.2**

Phoenix now supports setting and retrieving java.time.LocalDate, java.time.LocalDateTime and java.time.LocalTime objects via the ResultSet.getObject() and PreparedStatement.setObject() APIs, as defined by JDBC 4.2


---

* [PHOENIX-5066](https://issues.apache.org/jira/browse/PHOENIX-5066) | *Critical* | **The TimeZone is incorrectly used during writing or reading data**

Adds the phoenix.query.applyTimeZoneDisplacement attribute, which enables applying timezone displacement when setting or retriteving java.sql. time types via PreparedSatetement and ResultSet objects.

Setting the attribute will result in interpreting the epoch-based java.sql time types in the local timezone.

This attribute is off by default for backwards compatibility reasons, and needs to be enabled explicitly. This is a client side attribute, and can be set on a per connection basis.


---

* [PHOENIX-6877](https://issues.apache.org/jira/browse/PHOENIX-6877) | *Major* | **Remove HBase 2.3 support from 5.2**

Phoenix 5.2 no longer supports HBase 2.3.x


---

* [PHOENIX-6720](https://issues.apache.org/jira/browse/PHOENIX-6720) | *Blocker* | **CREATE TABLE can't recreate column encoded tables that had columns dropped**

PHOENIX now supports the COLUMN\_QUALIFIER\_COUNTER table option and the ENCODED\_QUALIFIER qualifier keyword for columns to set the "QUALIFIER\_COUNTER" and "COLUMN\_QUALIFIER" columns respectively in SYSTEM.CATALOG for the table.

The CREATE TABLE statement and SchemaTool will now use the above features when generating the DDL for column encoded tables with discontinuous qualifiers.

This enables re-generating the Phoenix metadata for tables that have discontinous encoded column qualifers, so that the re-generated Phoenix tables will work on copied or replicated HBase data tables from the source database.

For such tables, the generated CREATE TABLE statements will only be executable for Phoenix versions having this patch. However, systems that do not have this fix cannot correctly re-generate the table metadata. On older systems the only way to achieve HBase data table level compatibility is to manually edit SYSTEM.CATALOG.


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

* [PHOENIX-6715](https://issues.apache.org/jira/browse/PHOENIX-6715) | *Major* | **Update Omid to 1.1.0**

Phoenix now uses Omid 1.1.0.


---

* [PHOENIX-6751](https://issues.apache.org/jira/browse/PHOENIX-6751) | *Critical* | **Force using range scan vs skip scan when using the IN operator and large number of RVC elements**

Adds a new config parameter, phoenix.max.inList.skipScan.size, which controls the size of an IN clause before it will be automatically converted from a skip scan to a range scan.


---

* [PHOENIX-6491](https://issues.apache.org/jira/browse/PHOENIX-6491) | *Major* | **Phoenix High Availability**

Phoenix HA feature to allow clients to connect to a pair of phoenix/hbase clusters in order to improve the overall availability for the supported use-cases.

Supported use-cases:

1. Active-Standby HA for disaster recovery, enables end users to switch HBase clusters (triggered by administrators) collectively across multiple clients without restarting.
2. Active-Active HA for immutable use cases with point get queries without deletes, enables a client to connect to both clusters simultaneously for these use cases which inherently have relaxed consistency requirements.


---

* [PHOENIX-6692](https://issues.apache.org/jira/browse/PHOENIX-6692) | *Major* | **Add HBase 2.5 support**

Phoenix now supports HBase 2.5.
In Phoenix 5.1.x, Tephra support is not available with HBase 2.5.
(In 5.2.x Tephra support is fully removed)


---

* [PHOENIX-6627](https://issues.apache.org/jira/browse/PHOENIX-6627) | *Major* | **Remove all references to Tephra from 4.x and master**

Support for the Tephra transaction engine has been removed from Phoenix. 

Ordinals of \`TransactionFactory.Provider\` are maintained to preserve compatibility with deployed system catalogs. The enum label for the former Tephra provider is deliberately renamed to \`NOTAVAILABLE\` so any downstreams with ill-advised direct dependencies on it will fail to compile.

A coprocessor named \`TephraTransactionalProcessor\` is retained, with a no-op implementation, to prevent regionserver aborts in the worst case that a Tephra table remains deployed and the table coprocessor list has not changed. This should not happen. Users should be provided a migration runbook.

The \`commitDDLFence\` phase of \`MutationState\` is retained although it may no longer be necessary. Recommend a follow up issue.

\`PhoenixTransactionContext.PROPERTY\_TTL\` is retained. It is possible a future txn engine option will have a similar design feature and will need to alter cell timestamps. Recommend a follow up issue.

\`storeNulls\` for transactional tables remains enabled for compatibility. It is possible a future txn engine option will have a similar design feature and will need to manage tombstones in a non-default way. Recommend a follow up issue.

Requests to ALTER a table from transactional to non transactional are still rejected. It is very likely OMID has similar complex state cleanup and removal requirements, and txn engines in general are likely to have this class of problem.


---

* [PHOENIX-6756](https://issues.apache.org/jira/browse/PHOENIX-6756) | *Major* | **switch to using log4j2.properties instead of xml**

Phoenix now uses log4j2.properties files instead of log4j2.xml files to configure logging.
The default log format has also been changed to match the HBase 3 log format.


---

* [PHOENIX-6707](https://issues.apache.org/jira/browse/PHOENIX-6707) | *Minor* | **Use Configuration.getLongBytes where applicable**

Configuration properties representing Byte now can be specified with binary magnitue postfixes ( i.e. '1K' = 1024, '1M' = 1048576, etc. )


---

* [PHOENIX-6554](https://issues.apache.org/jira/browse/PHOENIX-6554) | *Minor* | **Pherf CLI option long/short option names do not follow conventions**

Pherf CLI now allows its long option names to use -- in addition to -, as the Unix convention suggests. While the existing behavior of using a single dash "-" for long options doesn't follow the Unix convention, we still allow it for backward compatibility.


---

* [PHOENIX-6723](https://issues.apache.org/jira/browse/PHOENIX-6723) | *Major* | **Phoenix client unable to connect on JDK14+**

Phoenix now internally depends on Apache ZooKeeper 3.5.7, and Apache Curator 4.2


---

* [PHOENIX-6685](https://issues.apache.org/jira/browse/PHOENIX-6685) | *Major* | **Change Data Capture - Populate Table / Topic Mappings**

Added STREAMING\_TOPIC\_NAME to System.Catalog, which can be set as a property on CREATE and ALTER statements


---

* [PHOENIX-6693](https://issues.apache.org/jira/browse/PHOENIX-6693) | *Major* | **Remove Hbase 2.1 and Hbase 2.2 support from Phoenix**

Phoenix no longer supports HBase 2.1 or HBase 2.2.


---

* [PHOENIX-6696](https://issues.apache.org/jira/browse/PHOENIX-6696) | *Major* | **Drop legacy phoenix-client jar**

Starting with Phoenix 5.2.0, Phoenix no longer provides the legacy phoenix-client jar.
Use the phoenix-client-embedded jar instead, and add the necessary logging backend and corresponding slf4j adapter to the classpath separately.


---

* [PHOENIX-6686](https://issues.apache.org/jira/browse/PHOENIX-6686) | *Major* | **Update Jackson to 2.12.6.1**

The Jackson dependency in Phoenix has been updated to 2.12.6.1


---

* [PHOENIX-6665](https://issues.apache.org/jira/browse/PHOENIX-6665) | *Major* | **PreparedStatement#getMetaData() fails on parametrized "select next ? values for SEQ"**

PreparedStatement#getMetaData() no longer fails on parametrized "select next ? values" sequence operations.
This also fixes a problem where parametrized sequence operations didn't work via Phoenix Query Server.


---

* [PHOENIX-6626](https://issues.apache.org/jira/browse/PHOENIX-6626) | *Major* | **Make the without.tephra profile behaviour the default in 4.x and master, and remove the profile**

Support for the Tephra transaction manager has been removed.


---

* [PHOENIX-6645](https://issues.apache.org/jira/browse/PHOENIX-6645) | *Minor* | **Remove unneccessary SCN related properties from SYSTEM tables on upgrade**

The KEEP\_DELETED\_CELLS property is removed, and the VERSIONS property is set to 1 for the SYSTEM.STATS and SYSTEM.LOG tables on upgrade now.


---

* [PHOENIX-6586](https://issues.apache.org/jira/browse/PHOENIX-6586) | *Critical* | **Set NORMALIZATION\_ENABLED to false on salted tables**

Phoenix now automatically sets NORMALIZATION\_ENABLED=false when creating salted tables.


---

* [PHOENIX-6227](https://issues.apache.org/jira/browse/PHOENIX-6227) | *Major* | **Option for DDL changes to export to external schema repository**

External\_Schema\_Id is now added to SYSTEM.CATALOG. When CHANGE\_DETECTION\_ENABLED = true, CREATE and ALTER statements to a table or view cause a call to an external schema registry is made to save a string representation of the schema to be made. The schema registry returns a schema id which is saved in the new SYSTEM.CATALOG field. 

When change detection is enabled, the WAL is now annotated with the external schema id, rather than the tuple of tenant id / schema name / logical table name / last DDL timestamp it was previously.

The implementation to generate the string representation can be set via the Configuration property org.apache.phoenix.export.schemawriter.impl . By default it is a String representation of the PTable protobuf. 

The implementation of the call to the schema repository can be set via the Configuration property org.apache.phoenix.export.schemaregistry.impl . By default it is an in-memory data structure unsuitable for production use. In production it is intended to be used with a standalone schema registry service.


---

* [PHOENIX-6551](https://issues.apache.org/jira/browse/PHOENIX-6551) | *Major* | **Bump HBase version to 2.4.6 and 2.2.7**

Phoenix now builds with HBase 2.4.6 and 2.2.7 for the respective hbase profiles.


---

* [PHOENIX-6357](https://issues.apache.org/jira/browse/PHOENIX-6357) | *Major* | **Change all command line tools to use the fixed commons-cli constructor**

Fixed command line parsing of double quoted identifiers.
The previous workaround of using double doublequotes as a workaround is no longer supported.


---

* [PHOENIX-6457](https://issues.apache.org/jira/browse/PHOENIX-6457) | *Major* | **Optionally store schema version string in SYSTEM.CATALOG**

Introduces SCHEMA\_VERSION property which can be updated during a CREATE or ALTER statement with a string value that gives a user-understood version tag.


---

* [PHOENIX-6359](https://issues.apache.org/jira/browse/PHOENIX-6359) | *Major* | **New module to support HBase 2.4.1+ releases**

Added phoenix-hbase-compat-2.4.1 to support latest patch versions of HBase 2.4 release line.


---

* [PHOENIX-6343](https://issues.apache.org/jira/browse/PHOENIX-6343) | *Major* | **Phoenix allows duplicate column names when one of them is a primary key**

Although user provided CF can have same column name as one of primary keys, default CF is no longer supported to have same column name as primary key columns.



