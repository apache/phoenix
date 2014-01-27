# New Features

As items are implemented from our road map, they are moved here to track the progress we've made:

1. **Joins**. Join support through hash joins (where one side of the query is small enough to fit into memory) is now available in our master branch.
2. **[Sequences](http://phoenix.incubator.apache.org/sequences.html)**. Support for CREATE/DROP SEQUENCE, NEXT VALUE FOR, and CURRENT VALUE FOR has been implemented and is now available in our master branch.
2. **Multi-tenancy**. Support for creating multi-tenant tables is now available in our master branch.
1. **[Secondary Indexes](secondary_indexing.html)**. Allows users to create indexes over mutable or immutable data through a new `CREATE INDEX` DDL command. Behind the scenes, Phoenix creates a separate HBase table with a different row key for the index. At query time, Phoenix takes care of choosing the best table to use based on how much of the row key can be formed. We support getting at the uncommitted <code>List&lt;KeyValue&gt;</code> for both the data and the index tables to allow an HFile to be built without needing an HBase connection using the "connectionless" of our JDBC driver.
2. **Row Value Constructors**. A standard SQL construct to efficiently locate the row at or after a composite key value. Enables a query-more capability to efficiently step through your data and optimizes IN list of composite key values to be point gets.
3. **[Map-reduce-based CSV Bulk Loader](mr_dataload.html)** Builds Phoenix-compliant HFiles and load them into HBase.
2. **Aggregation Enhancements**. <code>COUNT DISTINCT</code>, <code>PERCENTILE</code>, and <code>STDDEV</code> are now supported.
4. **Type Additions**. The <code>FLOAT</code>, <code>DOUBLE</code>, <code>TINYINT</code>, and <code>SMALLINT</code> are now supported.
2. **IN/OR/LIKE Optimizations**. When an IN (or the equivalent OR) and a LIKE appears in a query using the leading row key columns, compile it into a skip scanning filter to more efficiently retrieve the query results.
3. **Support ASC/DESC declaration of primary key columns**. Allow a primary key column to be declared as ascending (the default) or descending such that the row key order can match the desired sort order (thus preventing an extra sort).
3. **Salting Row Key**. To prevent hot spotting on writes, the row key may be *"salted"* by inserting a leading byte into the row key which is a mod over N buckets of the hash of the entire row key. This ensures even distribution of writes when the row key is a monotonically increasing value (often a timestamp representing the current time).
4. **TopN Queries**. Support a query that returns the top N rows, through support for ORDER BY when used in conjunction with TopN.
6. **Dynamic Columns**. For some use cases, it's difficult to model a schema up front. You may have columns that you'd like to specify only at query time. This is possible in HBase, in that every row (and column family) contains a map of values with keys that can be specified at run time. So, we'd like to support that.
7. **Phoenix package for the Apache Bigtop distribution**. See [BIGTOP-993](http://issues.apache.org/jira/browse/BIGTOP-993) for more information.
