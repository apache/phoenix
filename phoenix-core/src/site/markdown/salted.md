# Salted Tables

HBase sequential write may suffer from region server hotspotting if your row key is monotonically increasing. Salting the row key provides a way to mitigate the problem. Details of the method would be found on [this link](http://blog.sematext.com/2012/04/09/hbasewd-avoid-regionserver-hotspotting-despite-writing-records-with-sequential-keys/).

Phoenix provides a way to transparently salt the row key with a salting byte for a particular table. You need to specify this in table creation time by specifying a table property "SALT_BUCKETS" with a value from 1 to 256. Like this:

CREATE TABLE table (a_key VARCHAR PRIMARY KEY, a_col VARCHAR) SALT_BUCKETS = 20;

There are some cautions and difference in behavior you should be aware about when using a salted table.

### Sequential Scan
Since salting table would not store the data sequentially, a strict sequential scan would not return all the data in the natural sorted fashion. Clauses that currently would force a sequential scan, for example, clauses with LIMIT, would likely to return items that are different from a normal table.

### Splitting
If no split points are specified for the table, the salted table would be pre-split on salt bytes boundaries to ensure load distribution among region servers even during the initial phase of the table. If users are to provide split points manually, users need to include a salt byte in the split points they provide.

### Row Key Ordering
Pre-spliting also ensures that all entries in the region server all starts with the same salt byte, and therefore are stored in a sorted manner. When doing a parallel scan across all region servers, we can take advantage of this properties to perform a merge sort of the client side. The resulting scan would still be return sequentially as if it is from a normal table.

This Rowkey order scan can be turned on by setting the <code>phoenix.query.rowKeyOrderSaltedTable</code> config property to <code>true</code> in your hbase-sites.xml. When set, we disallow user specified split points on salted table to ensure that each bucket will only contains entries with the same salt byte. When this property is turned on, the salted table would behave just like a normal table and would return items in rowkey order for scans.

### Performance
Using salted table with pre-split would help uniformly distribute write workload across all the region servers, thus improves the write performance. Our own [performance evaluation](performance.html#Salting) shows that a salted table achieves 80% write throughput increases over non-salted table.

Reading from salted table can also reap benefits from the more uniform distribution of data. Our [performance evaluation](performance.html#Salting) shows much improved read performances on read queries with salted table over non-salted table when we focus our query on a subset of the data.
