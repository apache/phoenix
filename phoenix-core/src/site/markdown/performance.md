# Performance

Phoenix follows the philosophy of **bringing the computation to the data** by using:
* **coprocessors** to perform operations on the server-side thus minimizing client/server data transfer
* **custom filters** to prune data as close to the source as possible
In addition, to minimize any startup costs, Phoenix uses native HBase APIs rather than going through the map/reduce framework.
## Phoenix vs related products
Below are charts showing relative performance between Phoenix and some other related products.

### Phoenix vs Hive (running over HDFS and HBase)
![Phoenix vs Hive](images/PhoenixVsHive.png)

Query: select count(1) from table over 10M and 100M rows. Data is 5 narrow columns. Number of Region 
Servers: 4 (HBase heap: 10GB, Processor: 6 cores @ 3.3GHz Xeon)

### Phoenix vs Impala (running over HBase)
![Phoenix vs Impala](images/PhoenixVsImpala.png)

Query: select count(1) from table over 1M and 5M rows. Data is 3 narrow columns. Number of Region Server: 1 (Virtual Machine, HBase heap: 2GB, Processor: 2 cores @ 3.3GHz Xeon)

***
## Latest Automated Performance Run

[Latest Automated Performance Run](http://phoenix-bin.github.io/client/performance/latest.htm) | 
[Automated Performance Runs History](http://phoenix-bin.github.io/client/performance/)

***

## Performance improvements in Phoenix 1.2

### Essential Column Family
Phoenix 1.2 query filter leverages [HBase Filter Essential Column Family](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/SingleColumnValueFilter.html#isFamilyEssential(byte[]) feature which leads to improved performance when Phoenix query filters on data that is split in multiple column families (cf) by only loading essential cf. In second pass, all cf are are loaded as needed.

Consider the following schema in which data is split in two cf
`create table t (k varchar not null primary key, a.c1 integer, b.c2 varchar, b.c3 varchar, b.c4 varchar)`. 

Running a query similar to the following shows significant performance when a subset of rows match filter
`select count(c2) from t where c1 = ?` 

Following chart shows query in-memory performance of running the above query with 10M rows on 4 region servers when 10% of the rows matches the filter. Note: cf-a is approx 8 bytes and cf-b is approx 400 bytes wide.

![Ess. CF](images/perf-esscf.png)


### Skip Scan

Skip Scan Filter leverages [SEEK_NEXT_USING_HINT](http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/Filter.ReturnCode.html#SEEK_NEXT_USING_HINT) of HBase Filter. It significantly improves point queries over key columns.

Consider the following schema in which data is split in two cf
`create table t (k varchar not null primary key, a.c1 integer, b.c2 varchar, b.c3 varchar)`. 

Running a query similar to the following shows significant performance when a subset of rows match filter
`select count(c1) from t where k in (1% random k's)` 

Following chart shows query in-memory performance of running the above query with 10M rows on 4 region servers when 1% random keys over the entire range passed in query `IN` clause. Note: all varchar columns are approx 15 bytes.

![SkipScan](images/perf-skipscan.png)


### Salting
Salting in Phoenix 1.2 leads to both improved read and write performance by adding an extra hash byte at start of key and pre-splitting data in number of regions. This eliminates hot-spotting of single or few regions servers. Read more about this feature [here](salted.html).

Consider the following schema

`CREATE TABLE T (HOST CHAR(2) NOT NULL,DOMAIN VARCHAR NOT NULL,`
`FEATURE VARCHAR NOT NULL,DATE DATE NOT NULL,USAGE.CORE BIGINT,USAGE.DB BIGINT,STATS.ACTIVE_VISITOR`
`INTEGER CONSTRAINT PK PRIMARY KEY (HOST, DOMAIN, FEATURE, DATE)) SALT_BUCKETS = 4`. 

Following chart shows write performance with and without the use of Salting which splits table in 4 regions running on 4 region server cluster (Note: For optimal performance, number of salt buckets should match number of region servers).

![Salted-Write](images/perf-salted-write.png)

Following chart shows in-memory query performance for 10M row table where `host='NA'` filter matches 3.3M rows

`select count(1) from t where host='NA'`

![Salted-Read](images/perf-salted-read.png)


### Top-N 

Following chart shows in-memory query time of running the Top-N query over 10M rows using Phoenix 1.2 and Hive over HBase

`select core from t order by core desc limit 10`

![Phoenix vs Hive](images/perf-topn.png)
