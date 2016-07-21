<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

phoenix-spark extends Phoenix's MapReduce support to allow Spark to load Phoenix tables as RDDs or
DataFrames, and enables persisting RDDs of Tuples back to Phoenix.

## Reading Phoenix Tables

Given a Phoenix table with the following DDL

```sql
CREATE TABLE TABLE1 (ID BIGINT NOT NULL PRIMARY KEY, COL1 VARCHAR);
UPSERT INTO TABLE1 (ID, COL1) VALUES (1, 'test_row_1');
UPSERT INTO TABLE1 (ID, COL1) VALUES (2, 'test_row_2');
```

### Load as a DataFrame using the Data Source API
```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._

val sc = new SparkContext("local", "phoenix-test")
val sqlContext = new SQLContext(sc)

val df = sqlContext.load(
  "org.apache.phoenix.spark", 
  Map("table" -> "TABLE1", "zkUrl" -> "phoenix-server:2181")
)

df
  .filter(df("COL1") === "test_row_1" && df("ID") === 1L)
  .select(df("ID"))
  .show
```

### Load as a DataFrame directly using a Configuration object
```scala
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._

val configuration = new Configuration()
// Can set Phoenix-specific settings, requires 'hbase.zookeeper.quorum'

val sc = new SparkContext("local", "phoenix-test")
val sqlContext = new SQLContext(sc)

// Load the columns 'ID' and 'COL1' from TABLE1 as a DataFrame
val df = sqlContext.phoenixTableAsDataFrame(
  "TABLE1", Array("ID", "COL1"), conf = configuration
)

df.show
```

### Load as an RDD, using a Zookeeper URL
```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._

val sc = new SparkContext("local", "phoenix-test")

// Load the columns 'ID' and 'COL1' from TABLE1 as an RDD
val rdd: RDD[Map[String, AnyRef]] = sc.phoenixTableAsRDD(
  "TABLE1", Seq("ID", "COL1"), zkUrl = Some("phoenix-server:2181")
)

rdd.count()

val firstId = rdd1.first()("ID").asInstanceOf[Long]
val firstCol = rdd1.first()("COL1").asInstanceOf[String]
```

## Saving RDDs to Phoenix 

`saveToPhoenix` is an implicit method on RDD[Product], or an RDD of Tuples. The data types must
correspond to the Java types Phoenix supports (http://phoenix.apache.org/language/datatypes.html)

Given a Phoenix table with the following DDL

```sql
CREATE TABLE OUTPUT_TEST_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
```

```scala
import org.apache.spark.SparkContext
import org.apache.phoenix.spark._

val sc = new SparkContext("local", "phoenix-test")
val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

sc
  .parallelize(dataSet)
  .saveToPhoenix(
    "OUTPUT_TEST_TABLE",
    Seq("ID","COL1","COL2"),
    zkUrl = Some("phoenix-server:2181")
  )
```

## Saving DataFrames to Phoenix

The `save` is method on DataFrame allows passing in a data source type. You can use
`org.apache.phoenix.spark`, and must also pass in a `table` and `zkUrl` parameter to
specify which table and server to persist the DataFrame to. The column names are derived from
the DataFrame's schema field names, and must match the Phoenix column names.

The `save` method also takes a `SaveMode` option, for which only `SaveMode.Overwrite` is supported.

Given two Phoenix tables with the following DDL:

```sql
CREATE TABLE INPUT_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
CREATE TABLE OUTPUT_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
```

```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._

// Load INPUT_TABLE
val sc = new SparkContext("local", "phoenix-test")
val sqlContext = new SQLContext(sc)
val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> "INPUT_TABLE",
  "zkUrl" -> hbaseConnectionString))

// Save to OUTPUT_TABLE
df.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "OUTPUT_TABLE", 
  "zkUrl" -> hbaseConnectionString))
```

## Notes

The functions `phoenixTableAsDataFrame`, `phoenixTableAsRDD` and `saveToPhoenix` all support
optionally specifying a `conf` Hadoop configuration parameter with custom Phoenix client settings,
as well as an optional `zkUrl` parameter for the Phoenix connection URL.

If `zkUrl` isn't specified, it's assumed that the "hbase.zookeeper.quorum" property has been set
in the `conf` parameter. Similarly, if no configuration is passed in, `zkUrl` must be specified.

## Limitations

- Basic support for column and predicate pushdown using the Data Source API
- The Data Source API does not support passing custom Phoenix settings in configuration, you must
create the DataFrame or RDD directly if you need fine-grained configuration.
- No support for aggregate or distinct functions (http://phoenix.apache.org/phoenix_mr.html)
