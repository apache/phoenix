phoenix-spark extends Phoenix's MapReduce support to allow Spark to load Phoenix tables as RDDs or
DataFrames, and enables persisting RDDs of Tuples back to Phoenix.

## Reading Phoenix Tables

Given a Phoenix table with the following DDL

```sql
CREATE TABLE TABLE1 (ID BIGINT NOT NULL PRIMARY KEY, COL1 VARCHAR);
UPSERT INTO TABLE1 (ID, COL1) VALUES (1, 'test_row_1');
UPSERT INTO TABLE1 (ID, COL1) VALUES (2, 'test_row_2');
```

### Load as a DataFrame
```scala
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._

val sc = new SparkContext("local", "phoenix-test")
val sqlContext = new SQLContext(sc)

// Load the columns 'ID' and 'COL1' from TABLE1 as a DataFrame
val df = sqlContext.phoenixTableAsDataFrame(
  "TABLE1", Array("ID", "COL1"), zkUrl = Some("phoenix-server:2181")
)

df.show
```

### Load as an RDD
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

Given a Phoenix table with the following DDL

```sql
CREATE TABLE OUTPUT_TEST_TABLE (id BIGINT NOT NULL PRIMARY KEY, col1 VARCHAR, col2 INTEGER);
```

`saveToPhoenix` is an implicit method on RDD[Product], or an RDD of Tuples. The data types must
correspond to the Java types Phoenix supports (http://phoenix.apache.org/language/datatypes.html)

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

## Notes

The functions `phoenixTableAsDataFrame`, `phoenixTableAsRDD` and `saveToPhoenix` all support
optionally specifying a `conf` Hadoop configuration parameter with custom Phoenix client settings,
as well as an optional `zkUrl` parameter for the Phoenix connection URL.

If `zkUrl` isn't specified, it's assumed that the "hbase.zookeeper.quorum" property has been set
in the `conf` parameter. Similarly, if no configuration is passed in, `zkUrl` must be specified.

## Limitations

- No pushdown predicate support from Spark SQL (yet)
- No support for aggregate or distinct functions (http://phoenix.apache.org/phoenix_mr.html)