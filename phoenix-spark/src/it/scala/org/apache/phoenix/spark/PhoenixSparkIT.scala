/*
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package org.apache.phoenix.spark

import java.util.Date

import org.apache.phoenix.schema.types.PVarchar
import org.apache.phoenix.util.{ColumnInfo, SchemaUtil}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

/**
  * Note: If running directly from an IDE, these are the recommended VM parameters:
  * -Xmx1536m -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m
  */
class PhoenixSparkIT extends AbstractPhoenixSparkIT {

  test("Can convert Phoenix schema") {
    val phoenixSchema = List(
      new ColumnInfo("varcharColumn", PVarchar.INSTANCE.getSqlType)
    )

    val rdd = new PhoenixRDD(sc, "MyTable", Array("Foo", "Bar"),
      conf = hbaseConfiguration)

    val catalystSchema = rdd.phoenixSchemaToCatalystSchema(phoenixSchema)

    val expected = List(StructField("varcharColumn", StringType, nullable = true))

    catalystSchema shouldEqual expected
  }

  test("Can create schema RDD and execute query") {
    val sqlContext = new SQLContext(sc)

    val df1 = sqlContext.phoenixTableAsDataFrame("TABLE1", Array("ID", "COL1"), conf = hbaseConfiguration)

    df1.registerTempTable("sql_table_1")

    val df2 = sqlContext.phoenixTableAsDataFrame("TABLE2", Array("ID", "TABLE1_ID"),
      conf = hbaseConfiguration)

    df2.registerTempTable("sql_table_2")

    val sqlRdd = sqlContext.sql(
      """
        |SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1
        |INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)""".stripMargin
    )

    val count = sqlRdd.count()

    count shouldEqual 6L
  }

  test("Can create schema RDD and execute query on case sensitive table (no config)") {
    val sqlContext = new SQLContext(sc)


    val df1 = sqlContext.phoenixTableAsDataFrame(
      SchemaUtil.getEscapedArgument("table3"),
      Array("id", "col1"),
      zkUrl = Some(quorumAddress))

    df1.registerTempTable("table3")

    val sqlRdd = sqlContext.sql("SELECT * FROM table3")

    val count = sqlRdd.count()

    count shouldEqual 2L
  }

  test("Can create schema RDD and execute constrained query") {
    val sqlContext = new SQLContext(sc)

    val df1 = sqlContext.phoenixTableAsDataFrame("TABLE1", Array("ID", "COL1"),
      conf = hbaseConfiguration)

    df1.registerTempTable("sql_table_1")

    val df2 = sqlContext.phoenixTableAsDataFrame("TABLE2", Array("ID", "TABLE1_ID"),
      predicate = Some("\"ID\" = 1"),
      conf = hbaseConfiguration)

    df2.registerTempTable("sql_table_2")

    val sqlRdd = sqlContext.sql(
      """
        |SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1
        |INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)""".stripMargin
    )

    val count = sqlRdd.count()

    count shouldEqual 1L
  }

  test("Can create schema RDD with predicate that will never match") {
    val sqlContext = new SQLContext(sc)

    val df1 = sqlContext.phoenixTableAsDataFrame(
      SchemaUtil.getEscapedArgument("table3"),
      Array("id", "col1"),
      predicate = Some("\"id\" = -1"),
      conf = hbaseConfiguration)

    df1.registerTempTable("table3")

    val sqlRdd = sqlContext.sql("SELECT * FROM table3")

    val count = sqlRdd.count()

    count shouldEqual 0L
  }

  test("Can create schema RDD with complex predicate") {
    val sqlContext = new SQLContext(sc)

    val df1 = sqlContext.phoenixTableAsDataFrame(
      "DATE_PREDICATE_TEST_TABLE",
      Array("ID", "TIMESERIES_KEY"),
      predicate = Some(
        """
          |ID > 0 AND TIMESERIES_KEY BETWEEN
          |CAST(TO_DATE('1990-01-01 00:00:01', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AND
          |CAST(TO_DATE('1990-01-30 00:00:01', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)""".stripMargin),
      conf = hbaseConfiguration)

    df1.registerTempTable("date_predicate_test_table")

    val sqlRdd = df1.sqlContext.sql("SELECT * FROM date_predicate_test_table")

    val count = sqlRdd.count()

    count shouldEqual 0L
  }

  test("Can query an array table") {
    val sqlContext = new SQLContext(sc)

    val df1 = sqlContext.phoenixTableAsDataFrame("ARRAY_TEST_TABLE", Array("ID", "VCARRAY"),
      conf = hbaseConfiguration)

    df1.registerTempTable("ARRAY_TEST_TABLE")

    val sqlRdd = sqlContext.sql("SELECT * FROM ARRAY_TEST_TABLE")

    val count = sqlRdd.count()

    // get row 0, column 1, which should be "VCARRAY"
    val arrayValues = sqlRdd.collect().apply(0).apply(1)

    arrayValues should equal(Array("String1", "String2", "String3"))

    count shouldEqual 1L
  }

  test("Can read a table as an RDD") {
    val rdd1 = sc.phoenixTableAsRDD("ARRAY_TEST_TABLE", Seq("ID", "VCARRAY"),
      conf = hbaseConfiguration)

    val count = rdd1.count()

    val arrayValues = rdd1.take(1)(0)("VCARRAY")

    arrayValues should equal(Array("String1", "String2", "String3"))

    count shouldEqual 1L
  }

  test("Can save to phoenix table") {
    val sqlContext = new SQLContext(sc)

    val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        "OUTPUT_TEST_TABLE",
        Seq("ID", "COL1", "COL2"),
        hbaseConfiguration
      )

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT ID, COL1, COL2 FROM OUTPUT_TEST_TABLE")
    val results = ListBuffer[(Long, String, Int)]()
    while (rs.next()) {
      results.append((rs.getLong(1), rs.getString(2), rs.getInt(3)))
    }

    // Verify they match
    (0 to results.size - 1).foreach { i =>
      dataSet(i) shouldEqual results(i)
    }
  }

  test("Can save Java and Joda dates to Phoenix (no config)") {
    val dt = new DateTime()
    val date = new Date()

    val dataSet = List((1L, "1", 1, dt), (2L, "2", 2, date))
    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        "OUTPUT_TEST_TABLE",
        Seq("ID", "COL1", "COL2", "COL3"),
        zkUrl = Some(quorumAddress)
      )

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT COL3 FROM OUTPUT_TEST_TABLE WHERE ID = 1 OR ID = 2 ORDER BY ID ASC")
    val results = ListBuffer[java.sql.Date]()
    while (rs.next()) {
      results.append(rs.getDate(1))
    }

    // Verify the epochs are equal
    results(0).getTime shouldEqual dt.getMillis
    results(1).getTime shouldEqual date.getTime
  }

  test("Can infer schema without defining columns") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.phoenixTableAsDataFrame("TABLE2", Seq(), conf = hbaseConfiguration)
    df.schema("ID").dataType shouldEqual LongType
    df.schema("TABLE1_ID").dataType shouldEqual LongType
    df.schema("t2col1").dataType shouldEqual StringType
  }

  test("Spark SQL can use Phoenix as a data source with no schema specified") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> "TABLE1",
      "zkUrl" -> quorumAddress))
    df.count() shouldEqual 2
    df.schema("ID").dataType shouldEqual LongType
    df.schema("COL1").dataType shouldEqual StringType
  }

  test("Spark SQL can use Phoenix as a data source with PrunedFilteredScan") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> "TABLE1",
      "zkUrl" -> quorumAddress))
    val res = df.filter(df("COL1") === "test_row_1" && df("ID") === 1L).select(df("ID"))

    // Make sure we got the right value back
    assert(res.first().getLong(0) == 1L)

    /*
      NOTE: There doesn't appear to be any way of verifying from the Spark query planner that
      filtering is being pushed down and done server-side. However, since PhoenixRelation
      implements PrunedFilteredScan, debugging has shown that both the SELECT columns and WHERE
      predicates are being passed along to us, which we then forward it to Phoenix.
      TODO: investigate further to find a way to verify server-side pushdown
     */
  }

  test("Can persist a dataframe using 'DataFrame.saveToPhoenix'") {
    // Load from TABLE1
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> "TABLE1",
      "zkUrl" -> quorumAddress))

    // Save to TABLE1_COPY
    df.saveToPhoenix("TABLE1_COPY", zkUrl = Some(quorumAddress))

    // Verify results
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM TABLE1_COPY")

    val checkResults = List((1L, "test_row_1"), (2, "test_row_2"))
    val results = ListBuffer[(Long, String)]()
    while (rs.next()) {
      results.append((rs.getLong(1), rs.getString(2)))
    }
    stmt.close()

    results.toList shouldEqual checkResults
  }

  test("Can persist a dataframe using 'DataFrame.save()") {
    // Clear TABLE1_COPY
    var stmt = conn.createStatement()
    stmt.executeUpdate("DELETE FROM TABLE1_COPY")
    stmt.close()

    // Load TABLE1, save as TABLE1_COPY
    val sqlContext = new SQLContext(sc)
    val df = sqlContext
      .read
      .format("org.apache.phoenix.spark")
      .option("table", "TABLE1")
      .option("zkUrl", quorumAddress)
      .load()

    // Save to TABLE21_COPY
    df
      .write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .option("table", "TABLE1_COPY")
      .option("zkUrl", quorumAddress)
      .save()

    // Verify results
    stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT * FROM TABLE1_COPY")

    val checkResults = List((1L, "test_row_1"), (2, "test_row_2"))
    val results = ListBuffer[(Long, String)]()
    while (rs.next()) {
      results.append((rs.getLong(1), rs.getString(2)))
    }
    stmt.close()

    results.toList shouldEqual checkResults
  }

  test("Can save arrays back to phoenix") {
    val dataSet = List((2L, Array("String1", "String2", "String3")))

    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        "ARRAY_TEST_TABLE",
        Seq("ID", "VCARRAY"),
        zkUrl = Some(quorumAddress)
      )

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT VCARRAY FROM ARRAY_TEST_TABLE WHERE ID = 2")
    rs.next()
    val sqlArray = rs.getArray(1).getArray().asInstanceOf[Array[String]]

    // Verify the arrays are equal
    sqlArray shouldEqual dataSet(0)._2
  }

  test("Can read from table with schema and escaped table name") {
    // Manually escape
    val rdd1 = sc.phoenixTableAsRDD(
      "CUSTOM_ENTITY.\"z02\"",
      Seq("ID"),
      conf = hbaseConfiguration)

    var count = rdd1.count()

    count shouldEqual 1L

    // Use SchemaUtil
    val rdd2 = sc.phoenixTableAsRDD(
      SchemaUtil.getEscapedFullTableName("CUSTOM_ENTITY.z02"),
      Seq("ID"),
      conf = hbaseConfiguration)

    count = rdd2.count()

    count shouldEqual 1L

  }

  test("Ensure DataFrame field normalization (PHOENIX-2196)") {
    val rdd1 = sc
      .parallelize(Seq((1L, 1L, "One"), (2L, 2L, "Two")))
      .map(p => Row(p._1, p._2, p._3))

    val sqlContext = new SQLContext(sc)

    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("table1_id", LongType, nullable = true),
      StructField("\"t2col1\"", StringType, nullable = true)
    ))

    val df = sqlContext.createDataFrame(rdd1, schema)

    df.saveToPhoenix("TABLE2", zkUrl = Some(quorumAddress))
  }

  test("Ensure Dataframe supports LIKE and IN filters (PHOENIX-2328)") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> "TABLE1",
      "zkUrl" -> quorumAddress))

    // Prefix match
    val res1 = df.filter("COL1 like 'test_row_%'")
    res1.count() shouldEqual 2

    // Suffix match
    val res2 = df.filter("COL1 like '%_1'")
    res2.count() shouldEqual 1
    res2.first.getString(1) shouldEqual "test_row_1"

    // Infix match
    val res3 = df.filter("COL1 like '%_row_%'")
    res3.count() shouldEqual 2

    // Not like, match none
    val res4 = df.filter("COL1 not like '%_row_%'")
    res4.count() shouldEqual 0

    // Not like, match all
    val res5 = df.filter("COL1 not like '%_wor_%'")
    res5.count() shouldEqual 2

    // "IN", match all
    val res6 = df.filter("COL1 in ('test_row_1', 'test_row_2')")
    res6.count() shouldEqual 2

    // "IN", match none
    val res7 = df.filter("COL1 in ('foo', 'bar')")
    res7.count() shouldEqual 0

    // AND (and not again)
    val res8 = df.filter("COL1 like '%_row_%' AND COL1 not like '%_1'")
    res8.count() shouldEqual 1
    res8.first.getString(1) shouldEqual "test_row_2"

    // OR
    val res9 = df.filter("COL1 like '%_1' OR COL1 like '%_2'")
    res9.count() shouldEqual 2
  }

  test("Can load decimal types with accurate precision and scale (PHOENIX-2288)") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> "TEST_DECIMAL", "zkUrl" -> quorumAddress))
    assert(df.select("COL1").first().getDecimal(0) == BigDecimal("123.456789").bigDecimal)
  }

  test("Can load small and tiny integeger types (PHOENIX-2426)") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> "TEST_SMALL_TINY", "zkUrl" -> quorumAddress))
    assert(df.select("COL1").first().getShort(0).toInt == 32767)
    assert(df.select("COL2").first().getByte(0).toInt == 127)
  }

  test("Can save arrays from custom dataframes back to phoenix") {
    val dataSet = List(Row(2L, Array("String1", "String2", "String3"), Array(1, 2, 3)))

    val sqlContext = new SQLContext(sc)

    val schema = StructType(
      Seq(StructField("ID", LongType, nullable = false),
        StructField("VCARRAY", ArrayType(StringType)),
        StructField("INTARRAY", ArrayType(IntegerType))))

    val rowRDD = sc.parallelize(dataSet)

    // Apply the schema to the RDD.
    val df = sqlContext.createDataFrame(rowRDD, schema)

    df.write
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> "ARRAYBUFFER_TEST_TABLE", "zkUrl" -> quorumAddress))
      .mode(SaveMode.Overwrite)
      .save()

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT VCARRAY, INTARRAY FROM ARRAYBUFFER_TEST_TABLE WHERE ID = 2")
    rs.next()
    val stringArray = rs.getArray(1).getArray().asInstanceOf[Array[String]]
    val intArray = rs.getArray(2).getArray().asInstanceOf[Array[Int]]

    // Verify the arrays are equal
    stringArray shouldEqual dataSet(0).getAs[Array[String]](1)
    intArray shouldEqual dataSet(0).getAs[Array[Int]](2)
  }

  test("Can save arrays of AnyVal type back to phoenix") {
    val dataSet = List((2L, Array(1, 2, 3), Array(1L, 2L, 3L)))

    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        "ARRAY_ANYVAL_TEST_TABLE",
        Seq("ID", "INTARRAY", "BIGINTARRAY"),
        zkUrl = Some(quorumAddress)
      )

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT INTARRAY, BIGINTARRAY FROM ARRAY_ANYVAL_TEST_TABLE WHERE ID = 2")
    rs.next()
    val intArray = rs.getArray(1).getArray().asInstanceOf[Array[Int]]
    val longArray = rs.getArray(2).getArray().asInstanceOf[Array[Long]]

    // Verify the arrays are equal
    intArray shouldEqual dataSet(0)._2
    longArray shouldEqual dataSet(0)._3
  }

  test("Can save arrays of Byte type back to phoenix") {
    val dataSet = List((2L, Array(1.toByte, 2.toByte, 3.toByte)))

    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        "ARRAY_BYTE_TEST_TABLE",
        Seq("ID", "BYTEARRAY"),
        zkUrl = Some(quorumAddress)
      )

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT BYTEARRAY FROM ARRAY_BYTE_TEST_TABLE WHERE ID = 2")
    rs.next()
    val byteArray = rs.getArray(1).getArray().asInstanceOf[Array[Byte]]

    // Verify the arrays are equal
    byteArray shouldEqual dataSet(0)._2
  }

  test("Can save binary types back to phoenix") {
    val dataSet = List((2L, Array[Byte](1), Array[Byte](1, 2, 3), Array[Array[Byte]](Array[Byte](1), Array[Byte](2))))

    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        "VARBINARY_TEST_TABLE",
        Seq("ID", "BIN", "VARBIN", "BINARRAY"),
        zkUrl = Some(quorumAddress)
      )

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT BIN, VARBIN, BINARRAY FROM VARBINARY_TEST_TABLE WHERE ID = 2")
    rs.next()
    val byte = rs.getBytes("BIN")
    val varByte = rs.getBytes("VARBIN")
    val varByteArray = rs.getArray("BINARRAY").getArray().asInstanceOf[Array[Array[Byte]]]

    // Verify the arrays are equal
    byte shouldEqual dataSet(0)._2
    varByte shouldEqual dataSet(0)._3
    varByteArray shouldEqual dataSet(0)._4
  }

  test("Can load Phoenix DATE columns through DataFrame API") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> "DATE_TEST", "zkUrl" -> quorumAddress))
      .load
    val dt = df.select("COL1").first().getDate(0).getTime
    val epoch = new Date().getTime

    // NOTE: Spark DateType drops hour, minute, second, as per the java.sql.Date spec
    // Use 'dateAsTimestamp' option to coerce DATE to TIMESTAMP without losing resolution

    // Note that Spark also applies the timezone offset to the returned date epoch. Rather than perform timezone
    // gymnastics, just make sure we're within 24H of the epoch generated just now
    assert(Math.abs(epoch - dt) < 86400000)
  }

  test("Filter operation doesn't work for column names containing a white space (PHOENIX-2547)") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> SchemaUtil.getEscapedArgument("space"),
      "zkUrl" -> quorumAddress))
    val res = df.filter(df.col("first name").equalTo("xyz"))
    // Make sure we got the right value back
    assert(res.collectAsList().size() == 1L)
  }

  test("Spark Phoenix cannot recognize Phoenix view fields (PHOENIX-2290)") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> SchemaUtil.getEscapedArgument("small"),
      "zkUrl" -> quorumAddress))
    df.registerTempTable("temp")

    // limitation: filter / where expressions are not allowed with "double quotes", instead of that pass it as column expressions
    // reason: if the expression contains "double quotes" then spark sql parser, ignoring evaluating .. giving to next level to handle

    val res1 = sqlContext.sql("select * from temp where salary = '10000' ")
    assert(res1.collectAsList().size() == 1L)

    val res2 = sqlContext.sql("select * from temp where \"salary\" = '10000' ")
    assert(res2.collectAsList().size() == 0L)

    val res3 = sqlContext.sql("select * from temp where salary > '10000' ")
    assert(res3.collectAsList().size() == 2L)
  }

  test("Queries with small case column-names return empty result-set when working with Spark Datasource Plugin (PHOENIX-2336)") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> SchemaUtil.getEscapedArgument("small"),
      "zkUrl" -> quorumAddress))

    // limitation: filter / where expressions are not allowed with "double quotes", instead of that pass it as column expressions
    // reason: if the expression contains "double quotes" then spark sql parser, ignoring evaluating .. giving to next level to handle

    val res1 = df.filter(df.col("first name").equalTo("foo"))
    assert(res1.collectAsList().size() == 1L)

    val res2 = df.filter("\"first name\" = 'foo'")
    assert(res2.collectAsList().size() == 0L)

    val res3 = df.filter("salary = '10000'")
    assert(res3.collectAsList().size() == 1L)

    val res4 = df.filter("salary > '10000'")
    assert(res4.collectAsList().size() == 2L)
  }

  test("Can coerce Phoenix DATE columns to TIMESTAMP through DataFrame API") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> "DATE_TEST", "zkUrl" -> quorumAddress, "dateAsTimestamp" -> "true"))
      .load
    val dtRes = df.select("COL1").first()
    val ts = dtRes.getTimestamp(0).getTime
    val epoch = new Date().getTime

    assert(Math.abs(epoch - ts) < 300000)
  }

  test("Can load Phoenix Time columns through DataFrame API") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> "TIME_TEST", "zkUrl" -> quorumAddress))
      .load
    val time = df.select("COL1").first().getTimestamp(0).getTime
    val epoch = new Date().getTime
    assert(Math.abs(epoch - time) < 86400000)
  }

  test("can read all Phoenix data types") {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> "GIGANTIC_TABLE",
      "zkUrl" -> quorumAddress))

    df.write
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> "OUTPUT_GIGANTIC_TABLE", "zkUrl" -> quorumAddress))
      .mode(SaveMode.Overwrite)
      .save()

    df.count() shouldEqual 1
  }

}
