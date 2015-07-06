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

import java.sql.{Connection, DriverManager}
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, HBaseTestingUtility}
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT
import org.apache.phoenix.query.BaseTest
import org.apache.phoenix.schema.{TableNotFoundException, ColumnNotFoundException}
import org.apache.phoenix.schema.types.PVarchar
import org.apache.phoenix.util.{SchemaUtil, ColumnInfo}
import org.apache.spark.sql.{SaveMode, execution, SQLContext}
import org.apache.spark.sql.types.{LongType, DataType, StringType, StructField}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest._
import org.apache.phoenix.spark._

import scala.collection.mutable.ListBuffer

/*
  Note: If running directly from an IDE, these are the recommended VM parameters:
  -Xmx1536m -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m
 */

// Helper object to access the protected abstract static methods hidden in BaseHBaseManagedTimeIT
object PhoenixSparkITHelper extends BaseHBaseManagedTimeIT {
  def getTestClusterConfig = BaseHBaseManagedTimeIT.getTestClusterConfig
  def doSetup = BaseHBaseManagedTimeIT.doSetup()
  def doTeardown = BaseHBaseManagedTimeIT.doTeardown()
  def getUrl = BaseTest.getUrl
}

class PhoenixSparkIT extends FunSuite with Matchers with BeforeAndAfterAll {
  var conn: Connection = _
  var sc: SparkContext = _

  lazy val hbaseConfiguration = {
    val conf = PhoenixSparkITHelper.getTestClusterConfig
    // The zookeeper quorum address defaults to "localhost" which is incorrect, let's fix it
    val quorum = conf.get("hbase.zookeeper.quorum")
    val clientPort = conf.get("hbase.zookeeper.property.clientPort")
    val znodeParent = conf.get("zookeeper.znode.parent")
    conf.set(HConstants.ZOOKEEPER_QUORUM, s"$quorum:$clientPort:$znodeParent")
    conf
  }

  lazy val quorumAddress = {
    hbaseConfiguration.get(HConstants.ZOOKEEPER_QUORUM)
  }

  override def beforeAll() {
    PhoenixSparkITHelper.doSetup

    conn = DriverManager.getConnection(PhoenixSparkITHelper.getUrl)
    conn.setAutoCommit(true)

    // each SQL statement used to set up Phoenix must be on a single line. Yes, that
    // can potentially make large lines.
    val setupSqlSource = getClass.getClassLoader.getResourceAsStream("setup.sql")

    val setupSql = scala.io.Source.fromInputStream(setupSqlSource).getLines()
      .filter(line => ! line.startsWith("--") && ! line.isEmpty)

    for (sql <- setupSql) {
      val stmt = conn.createStatement()
      stmt.execute(sql)
    }
    conn.commit()

    val conf = new SparkConf()
      .setAppName("PhoenixSparkIT")
      .setMaster("local[2]") // 2 threads, some parallelism
      .set("spark.ui.showConsoleProgress", "false") // Disable printing stage progress

    sc = new SparkContext(conf)
  }

  override def afterAll() {
    conn.close()
    sc.stop()
    PhoenixSparkITHelper.doTeardown
  }

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

    val sqlRdd = sqlContext.sql("""
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

    val sqlRdd = sqlContext.sql("""
      |SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1
      |INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)""".stripMargin
    )

    val count = sqlRdd.count()

    count shouldEqual 1L
  }

  test("Using a predicate referring to a non-existent column should fail") {
    intercept[Exception] {
      val sqlContext = new SQLContext(sc)

      val df1 = sqlContext.phoenixTableAsDataFrame(
        SchemaUtil.getEscapedArgument("table3"),
        Array("id", "col1"),
        predicate = Some("foo = bar"),
        conf = hbaseConfiguration)

      df1.registerTempTable("table3")

      val sqlRdd = sqlContext.sql("SELECT * FROM table3")

      // we have to execute an action before the predicate failure can occur
      val count = sqlRdd.count()
    }.getCause shouldBe a[ColumnNotFoundException]
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
      predicate = Some("""
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
        Seq("ID","COL1","COL2","COL3"),
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
    val df = sqlContext.load("org.apache.phoenix.spark", Map("table" -> "TABLE1",
      "zkUrl" -> quorumAddress))

    // Save to TABLE21_COPY
    df.save("org.apache.phoenix.spark", SaveMode.Overwrite, Map("table" -> "TABLE1_COPY", "zkUrl" -> quorumAddress))

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
        Seq("ID","VCARRAY"),
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
}