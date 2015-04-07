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
import org.apache.hadoop.hbase.{HConstants, HBaseTestingUtility}
import org.apache.phoenix.schema.ColumnNotFoundException
import org.apache.phoenix.schema.types.PVarchar
import org.apache.phoenix.util.ColumnInfo
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.apache.phoenix.spark._

import scala.collection.mutable.ListBuffer

class PhoenixRDDTest extends FunSuite with Matchers with BeforeAndAfterAll {
  lazy val hbaseTestingUtility = {
    new HBaseTestingUtility()
  }

  lazy val hbaseConfiguration = {
    val conf = hbaseTestingUtility.getConfiguration

    val quorum = conf.get("hbase.zookeeper.quorum")
    val clientPort = conf.get("hbase.zookeeper.property.clientPort")
    val znodeParent = conf.get("zookeeper.znode.parent")

    // This is an odd one - the Zookeeper Quorum entry in the config is totally wrong. It's
    // just reporting localhost.
    conf.set(org.apache.hadoop.hbase.HConstants.ZOOKEEPER_QUORUM, s"$quorum:$clientPort:$znodeParent")

    conf
  }

  lazy val quorumAddress = {
    hbaseConfiguration.get("hbase.zookeeper.quorum")
  }

  lazy val zookeeperClientPort = {
    hbaseConfiguration.get("hbase.zookeeper.property.clientPort")
  }

  lazy val zookeeperZnodeParent = {
    hbaseConfiguration.get("zookeeper.znode.parent")
  }

  lazy val hbaseConnectionString = {
    s"$quorumAddress:$zookeeperClientPort:$zookeeperZnodeParent"
  }

  var conn: Connection = _

  override def beforeAll() {
    hbaseTestingUtility.startMiniCluster()

    conn = DriverManager.getConnection(s"jdbc:phoenix:$hbaseConnectionString")

    conn.setAutoCommit(true)

    // each SQL statement used to set up Phoenix must be on a single line. Yes, that
    // can potentially make large lines.
    val setupSqlSource = getClass.getClassLoader.getResourceAsStream("setup.sql")

    val setupSql = scala.io.Source.fromInputStream(setupSqlSource).getLines()

    for (sql <- setupSql) {
      val stmt = conn.createStatement()

      stmt.execute(sql)

      stmt.close()
    }

    conn.commit()
  }

  override def afterAll() {
    conn.close()
    hbaseTestingUtility.shutdownMiniCluster()
  }

  val conf = new SparkConf().set("spark.ui.showConsoleProgress", "false")

  val sc = new SparkContext("local[1]", "PhoenixSparkTest", conf)

  def buildSql(table: String, columns: Seq[String], predicate: Option[String]): String = {
    val query = "SELECT %s FROM \"%s\"" format(columns.map(f => "\"" + f + "\"").mkString(", "), table)

    query + (predicate match {
      case Some(p: String) => " WHERE " + p
      case _ => ""
    })
  }

  test("Can create valid SQL") {
    val rdd = new PhoenixRDD(sc, "MyTable", Array("Foo", "Bar"),
      conf = hbaseConfiguration)

    rdd.buildSql("MyTable", Array("Foo", "Bar"), None) should
      equal("SELECT \"Foo\", \"Bar\" FROM \"MyTable\"")
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

    val sqlRdd = sqlContext.sql("SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1 INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)")

    val count = sqlRdd.count()

    count shouldEqual 6L
  }

  test("Can create schema RDD and execute query on case sensitive table (no config)") {
    val sqlContext = new SQLContext(sc)

    val df1 = sqlContext.phoenixTableAsDataFrame("table3", Array("id", "col1"), zkUrl = Some(hbaseConnectionString))

    df1.registerTempTable("table3")

    val sqlRdd = sqlContext.sql("SELECT * FROM table3")

    val count = sqlRdd.count()

    count shouldEqual 2L
  }

  test("Can create schema RDD and execute constrained query") {
    val sqlContext = new SQLContext(sc)

    val df1 = sqlContext.phoenixTableAsDataFrame("TABLE1", Array("ID", "COL1"), conf = hbaseConfiguration)

    df1.registerTempTable("sql_table_1")

    val df2 = sqlContext.phoenixTableAsDataFrame("TABLE2", Array("ID", "TABLE1_ID"),
      predicate = Some("\"ID\" = 1"),
      conf = hbaseConfiguration)

    df2.registerTempTable("sql_table_2")

    val sqlRdd = sqlContext.sql("SELECT t1.ID, t1.COL1, t2.ID, t2.TABLE1_ID FROM sql_table_1 AS t1 INNER JOIN sql_table_2 AS t2 ON (t2.TABLE1_ID = t1.ID)")

    val count = sqlRdd.count()

    count shouldEqual 1L
  }

  test("Using a predicate referring to a non-existent column should fail") {
    intercept[RuntimeException] {
      val sqlContext = new SQLContext(sc)

      val df1 = sqlContext.phoenixTableAsDataFrame("table3", Array("id", "col1"),
        predicate = Some("foo = bar"),
        conf = hbaseConfiguration)

      df1.registerTempTable("table3")

      val sqlRdd = sqlContext.sql("SELECT * FROM table3")

      // we have to execute an action before the predicate failure can occur
      val count = sqlRdd.count()
    }.getCause shouldBe a [ColumnNotFoundException]
  }

  test("Can create schema RDD with predicate that will never match") {
    val sqlContext = new SQLContext(sc)

    val df1 = sqlContext.phoenixTableAsDataFrame("table3", Array("id", "col1"),
      predicate = Some("\"id\" = -1"),
      conf = hbaseConfiguration)

    df1.registerTempTable("table3")

    val sqlRdd = sqlContext.sql("SELECT * FROM table3")

    val count = sqlRdd.count()

    count shouldEqual 0L
  }

  test("Can create schema RDD with complex predicate") {
    val sqlContext = new SQLContext(sc)

    val df1 = sqlContext.phoenixTableAsDataFrame("DATE_PREDICATE_TEST_TABLE", Array("ID", "TIMESERIES_KEY"),
      predicate = Some("ID > 0 AND TIMESERIES_KEY BETWEEN CAST(TO_DATE('1990-01-01 00:00:01', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AND CAST(TO_DATE('1990-01-30 00:00:01', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)"),
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
        Seq("ID","COL1","COL2"),
        hbaseConfiguration
      )

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT ID, COL1, COL2 FROM OUTPUT_TEST_TABLE")
    val results = ListBuffer[(Long, String, Int)]()
    while(rs.next()) {
      results.append((rs.getLong(1), rs.getString(2), rs.getInt(3)))
    }
    stmt.close()

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
        zkUrl = Some(hbaseConnectionString)
      )

    // Load the results back
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT COL3 FROM OUTPUT_TEST_TABLE WHERE ID = 1 OR ID = 2 ORDER BY ID ASC")
    val results = ListBuffer[java.sql.Date]()
    while(rs.next()) {
      results.append(rs.getDate(1))
    }
    stmt.close()

    // Verify the epochs are equal
    results(0).getTime shouldEqual dt.getMillis
    results(1).getTime shouldEqual date.getTime
  }

  test("Not specifying a zkUrl or a config quorum URL should fail") {
    intercept[UnsupportedOperationException] {
      val sqlContext = new SQLContext(sc)
      val badConf = new Configuration(hbaseConfiguration)
      badConf.unset(HConstants.ZOOKEEPER_QUORUM)
      sqlContext.phoenixTableAsDataFrame("TABLE1", Array("ID", "COL1"), conf = badConf)
    }
  }
}