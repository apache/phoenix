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
import java.util.Properties

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT
import org.apache.phoenix.query.BaseTest
import org.apache.phoenix.util.PhoenixRuntime
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}


// Helper object to access the protected abstract static methods hidden in BaseHBaseManagedTimeIT
object PhoenixSparkITHelper extends BaseHBaseManagedTimeIT {
  def getTestClusterConfig = BaseHBaseManagedTimeIT.getTestClusterConfig

  def doSetup = {
    // The @ClassRule doesn't seem to be getting picked up, force creation here before setup
    BaseTest.tmpFolder.create()
    BaseHBaseManagedTimeIT.doSetup()
  }

  def doTeardown = BaseHBaseManagedTimeIT.doTeardown()

  def getUrl = BaseTest.getUrl
}

/**
  * Base class for PhoenixSparkIT
  */
class AbstractPhoenixSparkIT extends FunSuite with Matchers with BeforeAndAfterAll {

  // A global tenantId we can use across tests
  final val TenantId = "theTenant"

  var conn: Connection = _
  var sc: SparkContext = _

  lazy val hbaseConfiguration = {
    val conf = PhoenixSparkITHelper.getTestClusterConfig
    conf
  }

  lazy val quorumAddress = {
    ConfigurationUtil.getZookeeperURL(hbaseConfiguration).get
  }

  // Runs SQL commands located in the file defined in the sqlSource argument
  // Optional argument tenantId used for running tenant-specific SQL
  def setupTables(sqlSource: String, tenantId: Option[String]): Unit = {
    val props = new Properties
    val id = tenantId match {
      case Some(tid) => props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tid)
      case _ =>
    }

    conn = DriverManager.getConnection(PhoenixSparkITHelper.getUrl, props)
    conn.setAutoCommit(true)

    val setupSqlSource = getClass.getClassLoader.getResourceAsStream(sqlSource)

    // each SQL statement used to set up Phoenix must be on a single line. Yes, that
    // can potentially make large lines.
    val setupSql = scala.io.Source.fromInputStream(setupSqlSource).getLines()
      .filter(line => !line.startsWith("--") && !line.isEmpty)

    for (sql <- setupSql) {
      val stmt = conn.createStatement()
      stmt.execute(sql)
    }
    conn.commit()
  }

  override def beforeAll() {
    PhoenixSparkITHelper.doSetup

    // We pass in null for TenantId here since these tables will be globally visible
    setupTables("globalSetup.sql", null)
    // We pass in a TenantId to allow the DDL to create tenant-specific tables/views
    setupTables("tenantSetup.sql", Some(TenantId))

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
}
