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

import org.apache.phoenix.util.PhoenixRuntime
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

/**
  * Sub-class of PhoenixSparkIT used for tenant-specific test
  *
  * Note: If running directly from an IDE, these are the recommended VM parameters:
  * -Xmx1536m -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=512m
  *
  */
class PhoenixSparkITTenantSpecific extends AbstractPhoenixSparkIT {

  val SelectStatement = "SELECT " + OrgId + "," + TenantCol + " FROM " + ViewName
  val DataSet = List(("testOrg1", "data1"), ("testOrg2", "data2"), ("testOrg3", "data3"))

  def verifyResults(): Unit = {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(SelectStatement)

    val results = ListBuffer[(String, String)]()
    while (rs.next()) {
      results.append((rs.getString(1), rs.getString(2)))
    }
    stmt.close()

    results.toList shouldEqual DataSet
  }

  test("Can persist a dataframe using 'DataFrame.saveToPhoenix' on tenant-specific view") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val dataSet = List(("testOrg1", "data1"), ("testOrg2", "data2"), ("testOrg3", "data3"))

    val df = sc.parallelize(dataSet).toDF(OrgId, TenantCol)

    // Save to tenant-specific view
    df.saveToPhoenix("TENANT_VIEW", zkUrl = Some(quorumAddress), tenantId = Some(TenantId))

    verifyResults
  }

  test("Can persist a dataframe using 'DataFrame.write' on tenant-specific view") {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val dataSet = List(("testOrg1", "data1"), ("testOrg2", "data2"), ("testOrg3", "data3"))

    val df = sc.parallelize(dataSet).toDF(OrgId, TenantCol)

    df.write
      .format("org.apache.phoenix.spark")
      .mode("overwrite")
      .option("table", "TENANT_VIEW")
      .option(PhoenixRuntime.TENANT_ID_ATTRIB, "theTenant")
      .option("zkUrl", PhoenixSparkITHelper.getUrl)
      .save()

    verifyResults
  }

  test("Can save to Phoenix tenant-specific view") {
    val sqlContext = new SQLContext(sc)

    // This view name must match the view we create in phoenix-spark/src/it/resources/tenantSetup.sql
    val ViewName = "TENANT_VIEW"

    // Columns from the TENANT_VIEW schema
    val OrgId = "ORGANIZATION_ID"
    val TenantCol = "TENANT_ONLY_COL"

    // Data matching the schema for TENANT_VIEW created in tenantSetup.sql
    val dataSet = List(("testOrg1", "data1"), ("testOrg2", "data2"), ("testOrg3", "data3"))

    sc
      .parallelize(dataSet)
      .saveToPhoenix(
        ViewName,
        Seq(OrgId, TenantCol),
        hbaseConfiguration,
        tenantId = Some(TenantId)
      )

    verifyResults
  }
}
