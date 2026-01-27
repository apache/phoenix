/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.index;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.phoenix.end2end.AlterTableIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class AlterIndexIT extends ParallelStatsDisabledIT {

  @Test
  public void testAlterIndexRebuildNoAsync() throws Exception {
    String indexName = "I_" + generateUniqueName();
    String tableName = "T_" + generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      createAndPopulateTable(conn, tableName);
      Assert.assertEquals(2, TestUtil.getRowCount(conn, tableName));
      createIndex(conn, indexName, tableName, "val1", "val2, val3");
      Assert.assertEquals(2, TestUtil.getRowCount(conn, indexName));
      rebuildIndex(conn, indexName, tableName, false);
      Assert.assertEquals(2, TestUtil.getRowCount(conn, indexName));
    }
  }

  @Test
  public void testAlterReopenRegions() throws Exception {
    Assume.assumeTrue(
      "Reopen Regions for tables with coprocs is supported in hbase versions 2.5.14+ or 2.6.5+",
      AlterTableIT.isReopenRegionsSupportedForTablesWithCoprocs());
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String ddl = "CREATE TABLE " + tableName + "(k INTEGER PRIMARY KEY, v VARCHAR)";
      conn.createStatement().execute(ddl);
      String indexDdl = "CREATE INDEX " + indexName + " ON " + tableName + "(v)";
      conn.createStatement().execute(indexDdl);
      Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
      TableName hTableName = TableName.valueOf(indexName);

      conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + tableName
        + " ACTIVE SET \"phoenix.some.config\"=100 REOPEN_REGIONS = true");
      HRegion region = getUtility().getHBaseCluster().getRegions(hTableName).get(0);
      Assert.assertEquals("100", region.getTableDescriptor().getValue("phoenix.some.config"));

      conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + tableName
        + " ACTIVE SET \"phoenix.some.config\"=200 REOPEN_REGIONS = false");
      region = getUtility().getHBaseCluster().getRegions(hTableName).get(0);
      Assert.assertEquals("100", region.getTableDescriptor().getValue("phoenix.some.config"));
      Assert.assertEquals("200", admin.getDescriptor(hTableName).getValue("phoenix.some.config"));

      admin.disableTable(hTableName);
      admin.enableTable(hTableName);
      region = getUtility().getHBaseCluster().getRegions(hTableName).get(0);
      Assert.assertEquals("200", region.getTableDescriptor().getValue("phoenix.some.config"));
    }
  }

  private void createAndPopulateTable(Connection conn, String tableName) throws Exception {
    conn.createStatement().execute(
      "create table " + tableName + " (id varchar(10) not null primary key, val1 varchar(10), "
        + "val2 varchar(10), val3 varchar(10))");
    conn.createStatement()
      .execute("upsert into " + tableName + " " + "values ('a', 'ab', 'abc', 'abcd')");
    conn.commit();
    conn.createStatement()
      .execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
    conn.commit();
  }

  private void createIndex(Connection conn, String indexName, String tableName, String columns,
    String includeColumns) throws SQLException {
    String ddl = "CREATE INDEX " + indexName + " ON " + tableName + " (" + columns + ")"
      + " INCLUDE (" + includeColumns + ")";
    conn.createStatement().execute(ddl);
  }

  private void rebuildIndex(Connection conn, String indexName, String tableName, boolean async)
    throws SQLException {
    String format = "ALTER INDEX %s ON %s REBUILD" + (async ? " ASYNC" : "");
    String sql = String.format(format, indexName, tableName);
    conn.createStatement().execute(sql);
    conn.commit();

  }
}
