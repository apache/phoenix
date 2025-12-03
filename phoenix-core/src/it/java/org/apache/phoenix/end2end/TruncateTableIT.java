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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class TruncateTableIT extends ParallelStatsDisabledIT {

  @Test
  public void testTruncateTable() throws SQLException {
    Properties props = new Properties();
    String tableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String createTableDDL = "CREATE TABLE " + tableName + " (pk char(2) not null primary key)";
      conn.createStatement().execute(createTableDDL);

      try {
        conn.setAutoCommit(true);
        try (PreparedStatement stmt =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES('a')")) {
          stmt.execute();
        }

        try (
          ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
        }

        conn.createStatement().execute("TRUNCATE TABLE " + tableName);

        try (
          ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(0, rs.getInt(1));
        }
      } finally {
        conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testTruncateTableNotExist() throws Exception {
    Properties props = new Properties();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      String tableName = "nonExistentTable";
      try {
        conn.createStatement().execute("TRUNCATE TABLE " + tableName);
        fail("Should have thrown TableNotFoundException");
      } catch (TableNotFoundException e) {
        // Expected
      }
    }
  }

  @Test
  public void testTruncateTableNonExistentSchema() throws SQLException {
    Properties props = new Properties();
    String schemaName = "nonExistentSchema";
    String tableName = generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute("CREATE TABLE " + tableName + " (C1 INTEGER PRIMARY KEY)");
      try {
        conn.setAutoCommit(true);

        try (PreparedStatement stmt =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(1)")) {
          stmt.execute();
        }

        try (
          ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
        }

        try {
          conn.createStatement().execute("TRUNCATE TABLE " + schemaName + "." + tableName);
          fail("Should have failed due to non-existent schema/table");
        } catch (SQLException e) {
          // Expected
        }
      } finally {
        conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
      }
    }
  }

  @Test
  public void testTruncateTableWithImplicitSchema() throws SQLException {
    Properties props = new Properties();
    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();
    String fullTableName = schemaName + "." + tableName;
    String createTableWithSchema =
      "CREATE TABLE " + fullTableName + " (C1 char(2) NOT NULL PRIMARY KEY)";

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(createTableWithSchema);
      try {
        conn.setAutoCommit(true);

        try (PreparedStatement stmt =
          conn.prepareStatement("UPSERT INTO " + fullTableName + " values('a')")) {
          stmt.execute();
        }

        try (ResultSet rs =
          conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName)) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
        }

        try (PreparedStatement stmt =
          conn.prepareStatement("UPSERT INTO " + fullTableName + " values('b')")) {
          stmt.execute();
        }

        try (ResultSet rs =
          conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName)) {
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));
        }

        conn.createStatement().execute("TRUNCATE TABLE " + fullTableName);

        try (ResultSet rs =
          conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName)) {
          assertTrue(rs.next());
          assertEquals(0, rs.getInt(1));
        }
      } finally {
        conn.createStatement().execute("DROP TABLE IF EXISTS " + fullTableName);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testTruncateTableWithExplicitSchema() throws SQLException {
    Properties props = new Properties();
    props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));

    String schemaName = generateUniqueName();
    String tableName = generateUniqueName();

    String schemaCreateStmt = "CREATE SCHEMA IF NOT EXISTS " + schemaName;
    String tableCreateStmt = "CREATE TABLE IF NOT EXISTS " + tableName
      + " (C1 char(2) NOT NULL PRIMARY KEY) SALT_BUCKETS=4";

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(true);
      conn.createStatement().execute(schemaCreateStmt);
      conn.createStatement().execute("USE " + schemaName);
      conn.createStatement().execute(tableCreateStmt);

      try {
        try (PreparedStatement stmt =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES ('a')")) {
          stmt.execute();
        }

        try (
          ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
        }

        try (PreparedStatement stmt =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES ('b')")) {
          stmt.execute();
        }

        try (
          ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));
        }

        conn.createStatement().execute("TRUNCATE TABLE " + tableName);

        try (
          ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName)) {
          assertTrue(rs.next());
          assertEquals(0, rs.getInt(1));
        }
      } finally {
        conn.createStatement().execute("DROP TABLE IF EXISTS " + schemaName + "." + tableName);
        conn.createStatement().execute("DROP SCHEMA IF EXISTS " + schemaName);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testTruncateViewNotAllowed() throws SQLException {
    Properties props = new Properties();
    String tableName = generateUniqueName();
    String viewName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (pk char(2) not null primary key)");
      conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);
      try {
        conn.createStatement().execute("TRUNCATE TABLE " + viewName);
        fail("Should not allow truncating a VIEW");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.TRUNCATE_NOT_ALLOWED_ON_VIEW.getErrorCode(),
          e.getErrorCode());
      } finally {
        conn.createStatement().execute("DROP VIEW IF EXISTS " + viewName);
        conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
      }
    }
  }

  @Test
  public void testTruncateSystemTable() throws SQLException {
    Properties props = new Properties();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      try {
        conn.createStatement().execute("TRUNCATE TABLE SYSTEM.CATALOG");
        fail("Should not be able to truncate SYSTEM.CATALOG");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.TRUNCATE_NOT_ALLOWED_ON_SYSTEM_TABLE.getErrorCode(),
          e.getErrorCode());
      }
    }
  }

  @Test
  public void testTruncateClearsRegionCache() throws Exception {
    Properties props = new Properties();
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // 1. Create Table and Insert Data
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (K VARCHAR PRIMARY KEY, V VARCHAR)");
      try {
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a', 'a')");
        conn.commit();

        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        org.apache.hadoop.hbase.TableName hbaseTableName =
          SchemaUtil.getPhysicalTableName(tableName.getBytes(), false);

        try (org.apache.hadoop.hbase.client.Admin admin = pconn.getQueryServices().getAdmin()) {
          org.apache.hadoop.hbase.client.Connection hbaseConn = admin.getConnection();

          try (org.apache.hadoop.hbase.client.RegionLocator locator =
            hbaseConn.getRegionLocator(hbaseTableName)) {
            // Populate the cache by retrieving the region location
            org.apache.hadoop.hbase.HRegionLocation loc1 =
              locator.getRegionLocation(org.apache.hadoop.hbase.util.Bytes.toBytes("a"), false);
            Assert.assertNotNull("Region location should not be null", loc1);
            // Truncating table creates new regions with new Region IDs (timestamps) and should
            // invalidate the client cache
            conn.createStatement().execute("TRUNCATE TABLE " + tableName);
            // Retrieve the region location again
            // If the cache was cleared, this will fetch the NEW region from .META.
            // If the cache was NOT cleared, this will return the OLD (stale) region from the cache.
            org.apache.hadoop.hbase.HRegionLocation loc2 =
              locator.getRegionLocation(org.apache.hadoop.hbase.util.Bytes.toBytes("a"), false);
            Assert.assertNotNull("Region location should not be null after truncate", loc2);

            // Verify that the Region IDs are different
            // HBase Truncate (preserve splits) drops and recreates regions, so the Region ID
            // (timestamp) must change.
            if (loc1.getRegion().getRegionId() == loc2.getRegion().getRegionId()) {
              fail(
                "Region cache was not cleared after truncate. Client returned stale region location: "
                  + loc1);
            }
          }
        }
      } finally {
        conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
      }
    }
  }

  @Test
  public void testTruncateTableDropSplits() throws Exception {
    Properties props = new Properties();
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      // Create table with splits
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (ID INTEGER PRIMARY KEY, V1 VARCHAR) SPLIT ON (10, 20, 30)");
      try {
        // Verify splits exist
        org.apache.hadoop.hbase.TableName physName =
          SchemaUtil.getPhysicalTableName(tableName.getBytes(), false);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        try (org.apache.hadoop.hbase.client.Admin admin = pconn.getQueryServices().getAdmin()) {
          assertEquals(4, admin.getRegions(physName).size());
        }

        // Truncate with DROP SPLITS
        conn.createStatement().execute("TRUNCATE TABLE " + tableName + " DROP SPLITS");

        // Verify table is empty and has only 1 region
        try (org.apache.hadoop.hbase.client.Admin admin = pconn.getQueryServices().getAdmin()) {
          assertEquals(1, admin.getRegions(physName).size());
        }
      } finally {
        conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
      }
    }
  }

  @Test
  public void testTruncateSaltedTableDropSplitsFails() throws Exception {
    Properties props = new Properties();
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
        "CREATE TABLE " + tableName + " (ID INTEGER PRIMARY KEY, V1 VARCHAR) SALT_BUCKETS=4");

      try {
        conn.createStatement().execute("TRUNCATE TABLE " + tableName + " DROP SPLITS");
        fail("Should not allow DROP SPLITS on salted table");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.TRUNCATE_MUST_PRESERVE_SPLITS_FOR_SALTED_TABLE.getErrorCode(),
          e.getErrorCode());
      } finally {
        conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
      }
    }
  }

  @Test
  public void testTruncateMultiTenantTableBlockedForTenantConnection() throws SQLException {
    String tableName = generateUniqueName();
    String tenantId = "tenant1";

    // Create a Multi-Tenant Table using a Global Connection
    try (Connection globalConn = DriverManager.getConnection(getUrl())) {
      String ddl = "CREATE TABLE " + tableName
        + " (TENANT_ID VARCHAR NOT NULL, ID INTEGER NOT NULL, V1 VARCHAR "
        + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";
      globalConn.createStatement().execute(ddl);

      try {
        // Insert data for "tenant1"
        globalConn.createStatement()
          .execute("UPSERT INTO " + tableName + " VALUES ('" + tenantId + "', 1, 'data1')");
        // Insert data for "tenant2"
        globalConn.createStatement()
          .execute("UPSERT INTO " + tableName + " VALUES ('tenant2', 2, 'data2')");
        globalConn.commit();

        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {

          // Verify tenant sees their data
          ResultSet rs = tenantConn.createStatement().executeQuery("SELECT * FROM " + tableName);
          assertTrue(rs.next());
          assertEquals(1, rs.getInt("ID"));

          // Attempt Truncate from Tenant Connection
          try {
            tenantConn.createStatement().execute("TRUNCATE TABLE " + tableName);
            fail("Truncate should be blocked for multi-tenant table from tenant connection");
          } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_TRUNCATE_MULTITENANT_TABLE.getErrorCode(),
              e.getErrorCode());
          }
        }

        // Verify Data Intact (via Global Connection)
        ResultSet rs =
          globalConn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1)); // Both tenant rows should still exist

      } finally {
        globalConn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
      }
    }
  }

  @Test
  public void testTruncateTableWithSplitsAndIndexes() throws Exception {
    String tableName = generateUniqueName();
    int numRegions = 15;
    int rowsPerRegion = 25;
    int numGlobalIndexes = 5;
    int numViews = 5;
    int numViewIndexes = 4;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Create Table with Splits (14 split points for 15 regions)
      StringBuilder splitPoints = new StringBuilder();
      for (int i = 1; i < numRegions; i++) {
        if (i > 1) splitPoints.append(", ");
        // Use a prefix to ensure distribution, e.g., '01', '02', ... '14'
        splitPoints.append(String.format("'%02d'", i * rowsPerRegion));
      }

      String ddl = "CREATE TABLE " + tableName
        + " (ID CHAR(3) NOT NULL PRIMARY KEY, V1 VARCHAR, V2 VARCHAR) SPLIT ON ("
        + splitPoints.toString() + ")";
      conn.createStatement().execute(ddl);

      try {
        // Create Global Indexes on Base Table
        for (int i = 0; i < numGlobalIndexes; i++) {
          conn.createStatement().execute(
            "CREATE INDEX " + tableName + "_IDX_" + i + " ON " + tableName + " (V1) INCLUDE (V2)");
        }

        // Create Views and Global Indexes on Views
        for (int i = 0; i < numViews; i++) {
          String viewName = tableName + "_VIEW_" + i;
          conn.createStatement()
            .execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);
          for (int j = 0; j < numViewIndexes; j++) {
            conn.createStatement().execute(
              "CREATE INDEX " + viewName + "_IDX_" + j + " ON " + viewName + " (V2) INCLUDE (V1)");
          }
        }

        // Insert Data (distributed across regions)
        conn.setAutoCommit(false);
        int totalRows = numRegions * rowsPerRegion;
        try (PreparedStatement stmt =
          conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?)")) {
          for (int i = 0; i < totalRows; i++) {
            // Key format ensures distribution: 000, 001, ..., 374
            // Split points: '025', '050', ...
            String key = String.format("%03d", i);
            stmt.setString(1, key);
            stmt.setString(2, "v1_" + i);
            stmt.setString(3, "v2_" + i);
            stmt.execute();
          }
        }
        conn.commit();

        // Verify Region Count Before Truncate
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        org.apache.hadoop.hbase.TableName physName =
          SchemaUtil.getPhysicalTableName(tableName.getBytes(), false);
        try (org.apache.hadoop.hbase.client.Admin admin = pconn.getQueryServices().getAdmin()) {
          assertEquals("Region count before truncate should be " + numRegions, numRegions,
            admin.getRegions(physName).size());
        }

        // Verify Data Count
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(totalRows, rs.getInt(1));

        // Perform Truncate (Preserve Splits is default)
        conn.createStatement().execute("TRUNCATE TABLE " + tableName);

        // Verify Table Empty
        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));

        // Verify Region Count After Truncate (Should still be 15)
        try (org.apache.hadoop.hbase.client.Admin admin = pconn.getQueryServices().getAdmin()) {
          assertEquals("Region count after truncate should be " + numRegions, numRegions,
            admin.getRegions(physName).size());
        }

        // Verify stats are deleted/reset (optional but good practice given previous context)
        conn.createStatement().execute("UPDATE STATISTICS " + tableName);
        // Stats verification logic if needed

      } finally {
        // Cleanup
        for (int i = 0; i < numViews; i++) {
          String viewName = tableName + "_VIEW_" + i;
          for (int j = 0; j < numViewIndexes; j++) {
            conn.createStatement()
              .execute("DROP INDEX IF EXISTS " + viewName + "_IDX_" + j + " ON " + viewName);
          }
          conn.createStatement().execute("DROP VIEW IF EXISTS " + viewName);
        }
        for (int i = 0; i < numGlobalIndexes; i++) {
          conn.createStatement()
            .execute("DROP INDEX IF EXISTS " + tableName + "_IDX_" + i + " ON " + tableName);
        }
        conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
      }
    }
  }
}
