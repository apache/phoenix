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

import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_NOT_DEFINED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.phoenix.end2end.SystemCatalogTTLLookupIT.ScanCountingObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verifies that resolving TTL from a view hierarchy spread over several SYSTEM.CATALOG regions
 * reads the ancestors held by other regions through client scans (and only those), and resolves the
 * table's TTL.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class SystemCatalogTTLLookupSplitIT extends SplitSystemCatalogIT {
  private static final TableName SYSTEM_CATALOG =
    TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);

  @Test
  public void testTenantViewOnTableInAnotherRegion() throws Exception {
    String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
    String viewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
    createTableWithTTL(tableName);
    createTenantView(viewName, tableName);
    byte[] tableKey = SchemaUtil.getTableKeyFromFullName(tableName);
    byte[] viewKey = getTenantTableKey(viewName);
    assertInDifferentRegions(tableKey, viewKey);

    try (Connection conn = getTenantConnection()) {
      // The view is read from the local region.
      assertScansWhileBuilding(conn, viewName, viewKey, 0);
      // The table is client-scanned in its own region.
      PTable view = assertScansWhileBuilding(conn, viewName, tableKey, 1);
      assertTTLOf(tableName, view);
    }
  }

  @Test
  public void testTenantViewHierarchyAcrossRegions() throws Exception {
    String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
    String parentViewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
    String childViewName = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
    createTableWithTTL(tableName);
    createTenantView(parentViewName, tableName);
    createTenantView(childViewName, parentViewName);
    byte[] tableKey = SchemaUtil.getTableKeyFromFullName(tableName);
    byte[] parentViewKey = getTenantTableKey(parentViewName);
    byte[] childViewKey = getTenantTableKey(childViewName);
    assertInDifferentRegions(tableKey, parentViewKey, childViewKey);

    try (Connection conn = getTenantConnection()) {
      // The child view is read from the local region.
      assertScansWhileBuilding(conn, childViewName, childViewKey, 0);
      // The parent view is client-scanned in the parent's region, only by the child view's TTL
      // recursion. (Without the fix, building the parent view, which the client resolves along
      // with the child view, would also client-scan it.)
      assertScansWhileBuilding(conn, childViewName, parentViewKey, 1);
      // The table is client-scanned in its own region, by the child view's TTL recursion and,
      // depending on how the client resolves the parent view, by building the parent view.
      ScanCountingObserver.WATCHED_KEYS.clear();
      ScanCountingObserver.WATCHED_KEYS.add(tableKey);
      PTable childView = clearCachesAndResetCount(conn).getTableNoCache(childViewName);
      assertTrue(ScanCountingObserver.SCANS.get() >= 1);
      assertTTLOf(tableName, childView);
    }
  }

  /**
   * Rebuilds the given table or view and asserts the number of client scans of SYSTEM.CATALOG
   * starting at the given row key.
   */
  private static PTable assertScansWhileBuilding(Connection conn, String name, byte[] key,
    int expectedScans) throws Exception {
    ScanCountingObserver.WATCHED_KEYS.clear();
    ScanCountingObserver.WATCHED_KEYS.add(key);
    PTable table = clearCachesAndResetCount(conn).getTableNoCache(name);
    assertEquals(expectedScans, ScanCountingObserver.SCANS.get());
    return table;
  }

  private static void createTableWithTTL(String tableName) throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (TENANT_ID VARCHAR NOT NULL, ID VARCHAR NOT NULL, V1 VARCHAR"
          + " CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true, TTL=86400");
      TestUtil.addCoprocessor(conn, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME,
        ScanCountingObserver.class);
    }
  }

  private static void createTenantView(String viewName, String parentName) throws Exception {
    try (Connection conn = getTenantConnection()) {
      conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + parentName);
    }
  }

  private static Connection getTenantConnection() throws Exception {
    Properties props = new Properties();
    props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, TENANT1);
    return DriverManager.getConnection(getUrl(), props);
  }

  private static byte[] getTenantTableKey(String fullName) {
    return SchemaUtil.getTableKey(TENANT1, SchemaUtil.getSchemaNameFromFullName(fullName),
      SchemaUtil.getTableNameFromFullName(fullName));
  }

  private static void assertInDifferentRegions(byte[]... keys) throws Exception {
    try (RegionLocator locator = getUtility().getConnection().getRegionLocator(SYSTEM_CATALOG)) {
      for (int i = 0; i < keys.length; i++) {
        for (int j = i + 1; j < keys.length; j++) {
          assertNotEquals("Expected the keys in different SYSTEM.CATALOG regions",
            locator.getRegionLocation(keys[i], true).getRegion(),
            locator.getRegionLocation(keys[j], true).getRegion());
        }
      }
    }
  }

  private static PhoenixConnection clearCachesAndResetCount(Connection conn) throws Exception {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    // Clear the client and server metadata caches so that getTable rebuilds the PTables.
    pconn.getQueryServices().clearCache();
    ScanCountingObserver.SCANS.set(0);
    return pconn;
  }

  private static void assertTTLOf(String tableName, PTable view) throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      PTable table = conn.unwrap(PhoenixConnection.class).getTableNoCache(tableName);
      assertNotEquals(TTL_EXPRESSION_NOT_DEFINED, table.getTTLExpression());
      assertEquals(table.getTTLExpression(), view.getTTLExpression());
    }
  }
}
