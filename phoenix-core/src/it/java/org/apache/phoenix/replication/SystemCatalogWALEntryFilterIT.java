/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication;

import java.io.IOException;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.replication.ChainWALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.hbase.index.wal.IndexedKeyValue;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class SystemCatalogWALEntryFilterIT extends ParallelStatsDisabledIT {

  private static final byte[] REGION = Bytes.toBytes("REGION");
  private static final UUID uuid = UUID.randomUUID();
  private static final String TENANT_ID = "1234567";
  private static final byte[] TENANT_BYTES = Bytes.toBytes(TENANT_ID);
  private static final byte[] DEFAULT_TENANT_BYTES = null;

  private static final String SCHEMA_NAME = "SYSTEMCATALOGWALSCHEMA";
  private static final String TENANT_VIEW_NAME = generateUniqueName();
  private static final String NONTENANT_VIEW_NAME = generateUniqueName();
  private static final byte[] VIEW_COLUMN_FAMILY_BYTES = Bytes.toBytes("0");
  private static final String VIEW_COLUMN_NAME = "OLD_VALUE_VIEW";
  private static final String CREATE_TENANT_VIEW_SQL = "CREATE VIEW IF NOT EXISTS  " + SCHEMA_NAME + "."
    +TENANT_VIEW_NAME + "(" + VIEW_COLUMN_NAME + " varchar)  AS SELECT * FROM "
      + TestUtil.ENTITY_HISTORY_TABLE_NAME  + " WHERE OLD_VALUE like 'E%'";

  private static final String CREATE_NONTENANT_VIEW_SQL = "CREATE VIEW IF NOT EXISTS  " + SCHEMA_NAME + "."
      + NONTENANT_VIEW_NAME + "(" + VIEW_COLUMN_NAME + " varchar)  AS SELECT * FROM "
      + TestUtil.ENTITY_HISTORY_TABLE_NAME  + " WHERE OLD_VALUE like 'E%'";

  private static final String DROP_TENANT_VIEW_SQL = "DROP VIEW IF EXISTS " + SCHEMA_NAME + "." + TENANT_VIEW_NAME;
  private static final String DROP_NONTENANT_VIEW_SQL = "DROP VIEW IF EXISTS " + SCHEMA_NAME + "." + NONTENANT_VIEW_NAME;
  private static PTable catalogTable;
  private static PTable childLinkTable;
  private static WALKeyImpl walKeyCatalog = null;
  private static WALKeyImpl walKeyChildLink = null;
  private static TableName systemCatalogTableName =
      TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
  private static TableName systemChildLinkTableName =
    TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME);

  @BeforeClass
  public static synchronized void setup() throws Exception {
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
    Properties tenantProperties = new Properties();
    tenantProperties.setProperty("TenantId", TENANT_ID);
    //create two versions of a view -- one with a tenantId and one without
    try (PhoenixConnection connection = (PhoenixConnection) ConnectionUtil
            .getInputConnection(getUtility().getConfiguration(), tenantProperties)) {
      ensureTableCreated(getUrl(), TestUtil.ENTITY_HISTORY_TABLE_NAME);
      connection.createStatement().execute(CREATE_TENANT_VIEW_SQL);
      catalogTable = connection.getTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
      childLinkTable = connection.getTable(PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME);
      walKeyCatalog = new WALKeyImpl(REGION, TableName.valueOf(
        PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME), 0, 0, uuid);
      walKeyChildLink = new WALKeyImpl(REGION, TableName.valueOf(
        PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME), 0, 0, uuid);
    };
    Assert.assertNotNull(catalogTable);
    createNonTenantView();
  }

  @AfterClass
  public static synchronized void tearDown() throws Exception {
    dropTenantView();
    dropNonTenantView();
  }

  @Test
  public void testOtherTablesAutoPass() throws Exception {
    //Cell is nonsense but we should auto pass because the table name's not System.Catalog
    WAL.Entry entry = new WAL.Entry(new WALKeyImpl(REGION,
        TableName.valueOf(TestUtil.ENTITY_HISTORY_TABLE_NAME), System.currentTimeMillis()), new WALEdit());
    entry.getEdit().add(
            CellBuilderFactory.create(
                    CellBuilderType.DEEP_COPY)
                    .setRow(Bytes.toBytes("foo"))
                    .setType(Cell.Type.Put)
                    .build());
    SystemCatalogWALEntryFilter filter = new SystemCatalogWALEntryFilter();
    Assert.assertEquals(1, filter.filter(entry).getEdit().size());
  }

  @Test
  public void testSystemCatalogWALEntryFilter() throws Exception {
    // now create WAL.Entry objects that refer to cells in those view rows in
    // System.Catalog
    Get tenantGetCatalog = getGet(catalogTable, TENANT_BYTES, TENANT_VIEW_NAME);
    Get nonTenantGetCatalog = getGet(catalogTable, DEFAULT_TENANT_BYTES, NONTENANT_VIEW_NAME);

    WAL.Entry nonTenantEntryCatalog = getEntry(systemCatalogTableName, nonTenantGetCatalog);
    WAL.Entry tenantEntryCatalog = getEntry(systemCatalogTableName, tenantGetCatalog);
    int tenantRowCount = getAndAssertCountInEdit(tenantEntryCatalog, true);
    Assert.assertTrue(tenantRowCount > 0);

    //verify that the tenant view WAL.Entry passes the filter and the non-tenant view does not
    SystemCatalogWALEntryFilter filter = new SystemCatalogWALEntryFilter();
    // Chain the system catalog WAL entry filter to ChainWALEntryFilter
    ChainWALEntryFilter chainWALEntryFilter = new ChainWALEntryFilter(filter);
    // Asserting the WALEdit for non tenant has cells before getting filtered
    Assert.assertTrue(nonTenantEntryCatalog.getEdit().size() > 0);
    // All the cells will get removed by the filter since they do not belong to tenant
    Assert.assertTrue("Non tenant edits for system catalog should not get filtered",
        chainWALEntryFilter.filter(nonTenantEntryCatalog).getEdit().isEmpty());

    WAL.Entry filteredTenantEntryCatalog = chainWALEntryFilter.filter(tenantEntryCatalog);
    Assert.assertNotNull("Tenant view was filtered when it shouldn't be!",
      filteredTenantEntryCatalog);
    Assert.assertEquals("Not all data for replicated for tenant", tenantRowCount,
        getAndAssertCountInEdit(filteredTenantEntryCatalog, true));

    //now check that a WAL.Entry with cells from both a tenant and a non-tenant
    //catalog row only allow the tenant cells through
    WALEdit comboEdit = new WALEdit();
    nonTenantEntryCatalog = getEntry(systemCatalogTableName, nonTenantGetCatalog);
    tenantEntryCatalog = getEntry(systemCatalogTableName, tenantGetCatalog);
    comboEdit.getCells().addAll(nonTenantEntryCatalog.getEdit().getCells());
    comboEdit.getCells().addAll(tenantEntryCatalog.getEdit().getCells());
    WAL.Entry comboEntry = new WAL.Entry(walKeyCatalog, comboEdit);

    Assert.assertEquals(tenantEntryCatalog.getEdit().size() + nonTenantEntryCatalog.getEdit().size()
        , comboEntry.getEdit().size());
    Assert.assertEquals(tenantEntryCatalog.getEdit().size(),
        chainWALEntryFilter.filter(comboEntry).getEdit().size());
  }

  @Test
  public void testSystemChildLinkWALEntryFilter() throws Exception {
    // now create WAL.Entry objects that refer to cells in those view rows in
    // System.Child_Link
    Get tenantGetChildLink = getGetChildLink(childLinkTable, TENANT_BYTES, TENANT_VIEW_NAME);
    Get nonTenantGetChildLink = getGetChildLink(childLinkTable, DEFAULT_TENANT_BYTES, NONTENANT_VIEW_NAME);

    WAL.Entry tenantEntryChildLink = getEntry(systemChildLinkTableName, tenantGetChildLink);
    WAL.Entry nonTenantEntryChildLink = getEntry(systemChildLinkTableName, nonTenantGetChildLink);
    int tenantRowCount = getAndAssertCountInEdit(tenantEntryChildLink, true);
    Assert.assertTrue(tenantRowCount > 0);

    //verify that the tenant view WAL.Entry passes the filter and the non-tenant view does not
    SystemCatalogWALEntryFilter filter = new SystemCatalogWALEntryFilter();
    // Chain the system catalog WAL entry filter to ChainWALEntryFilter
    ChainWALEntryFilter chainWALEntryFilter = new ChainWALEntryFilter(filter);
    // Asserting the WALEdit for non tenant has cells before getting filtered
    Assert.assertTrue(nonTenantEntryChildLink.getEdit().size() > 0);
    // All the cells will get removed by the filter since they do not belong to tenant
    Assert.assertTrue("Non tenant edits for system child link should not get filtered",
      chainWALEntryFilter.filter(nonTenantEntryChildLink).getEdit().isEmpty());

    WAL.Entry filteredTenantEntryChildLink = chainWALEntryFilter.filter(tenantEntryChildLink);
    Assert.assertNotNull("Tenant view was filtered when it shouldn't be!",
      filteredTenantEntryChildLink);
    Assert.assertEquals("Not all data for replicated for tenant", tenantRowCount,
        getAndAssertCountInEdit(filteredTenantEntryChildLink, true));

    //now check that a WAL.Entry with cells from both a tenant and a non-tenant
    // child link row only allow the tenant cells through
    WALEdit comboEdit = new WALEdit();
    comboEdit.getCells().addAll(nonTenantEntryChildLink.getEdit().getCells());
    comboEdit.getCells().addAll(tenantEntryChildLink.getEdit().getCells());
    WAL.Entry comboEntry = new WAL.Entry(walKeyChildLink, comboEdit);

    Assert.assertEquals(tenantEntryChildLink.getEdit().size() + nonTenantEntryChildLink.getEdit().size()
      , comboEntry.getEdit().size());
    Assert.assertEquals(tenantEntryChildLink.getEdit().size(),
      chainWALEntryFilter.filter(comboEntry).getEdit().size());
  }

  /**
   * Validates the behavior for parent-child link's delete marker via SystemCatalogWalEntryFilter.
   * 1. Filtered for non-tenant views.
   * 2. Not filtered for tenant views.
   * */
  @Test
  public void testDeleteMarkerForParentChildLink() throws Exception{
    // Since for 4.16+ all parent-child links are stored in SYSTEM.CHILD_LINK, only
    // checking for that table in this test.

    // Make sure link row exists.
    WAL.Entry childLinkEntry = getEntry(systemChildLinkTableName, new Scan(),
        false);
    int tenantRowCount = getAndAssertCountInEdit(childLinkEntry, true);
    int nonTenantRowCount = getAndAssertCountInEdit(childLinkEntry, false);
    Assert.assertTrue(tenantRowCount > 0 && nonTenantRowCount > 0 );

    // Drop both tenant and non-tenant view.
    dropTenantView();
    dropNonTenantView();

    // Delete Marker for non-tenant view should get filtered and for tenant-view it should not.
    SystemCatalogWALEntryFilter filter = new SystemCatalogWALEntryFilter();
    // Chain the system catalog WAL entry filter to ChainWALEntryFilter
    ChainWALEntryFilter chainWALEntryFilter = new ChainWALEntryFilter(filter);
    childLinkEntry = getEntry(systemChildLinkTableName, new Scan(),
        false);
    int tenantDeleteCountBeforeFilter = getDeleteFamilyCellCountInEntry(childLinkEntry, true);
    int nonTenantDeleteCountBeforeFilter = getDeleteFamilyCellCountInEntry(childLinkEntry, false);
    // Make sure both tenant and non-tenant delete marker exists before filtering
    Assert.assertTrue(tenantDeleteCountBeforeFilter > 0 && nonTenantDeleteCountBeforeFilter > 0 );

    WAL.Entry filteredEntry = chainWALEntryFilter.filter(childLinkEntry);
    int tenantDeleteCountAfterFilter = getDeleteFamilyCellCountInEntry(filteredEntry, true);
    int nonTenantDeleteCountAfterFilter = getDeleteFamilyCellCountInEntry(filteredEntry, false);
    Assert.assertTrue(tenantDeleteCountAfterFilter == tenantDeleteCountBeforeFilter  && nonTenantDeleteCountAfterFilter == 0 );

    // setup views again.
    createTenantView();
    createNonTenantView();
  }

  private Get getGet(PTable catalogTable, byte[] tenantId, String viewName) {
    byte[][] tenantKeyParts = new byte[5][];
    tenantKeyParts[0] = tenantId;
    tenantKeyParts[1] = Bytes.toBytes(SCHEMA_NAME.toUpperCase());
    tenantKeyParts[2] = Bytes.toBytes(viewName.toUpperCase());
    tenantKeyParts[3] = Bytes.toBytes(VIEW_COLUMN_NAME);
    tenantKeyParts[4] = VIEW_COLUMN_FAMILY_BYTES;
    ImmutableBytesWritable key = new ImmutableBytesWritable();
    catalogTable.newKey(key, tenantKeyParts);
    //the backing byte array of key might have extra space at the end.
    // need to just slice "the good parts" which we do by calling copyBytes
    return new Get(key.copyBytes());
  }

  private Get getGetChildLink(PTable catalogTable, byte[] tenantId, String viewName) {
    byte[][] tenantKeyParts = new byte[5][];
    tenantKeyParts[0] = ByteUtil.EMPTY_BYTE_ARRAY;
    tenantKeyParts[1] = ByteUtil.EMPTY_BYTE_ARRAY;
    tenantKeyParts[2] = Bytes.toBytes(TestUtil.ENTITY_HISTORY_TABLE_NAME);
    tenantKeyParts[3] = tenantId;
    tenantKeyParts[4] = Bytes.toBytes(SCHEMA_NAME + "." +viewName.toUpperCase());;
    ImmutableBytesWritable key = new ImmutableBytesWritable();
    catalogTable.newKey(key, tenantKeyParts);
    //the backing byte array of key might have extra space at the end.
    // need to just slice "the good parts" which we do by calling copyBytes
    return new Get(key.copyBytes());
  }

  private boolean isTenantOwnedCell(Cell cell, String tenantId) {
    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(),
      cell.getRowLength());
    boolean isTenantIdLeading = row.startsWith(tenantId);
    boolean isChildLinkForTenantId = row.contains(tenantId)
      && CellUtil.matchingQualifier(cell,
      PhoenixDatabaseMetaData.LINK_TYPE_BYTES);
    boolean isDeleteMarkerForLinkRow = row.contains(tenantId) && cell.getType() == Cell.Type.DeleteFamily;
    return isTenantIdLeading || isChildLinkForTenantId || isDeleteMarkerForLinkRow;
  }

  /**
   * Asserts and returns cell count in the WAL.Entry. if tenantOwned is true, tenant owned cell count is
   * returned else non-tenant cell count.
   * @Param entry {@link WAL.Entry}
   * @Param tenantOwned {@link Boolean}
   * */
  private int getAndAssertCountInEdit(WAL.Entry entry, boolean tenantOwned) {
    int tenantCount = 0;
    int nonTenantCount = 0;
    for (Cell cell : entry.getEdit().getCells()) {
      if (isTenantOwnedCell(cell, TENANT_ID)) {
        tenantCount = tenantCount + 1;
      } else {
        nonTenantCount = nonTenantCount + 1;
      }
    }
    int count = tenantOwned ? tenantCount : nonTenantCount;
    Assert.assertTrue(count > 0);
    return count;
  }

  /**
   * Returns delete family cell count in the WAL.Entry. if tenantOwned is true, tenant owned cell count is
   * returned else non-tenant cell count.
   * @Param entry {@link WAL.Entry}
   * @Param tenantOwned {@link Boolean}
   * */
  private int getDeleteFamilyCellCountInEntry(WAL.Entry entry, boolean tenantOwned) {
    int tenantCount = 0;
    int nonTenantCount = 0;
    for (Cell cell : entry.getEdit().getCells()) {
      if (cell.getType() == Cell.Type.DeleteFamily) {
        if (isTenantOwnedCell(cell, TENANT_ID)) {
          tenantCount = tenantCount + 1;
        } else {
          nonTenantCount = nonTenantCount + 1;
        }
      }
    }
    return tenantOwned ? tenantCount : nonTenantCount;
  }

  private WAL.Entry getEntry(TableName tableName, Get get) throws IOException {
    WAL.Entry entry = null;
    try(Connection conn = ConnectionFactory.createConnection(getUtility().getConfiguration())){
      Table htable = conn.getTable(tableName);
      Result result = htable.get(get);
      WALEdit edit = new WALEdit();
      if (result != null) {
        List<Cell> cellList = result.listCells();
        Assert.assertNotNull(String.format("Didn't retrieve any cells from table %s",
          tableName.getNameAsString()), cellList);
        for (Cell c : cellList) {
          edit.add(c);
        }
      }
      Assert.assertTrue(String.format("Didn't retrieve any cells from table %s",
        tableName.getNameAsString()), edit.getCells().size() > 0);
      WALKeyImpl key = new WALKeyImpl(REGION, tableName, 0, 0, uuid);
      entry = new WAL.Entry(key, edit);
    }
    return entry;
  }

  private WAL.Entry getEntry(TableName tableName, Scan scan, boolean addIndexedKeyValueCell)
      throws IOException {
    WAL.Entry entry = null;
    try(Connection conn = ConnectionFactory.createConnection(getUtility().getConfiguration())) {
      Table htable = conn.getTable(tableName);
      scan.setRaw(true);
      ResultScanner scanner = htable.getScanner(scan);
      WALEdit edit = new WALEdit();
      if (addIndexedKeyValueCell) {
        // add IndexedKeyValue type cell as the first cell
        edit.add(new IndexedKeyValue());
      }

      for (Result r : scanner) {
        if (r != null) {
          List<Cell> cellList = r.listCells();
          for (Cell c : cellList) {
            edit.add(c);
          }
        }
      }
      Assert.assertFalse("No WALEdits were loaded!", edit.isEmpty());
      WALKeyImpl key = new WALKeyImpl(REGION, tableName, 0, 0, uuid);
      entry = new WAL.Entry(key, edit);
    }
    return entry;
  }

  private static void dropTenantView() throws Exception {
    Properties tenantProperties = new Properties();
    tenantProperties.setProperty("TenantId", TENANT_ID);
    try (java.sql.Connection connection = DriverManager.getConnection(getUrl(), tenantProperties)) {
      connection.createStatement().execute(DROP_TENANT_VIEW_SQL);
      connection.commit();
    }
  }

  private static void dropNonTenantView() throws Exception {
    try (java.sql.Connection connection = DriverManager.getConnection(getUrl())) {
      connection.createStatement().execute(DROP_NONTENANT_VIEW_SQL);
    }
  }

  private static void createTenantView() throws Exception {
    Properties tenantProperties = new Properties();
    tenantProperties.setProperty("TenantId", TENANT_ID);
    try (java.sql.Connection connection = DriverManager.getConnection(getUrl(), tenantProperties)) {
      connection.createStatement().execute(CREATE_TENANT_VIEW_SQL);
      connection.commit();
    }
  }

  private static void createNonTenantView() throws Exception {
    try (java.sql.Connection connection = DriverManager.getConnection(getUrl())) {
      connection.createStatement().execute(CREATE_NONTENANT_VIEW_SQL);
      connection.commit();
    }
  }
}
