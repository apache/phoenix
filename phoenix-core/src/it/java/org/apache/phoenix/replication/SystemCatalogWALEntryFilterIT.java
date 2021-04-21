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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.replication.ChainWALEntryFilter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
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

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;


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

  private static final String DROP_TENANT_VIEW_SQL = "DROP VIEW IF EXISTS " + TENANT_VIEW_NAME;
  private static final String DROP_NONTENANT_VIEW_SQL = "DROP VIEW IF EXISTS " + NONTENANT_VIEW_NAME;
  private static PTable catalogTable;
  private static PTable childLinkTable;
  private static WALKey walKeyCatalog = null;
  private static WALKey walKeyChildLink = null;
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
    try (java.sql.Connection connection =
             ConnectionUtil.getInputConnection(getUtility().getConfiguration(), tenantProperties)) {
      ensureTableCreated(getUrl(), TestUtil.ENTITY_HISTORY_TABLE_NAME);
      connection.createStatement().execute(CREATE_TENANT_VIEW_SQL);
      catalogTable = PhoenixRuntime.getTable(connection, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
      childLinkTable = PhoenixRuntime.getTable(connection, PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME);
      walKeyCatalog = new WALKey(REGION, TableName.valueOf(
        PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME), 0, 0, uuid);
      walKeyChildLink = new WALKey(REGION, TableName.valueOf(
        PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME), 0, 0, uuid);
    };
    Assert.assertNotNull(catalogTable);
    try (java.sql.Connection connection =
             ConnectionUtil.getInputConnection(getUtility().getConfiguration(), new Properties())) {
      connection.createStatement().execute(CREATE_NONTENANT_VIEW_SQL);
    };
  }

  @AfterClass
  public static synchronized void tearDown() throws Exception {
    Properties tenantProperties = new Properties();
    tenantProperties.setProperty("TenantId", TENANT_ID);
    try (java.sql.Connection connection =
             ConnectionUtil.getInputConnection(getUtility().getConfiguration(), tenantProperties)) {
      connection.createStatement().execute(DROP_TENANT_VIEW_SQL);
    }
    try (java.sql.Connection connection =
             ConnectionUtil.getInputConnection(getUtility().getConfiguration(), new Properties())) {
      connection.createStatement().execute(DROP_NONTENANT_VIEW_SQL);
    }
  }

  @Test
  public void testOtherTablesAutoPass() throws Exception {
    //Cell is nonsense but we should auto pass because the table name's not System.Catalog
    WAL.Entry entry = new WAL.Entry(new WALKey(REGION,
        TableName.valueOf(TestUtil.ENTITY_HISTORY_TABLE_NAME)), new WALEdit());
    entry.getEdit().add(CellUtil.createCell(Bytes.toBytes("foo")));
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
    int tenantRowCount = getAndAssertTenantCountInEdit(tenantEntryCatalog);
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
      getAndAssertTenantCountInEdit(filteredTenantEntryCatalog));

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
    int tenantRowCount = getAndAssertTenantCountInEdit(tenantEntryChildLink);
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
      getAndAssertTenantCountInEdit(filteredTenantEntryChildLink));

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

  public Get getGet(PTable catalogTable, byte[] tenantId, String viewName) {
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

  public Get getGetChildLink(PTable catalogTable, byte[] tenantId, String viewName) {
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
    return isTenantIdLeading || isChildLinkForTenantId;
  }

  private int getAndAssertTenantCountInEdit(WAL.Entry entry) {
    int count = 0;
    for (Cell cell : entry.getEdit().getCells()) {
      if (isTenantOwnedCell(cell, TENANT_ID)) {
        count = count + 1;
      }
    }
    Assert.assertTrue(count > 0);
    return count;
  }

  public WAL.Entry getEntry(TableName tableName, Get get) throws IOException {
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
      WALKey key = new WALKey(REGION, tableName, 0, 0, uuid);
      entry = new WAL.Entry(key, edit);
    }
    return entry;
  }
}
