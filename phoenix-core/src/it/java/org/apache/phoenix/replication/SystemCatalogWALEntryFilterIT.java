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
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.schema.PTable;
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
  private static WALKey walKey = null;
  private static TableName systemCatalogTableName =
      TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);


  @BeforeClass
  public static void setup() throws Exception {
    setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
    Properties tenantProperties = new Properties();
    tenantProperties.setProperty("TenantId", TENANT_ID);
    //create two versions of a view -- one with a tenantId and one without
    try (java.sql.Connection connection =
             ConnectionUtil.getInputConnection(getUtility().getConfiguration(), tenantProperties)) {
      ensureTableCreated(getUrl(), TestUtil.ENTITY_HISTORY_TABLE_NAME);
      connection.createStatement().execute(CREATE_TENANT_VIEW_SQL);
      catalogTable = PhoenixRuntime.getTable(connection, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
      walKey = new WALKey(REGION, TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME), 0, 0, uuid);
    };
    Assert.assertNotNull(catalogTable);
    try (java.sql.Connection connection =
             ConnectionUtil.getInputConnection(getUtility().getConfiguration(), new Properties())) {
      connection.createStatement().execute(CREATE_NONTENANT_VIEW_SQL);
    };
  }

  @AfterClass
  public static void tearDown() throws Exception {
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

    //now create WAL.Entry objects that refer to cells in those view rows in System.Catalog

    Get tenantGet = getGet(catalogTable, TENANT_BYTES, TENANT_VIEW_NAME);
    Get nonTenantGet = getGet(catalogTable, DEFAULT_TENANT_BYTES, NONTENANT_VIEW_NAME);

    WAL.Entry nonTenantEntry = getEntry(systemCatalogTableName, nonTenantGet);
    WAL.Entry tenantEntry = getEntry(systemCatalogTableName, tenantGet);

    //verify that the tenant view WAL.Entry passes the filter and the non-tenant view does not
    SystemCatalogWALEntryFilter filter = new SystemCatalogWALEntryFilter();
    Assert.assertNull(filter.filter(nonTenantEntry));
    WAL.Entry filteredTenantEntry = filter.filter(tenantEntry);
    Assert.assertNotNull("Tenant view was filtered when it shouldn't be!", filteredTenantEntry);
    Assert.assertEquals(tenantEntry.getEdit().size(),
        filter.filter(tenantEntry).getEdit().size());

    //now check that a WAL.Entry with cells from both a tenant and a non-tenant
    //catalog row only allow the tenant cells through
    WALEdit comboEdit = new WALEdit();
    comboEdit.getCells().addAll(nonTenantEntry.getEdit().getCells());
    comboEdit.getCells().addAll(tenantEntry.getEdit().getCells());
    WAL.Entry comboEntry = new WAL.Entry(walKey, comboEdit);

    Assert.assertEquals(tenantEntry.getEdit().size() + nonTenantEntry.getEdit().size()
        , comboEntry.getEdit().size());
    Assert.assertEquals(tenantEntry.getEdit().size(),
        filter.filter(comboEntry).getEdit().size());
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

  public WAL.Entry getEntry(TableName tableName, Get get) throws IOException {
    WAL.Entry entry = null;
    try(Connection conn = ConnectionFactory.createConnection(getUtility().getConfiguration())){
      Table htable = conn.getTable(tableName);
      Result result = htable.get(get);
      WALEdit edit = new WALEdit();
      if (result != null) {
        List<Cell> cellList = result.listCells();
        Assert.assertNotNull("Didn't retrieve any cells from SYSTEM.CATALOG", cellList);
        for (Cell c : cellList) {
          edit.add(c);
        }
      }
      Assert.assertTrue("Didn't retrieve any cells from SYSTEM.CATALOG", edit.getCells().size() > 0);
      WALKey key = new WALKey(REGION, tableName, 0, 0, uuid);
      entry = new WAL.Entry(key, edit);
    }
    return entry;
  }
}
