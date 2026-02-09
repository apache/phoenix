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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.types.IndexConsistency;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * IndexMaintainer consistency tests.
 */
@Category(ParallelStatsDisabledTest.class)
public class IndexMaintainerConsistencyIT extends ParallelStatsDisabledIT {

  @After
  public synchronized void afterTest() throws Exception {
    boolean refCountLeaked = isAnyStoreRefCountLeaked();
    Assert.assertFalse("refCount leaked", refCountLeaked);
  }

  @Test
  public void testIndexMaintainerConsistencyFromPTable() throws Exception {
    String dataTableName = "XYZ.\"tablENam1.1_0-001\"";
    String dataTableNameWithoutQuotes = "XYZ.tablENam1.1_0-001";
    String strongIndexName1 = "\"idXNam1.12._0-001\"";
    String strongIndexName2 = "\"idXNam1.12._0-002\"";
    String eventualIndexName1 = "\"idXNam2.12._0-002\"";
    String eventualIndexName2 = "\"idXNam3.12._0-003\"";

    try (Connection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      conn.createStatement().execute(
        "CREATE TABLE " + dataTableName + " (id INTEGER PRIMARY KEY, name VARCHAR, city VARCHAR)"
          + " IS_STRICT_TTL=false, UPDATE_CACHE_FREQUENCY=2, "
          + "\"phoenix.max.lookback.age.seconds\"=97200");
      conn.createStatement().execute(
        "CREATE INDEX " + strongIndexName1 + " ON " + dataTableName + " (name) INCLUDE (city)");
      conn.createStatement().execute("CREATE INDEX " + strongIndexName2 + " ON " + dataTableName
        + " (name) INCLUDE (city) CONSISTENCY=STRONG");
      conn.createStatement().execute("CREATE INDEX " + eventualIndexName1 + " ON " + dataTableName
        + " (name) INCLUDE (city) CONSISTENCY=EVENTUAL");
      conn.createStatement().execute("CREATE UNCOVERED INDEX " + eventualIndexName2 + " ON "
        + dataTableName + " (city, name) CONSISTENCY=EVENTUAL");
      PTable strongIndex1 =
        pconn.getTable(new PTableKey(pconn.getTenantId(), "XYZ.idXNam1.12._0-001"));
      PTable strongIndex2 =
        pconn.getTable(new PTableKey(pconn.getTenantId(), "XYZ.idXNam1.12._0-002"));
      PTable eventualIndex1 =
        pconn.getTable(new PTableKey(pconn.getTenantId(), "XYZ.idXNam2.12._0-002"));
      PTable eventualIndex2 =
        pconn.getTable(new PTableKey(pconn.getTenantId(), "XYZ.idXNam3.12._0-003"));
      PTable dataTable =
        pconn.getTable(new PTableKey(pconn.getTenantId(), dataTableNameWithoutQuotes));
      assertNull(strongIndex1.getIndexConsistency());
      assertEquals(IndexConsistency.STRONG, strongIndex2.getIndexConsistency());
      assertEquals(IndexConsistency.EVENTUAL, eventualIndex1.getIndexConsistency());
      assertEquals(IndexConsistency.EVENTUAL, eventualIndex2.getIndexConsistency());
      assertEquals(2, strongIndex1.getUpdateCacheFrequency());
      assertEquals(2, strongIndex2.getUpdateCacheFrequency());
      assertEquals(2, eventualIndex1.getUpdateCacheFrequency());
      assertEquals(2, eventualIndex2.getUpdateCacheFrequency());
      assertEquals(2, dataTable.getUpdateCacheFrequency());
      assertFalse(eventualIndex1.isStrictTTL());
      assertFalse(eventualIndex2.isStrictTTL());
      assertFalse(strongIndex1.isStrictTTL());
      assertFalse(strongIndex2.isStrictTTL());
      assertFalse(dataTable.isStrictTTL());
      assertNull(dataTable.getIndexConsistency());

      IndexMaintainer strongMaintainer1 = strongIndex1.getIndexMaintainer(dataTable, pconn);
      IndexMaintainer strongMaintainer2 = strongIndex2.getIndexMaintainer(dataTable, pconn);
      IndexMaintainer eventualMaintainer1 = eventualIndex1.getIndexMaintainer(dataTable, pconn);
      IndexMaintainer eventualMaintainer2 = eventualIndex2.getIndexMaintainer(dataTable, pconn);

      assertNotNull(strongMaintainer1);
      assertNotNull(strongMaintainer2);
      assertNotNull(eventualMaintainer1);
      assertNotNull(eventualMaintainer2);

      verifyIndexMaintainerConsistency(strongMaintainer1, null, dataTable);
      verifyIndexMaintainerConsistency(strongMaintainer2, IndexConsistency.STRONG, dataTable);
      verifyIndexMaintainerConsistency(eventualMaintainer1, IndexConsistency.EVENTUAL, dataTable);
      verifyIndexMaintainerConsistency(eventualMaintainer2, IndexConsistency.EVENTUAL, dataTable);

      RowKeySchema schema = RowKeySchema.EMPTY_SCHEMA;
      IndexMaintainer maintainer = new IndexMaintainer(schema, false) {
      };

      assertNull(maintainer.getIndexConsistency());

      assertTrue("Data table should have CDC index after creating eventually consistent index",
        CDCUtil.hasCDCIndex(dataTable));
      assertTrue("Data table should have active CDC index", CDCUtil.hasActiveCDCIndex(dataTable));

      String cdcObjectName = CDCUtil.getCDCObjectName(dataTable, false);
      assertNotNull("CDC object name should be retrievable from data table", cdcObjectName);

      String expectedCDCName = "CDC_tablENam1.1_0-001";
      assertEquals("CDC object name should follow expected convention", expectedCDCName,
        cdcObjectName);

      PTable cdcIndex = CDCUtil.getActiveCDCIndex(dataTable);
      String expectedCDCIndexName = CDCUtil.getCDCIndexName(cdcObjectName);
      assertEquals("CDC index name should follow expected convention", expectedCDCIndexName,
        cdcIndex.getTableName().getString());

      PTable cdcTable = pconn.getTable(new PTableKey(pconn.getTenantId(), "XYZ." + cdcObjectName));
      assertEquals("CDC table should have CDC type", PTableType.CDC, cdcTable.getType());

      List<PTable> indexes = dataTable.getIndexes();
      assertEquals("Data table should have total 5 indexes", 5, indexes.size());
    }
  }

  @Test
  public void testAlterIndexConsistency() throws Exception {
    String dataTableName = "\"ScheMa1234-_\".\"alterTable1.1_0-001\"";
    String dataTableNameWithoutQuotes = "ScheMa1234-_.alterTable1.1_0-001";
    String indexName = "\"alterIdx1.12._0-001\"";
    String fullIdxNameWithoutQuotes = "ScheMa1234-_.alterIdx1.12._0-001";

    try (Connection conn =
      DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      conn.createStatement().execute(
        "CREATE TABLE " + dataTableName + " (id INTEGER PRIMARY KEY, name VARCHAR, city VARCHAR)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + dataTableName + " (name) INCLUDE (city)");

      PTable strongIndex =
        pconn.getTable(new PTableKey(pconn.getTenantId(), fullIdxNameWithoutQuotes));
      PTable dataTable =
        pconn.getTable(new PTableKey(pconn.getTenantId(), dataTableNameWithoutQuotes));
      assertNull(strongIndex.getIndexConsistency());
      assertFalse("Data table should not have CDC index initially", CDCUtil.hasCDCIndex(dataTable));

      IndexMaintainer strongMaintainer = strongIndex.getIndexMaintainer(dataTable, pconn);
      verifyIndexMaintainerConsistency(strongMaintainer, null, dataTable);

      conn.createStatement()
        .execute("ALTER INDEX " + indexName + " ON " + dataTableName + " CONSISTENCY=EVENTUAL");
      PTable eventualIndex =
        pconn.getTable(new PTableKey(pconn.getTenantId(), fullIdxNameWithoutQuotes));
      dataTable = pconn.getTable(new PTableKey(pconn.getTenantId(), dataTableNameWithoutQuotes));

      assertEquals(IndexConsistency.EVENTUAL, eventualIndex.getIndexConsistency());
      assertTrue("Data table should have CDC index after altering to eventual consistency",
        CDCUtil.hasCDCIndex(dataTable));

      IndexMaintainer eventualMaintainer = eventualIndex.getIndexMaintainer(dataTable, pconn);
      verifyIndexMaintainerConsistency(eventualMaintainer, IndexConsistency.EVENTUAL, dataTable);
      conn.createStatement()
        .execute("ALTER INDEX " + indexName + " ON " + dataTableName + " CONSISTENCY=STRONG");
      PTable strongIdx =
        pconn.getTable(new PTableKey(pconn.getTenantId(), fullIdxNameWithoutQuotes));
      assertEquals(IndexConsistency.STRONG, strongIdx.getIndexConsistency());
      IndexMaintainer strongIdxMaintainer = strongIdx.getIndexMaintainer(dataTable, pconn);
      verifyIndexMaintainerConsistency(strongIdxMaintainer, IndexConsistency.STRONG, dataTable);
    }
  }

  private void verifyIndexMaintainerConsistency(IndexMaintainer maintainer,
    IndexConsistency expectedConsistency, PTable dataTable) throws IOException {
    assertEquals(expectedConsistency, maintainer.getIndexConsistency());
    ServerCachingProtos.IndexMaintainer proto = IndexMaintainer.toProto(maintainer);
    if (expectedConsistency != null) {
      assertTrue(proto.hasIndexConsistency());
      assertEquals(expectedConsistency.toString(), proto.getIndexConsistency());
    } else {
      assertFalse(proto.hasIndexConsistency());
    }
    IndexMaintainer deserializedMaintainer = IndexMaintainer.fromProto(proto,
      dataTable.getRowKeySchema(), dataTable.getBucketNum() != null);
    assertEquals(expectedConsistency, deserializedMaintainer.getIndexConsistency());
  }

}
