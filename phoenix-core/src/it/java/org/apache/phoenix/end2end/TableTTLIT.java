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

import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_POST_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_TTL_DELETE_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_UPSERT_EVENT_TYPE;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class TableTTLIT extends BaseTest {
  private static final Logger LOG = LoggerFactory.getLogger(TableTTLIT.class);
  private static final Random RAND = new Random(11);
  private static final int MAX_COLUMN_INDEX = 7;
  private static final int MAX_LOOKBACK_AGE = 10;
  private final int ttl;
  private String tableDDLOptions;
  private StringBuilder optionBuilder;
  ManualEnvironmentEdge injectEdge;
  private int versions;
  private final boolean multiCF;
  private final boolean columnEncoded;
  private final KeepDeletedCells keepDeletedCells;
  private final Integer tableLevelMaxLookback;

  public TableTTLIT(boolean multiCF, boolean columnEncoded, KeepDeletedCells keepDeletedCells,
    int versions, int ttl, Integer tableLevelMaxLookback) {
    this.multiCF = multiCF;
    this.columnEncoded = columnEncoded;
    this.keepDeletedCells = keepDeletedCells;
    this.versions = versions;
    this.ttl = ttl;
    this.tableLevelMaxLookback = tableLevelMaxLookback;
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(MAX_LOOKBACK_AGE));
    props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
    props.put(QueryServices.PHOENIX_VIEW_TTL_ENABLED, Boolean.toString(false));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Before
  public void beforeTest() {
    EnvironmentEdgeManager.reset();
    optionBuilder = new StringBuilder();
    optionBuilder.append(" TTL=" + ttl);
    optionBuilder.append(", VERSIONS=" + versions);
    if (keepDeletedCells == KeepDeletedCells.FALSE) {
      optionBuilder.append(", KEEP_DELETED_CELLS=FALSE");
    } else if (keepDeletedCells == KeepDeletedCells.TRUE) {
      optionBuilder.append(", KEEP_DELETED_CELLS=TRUE");
    } else {
      optionBuilder.append(", KEEP_DELETED_CELLS=TTL");
    }
    if (columnEncoded) {
      optionBuilder.append(", COLUMN_ENCODED_BYTES=2");
    } else {
      optionBuilder.append(", COLUMN_ENCODED_BYTES=0");
    }
    this.tableDDLOptions = optionBuilder.toString();
    injectEdge = new ManualEnvironmentEdge();
    injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
  }

  @After
  public synchronized void afterTest() throws Exception {
    boolean refCountLeaked = isAnyStoreRefCountLeaked();

    EnvironmentEdgeManager.reset();
    Assert.assertFalse("refCount leaked", refCountLeaked);
  }

  @Parameterized.Parameters(name = "TableTTLIT_multiCF={0}, columnEncoded={1}, "
    + "keepDeletedCells={2}, versions={3}, ttl={4}, tableLevelMaxLookback={5}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { false, false, KeepDeletedCells.FALSE, 1, 100, null },
      { false, false, KeepDeletedCells.TRUE, 5, 50, null },
      { false, false, KeepDeletedCells.TTL, 1, 25, null },
      { true, false, KeepDeletedCells.FALSE, 5, 50, null },
      { true, false, KeepDeletedCells.TRUE, 1, 25, null },
      { true, false, KeepDeletedCells.TTL, 5, 100, null },
      { false, false, KeepDeletedCells.FALSE, 1, 100, 0 },
      { false, false, KeepDeletedCells.TRUE, 5, 50, 0 },
      { false, false, KeepDeletedCells.TTL, 1, 25, 15 } });
  }

  /**
   * This test creates two tables with the same schema. The same row is updated in a loop on both
   * tables with the same content. Each update changes one or more columns chosen randomly with
   * randomly generated values. After every upsert, all versions of the rows are retrieved from each
   * table and compared. The test also occasionally deletes the row from both tables and but
   * compacts only the first table during this test. Both tables are subject to masking during
   * queries. This test expects that both tables return the same row content for the same row
   * version.
   */

  @Test
  public void testMaskingAndMajorCompaction() throws Exception {
    final int maxLookbackAge =
      tableLevelMaxLookback != null ? tableLevelMaxLookback : MAX_LOOKBACK_AGE;
    final int maxDeleteCounter = maxLookbackAge == 0 ? 1 : maxLookbackAge;
    final int maxCompactionCounter = ttl / 2;
    final int maxFlushCounter = ttl;
    final int maxMaskingCounter = 2 * ttl;
    final int maxVerificationCounter = 2 * ttl;
    final byte[] rowKey = Bytes.toBytes("a");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      createTable(tableName);
      conn.createStatement().execute("Alter Table " + tableName
        + " set \"phoenix.max.lookback.age.seconds\" = " + maxLookbackAge);
      conn.commit();
      String noCompactTableName = generateUniqueName();
      createTable(noCompactTableName);
      conn.createStatement().execute("ALTER TABLE " + noCompactTableName
        + " set \"phoenix.max.lookback.age.seconds\" = " + maxLookbackAge);
      conn.commit();
      long startTime = System.currentTimeMillis() + 1000;
      startTime = (startTime / 1000) * 1000;
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);
      int deleteCounter = RAND.nextInt(maxDeleteCounter) + 1;
      int compactionCounter = RAND.nextInt(maxCompactionCounter) + 1;
      int flushCounter = RAND.nextInt(maxFlushCounter) + 1;
      int maskingCounter = RAND.nextInt(maxMaskingCounter) + 1;
      int verificationCounter = RAND.nextInt(maxVerificationCounter) + 1;
      int maxIterationCount = multiCF ? 250 : 500;
      for (int i = 0; i < maxIterationCount; i++) {
        if (flushCounter-- == 0) {
          injectEdge.incrementValue(1000);
          LOG.info("Flush " + i + " current time: " + injectEdge.currentTime());
          flush(TableName.valueOf(tableName));
          flushCounter = RAND.nextInt(maxFlushCounter) + 1;
        }
        if (compactionCounter-- == 0) {
          injectEdge.incrementValue(1000);
          LOG.info("Compaction " + i + " current time: " + injectEdge.currentTime());
          flush(TableName.valueOf(tableName));
          majorCompact(TableName.valueOf(tableName));
          compactionCounter = RAND.nextInt(maxCompactionCounter) + 1;
        }
        if (maskingCounter-- == 0) {
          updateRow(conn, tableName, noCompactTableName, "a");
          injectEdge.incrementValue((ttl + maxLookbackAge + 1) * 1000);
          LOG.info("Masking " + i + " current time: " + injectEdge.currentTime());
          ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + tableName);
          Assert.assertTrue(rs.next());
          Assert.assertEquals(rs.getLong(1), 0);
          rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + noCompactTableName);
          Assert.assertTrue(rs.next());
          Assert.assertEquals(rs.getLong(1), 0);
          flush(TableName.valueOf(tableName));
          majorCompact(TableName.valueOf(tableName));
          TestUtil.assertRawCellCount(conn, TableName.valueOf(tableName), rowKey, 0);
          maskingCounter = RAND.nextInt(maxMaskingCounter) + 1;
        }
        if (deleteCounter-- == 0) {
          LOG.info("Delete " + i + " current time: " + injectEdge.currentTime());
          deleteRow(conn, tableName, "a");
          deleteRow(conn, noCompactTableName, "a");
          deleteCounter = RAND.nextInt(maxDeleteCounter) + 1;
          injectEdge.incrementValue(1000);
        }
        injectEdge.incrementValue(1000);
        updateRow(conn, tableName, noCompactTableName, "a");
        if (verificationCounter-- > 0) {
          continue;
        }
        verificationCounter = RAND.nextInt(maxVerificationCounter) + 1;
        compareRow(conn, tableName, noCompactTableName, "a", MAX_COLUMN_INDEX);
        long scn = injectEdge.currentTime() - (Math.min(maxLookbackAge, MAX_LOOKBACK_AGE) * 1000L);
        long scnEnd = injectEdge.currentTime();
        long scnStart = Math.max(scn, startTime);
        for (scn = scnEnd; scn >= scnStart; scn -= 1000) {
          // Compare all row versions using scn queries
          Properties props = new Properties();
          props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
          try (Connection scnConn = DriverManager.getConnection(url, props)) {
            compareRow(scnConn, tableName, noCompactTableName, "a", MAX_COLUMN_INDEX);
          }
        }
      }
    }
  }

  @Test
  public void testMinorCompactionShouldNotRetainCellsWhenMaxLookbackIsDisabled() throws Exception {
    if (tableLevelMaxLookback == null || tableLevelMaxLookback != 0) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      createTable(tableName);
      conn.createStatement().execute("Alter Table " + tableName
        + " set \"phoenix.max.lookback.age.seconds\" = " + tableLevelMaxLookback);
      final int flushCount = 10;
      byte[] row = Bytes.toBytes("a");
      int rowUpdateCounter = 0;
      try {
        for (int i = 0; i < flushCount; i++) {
          // Generate more row versions than the maximum cell versions for the table
          int updateCount = RAND.nextInt(10) + versions;
          rowUpdateCounter += updateCount;
          LOG.info(String.format("Iteration:%d uc:%d cntr:%d", i, updateCount, rowUpdateCounter));
          for (int j = 0; j < updateCount; j++) {
            updateRow(conn, tableName, "a");
            // Sometimes multiple updates to the same row are done in the same millisecond
            // This results in overwriting the previous version which breaks the test
            Thread.sleep(1);
          }
          flush(TableName.valueOf(tableName));
          // Flushes dump and retain all the cells to HFile.
          // Doing MAX_COLUMN_INDEX + 1 to account for empty cells
          assertEquals(TestUtil.getRawCellCount(conn, TableName.valueOf(tableName), row),
            rowUpdateCounter * (MAX_COLUMN_INDEX + 1));
        }
        // Run one minor compaction (in case no minor compaction has happened yet)
        TestUtil.minorCompact(utility, TableName.valueOf(tableName));
        assertEquals(
          TestUtil.getRawCellCount(conn, TableName.valueOf(tableName), Bytes.toBytes("a")),
          (MAX_COLUMN_INDEX + 1) * versions);
      } catch (AssertionError e) {
        TestUtil.dumpTable(conn, TableName.valueOf(tableName));
        throw e;
      }
    }
  }

  @Test
  public void testRowSpansMultipleTTLWindows() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      createTable(tableName);
      String noCompactTableName = generateUniqueName();
      createTable(noCompactTableName);
      long startTime = System.currentTimeMillis() + 1000;
      startTime = (startTime / 1000) * 1000;
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);
      for (int columnIndex = 1; columnIndex <= MAX_COLUMN_INDEX; columnIndex++) {
        String value = Integer.toString(RAND.nextInt(1000));
        updateColumn(conn, tableName, "a", columnIndex, value);
        updateColumn(conn, noCompactTableName, "a", columnIndex, value);
        conn.commit();
        injectEdge.incrementValue(ttl * 1000 - 1000);
      }
      flush(TableName.valueOf(tableName));
      majorCompact(TableName.valueOf(tableName));
      compareRow(conn, tableName, noCompactTableName, "a", MAX_COLUMN_INDEX);
      injectEdge.incrementValue(1000);
    }
  }

  @Test
  public void testRowSpansMultipleTTLWindowsWithCdc() throws Exception {
    final int maxLookbackAge =
      tableLevelMaxLookback != null ? tableLevelMaxLookback : MAX_LOOKBACK_AGE;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String schemaName = generateUniqueName();
      String tableName = schemaName + "." + generateUniqueName();
      String noCompactTableName = generateUniqueName();
      createTable(tableName);
      createTable(noCompactTableName);
      conn.createStatement().execute("ALTER TABLE " + tableName
        + " SET \"phoenix.max.lookback.age.seconds\" = " + maxLookbackAge);

      // Create CDC index for TTL verification
      String cdcName = generateUniqueName();
      String cdcSql = "CREATE CDC " + cdcName + " ON " + tableName + " INCLUDE (PRE, POST)";
      conn.createStatement().execute(cdcSql);
      conn.commit();

      String cdcFullName = SchemaUtil.getTableName(null, schemaName + "." + cdcName);

      ObjectMapper mapper = new ObjectMapper();
      long startTime = System.currentTimeMillis() + 1000;
      startTime = (startTime / 1000) * 1000;
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);

      // Track the last post-image from normal CDC events
      Map<String, Object> lastPostImage = null;

      for (int columnIndex = 1; columnIndex <= MAX_COLUMN_INDEX; columnIndex++) {
        String value = Integer.toString(RAND.nextInt(1000));
        updateColumn(conn, tableName, "a", columnIndex, value);
        updateColumn(conn, noCompactTableName, "a", columnIndex, value);
        conn.commit();

        // Capture the last post-image from CDC events
        String cdcQuery = "SELECT PHOENIX_ROW_TIMESTAMP(), \"CDC JSON\" FROM " + cdcFullName
          + " ORDER BY PHOENIX_ROW_TIMESTAMP() DESC LIMIT 1";
        try (ResultSet rs = conn.createStatement().executeQuery(cdcQuery)) {
          if (rs.next()) {
            Map<String, Object> cdcEvent = mapper.readValue(rs.getString(2), HashMap.class);
            if (cdcEvent.containsKey(CDC_POST_IMAGE)) {
              lastPostImage = (Map<String, Object>) cdcEvent.get(CDC_POST_IMAGE);
            }
          }
        }

        injectEdge.incrementValue(ttl * 1000 - 1000);
      }
      assertNotNull("Last post-image should not be null", lastPostImage);

      // Advance time past TTL to expire the row
      injectEdge.incrementValue((ttl + maxLookbackAge + 1) * 1000);

      flush(TableName.valueOf(tableName));
      majorCompact(TableName.valueOf(tableName));

      // Verify row is expired from data table
      String dataQuery = "SELECT * FROM " + tableName + " WHERE id = 'a'";
      try (ResultSet rs = conn.createStatement().executeQuery(dataQuery)) {
        assertFalse("Row should be expired from data table", rs.next());
      }

      // Verify TTL_DELETE CDC event was generated and compare pre-image
      String cdcQuery = "SELECT \"CDC JSON\" FROM " + cdcFullName
        + " ORDER BY PHOENIX_ROW_TIMESTAMP() DESC LIMIT 1";
      try (ResultSet rs = conn.createStatement().executeQuery(cdcQuery)) {
        assertTrue("Should find TTL delete event", rs.next());
        Map<String, Object> ttlDeleteEvent = mapper.readValue(rs.getString(1), HashMap.class);
        LOG.info("TTL delete event: {}", ttlDeleteEvent);

        assertEquals("Should be ttl_delete event", CDC_TTL_DELETE_EVENT_TYPE,
          ttlDeleteEvent.get(CDC_EVENT_TYPE));

        Map<String, Object> ttlPreImage = (Map<String, Object>) ttlDeleteEvent.get(CDC_PRE_IMAGE);
        assertNotNull("TTL pre-image should not be null", ttlPreImage);

        assertEquals("TTL delete pre-image should match last post-image from normal CDC events",
          lastPostImage, ttlPreImage);

        assertFalse("No more event should be found", rs.next());
      }

      compareRow(conn, tableName, noCompactTableName, "a", MAX_COLUMN_INDEX);
      injectEdge.incrementValue(1000);
    }
  }

  @Test
  public void testMultipleRowsWithUpdatesMoreThanTTLApart() throws Exception {
    // for the purpose of this test only considering cases when maxlookback is 0
    if (tableLevelMaxLookback == null || tableLevelMaxLookback != 0) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      createTable(tableName);
      conn.createStatement().execute("Alter Table " + tableName
        + " set \"phoenix.max.lookback.age.seconds\" = " + tableLevelMaxLookback);
      long startTime = System.currentTimeMillis() + 1000;
      startTime = (startTime / 1000) * 1000;
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);
      updateRow(conn, tableName, "a1");
      updateRow(conn, tableName, "a2");
      updateRow(conn, tableName, "a3");
      // advance the time to create a gap > TTL
      injectEdge.incrementValue((ttl + 1) * 1000);
      updateColumn(conn, tableName, "a1", 2, "col2");
      updateColumn(conn, tableName, "a2", 3, "col3");
      updateColumn(conn, tableName, "a3", 5, "col5");
      conn.commit();
      String dql = "SELECT * from " + tableName;
      ResultSet rs = conn.createStatement().executeQuery(dql);
      // check that all older columns are masked
      while (rs.next()) {
        String id = rs.getString(1);
        int updatedColIndex = 0;
        if (id.equals("a1")) {
          updatedColIndex = 2;
        } else if (id.equals("a2")) {
          updatedColIndex = 3;
        } else if (id.equals("a3")) {
          updatedColIndex = 5;
        } else {
          fail(String.format("Got unexpected row key %s", id));
        }
        for (int colIndex = 1; colIndex <= MAX_COLUMN_INDEX; ++colIndex) {
          if (colIndex != updatedColIndex) {
            assertNull(rs.getString(colIndex + 1));
          } else {
            assertNotNull(rs.getString(colIndex + 1));
          }
        }
      }
      flush(TableName.valueOf(tableName));
      majorCompact(TableName.valueOf(tableName));
      dql = "SELECT count(*) from " + tableName;
      rs = conn.createStatement().executeQuery(dql);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
    }
  }

  @Test
  public void testMaskingGapAnalysis() throws Exception {
    // for the purpose of this test only considering cases when maxlookback is 0
    if (tableLevelMaxLookback == null || tableLevelMaxLookback != 0) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      createTable(tableName);
      conn.createStatement().execute("Alter Table " + tableName
        + " set \"phoenix.max.lookback.age.seconds\" = " + tableLevelMaxLookback);
      long startTime = System.currentTimeMillis() + 1000;
      startTime = (startTime / 1000) * 1000;
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);
      updateRow(conn, tableName, "a1"); // min timestamp
      conn.commit();
      String dql = String.format("SELECT * from %s where id='%s'", tableName, "a1");
      String[] colValues = new String[MAX_COLUMN_INDEX + 1];
      try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
        assertTrue(rs.next());
        for (int i = 1; i <= MAX_COLUMN_INDEX; ++i) {
          // initialize the column values to the current row version
          colValues[i] = rs.getString("val" + i);
        }
      }
      for (int iter = 1; iter <= 20; ++iter) {
        injectEdge.incrementValue(1);
        int colIndex = RAND.nextInt(MAX_COLUMN_INDEX) + 1;
        String value = Integer.toString(RAND.nextInt(1000));
        updateColumn(conn, tableName, "a1", colIndex, value);
        conn.commit();
        colValues[colIndex] = value;

        injectEdge.incrementValue(ttl * 1000);
        colIndex = RAND.nextInt(MAX_COLUMN_INDEX) + 1;
        value = Integer.toString(RAND.nextInt(1000));
        updateColumn(conn, tableName, "a1", colIndex, value);
        conn.commit();
        colValues[colIndex] = value;

        if (iter % 5 == 0) {
          // every 5th iteration introduce a gap
          // first verify the current row
          try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
            assertTrue(rs.next());
            for (int i = 1; i <= MAX_COLUMN_INDEX; ++i) {
              Assert.assertEquals(colValues[i], rs.getString("val" + i));
            }
          }
          // inject the gap
          injectEdge.incrementValue(ttl * 1000 + RAND.nextInt(ttl * 1000));
          // row expires after the gap so reset the col values to null
          colValues = new String[MAX_COLUMN_INDEX + 1];
        }
      }
    }
  }

  @Test
  public void testMultipleUpdatesToSingleColumn() throws Exception {
    // for the purpose of this test only considering cases when maxlookback is 0
    if (tableLevelMaxLookback == null || tableLevelMaxLookback != 0) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      createTable(tableName);
      conn.createStatement().execute("Alter Table " + tableName
        + " set \"phoenix.max.lookback.age.seconds\" = " + tableLevelMaxLookback);
      long startTime = System.currentTimeMillis() + 1000;
      startTime = (startTime / 1000) * 1000;
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);
      updateRow(conn, tableName, "a1");
      injectEdge.incrementValue(1);
      for (int i = 0; i < 15; ++i) {
        updateColumn(conn, tableName, "a1", 2, "col2_" + i);
        conn.commit();
        injectEdge.incrementValue((ttl / 10) * 1000);
      }
      conn.commit();
      String dql = "select * from " + tableName + " where id='a1'";
      try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
        while (rs.next()) {
          for (int col = 1; col <= MAX_COLUMN_INDEX + 1; col++) {
            System.out.println(rs.getString(col));
          }
        }
      }
    }
  }

  @Test
  public void testDeleteFamilyVersion() throws Exception {
    // for the purpose of this test only considering cases when maxlookback is 0
    if (tableLevelMaxLookback == null || tableLevelMaxLookback != 0) {
      return;
    }
    if (multiCF == true) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = "T_" + generateUniqueName();
      createTable(tableName);
      conn.createStatement().execute("Alter Table " + tableName
        + " set \"phoenix.max.lookback.age.seconds\" = " + tableLevelMaxLookback);
      String indexName = "I_" + generateUniqueName();
      String indexDDL =
        String.format("create index %s on %s (val1) include (val2, val3) ", indexName, tableName);
      conn.createStatement().execute(indexDDL);
      updateRow(conn, tableName, "a1");
      String indexColumnValue;
      String expectedValue;
      String dql = "select val1, val2 from " + tableName + " where id = 'a1'";
      try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
        PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
        String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
        assertFalse(explainPlan.contains(indexName));
        assertTrue(rs.next());
        indexColumnValue = rs.getString(1);
        expectedValue = rs.getString(2);
        assertFalse(rs.next());
      }
      // Insert an orphan index row by failing data table update
      IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
      try {
        updateColumn(conn, tableName, "a1", 2, "col2_xyz");
        conn.commit();
        fail("An exception should have been thrown");
      } catch (Exception ignored) {
        // Ignore the exception
      } finally {
        IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
      }
      // Insert another orphan index row
      IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
      try {
        updateColumn(conn, tableName, "a1", 2, "col2_abc");
        conn.commit();
        fail("An exception should have been thrown");
      } catch (Exception ignored) {
        // Ignore the exception
      } finally {
        IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
      }
      TestUtil.dumpTable(conn, TableName.valueOf(indexName));
      // do a read on the index which should trigger a read repair
      dql = "select val2 from " + tableName + " where val1 = '" + indexColumnValue + "'";
      try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
        PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
        String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
        assertTrue(explainPlan.contains(indexName));
        assertTrue(rs.next());
        assertEquals(rs.getString(1), expectedValue);
        assertFalse(rs.next());
      }
      TestUtil.dumpTable(conn, TableName.valueOf(indexName));
      flush(TableName.valueOf(indexName));
      majorCompact(TableName.valueOf(indexName));
      TestUtil.dumpTable(conn, TableName.valueOf(indexName));
      // run the same query again after compaction
      try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
        PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
        String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
        assertTrue(explainPlan.contains(indexName));
        assertTrue(rs.next());
        assertEquals(rs.getString(1), expectedValue);
        assertFalse(rs.next());
      }
    }
  }

  /**
   * Test CDC events for TTL expired rows. This test creates a table with TTL and CDC index,
   * verifies insert/update CDC events with pre/post images, then triggers major compaction to
   * expire rows and verifies TTL_DELETE events with pre-image data.
   */
  @Test
  public void testCDCTTLExpiredRows() throws Exception {
    final int maxLookbackAge =
      tableLevelMaxLookback != null ? tableLevelMaxLookback : MAX_LOOKBACK_AGE;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String schemaName = generateUniqueName();
      String tableName = schemaName + "." + generateUniqueName();
      String cdcName = generateUniqueName();
      ObjectMapper mapper = new ObjectMapper();

      createTable(tableName);
      conn.createStatement().execute("ALTER TABLE " + tableName
        + " SET \"phoenix.max.lookback.age.seconds\" = " + maxLookbackAge);

      String cdcSql = "CREATE CDC " + cdcName + " ON " + tableName + " INCLUDE (PRE, POST)";
      conn.createStatement().execute(cdcSql);
      conn.commit();

      String cdcIndexName = schemaName + "." + CDCUtil.getCDCIndexName(schemaName + "." + cdcName);
      String cdcFullName = SchemaUtil.getTableName(null, schemaName + "." + cdcName);

      PTable cdcIndex = ((PhoenixConnection) conn).getTableNoCache(cdcIndexName);
      assertNotNull("CDC index should be created", cdcIndex);
      assertTrue("CDC index should be CDC type", CDCUtil.isCDCIndex(cdcIndex));

      // Setup time injection
      long startTime = System.currentTimeMillis() + 1000;
      startTime = (startTime / 1000) * 1000;
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);

      // Insert initial row
      updateRow(conn, tableName, "row1");
      long insertTime = injectEdge.currentTime();
      injectEdge.incrementValue(1000);

      // Update the row
      updateColumn(conn, tableName, "row1", 1, "updated_val1");
      updateColumn(conn, tableName, "row1", 2, "updated_val2");
      conn.commit();
      long updateTime = injectEdge.currentTime();
      injectEdge.incrementValue(1000);

      // Verify CDC events for insert and update
      String cdcQuery = "SELECT PHOENIX_ROW_TIMESTAMP(), \"CDC JSON\" FROM " + cdcFullName;
      Map<String, Object> postImage;
      try (ResultSet rs = conn.createStatement().executeQuery(cdcQuery)) {
        // First event - insert
        assertTrue("Should have insert CDC event", rs.next());
        long eventTimestamp = rs.getTimestamp(1).getTime();
        assertTrue("Insert event timestamp should be close to insert time",
          Math.abs(eventTimestamp - insertTime) < 2000);

        Map<String, Object> cdcEvent = mapper.readValue(rs.getString(2), HashMap.class);
        assertEquals("Should be upsert event", CDC_UPSERT_EVENT_TYPE, cdcEvent.get(CDC_EVENT_TYPE));
        assertTrue("Should have post-image", cdcEvent.containsKey(CDC_POST_IMAGE));

        postImage = (Map<String, Object>) cdcEvent.get(CDC_POST_IMAGE);
        assertFalse("post image must contain something", postImage.isEmpty());

        // Second event - update
        assertTrue("Should have update CDC event", rs.next());
        eventTimestamp = rs.getTimestamp(1).getTime();
        assertTrue("Update event timestamp should be close to update time",
          Math.abs(eventTimestamp - updateTime) < 2000);

        cdcEvent = mapper.readValue(rs.getString(2), HashMap.class);
        assertEquals("Should be upsert event", CDC_UPSERT_EVENT_TYPE, cdcEvent.get(CDC_EVENT_TYPE));
        assertTrue("Should have pre-image", cdcEvent.containsKey(CDC_PRE_IMAGE));
        assertTrue("Should have post-image", cdcEvent.containsKey(CDC_POST_IMAGE));

        Map<String, Object> preImage = (Map<String, Object>) cdcEvent.get(CDC_PRE_IMAGE);
        assertEquals("Comparison of last post-image with new pre-image", postImage, preImage);
        postImage = (Map<String, Object>) cdcEvent.get(CDC_POST_IMAGE);
        LOG.info("Post-image {}", postImage);
      }

      // Advance time past TTL to expire the row
      injectEdge.incrementValue((ttl + maxLookbackAge + 1) * 1000);

      TestUtil.dumpTable(conn, TableName.valueOf(tableName));
      TestUtil.dumpTable(conn, TableName.valueOf(cdcIndexName));
      flush(TableName.valueOf(tableName));
      majorCompact(TableName.valueOf(tableName));
      TestUtil.dumpTable(conn, TableName.valueOf(tableName));
      TestUtil.dumpTable(conn, TableName.valueOf(cdcIndexName));

      // Verify row is expired from data table
      String dataQuery = "SELECT * FROM " + tableName + " WHERE id = 'row1'";
      try (ResultSet rs = conn.createStatement().executeQuery(dataQuery)) {
        assertFalse("Row should be expired from data table", rs.next());
      }

      // Verify TTL_DELETE CDC event was generated
      try (ResultSet rs = conn.createStatement().executeQuery(cdcQuery)) {
        int eventCount = 0;
        Map<String, Object> ttlDeleteEvent = null;

        while (rs.next()) {
          eventCount++;
          Map<String, Object> cdcEvent = mapper.readValue(rs.getString(2), HashMap.class);
          String eventType = (String) cdcEvent.get(CDC_EVENT_TYPE);
          assertEquals(
            "Event type must be " + CDC_TTL_DELETE_EVENT_TYPE + " but found " + eventType,
            CDC_TTL_DELETE_EVENT_TYPE, eventType);
          if (CDC_TTL_DELETE_EVENT_TYPE.equals(eventType)) {
            ttlDeleteEvent = cdcEvent;
          }
        }

        assertEquals("Should have only 1 event for TTL_DELETE because other events are "
          + "expired due to major compaction", 1, eventCount);
        assertNotNull("Should have TTL delete event", ttlDeleteEvent);

        // Verify TTL delete event structure
        assertEquals("Should be ttl_delete event", CDC_TTL_DELETE_EVENT_TYPE,
          ttlDeleteEvent.get(CDC_EVENT_TYPE));
        assertTrue("TTL delete should have pre-image", ttlDeleteEvent.containsKey(CDC_PRE_IMAGE));

        Map<String, Object> preImage = (Map<String, Object>) ttlDeleteEvent.get(CDC_PRE_IMAGE);
        assertEquals("Comparison of last post-image with new pre-image", postImage, preImage);
        LOG.info("TTL delete event verified: {}", ttlDeleteEvent);
      }

      String cdcScanQuery =
        "SELECT \"CDC JSON\" FROM " + cdcFullName + " WHERE \"CDC JSON\" LIKE '%ttl_delete%'";
      try (ResultSet rs = conn.createStatement().executeQuery(cdcScanQuery)) {
        assertTrue("Should find TTL delete event via scan", rs.next());
        Map<String, Object> cdcEvent = mapper.readValue(rs.getString(1), HashMap.class);
        assertEquals("Should be ttl_delete event", CDC_TTL_DELETE_EVENT_TYPE,
          cdcEvent.get(CDC_EVENT_TYPE));
      }

      LOG.info("CDC TTL test completed successfully for table: {}", tableName);
    }
  }

  private void flush(TableName table) throws IOException {
    Admin admin = getUtility().getAdmin();
    admin.flush(table);
  }

  private void majorCompact(TableName table) throws Exception {
    TestUtil.majorCompact(getUtility(), table);
  }

  /**
   * Test CDC batch mutations for TTL expired rows. This test creates a table with TTL and CDC
   * index, inserts 82 rows (to test batching: 25+25+25+7), lets them expire via TTL, and verifies
   * that all 82 rows have CDC TTL_DELETE events recorded with correct pre-image data.
   */
  @Test
  public void testCDCBatchMutationsForTTLExpiredRows() throws Exception {
    final int maxLookbackAge =
      tableLevelMaxLookback != null ? tableLevelMaxLookback : MAX_LOOKBACK_AGE;
    final int numRows = 182;

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String tableName = generateUniqueName();
      String cdcName = generateUniqueName();
      ObjectMapper mapper = new ObjectMapper();

      createTable(tableName);
      conn.createStatement().execute("ALTER TABLE " + tableName
        + " SET \"phoenix.max.lookback.age.seconds\" = " + maxLookbackAge);

      String cdcSql = "CREATE CDC " + cdcName + " ON " + tableName + " INCLUDE (PRE, POST)";
      conn.createStatement().execute(cdcSql);
      conn.commit();

      String cdcFullName = SchemaUtil.getTableName(null, cdcName);

      long startTime = System.currentTimeMillis() + 1000;
      startTime = (startTime / 1000) * 1000;
      EnvironmentEdgeManager.injectEdge(injectEdge);
      injectEdge.setValue(startTime);

      // Track post-images for each row to verify against pre-images later
      Map<String, Map<String, Object>> lastPostImages = new HashMap<>();

      for (int i = 1; i <= numRows; i++) {
        String rowId = "row" + i;
        updateRow(conn, tableName, rowId);
        injectEdge.incrementValue(100);
      }

      // Get the post-images from the UPSERT events
      String cdcQuery = "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcFullName;
      try (ResultSet rs = conn.createStatement().executeQuery(cdcQuery)) {
        while (rs.next()) {
          Map<String, Object> cdcEvent = mapper.readValue(rs.getString(3), HashMap.class);
          assertEquals("Should be upsert event", CDC_UPSERT_EVENT_TYPE,
            cdcEvent.get(CDC_EVENT_TYPE));

          Map<String, Object> postImage = (Map<String, Object>) cdcEvent.get(CDC_POST_IMAGE);
          String rowId = rs.getString(2);
          lastPostImages.put(rowId, postImage);
        }
      }

      assertEquals("Should have captured post-images for all " + numRows + " rows", numRows,
        lastPostImages.size());

      // Advance time past TTL to expire all rows
      injectEdge.incrementValue((ttl + maxLookbackAge + 1) * 1000);

      EnvironmentEdgeManager.reset();
      flush(TableName.valueOf(tableName));
      EnvironmentEdgeManager.injectEdge(injectEdge);

      Timestamp ts = new Timestamp(injectEdge.currentTime());
      majorCompact(TableName.valueOf(tableName));

      // Verify all rows are expired from data table
      String dataQuery = "SELECT COUNT(*) FROM " + tableName;
      try (ResultSet rs = conn.createStatement().executeQuery(dataQuery)) {
        assertTrue("Should have count result", rs.next());
        assertEquals("All rows should be expired from data table", 0, rs.getInt(1));
      }

      // Verify all TTL_DELETE CDC events were generated
      String ttlDeleteQuery = "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " + cdcFullName
        + " WHERE PHOENIX_ROW_TIMESTAMP() >= ?";

      Map<String, Map<String, Object>> ttlDeletePreImages = new HashMap<>();
      int ttlDeleteEventCount = 0;

      try (PreparedStatement pst = conn.prepareStatement(ttlDeleteQuery)) {
        pst.setTimestamp(1, ts);

        try (ResultSet rs = pst.executeQuery(ttlDeleteQuery)) {
          while (rs.next()) {
            ttlDeleteEventCount++;
            Map<String, Object> cdcEvent = mapper.readValue(rs.getString(3), HashMap.class);

            assertEquals("Should be ttl_delete event", CDC_TTL_DELETE_EVENT_TYPE,
              cdcEvent.get(CDC_EVENT_TYPE));

            assertTrue("TTL delete should have pre-image", cdcEvent.containsKey(CDC_PRE_IMAGE));
            Map<String, Object> preImage = (Map<String, Object>) cdcEvent.get(CDC_PRE_IMAGE);
            assertNotNull("Pre-image should not be null", preImage);
            assertFalse("Pre-image should not be empty", preImage.isEmpty());

            String rowId = rs.getString(2);
            ttlDeletePreImages.put(rowId, preImage);
          }
        }
      }

      assertEquals("Should have exactly " + numRows + " TTL_DELETE events", numRows,
        ttlDeleteEventCount);
      assertEquals("Should have pre-images for all " + numRows + " rows", numRows,
        ttlDeletePreImages.size());

      // Verify that pre-images in TTL_DELETE events match the last post-images from UPSERT events
      for (String rowId : lastPostImages.keySet()) {
        assertTrue("Should have TTL_DELETE pre-image for row " + rowId,
          ttlDeletePreImages.containsKey(rowId));

        Map<String, Object> lastPostImage = lastPostImages.get(rowId);
        Map<String, Object> ttlDeletePreImage = ttlDeletePreImages.get(rowId);

        assertEquals(
          "Pre-image in TTL_DELETE should match last post-image from UPSERT for row " + rowId,
          lastPostImage, ttlDeletePreImage);
      }

    }
  }

  private void deleteRow(Connection conn, String tableName, String id) throws SQLException {
    String dml = "DELETE from " + tableName + " WHERE id = '" + id + "'";
    conn.createStatement().executeUpdate(dml);
    conn.commit();
  }

  private void updateColumn(Connection conn, String dataTableName, String id, int columnIndex,
    String value) throws SQLException {
    String upsertSql;
    if (value == null) {
      upsertSql = String.format("UPSERT INTO %s (id, %s) VALUES ('%s', null)", dataTableName,
        "val" + columnIndex, id);
    } else {
      upsertSql = String.format("UPSERT INTO %s (id, %s) VALUES ('%s', '%s')", dataTableName,
        "val" + columnIndex, id, value);
    }
    conn.createStatement().execute(upsertSql);
  }

  private void updateRow(Connection conn, String tableName1, String tableName2, String id)
    throws SQLException {

    int columnCount = RAND.nextInt(MAX_COLUMN_INDEX) + 1;
    for (int i = 0; i < columnCount; i++) {
      int columnIndex = RAND.nextInt(MAX_COLUMN_INDEX) + 1;
      String value = null;
      // Leave the value null once in a while
      if (RAND.nextInt(MAX_COLUMN_INDEX) > 0) {
        value = Integer.toString(RAND.nextInt(1000));
      }
      updateColumn(conn, tableName1, id, columnIndex, value);
      updateColumn(conn, tableName2, id, columnIndex, value);
    }
    conn.commit();
  }

  private void updateRow(Connection conn, String tableName, String id) throws SQLException {

    for (int i = 1; i <= MAX_COLUMN_INDEX; i++) {
      String value = Integer.toString(RAND.nextInt(1000));
      updateColumn(conn, tableName, id, i, value);
    }
    conn.commit();
  }

  private void compareRow(Connection conn, String tableName1, String tableName2, String id,
    int maxColumnIndex) throws SQLException, IOException {
    StringBuilder queryBuilder = new StringBuilder("SELECT ");
    for (int i = 1; i < maxColumnIndex; i++) {
      queryBuilder.append("val" + i + ", ");
    }
    queryBuilder.append("val" + maxColumnIndex + " FROM %s ");
    queryBuilder.append("where id='" + id + "'");
    ResultSet rs1 =
      conn.createStatement().executeQuery(String.format(queryBuilder.toString(), tableName1));
    ResultSet rs2 =
      conn.createStatement().executeQuery(String.format(queryBuilder.toString(), tableName2));

    boolean hasRow1 = rs1.next();
    boolean hasRow2 = rs2.next();
    Assert.assertEquals(hasRow1, hasRow2);
    if (hasRow1) {
      int i;
      for (i = 1; i <= maxColumnIndex; i++) {
        if (rs1.getString(i) != null) {
          if (!rs1.getString(i).equals(rs2.getString(i))) {
            LOG.debug("VAL" + i + " " + rs2.getString(i) + " : " + rs1.getString(i));
          }
        } else if (rs2.getString(i) != null) {
          LOG.debug("VAL" + i + " " + rs2.getString(i) + " : " + rs1.getString(i));
        }
        Assert.assertEquals("VAL" + i, rs2.getString(i), rs1.getString(i));
      }
    }
  }

  private void createTable(String tableName) throws SQLException {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      String createSql;
      if (multiCF) {
        createSql =
          "create table " + tableName + " (id varchar not null primary key, val1 varchar, "
            + "a.val2 varchar, a.val3 varchar, a.val4 varchar, "
            + "b.val5 varchar, a.val6 varchar, b.val7 varchar) " + tableDDLOptions;
      } else {
        createSql =
          "create table " + tableName + " (id varchar not null primary key, val1 varchar, "
            + "val2 varchar, val3 varchar, val4 varchar, "
            + "val5 varchar, val6 varchar, val7 varchar) " + tableDDLOptions;
      }
      LOG.debug(String.format("Creating table %s, %s", tableName, createSql));
      conn.createStatement().execute(createSql);
      conn.commit();
    }
  }
}
