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
package org.apache.phoenix.schema;

import static org.apache.phoenix.end2end.IndexToolIT.verifyIndexTable;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.REBUILT_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.SCANNED_DATA_ROW_COUNT;
import static org.apache.phoenix.schema.LiteralTTLExpression.TTL_EXPRESSION_FOREVER;
import static org.apache.phoenix.util.TestUtil.retainSingleQuotes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.OtherOptions;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.TestUtil.CellCount;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ConditionalTTLExpressionIT extends ParallelStatsDisabledIT {
    private static final Logger LOG = LoggerFactory.getLogger(ConditionalTTLExpressionIT.class);
    private static final Random RAND = new Random(11);
    private static final int MAX_ROWS = 1000;
    private static final String[] PK_COLUMNS = {"ID1", "ID2"};
    private static final String[] PK_COLUMN_TYPES = {"VARCHAR", "BIGINT"};
    private static final String[] COLUMNS = {
            "VAL1", "VAL2", "VAL3", "VAL4", "VAL5", "VAL6"
    };
    private static final String[] COLUMN_TYPES = {
            "CHAR(15)", "SMALLINT", "DATE", "TIMESTAMP", "BOOLEAN", "BSON"
    };
    private static final String[] DEFAULT_COLUMN_FAMILIES = new String [COLUMNS.length];

    static {
        assert PK_COLUMNS.length == PK_COLUMN_TYPES.length;
        assert COLUMNS.length == COLUMN_TYPES.length;
    }

    private ManualEnvironmentEdge injectEdge;
    private String tableDDLOptions;
    private final boolean columnEncoded;
    private final Integer tableLevelMaxLookback;
    private final boolean isStrictTTL;
    // column names -> fully qualified column names
    private SchemaBuilder schemaBuilder;
    // map of row-pos -> HBase row-key, used for verification
    private Map<Integer, String> dataRowPosToKey = Maps.newHashMap();
    private Map<Integer, String> indexRowPosToKey = Maps.newHashMap();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public ConditionalTTLExpressionIT(boolean columnEncoded,
                                      Integer tableLevelMaxLooback,
                                      boolean isStrictTTL) {
        this.columnEncoded = columnEncoded;
        this.tableLevelMaxLookback = tableLevelMaxLooback; // in ms
        this.isStrictTTL = isStrictTTL;
        schemaBuilder = new SchemaBuilder(getUrl());
    }

    @Parameterized.Parameters(
            name = "columnEncoded={0}, tableLevelMaxLookback={1}, isStrictTTL={2}")
    public static synchronized Collection<Object[]> data() {
        // maxlookback value is in sec
        return Arrays.asList(new Object[][]{
                {false, 0, false},
                {true, 15, false},
                {false, 0, true},
                {true, 0, true},
                {false, 15, true},
                {true, 15, true}
        });
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        // disabling global max lookback, will use table level max lookback
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(0));
        props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
                Long.toString(0));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest() {
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append(" TTL = '%s'"); // placeholder for TTL
        optionBuilder.append(", \"phoenix.max.lookback.age.seconds\" = " + tableLevelMaxLookback);
        if (columnEncoded) {
            optionBuilder.append(", COLUMN_ENCODED_BYTES=2");
        } else {
            optionBuilder.append(", COLUMN_ENCODED_BYTES=0");
        }
        if (!isStrictTTL) {
            optionBuilder.append(", IS_STRICT_TTL = false");
        } else {
            optionBuilder.append(", IS_STRICT_TTL = true");
        }
        this.tableDDLOptions = optionBuilder.toString();
        EnvironmentEdgeManager.reset();
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    }

    @After
    public synchronized void afterTest() {
        EnvironmentEdgeManager.reset();
    }

    @Test
    public void testBasicMaskingAndCompaction() throws Exception {
        String ttlCol = "VAL5";
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        List<String> indexedColumns = Lists.newArrayList("VAL1");
        List<String> includedColumns = Lists.newArrayList(ttlCol);
        String indexName = createIndex(indexedColumns, includedColumns, false);
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            // populate index row key map
            populateRowPosToRowKey(conn, true);
            ResultSet rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(ttlCol));
            // expire 1 row by setting to true
            updateColumn(conn, 3, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(isStrictTTL ? rowCount - 1 : rowCount, actual);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            Assert.assertEquals(isStrictTTL ? rowCount - 1 : rowCount, actual);

            // read the row again, this time it should be masked
            rs = readRow(conn, 3);
            assertNotEquals(isStrictTTL, rs.next());

            // expire 1 more row
            updateColumn(conn, 2, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(isStrictTTL ? rowCount - 2 : rowCount, actual);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            Assert.assertEquals(isStrictTTL ? rowCount - 2 : rowCount, actual);

            // refresh the row again, this update should behave like a new row
            updateColumn(conn, 3, ttlCol, false);
            rs = readRow(conn, 3);
            assertTrue(rs.next());
            for (String col : COLUMNS) {
                if (!col.equals(ttlCol)) {
                    if (isStrictTTL) {
                        assertNull(rs.getObject(col));
                    }
                } else {
                    assertFalse(rs.getBoolean(ttlCol));
                }
            }
            actual = TestUtil.getRowCount(conn, tableName, true);
            Assert.assertEquals(isStrictTTL ? rowCount - 1 : rowCount, actual);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            Assert.assertEquals(isStrictTTL ? rowCount - 1 : rowCount, actual);

            // expire the row again
            updateColumn(conn, 3, ttlCol, true);

            // increment by at least 2*maxlookback so that there are no updates within the
            // maxlookback window and no updates visible through the maxlookback window
            injectEdge.incrementValue(2 * tableLevelMaxLookback * 1000L + 5);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.insertRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
            }
            // remove the expired rows
            expectedCellCount.removeRow(dataRowPosToKey.get(2));
            expectedCellCount.removeRow(dataRowPosToKey.get(3));
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());
            doMajorCompaction(indexName);
            expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.insertRow(indexRowPosToKey.get(i), includedColumns.size() + 1);
            }
            // remove the expired rows
            expectedCellCount.removeRow(indexRowPosToKey.get(2));
            expectedCellCount.removeRow(indexRowPosToKey.get(3));
            validateTable(conn, indexName, expectedCellCount, indexRowPosToKey.values());
        }
    }

    @Test
    public void testEverythingRetainedWithinMaxLookBack() throws Exception {
        Assume.assumeTrue(tableLevelMaxLookback > 0 && isStrictTTL);
        String ttlCol = "VAL5";
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        long startTime = injectEdge.currentTime();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            ResultSet rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(ttlCol));

            // expire 1 row by setting to true
            updateColumn(conn, 3, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 1, actual);

            // read the row again, this time it should be masked
            rs = readRow(conn, 3);
            assertFalse(rs.next());

            // expire 1 more row
            updateColumn(conn, 2, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 2, actual);

            // refresh the row again
            updateColumn(conn, 3, ttlCol, false);
            rs = readRow(conn, 3);
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(ttlCol));
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 1, actual);

            // expire the row again
            updateColumn(conn, 3, ttlCol, true);

            // all the updates are within the maxlookback window
            injectEdge.setValue(startTime + tableLevelMaxLookback * 1000L);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.insertRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
            }
            // row position 2 updated 1 time 2 cells (column and empty column)
            expectedCellCount.addOrUpdateCells(dataRowPosToKey.get(2), 1*2);
            // row position 3 updated 3 times ( not expired -> expired -> not expired -> expired)
            // update on an expired row is a full row update
            expectedCellCount.addOrUpdateCells(dataRowPosToKey.get(3), 2*2 + COLUMNS.length + 1);
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());
        }
    }

    @Test
    public void testPartialRowRetainedInMaxLookBack() throws Exception {
        Assume.assumeTrue(tableLevelMaxLookback > 0 && isStrictTTL);
        String ttlCol = "VAL5";
        String ttlExpression = String.format("%s=TRUE", ttlCol, ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 1;
        long actual;
        injectEdge.currentTime();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            // expire 1 row by setting to true
            updateColumn(conn, 0, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount - 1, actual);
            // all previous updates of the expired row fall out of maxlookback window
            injectEdge.incrementValue(tableLevelMaxLookback * 1000L + 5);
            // update another column not part of ttl expression
            // This is an update on an expired row, all the other columns will be masked but the
            // row is no longer masked since the ttl column is now null
            updateColumn(conn, 0, "VAL2", 2345);
            // verify that the row is not masked
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);
            // only the last update should be visible in the maxlookback window
            injectEdge.incrementValue(1);
            doMajorCompaction(tableName);
            // the row should still be present because of maxlookback but not masked
            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.insertRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
            }
            // 2 cells for the updated + remaining DeleteColumn cells
            expectedCellCount.addOrUpdateCells(dataRowPosToKey.get(0), COLUMNS.length + 1);
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());
            // verify that the row is not masked
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);
            // no row versions in maxlookback
            injectEdge.incrementValue(tableLevelMaxLookback * 1000L + 5);
            doMajorCompaction(tableName);
            // beyond max lookback all the DeleteColumn cells should be purged
            expectedCellCount.insertRow(dataRowPosToKey.get(0), 2);
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws Exception {
        int ttl = 50 * 1000;
        // equivalent to a ttl of 50 sec
        String ttlExpression = String.format(
                "TO_NUMBER(CURRENT_TIME()) - TO_NUMBER(PHOENIX_ROW_TIMESTAMP()) >= %d", ttl);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);

            // bump the time so that the ttl expression evaluates to true
            injectEdge.incrementValue(ttl);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? 0 : 5, actual);

            // update VAL4 column of row 1
            // This is an update on an expired row so only 2 columns should be visible
            long currentTime = injectEdge.currentTime();
            updateColumn(conn, 1, "VAL4", currentTime);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? 1 : 5, actual);
            try (ResultSet rs = readRow(conn, 1)) {
                assertTrue(rs.next());
                for (String col : COLUMNS) {
                    if (!col.equals("VAL4")) {
                        if (isStrictTTL) {
                            assertNull(rs.getObject(col));
                        }
                    } else {
                        assertEquals(currentTime, rs.getTimestamp("VAL4").getTime());
                    }
                }
            }

            // advance the time by more than maxlookbackwindow
            injectEdge.incrementValue(tableLevelMaxLookback * 1000L + 2);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            expectedCellCount.insertRow(dataRowPosToKey.get(1),
                    isStrictTTL ? 2 : COLUMNS.length + 1);
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());

            // advance the time by more than TTL
            injectEdge.incrementValue(ttl + 2);
            doMajorCompaction(tableName);
            expectedCellCount = new CellCount();
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());
        }
    }

    /**
     * Tests CDC (Change Data Capture) functionality with TTL (Time To Live) expired rows.
     * This test validates the complete CDC lifecycle including:
     */
    @Test
    public void testPhoenixRowTimestampWithCdc() throws Exception {
        int ttl = 50 * 1000;
        String ttlExpression = String.format(
                "TO_NUMBER(CURRENT_TIME()) - TO_NUMBER(PHOENIX_ROW_TIMESTAMP()) >= %d", ttl);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        String cdcName = "cdc_" + generateUniqueName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Initial Setup - Create CDC index on the table
            conn.createStatement().execute("CREATE CDC " + cdcName + " ON " + tableName);
            populateTable(conn, rowCount);

            // Verify initial row count
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals("Table should contain all inserted rows", 5, actual);

            // Query initial CDC events (inserts)
            String cdcQuery1 = "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM "
                    + PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME + "." + cdcName;

            String cdcQuery2 = "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM "
                    + PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME + "." + cdcName
                    + " LIMIT 7";

            String cdcQuery3 = "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM "
                    + PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME + "." + cdcName
                    + " LIMIT 10 OFFSET 2";

            List<Map<String, Object>> postImageList1 = getPostImageList(conn, cdcQuery1, 5);
            List<Map<String, Object>> postImageList2 = getPostImageList(conn, cdcQuery2, 5);
            List<Map<String, Object>> postImageList3 = getPostImageList(conn, cdcQuery3, 3);

            // TTL Expiration - Advance time to trigger TTL expiration
            injectEdge.incrementValue(ttl);
            doMajorCompaction(tableName);

            // Verify all rows are expired from data table
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals("All rows should be expired after TTL", 0, actual);

            cdcQuery2 = "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM "
                    + PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME + "." + cdcName
                    + " LIMIT 11";
            // TTL CDC Events - Validate TTL_DELETE events are generated
            compareTtlPreImagesWithLastPostImages(conn, cdcQuery1, postImageList1, 5);
            compareTtlPreImagesWithLastPostImages(conn, cdcQuery2, postImageList2, 5);
            compareTtlPreImagesWithLastPostImages(conn, cdcQuery3, postImageList3, 3);

            // Update an expired row to bring it back
            injectEdge.incrementValue(1);
            long currentTime = injectEdge.currentTime();
            updateColumn(conn, 1, "VAL4", currentTime);

            // Verify the row
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals("Only one row should be resurrected after update", 1, actual);

            // Verify resurrected row has only updated column visible
            try (ResultSet rs = readRow(conn, 1)) {
                assertTrue("Resurrected row should exist", rs.next());
                for (String col : COLUMNS) {
                    if (!col.equals("VAL4")) {
                        assertNull("Non-updated columns should be null in resurrected row",
                                rs.getObject(col));
                    } else {
                        assertEquals("Updated column should have new timestamp",
                                currentTime, rs.getTimestamp("VAL4").getTime());
                    }
                }
            }

            // Advance time beyond max lookback window
            injectEdge.incrementValue(tableLevelMaxLookback * 1000L + 2);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            expectedCellCount.insertRow(dataRowPosToKey.get(1), 2);
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());

            // Query CDC events
            String cdcQuery = "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * FROM " +
                    PhoenixTestBuilder.DDLDefaults.DEFAULT_SCHEMA_NAME + "." + cdcName
                    + " WHERE PHOENIX_ROW_TIMESTAMP() >= ?";
            PreparedStatement ps = conn.prepareStatement(cdcQuery);
            ps.setTimestamp(1, new Timestamp(currentTime));
            ResultSet resultSet = ps.executeQuery();
            List<Map<String, Object>> postImageList = new ArrayList<>();
            while (resultSet.next()) {
                String cdcVal = resultSet.getString(4);
                Map<String, Object> map = OBJECT_MAPPER.readValue(cdcVal, Map.class);

                assertEquals("Resurrection event should be UPSERT type",
                        QueryConstants.CDC_UPSERT_EVENT_TYPE,
                        map.get(QueryConstants.CDC_EVENT_TYPE));

                Map<String, Object> preImage =
                        (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
                assertTrue("Resurrection event should have empty pre-image", preImage.isEmpty());

                Map<String, Object> postImage =
                        (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
                assertFalse("Resurrection event should have non-empty post-image",
                        postImage.isEmpty());
                postImageList.add(postImage);
            }
            assertEquals("Post image list size should be 5 but it is " + postImageList.size(), 1,
                    postImageList.size());

            // Trigger TTL expiration again
            injectEdge.incrementValue(ttl);
            doMajorCompaction(tableName);

            // Verify all rows are expired from data table
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals("All rows should be expired after TTL", 0, actual);

            expectedCellCount = new CellCount();
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());

            // Validate second round of TTL_DELETE events
            ps = conn.prepareStatement(cdcQuery);
            ps.setTimestamp(1, new Timestamp(currentTime));
            resultSet = ps.executeQuery();
            int i = 0;
            while (resultSet.next()) {
                String cdcVal = resultSet.getString(4);
                Map<String, Object> map = OBJECT_MAPPER.readValue(cdcVal, Map.class);

                assertEquals("Second TTL expiration should generate TTL_DELETE events",
                        QueryConstants.CDC_TTL_DELETE_EVENT_TYPE,
                        map.get(QueryConstants.CDC_EVENT_TYPE));

                Map<String, Object> preImage =
                        (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
                assertFalse("Second TTL_DELETE should have non-empty pre-image",
                        preImage.isEmpty());

                assertNull("TTL_DELETE events should have empty post-image",
                        map.get(QueryConstants.CDC_POST_IMAGE));

                assertEquals("Second TTL_DELETE pre-image should match resurrection post-image",
                        postImageList.get(i), preImage);
                i++;
            }
            assertEquals("Num of TTL_DELETE events verified should be 5 but it is " + i, 1, i);
        }
    }

    private static void compareTtlPreImagesWithLastPostImages(Connection conn, String cdcQuery,
                                                              List<Map<String, Object>> postImageList,
                                                              int expectedCount)
            throws SQLException, JsonProcessingException {
        ResultSet resultSet = conn.createStatement().executeQuery(cdcQuery);
        int i = 0;
        while (resultSet.next()) {
            String cdcVal = resultSet.getString(4);
            Map<String, Object> map = OBJECT_MAPPER.readValue(cdcVal, Map.class);

            // Validate TTL delete events
            assertEquals("TTL expired rows should generate TTL_DELETE events",
                    QueryConstants.CDC_TTL_DELETE_EVENT_TYPE,
                    map.get(QueryConstants.CDC_EVENT_TYPE));

            Map<String, Object> preImage =
                    (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
            assertFalse("TTL_DELETE events should have non-empty pre-image",
                    preImage.isEmpty());

            assertNull("TTL_DELETE events should have empty post-image",
                    map.get(QueryConstants.CDC_POST_IMAGE));

            // TTL delete pre-image should match previous upsert post-image
            assertEquals("TTL_DELETE pre-image should match original insert post-image",
                    postImageList.get(i), preImage);
            i++;
        }
        assertEquals(
                "Num of TTL_DELETE events verified should be " + expectedCount + " but it is " + i,
                expectedCount, i);
    }

    private static List<Map<String, Object>> getPostImageList(Connection conn, String cdcQuery,
                                                              int expectedCount)
            throws SQLException, JsonProcessingException {
        ResultSet resultSet = conn.createStatement().executeQuery(cdcQuery);
        List<Map<String, Object>> postImageList = new ArrayList<>();
        while (resultSet.next()) {
            String cdcVal = resultSet.getString(4);
            Map<String, Object> map = OBJECT_MAPPER.readValue(cdcVal, Map.class);

            // Validate insert events have no pre-image but have post-image
            Map<String, Object> preImage =
                    (Map<String, Object>) map.get(QueryConstants.CDC_PRE_IMAGE);
            assertTrue("Insert events should have empty pre-image", preImage.isEmpty());

            Map<String, Object> postImage =
                    (Map<String, Object>) map.get(QueryConstants.CDC_POST_IMAGE);
            assertFalse("Insert events should have non-empty post-image", postImage.isEmpty());
            postImageList.add(postImage);

            assertEquals("Initial events should be UPSERT type",
                    QueryConstants.CDC_UPSERT_EVENT_TYPE,
                    map.get(QueryConstants.CDC_EVENT_TYPE));
        }
        assertEquals("Post image list size should be " + expectedCount + " but it is "
                + postImageList.size(), expectedCount, postImageList.size());
        return postImageList;
    }

    @Test
    public void testDeleteMarkers() throws Exception {
        String ttlCol = "VAL5";
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            int [] rowsToDelete = new int[]{2, 3};
            for (int rowPosition : rowsToDelete) {
                deleteRow(conn, rowPosition);
            }
            // expire row # 1
            updateColumn(conn, 1, ttlCol, true);
            actual = TestUtil.getRowCount(conn, tableName, true);
            // 1 row expired, 2 deleted
            assertEquals(isStrictTTL ? 2 : 3, actual);
            if (tableLevelMaxLookback == 0) {
                // increment so that all updates are outside of max lookback
                injectEdge.incrementValue(2);
                doMajorCompaction(tableName);
                // only 2 rows should be retained
                CellCount expectedCellCount = new CellCount();
                expectedCellCount.insertRow(dataRowPosToKey.get(0), COLUMNS.length + 1);
                expectedCellCount.insertRow(dataRowPosToKey.get(4), COLUMNS.length + 1);
                validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());
            } else {
                // all updates within the max lookback window, retain everything
                doMajorCompaction(tableName);
                CellCount expectedCellCount = new CellCount();
                for (int i = 0; i < rowCount; ++i) {
                    // additional cell for empty column
                    expectedCellCount.insertRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
                }
                // update cell count for expired rows 1 for the column and 1 for empty column
                expectedCellCount.addOrUpdateCells(dataRowPosToKey.get(1), 2);
                for (int rowPosition : rowsToDelete) {
                    // one DeleteFamily cell
                    expectedCellCount.addOrUpdateCell(dataRowPosToKey.get(rowPosition));
                }
                validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());
                // increment so that the delete markers are outside of max lookback
                injectEdge.incrementValue(tableLevelMaxLookback * 1000L + 1);
                doMajorCompaction(tableName);
                for (int rowPosition : rowsToDelete) {
                    expectedCellCount.removeRow(dataRowPosToKey.get(rowPosition));
                }
                // purge the expired row also
                expectedCellCount.removeRow(dataRowPosToKey.get(1));
                validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());
            }
        }
    }

    @Test
    public void testDateExpression() throws Exception {
        // ttl = 'CURRENT_DATE() >= VAL3 + 1'  // 1 day beyond the value stored in VAL3
        String ttlCol = "VAL3";
        String ttlExpression = String.format("CURRENT_DATE() >= %s + 1", ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            // bump the time so that the ttl expression evaluates to true
            injectEdge.incrementValue(QueryConstants.MILLIS_IN_DAY + 1200);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? 0 : 5, actual);

            // update column of row 2
            // This is an update on an expired row so only the updated columns should be visible
            int newVal = -1;
            Date d = new Date(injectEdge.currentTime());
            List<String> updatedCols = Lists.newArrayList("VAL2", ttlCol);
            List<Object> newVals = Lists.newArrayList(newVal, d);
            updateColumns(conn, 2, updatedCols, newVals);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? 1 : 5, actual);
            try (ResultSet rs = readRow(conn, 2)) {
                assertTrue(rs.next());
                for (String col : COLUMNS) {
                    if (col.equals("VAL2")) {
                        assertEquals(newVal, rs.getInt("VAL2"));
                    } else if (col.equals(ttlCol)) {
                        assertEquals(d, rs.getDate(ttlCol));
                    } else {
                        if (isStrictTTL) {
                            assertNull(rs.getObject(col));
                        }
                    }
                }
            }

            // advance the time by more than maxlookbackwindow
            injectEdge.incrementValue(tableLevelMaxLookback * 1000L + 2);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            expectedCellCount.insertRow(dataRowPosToKey.get(2),
                    isStrictTTL ? updatedCols.size() + 1 : COLUMNS.length + 1);
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());

            // advance the time by more than TTL
            injectEdge.incrementValue(QueryConstants.MILLIS_IN_DAY + 1400);
            doMajorCompaction(tableName);
            expectedCellCount = new CellCount();
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());
        }
    }

    @Ignore("CURRENT_TIME() doesn't honour scn")
    public void testSCN() throws Exception {
        int ttl = 2000;
        // equivalent to a ttl of 2s
        String ttlExpression = String.format(
                "TO_NUMBER(CURRENT_TIME()) - TO_NUMBER(PHOENIX_ROW_TIMESTAMP()) >= %d", ttl);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual = 0;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
        }
        injectEdge.incrementValue(ttl + rowCount + 1);
        Properties props = new Properties();
        long scn = injectEdge.currentTime() - ttl;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(0, actual);
        }
    }

    @Test
    public void testRowWithExpressionEvalFailure() throws Exception {
        String ttlCol = "VAL2";
        String ttlExpression = String.format("%s > 5", ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        int rowCount = 1;
        injectEdge();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            // set the ttl column to null so that expression evaluates fail
            updateColumn(conn, 0, ttlCol, null);
            long actual = TestUtil.getRowCount(conn, tableName, true);
            // the row shouldn't be masked
            assertEquals(1, actual);
        }
    }

    @Test
    public void testIndexTool() throws Exception {
        String ttlCol = "VAL5";
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression);
        String fullDataTableName = schemaBuilder.getEntityTableName();
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullDataTableName);
        String tableName = SchemaUtil.getTableNameFromFullName(fullDataTableName);
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            deleteRow(conn, 2);
            // expire some rows
            updateColumn(conn, 0, ttlCol, true);
            updateColumn(conn, 4, ttlCol, true);
            // now create the index async
            String indexName = generateUniqueName();
            String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
            String indexDDL = String.format("create index %s on %s (%s) include (%s) async "
                            + "\"phoenix.max.lookback.age.seconds\" = %d",
                    indexName, fullDataTableName, "VAL1", ttlCol, tableLevelMaxLookback);
            conn.createStatement().execute(indexDDL);
            IndexTool it = IndexToolIT.runIndexTool(false, schemaName, tableName, indexName,
                    null, 0, IndexTool.IndexVerifyType.BEFORE);
            CounterGroup mrJobCounters = IndexToolIT.getMRJobCounters(it);
            try {
                assertEquals(isStrictTTL ? rowCount - 2 : rowCount, // only the expired rows are masked but not deleted rows
                        mrJobCounters.findCounter(SCANNED_DATA_ROW_COUNT.name()).getValue());
                assertEquals(isStrictTTL ? rowCount - 2 : rowCount,
                        mrJobCounters.findCounter(REBUILT_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(
                                BEFORE_REBUILD_VALID_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(
                                BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(
                                BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT.name()).getValue());
                String missingIndexRowCounter = tableLevelMaxLookback != 0 ?
                        BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT.name() :
                        BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT.name();
                assertEquals(isStrictTTL ? rowCount - 2 : rowCount,
                            mrJobCounters.findCounter(missingIndexRowCounter).getValue());
            } catch (AssertionError e) {
                IndexToolIT.dumpMRJobCounters(mrJobCounters);
                throw e;
            }
            populateRowPosToRowKey(conn, true);

            // Both the tables should have the same row count from Phoenix
            actual = TestUtil.getRowCount(conn, fullDataTableName, true);
            // 2 expired, 1 deleted
            assertEquals(isStrictTTL ? rowCount - (2 + 1) : rowCount - 1, actual);
            actual = TestUtil.getRowCountFromIndex(conn, fullDataTableName, fullIndexName);
            assertEquals(isStrictTTL ? rowCount - (2 + 1) : rowCount - 1, actual);

            injectEdge.incrementValue(2 * tableLevelMaxLookback * 1000L + 5);
            doMajorCompaction(fullDataTableName);
            doMajorCompaction(fullIndexName);

            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.insertRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
            }
            // remove the expired rows
            expectedCellCount.removeRow(dataRowPosToKey.get(0));
            expectedCellCount.removeRow(dataRowPosToKey.get(4));
            // remove the deleted row
            expectedCellCount.removeRow(dataRowPosToKey.get(2));
            validateTable(conn, fullDataTableName, expectedCellCount, dataRowPosToKey.values());

            expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // 1 cell for empty column and 1 for included column
                expectedCellCount.insertRow(indexRowPosToKey.get(i), 2);
            }
            // remove the expired rows
            expectedCellCount.removeRow(indexRowPosToKey.get(0));
            expectedCellCount.removeRow(indexRowPosToKey.get(4));
            // remove the deleted row
            expectedCellCount.removeRow(indexRowPosToKey.get(2));
            validateTable(conn, fullIndexName, expectedCellCount, indexRowPosToKey.values());

            // run index verification
            it = IndexToolIT.runIndexTool(false, schemaName, tableName, indexName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);
            mrJobCounters = IndexToolIT.getMRJobCounters(it);
            try {
                assertEquals(rowCount -(2+1), // deleted rows and expired rows should be purged
                        mrJobCounters.findCounter(SCANNED_DATA_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(REBUILT_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(rowCount - (2+1),
                        mrJobCounters.findCounter(
                                BEFORE_REBUILD_VALID_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(
                                BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(
                                BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(
                                BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT.name()).getValue());
            } catch (AssertionError e) {
                IndexToolIT.dumpMRJobCounters(mrJobCounters);
                throw e;
            }
        }
    }

    @Test
    public void testNullPKColumn() throws Exception {
        Assume.assumeTrue(tableLevelMaxLookback == 0);
        String tableName = "T_" + generateUniqueName();
        String indexName = "I_" + generateUniqueName();
        String ddlTemplate = "create table %s (id1 varchar, id2 varchar, col1 varchar, " +
                "col2 varchar constraint pk primary key(id1, id2)) %s";
        String ttlExpression = "id1='a'";
        tableDDLOptions += " ,IMMUTABLE_ROWS=true";
        if (!columnEncoded) {
            tableDDLOptions += " ,IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN'";
        } else {
            tableDDLOptions += " ,IMMUTABLE_STORAGE_SCHEME='SINGLE_CELL_ARRAY_WITH_OFFSETS'";
        }
        String ddl = String.format(ddlTemplate, tableName,
                String.format(tableDDLOptions, retainSingleQuotes(ttlExpression)));
        String indexDDL = String.format("create index %s ON %s (col1) INCLUDE(col2) " +
                "\"phoenix.max.lookback.age.seconds\" = %d",
                indexName, tableName, tableLevelMaxLookback, isStrictTTL);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(indexDDL);
            // populate rows
            conn.createStatement().execute("upsert into " + tableName +
                    " values('a', '0', 'col1', 'col2')");
            conn.createStatement().execute("upsert into " + tableName +
                    " values('a', '1', null, 'col2')");
            conn.createStatement().execute("upsert into " + tableName +
                    " values('b','0', 'col1', 'col2')");
            conn.createStatement().execute("upsert into " + tableName +
                    " values('b','1', null, 'col2')");
            conn.createStatement().execute("upsert into " + tableName +
                    " values(null, '0', 'col1', 'col2')");
            conn.commit();
            long actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? 3 : 5, actual);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            assertEquals(isStrictTTL ? 3 : 5, actual);

            // alter the ttl
            ttlExpression = "id1 is null";
            ddl = "alter table %s set TTL='%s'";
            conn.createStatement().execute(String.format(ddl, tableName, ttlExpression));
            conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? 4 : 5, actual);
            PTable table = PhoenixRuntime.getTableNoCache(conn, tableName);
            assertEquals(TTLExpressionFactory.create(ttlExpression), table.getTTLExpression());
            PTable index = PhoenixRuntime.getTableNoCache(conn, indexName);
            assertEquals(TTLExpressionFactory.create(ttlExpression), index.getTTLExpression());
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            assertEquals(isStrictTTL ? 4 : 5, actual);

            // alter the ttl
            ttlExpression = "col1='col1'";
            ddl = "alter table %s set TTL='%s'";
            conn.createStatement().execute(String.format(ddl, tableName,
                    retainSingleQuotes(ttlExpression)));
            conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? 2 : 5, actual);
            table = PhoenixRuntime.getTableNoCache(conn, tableName);
            assertEquals(TTLExpressionFactory.create(ttlExpression), table.getTTLExpression());
            index = PhoenixRuntime.getTableNoCache(conn, indexName);
            assertEquals(TTLExpressionFactory.create(ttlExpression), index.getTTLExpression());
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            assertEquals(isStrictTTL ? 2 : 5, actual);
        }
    }

    @Test
    public void testUnverifiedRows() throws Exception {
        Assume.assumeTrue(tableLevelMaxLookback == 0);
        String ttlCol = "VAL5";
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression);
        String fullDataTableName = schemaBuilder.getEntityTableName();
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullDataTableName);
        String tableName = SchemaUtil.getTableNameFromFullName(fullDataTableName);
        List<String> indexedColumns = Lists.newArrayList("VAL1");
        List<String> includedColumns = Lists.newArrayList(ttlCol, "VAL2");
        String fullIndexName = createIndex(indexedColumns, includedColumns, false);
        String indexName = SchemaUtil.getTableNameFromFullName(fullIndexName);
        injectEdge();
        int rowCount = 2;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            // populate index row key map
            populateRowPosToRowKey(conn, true);
            ResultSet rs = readRow(conn, 0);
            assertTrue(rs.next());
            String val1_0 = rs.getString("VAL1");
            rs = readRow(conn, 1);
            assertTrue(rs.next());
            String val1_1 = rs.getString("VAL1");
            deleteRow(conn, 0);
            int val2 = 4567;
            updateColumns(conn, 0,
                    Lists.newArrayList("VAL1", "VAL2", ttlCol),
                    Lists.newArrayList(val1_0, val2, false));
            deleteRow(conn, 1);
            updateColumns(conn, 1,
                   Lists.newArrayList("VAL1", "VAL2", ttlCol),
                   Lists.newArrayList(val1_1, val2, true)); // expired row
            // make the index row unverified
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            try {
                updateColumns(conn, 0,
                        Lists.newArrayList("VAL1", "VAL2", ttlCol),
                        Lists.newArrayList(val1_0, 2345, false));
                fail("An exception should have been thrown");
            } catch (Exception ignored) {
                // Ignore the exception
            } finally {
                IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            }
            // make the index row unverified
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            try {
                updateColumns(conn, 1,
                        Lists.newArrayList("VAL1", "VAL2", ttlCol),
                        Lists.newArrayList(val1_1, 2345, false));
                fail("An exception should have been thrown");
            } catch (Exception ignored) {
                // Ignore the exception
            } finally {
                IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            }
            injectEdge.incrementValue(10);
            TestUtil.dumpTable(conn, TableName.valueOf(fullIndexName));
            // Read the unverified rows to trigger read repair
            actual = TestUtil.getRowCountFromIndex(conn, fullDataTableName, fullIndexName);
            TestUtil.dumpTable(conn, TableName.valueOf(fullIndexName));
            assertEquals(isStrictTTL ? rowCount - 1 : rowCount, actual);
            // First read row 0 which is not expired
            String dql = String.format("select VAL2, VAL5 from %s where VAL1='%s' AND ID2=0",
                    fullDataTableName, val1_0);
            try (ResultSet rs1 = conn.createStatement().executeQuery(dql)) {
                PhoenixResultSet prs = rs1.unwrap(PhoenixResultSet.class);
                String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
                assertTrue(explainPlan.contains(fullIndexName));
                assertTrue(rs1.next());
                assertEquals(rs1.getInt("VAL2"), val2);
                assertFalse(rs1.getBoolean(ttlCol));
            }
            // row 1 is expired
            dql = String.format("select VAL2, VAL5 from %s where VAL1='%s' AND ID2=1",
                    fullDataTableName, val1_1);
            try (ResultSet rs1 = conn.createStatement().executeQuery(dql)) {
                PhoenixResultSet prs = rs1.unwrap(PhoenixResultSet.class);
                String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
                assertTrue(explainPlan.contains(fullIndexName));
                assertNotEquals(isStrictTTL, rs1.next());
            }
            // run the reverse index verification tool
            IndexTool it = IndexToolIT.runIndexTool(false, schemaName, tableName, indexName,
                    null, 0, IndexTool.IndexVerifyType.ONLY, "-fi");
            CounterGroup mrJobCounters = IndexToolIT.getMRJobCounters(it);
            try {
                // 1 row is expired for strict TTL
                assertEquals(isStrictTTL ? 1 : 2,
                        mrJobCounters.findCounter(SCANNED_DATA_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(REBUILT_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(isStrictTTL ? 1 : 2,
                        mrJobCounters.findCounter(
                                BEFORE_REBUILD_VALID_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(
                                BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(
                                BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());
                assertEquals(0,
                        mrJobCounters.findCounter(
                                BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
            } catch (AssertionError e) {
                IndexToolIT.dumpMRJobCounters(mrJobCounters);
                throw e;
            }
            doMajorCompaction(fullIndexName);
            TestUtil.dumpTable(conn, TableName.valueOf(fullIndexName));
            actual = TestUtil.getRowCountFromIndex(conn, fullDataTableName, fullIndexName);
            assertEquals(rowCount - 1, actual);
            CellCount expectedCellCount = new CellCount();
            expectedCellCount.insertRow(indexRowPosToKey.get(0), includedColumns.size() + 1);
            validateTable(conn, fullIndexName, expectedCellCount, indexRowPosToKey.values());
        }
    }

    @Test
    public void testLocalIndex() throws Exception {
        Assume.assumeTrue(isStrictTTL);
        String ttlCol = "VAL5";
        String ttlExpression = String.format("%s=TRUE", ttlCol);
        createTable(ttlExpression);
        String fullDataTableName = schemaBuilder.getEntityTableName();
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullDataTableName);
        String tableName = SchemaUtil.getTableNameFromFullName(fullDataTableName);
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            deleteRow(conn, 2);
            // expire some rows
            updateColumn(conn, 0, ttlCol, true);
            updateColumn(conn, 4, ttlCol, true);
            // now create the index async
            String indexName = generateUniqueName();
            String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
            // Not setting table level max lookback explicitly on index table as physical table is
            // same for data table and local index on data table
            String indexDDL = String.format("create local index %s on %s (%s) include (%s) async",
                    indexName, fullDataTableName, "VAL1", ttlCol);
            conn.createStatement().execute(indexDDL);
            IndexToolIT.runIndexTool(false, schemaName, tableName, indexName,
                    null, 0, IndexTool.IndexVerifyType.BEFORE);
            populateRowPosToRowKey(conn, true);

            // Both the tables should have the same row count from Phoenix
            actual = TestUtil.getRowCount(conn, fullDataTableName, true);
            assertEquals(rowCount -(2+1), actual); // 2 expired, 1 deleted
            actual = TestUtil.getRowCountFromIndex(conn, fullDataTableName, fullIndexName);
            assertEquals(rowCount -(2+1), actual);

            injectEdge.incrementValue(2 * tableLevelMaxLookback * 1000L + 5);
            // local index tables rows are in a separate column family store and will be compacted
            doMajorCompaction(fullDataTableName);

            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // additional cell for empty column
                expectedCellCount.insertRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
            }
            // remove the expired rows
            expectedCellCount.removeRow(dataRowPosToKey.get(0));
            expectedCellCount.removeRow(dataRowPosToKey.get(4));
            // remove the deleted row
            expectedCellCount.removeRow(dataRowPosToKey.get(2));
            // add the local index row keys
            expectedCellCount.insertRow(indexRowPosToKey.get(1), 2);
            expectedCellCount.insertRow(indexRowPosToKey.get(3), 2);

            List<String> rowKeys = Stream.concat(dataRowPosToKey.values().stream(),
                    indexRowPosToKey.values().stream()).collect(Collectors.toList());
            validateTable(conn, fullDataTableName, expectedCellCount, rowKeys);
        }
    }

    @Test
    public void testNulls() throws Exception {
        Assume.assumeTrue(tableLevelMaxLookback == 0);
        String ttlExpression = "VAL2 = -1 AND VAL6 IS NULL";
        createTable(ttlExpression);
        List<String> indexedColumns = Lists.newArrayList("VAL1");
        List<String> includedColumns = Lists.newArrayList("VAL2", "VAL6");
        String indexName = createIndex(indexedColumns, includedColumns, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 10;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);
            for (int i = 0; i < rowCount; ++i) {
                // all odd rows set VAL6 to null
                if (i % 2 != 0) {
                    updateColumn(conn, i, "VAL6", null);
                }
            }
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);
            for (int i = 0; i < rowCount; ++i) {
                // all odd rows set VAL2 to -1
                if (i % 2 != 0) {
                    updateColumn(conn, i, "VAL2", -1);
                }
            }
            // odd rows should be expired
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? rowCount / 2 : rowCount, actual);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            assertEquals(isStrictTTL ? rowCount / 2 : rowCount, actual);

            // partial update on a row which is expired is treated like a new row
            updateColumn(conn, 3, "VAL4", null);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? rowCount / 2 + 1 : rowCount, actual);

            // Delete an expired row
            deleteRow(conn, 5);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? rowCount / 2 + 1 : rowCount - 1, actual);

            injectEdge.incrementValue(2);
            doMajorCompaction(tableName);
            actual = TestUtil.getRowCount(conn, tableName, true);
            // Strict TTL : 5 rows masked and expired, 5 visible, 1 new row
            // Relaxed TTL: 5 rows expired, no new row
            assertEquals(isStrictTTL ? rowCount / 2 + 1 : rowCount / 2, actual);
            doMajorCompaction(indexName);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            assertEquals(isStrictTTL ? rowCount / 2 + 1 : rowCount / 2, actual);
        }
    }

    @Test
    public void testNulls2() throws Exception {
        Assume.assumeTrue(tableLevelMaxLookback == 0);
        String ttlExpression = "VAL2 IS NULL AND VAL4 IS NULL";
        createTable(ttlExpression);
        List<String> indexedColumns = Lists.newArrayList("VAL2"); // indexed column is null
        List<String> includedColumns = Lists.newArrayList("VAL4");
        String indexName = createIndex(indexedColumns, includedColumns, false);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 1;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);
            // expire the row
            updateColumns(conn, 0,
                    Lists.newArrayList("VAL2", "VAL4"), Lists.newArrayList(null, null));
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(isStrictTTL ? 0 : 1, actual);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            assertEquals(isStrictTTL ? 0 : 1, actual);
            // now do a partial update over the expired row,
            int newVal =123;
            updateColumn(conn, 0, "VAL2", newVal);
            try (ResultSet rs = readRow(conn, 0)) {
                assertTrue(rs.next());
                for (String col : COLUMNS) {
                    if (!col.equals("VAL2")) {
                        if (isStrictTTL) {
                            assertNull(rs.getObject(col));
                        }
                    } else {
                        assertEquals(newVal, rs.getInt("VAL2"));
                    }
                }
            }
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(1, actual);
            actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
            assertEquals(1, actual);
            verifyIndexTable(tableName, indexName, conn);
        }
    }

    @Test
    public void testImmutableTable() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String ttlExpression = "CURRENT_TIME() >= CREATED_TS + " +
                "CASE WHEN EVENT_TYPE = ''ERROR'' THEN 7 ELSE 1 END";
        tableDDLOptions += " ,IMMUTABLE_ROWS=true";
        if (!columnEncoded) {
            tableDDLOptions += " ,IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN'";
        } else {
            tableDDLOptions += " ,IMMUTABLE_STORAGE_SCHEME='SINGLE_CELL_ARRAY_WITH_OFFSETS'";
        }
        String ddl = String.format("CREATE TABLE %s (ID BIGINT NOT NULL PRIMARY KEY, " +
                        "EVENT_TYPE CHAR(15), CREATED_TS TIMESTAMP) %s", tableName,
                String.format(tableDDLOptions, ttlExpression));
        String indexDDL = String.format("CREATE INDEX %s ON %s (EVENT_TYPE) INCLUDE(CREATED_TS) "
                        + "\"phoenix.max.lookback.age.seconds\" = %d",
                indexName, tableName, tableLevelMaxLookback);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(indexDDL);
            conn.commit();
            injectEdge();
            int rowCount = 10;
            String dml = String.format("UPSERT INTO %s VALUES (?, ?, ?)", tableName);
            try (PreparedStatement ps = conn.prepareStatement(dml)) {
                for (int i = 0; i < rowCount; ++i) {
                    ps.setInt(1, i);
                    ps.setString(2, i % 2 == 0 ? "INFO" : "ERROR");
                    ps.setTimestamp(3, new Timestamp(injectEdge.currentTime()));
                    ps.executeUpdate();
                }
                conn.commit();
                long actual = TestUtil.getRowCount(conn, tableName, true);
                assertEquals(rowCount, actual);

                injectEdge.incrementValue(QueryConstants.MILLIS_IN_DAY);
                actual = TestUtil.getRowCount(conn, tableName, true);
                assertEquals(isStrictTTL ? rowCount/2 : rowCount, actual);
                actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
                assertEquals(isStrictTTL ? rowCount / 2 : rowCount, actual);

                injectEdge.incrementValue(QueryConstants.MILLIS_IN_DAY*6);
                actual = TestUtil.getRowCount(conn, tableName, true);
                assertEquals(isStrictTTL ? 0 : rowCount, actual);
                actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
                assertEquals(isStrictTTL ? 0 : rowCount, actual);

                injectEdge.incrementValue(1);
                doMajorCompaction(tableName);
                actual = TestUtil.getRowCount(conn, tableName, true);
                assertEquals(0, actual);

                doMajorCompaction(indexName);
                actual = TestUtil.getRowCountFromIndex(conn, tableName, indexName);
                assertEquals(0, actual);
            }
        }
    }

    @Test
    public void testConcurrentUpserts() throws Exception {
        Assume.assumeTrue(tableLevelMaxLookback == 0);
        final int nThreads = 10;
        final int batchSize = 100;
        final int nRows = 499;
        final int nIndexVals = 23;
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String ttlExpression = "v1 is null and v3 is null";
        String ddl = String.format("CREATE TABLE %s (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, " +
                "v1 INTEGER, v2 INTEGER, v3 INTEGER, v4 INTEGER " +
                "CONSTRAINT pk PRIMARY KEY (k1,k2)) COLUMN_ENCODED_BYTES=%d, TTL='%s'",
                tableName, columnEncoded ? 2 : 0, ttlExpression);
        String indexDDL = String.format("CREATE INDEX %s ON %s (v2) INCLUDE (v1, v3)",
                indexName, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(indexDDL);
        }

        final CountDownLatch doneSignal = new CountDownLatch(nThreads);
        Runnable[] runnables = new Runnable[nThreads];
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        for (int i = 0; i < nThreads; i++) {
            runnables[i] = () -> {
                try {
                    Connection conn = DriverManager.getConnection(getUrl());
                    String dml = "UPSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?, ?)";
                    try (PreparedStatement ps = conn.prepareStatement(dml)) {
                        for (int i1 = 0; i1 < 10000; i1++) {
                            int k1 = i1 % nRows;
                            int k2 = 0;
                            Integer v1 = RAND.nextBoolean() ? null : (RAND.nextInt() % nIndexVals);
                            Integer v2 = RAND.nextBoolean() ? null : RAND.nextInt();
                            Integer v3 = RAND.nextBoolean() ? null : RAND.nextInt();
                            Integer v4 = RAND.nextBoolean() ? null : RAND.nextInt();
                            ps.setInt(1, k1);
                            ps.setInt(2, k2);
                            ps.setObject(3, v1);
                            ps.setObject(4, v2);
                            ps.setObject(5, v3);
                            ps.setObject(6, v4);
                            ps.executeUpdate();
                            if ((i1 % batchSize) == 0) {
                                conn.commit();
                            }
                        }
                        conn.commit();
                    }
                } catch (SQLException e) {
                    LOG.warn("Exception during upsert : " + e);
                } finally {
                    doneSignal.countDown();
                }
            };
        }
        for (int i = 0; i < nThreads; i++) {
            Thread t = new Thread(runnables[i]);
            t.start();
        }
        assertTrue("Ran out of time", doneSignal.await(120, TimeUnit.SECONDS));
        LOG.info("Total upsert time in ms : "
                + (EnvironmentEdgeManager.currentTimeMillis() - startTime));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            verifyIndexTable(tableName, indexName, conn);
        }
    }

    @Test
    public void testBsonDataType() throws Exception {
        String ttlCol = "VAL6";
        String ttlExpression = String.format(
                "BSON_VALUE(%s, ''attr_0'', ''VARCHAR'') IS NULL", ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(conn, rowCount);
            actual = TestUtil.getRowCount(conn, tableName, true);
            // only odd rows (1,3) have non null attribute value
            assertEquals(isStrictTTL ? 2 : 5, actual);

            // increment by at least 2*maxlookback so that there are no updates within the
            // maxlookback window and no updates visible through the maxlookback window
            injectEdge.incrementValue(2 * tableLevelMaxLookback * 1000L + 5);
            doMajorCompaction(tableName);
            CellCount expectedCellCount = new CellCount();
            for (int i = 0; i < rowCount; ++i) {
                // only odd rows should be retained
                if (i % 2 != 0) {
                    // additional cell for empty column
                    expectedCellCount.insertRow(dataRowPosToKey.get(i), COLUMNS.length + 1);
                }
            }
            validateTable(conn, tableName, expectedCellCount, dataRowPosToKey.values());
        }
    }

    @Test
    public void testCDCIndex() throws Exception {
        String ttlCol = "VAL2";
        // VAL2 = -1
        String ttlExpression = String.format("%s = -1", ttlCol);
        createTable(ttlExpression);
        String tableName = schemaBuilder.getEntityTableName();
        injectEdge();
        int rowCount = 5;
        long actual;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String cdcName = generateUniqueName();
            String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
            conn.createStatement().execute(cdc_sql);
            populateTable(conn, rowCount);
            String schemaName = SchemaUtil.getSchemaNameFromFullName(tableName);
            String cdcIndexName = CDCUtil.getCDCIndexName(cdcName);
            String fullCdcIndexName = SchemaUtil.getTableName(schemaName,
                    CDCUtil.getCDCIndexName(cdcName));
            // Explicitly set table level max lookback on CDC index
            String cdcIndexSetMaxLookbackDdl = String.format("ALTER INDEX %s ON %s ACTIVE SET "
                    + "\"phoenix.max.lookback.age.seconds\" = %d",
                    cdcIndexName, tableName, tableLevelMaxLookback);
            conn.createStatement().execute(cdcIndexSetMaxLookbackDdl);
            PTable cdcIndex = ((PhoenixConnection) conn).getTableNoCache(fullCdcIndexName);
            assertEquals(cdcIndex.getTTLExpression(), TTL_EXPRESSION_FOREVER);

            // get row count on base table no row should be masked
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);

            // get raw row count on cdc index table
            actual = TestUtil.getRawRowCount(conn, TableName.valueOf(fullCdcIndexName));
            assertEquals(rowCount, actual);

            // Advance time by the max lookback age. This will cause all rows in cdc index to expire
            injectEdge.incrementValue(tableLevelMaxLookback * 1000L + 2);

            // Major compact the CDC index. This will remove all expired rows
            TestUtil.doMajorCompaction(conn, fullCdcIndexName);
            // get raw row count on cdc index table
            actual = TestUtil.getRawRowCount(conn, TableName.valueOf(fullCdcIndexName));
            assertEquals(0, actual);

            // table should still have all the rows intact
            actual = TestUtil.getRowCount(conn, tableName, true);
            assertEquals(rowCount, actual);

            String alterDDL = String.format("alter table %s set TTL='%s = %d'", tableName, ttlCol, 0);
            conn.createStatement().execute(alterDDL);
            cdcIndex = ((PhoenixConnection) conn).getTableNoCache(fullCdcIndexName);
            assertEquals(cdcIndex.getTTLExpression(), TTL_EXPRESSION_FOREVER);
        }
    }

    private void validateTable(Connection conn,
                               String tableName,
                               CellCount expectedCellCount,
                               Collection<String> rowKeys) throws Exception {

        CellCount actualCellCount = TestUtil.getRawCellCount(conn, TableName.valueOf(tableName));
        try {
            assertEquals(
                    "Expected cellCount: " + expectedCellCount + " , actual: " + actualCellCount,
                    expectedCellCount, actualCellCount);
        } catch (AssertionError e) {
            try {
                TestUtil.dumpTable(conn, TableName.valueOf(tableName));
                for (String rowKey : rowKeys) {
                    LOG.info(String.format("Key=%s expected=%d, actual=%d",
                            Bytes.toStringBinary(rowKey.getBytes()),
                            expectedCellCount.getCellCount(rowKey),
                            actualCellCount.getCellCount(rowKey)));
                }
            } finally {
                throw e;
            }
        }
    }

    private void doMajorCompaction(String tableName) throws IOException, InterruptedException {
        TestUtil.flush(getUtility(), TableName.valueOf(tableName));
        TestUtil.majorCompact(getUtility(), TableName.valueOf(tableName));
    }

    private void createTable(String ttlExpression) throws Exception {
        TableOptions tableOptions = new TableOptions();
        tableOptions.setTablePKColumns(Arrays.asList(PK_COLUMNS));
        tableOptions.setTablePKColumnTypes(Arrays.asList(PK_COLUMN_TYPES));
        tableOptions.setTableColumns(Arrays.asList(COLUMNS));
        tableOptions.setTableColumnTypes(Arrays.asList(COLUMN_TYPES));
        tableOptions.setTableProps(String.format(tableDDLOptions, ttlExpression));
        tableOptions.setMultiTenant(false);
        OtherOptions otherOptions = new OtherOptions();
        otherOptions.setTableCFs(Arrays.asList(DEFAULT_COLUMN_FAMILIES));
        schemaBuilder.withTableOptions(tableOptions).withOtherOptions(otherOptions).build();
    }

    private String createIndex(List<String> indexedColumns,
                               List<String> includedColumns,
                               boolean isAsync) throws SQLException {
        String indexName = "I_" + generateUniqueName();
        String tableName = schemaBuilder.getEntityTableName();
        String schema = SchemaUtil.getSchemaNameFromFullName(tableName);
        String indexDDL = String.format("create index %s on %s (%s) include (%s) "
                        + "\"phoenix.max.lookback.age.seconds\" = %d",
                indexName, tableName,
                Joiner.on(",").join(indexedColumns),
                Joiner.on(",").join(includedColumns), tableLevelMaxLookback);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(indexDDL);
        }
        return SchemaUtil.getTableName(schema, indexName);
    }

    private void injectEdge() {
        long startTime = System.currentTimeMillis() + 1000;
        startTime = (startTime / 1000) * 1000;
        injectEdge.setValue(startTime);
        EnvironmentEdgeManager.injectEdge(injectEdge);
    }

    private List<Object> generatePKColumnValues(int rowPosition) {
        final String ID1_FORMAT = "id1_%d";
        String id1 = String.format(ID1_FORMAT, rowPosition / 2);
        int id2 = rowPosition;
        return Lists.newArrayList(id1, id2);
    }

    private BsonDocument generateBsonDocument(int rowPosition) {
        BsonDocument bsonDocument = new BsonDocument();
        if (rowPosition % 2 != 0) {
            bsonDocument.put("attr_0", new BsonString("str_val_" + rowPosition));
        }
        bsonDocument.put("attr_1", new BsonInt32(rowPosition * rowPosition));
        bsonDocument.put("attr_2", new BsonBoolean(rowPosition % 2 == 0));
        return bsonDocument;
    }

    private List<Object> generateRow(int rowPosition) {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        List<Object> pkCols = generatePKColumnValues(rowPosition);
        String val1 = "val1_" + RAND.nextInt(MAX_ROWS);
        int val2 = RAND.nextInt(MAX_ROWS);
        Date val3 = new Date(startTime + RAND.nextInt(MAX_ROWS));
        Timestamp val4 = new Timestamp(val3.getTime());
        boolean val5 = false;
        BsonDocument val6 = generateBsonDocument(rowPosition);
        List<Object> cols = Lists.newArrayList(val1, val2, val3, val4, val5, val6);
        List<Object> values = Lists.newArrayListWithExpectedSize(pkCols.size() + cols.size());
        values.addAll(pkCols);
        values.addAll(cols);
        return values;
    }

    private void updateColumn(Connection conn,
                              int rowPosition,
                              String columnName,
                              Object newColumnValue) throws Exception {
        updateColumns(
                conn,
                rowPosition,
                Lists.newArrayList(columnName),
                Lists.newArrayList(newColumnValue)
        );
    }

    private void updateColumns(Connection conn,
                              int rowPosition,
                              List<String> columnNames,
                              List<Object> newColumnValues) throws Exception {
        assert columnNames.size() == newColumnValues.size();
        String tableName = schemaBuilder.getEntityTableName();
        List<String> upsertColumns = Lists.newArrayList();
        upsertColumns.addAll(Arrays.asList(PK_COLUMNS));
        upsertColumns.addAll(columnNames);
        StringBuilder buf = new StringBuilder("UPSERT INTO ");
        buf.append(tableName);
        buf.append(" (").append(Joiner.on(",").join(upsertColumns)).append(") VALUES(");
        for (int i = 0; i < upsertColumns.size(); i++) {
            buf.append("?,");
        }
        buf.setCharAt(buf.length() - 1, ')');
        List<Object> upsertValues = Lists.newArrayList();
        upsertValues.addAll(generatePKColumnValues(rowPosition));
        upsertValues.addAll(newColumnValues);
        try (PreparedStatement stmt = conn.prepareStatement(buf.toString())) {
            for (int i = 0; i < upsertValues.size(); i++) {
                stmt.setObject(i + 1, upsertValues.get(i));
            }
            stmt.executeUpdate();
            conn.commit();
        }
        injectEdge.incrementValue(1);
    }

    private void updateRow(Connection conn, int rowPosition) throws Exception {
        String tableName = schemaBuilder.getEntityTableName();
        List<Object> upsertValues = generateRow(rowPosition);
        StringBuilder buf = new StringBuilder("UPSERT INTO ");
        buf.append(tableName);
        buf.append(" VALUES(");
        for (int i = 0; i < upsertValues.size(); i++) {
            buf.append("?,");
        }
        buf.setCharAt(buf.length() - 1, ')');
        try (PreparedStatement stmt = conn.prepareStatement(buf.toString())) {
            for (int i = 0; i < upsertValues.size(); i++) {
                stmt.setObject(i + 1, upsertValues.get(i));
            }
            stmt.executeUpdate();
            conn.commit();
        }
        injectEdge.incrementValue(1);
    }

    private void deleteRow(Connection conn, int rowPosition) throws SQLException {
        String tableName = schemaBuilder.getEntityTableName();
        String dml = String.format("delete from %s where ID1 = ? and ID2 = ?", tableName);
        try (PreparedStatement ps = conn.prepareStatement(dml)) {
            List<Object> pkCols = generatePKColumnValues(rowPosition);
            for (int i = 0; i < pkCols.size(); ++i) {
                ps.setObject(i + 1, pkCols.get(i));
            }
            ps.executeUpdate();
            conn.commit();
        }
        injectEdge.incrementValue(1);
    }

    private void populateTable(Connection conn, int rowCount) throws Exception {
        for (int i = 0; i < rowCount; ++i) {
            updateRow(conn, i);
        }
        // used for verification purposes
        populateRowPosToRowKey(conn, false);
    }

    /**
     * TODO
     * @param conn
     * @throws SQLException
     */
    private void populateRowPosToRowKey(Connection conn, boolean useIndex) throws SQLException {
        String tableName = schemaBuilder.getEntityTableName();
        String query = String.format("SELECT %s ID2, ROWKEY_BYTES_STRING() FROM %s",
                (useIndex ? "" : "/*+ NO_INDEX */"), tableName);
        Map<Integer, String> rowPosToKey = useIndex ? indexRowPosToKey : dataRowPosToKey;
        try (ResultSet rs = conn.createStatement().executeQuery(query)) {
            while (rs.next()) {
                int rowPos = rs.getInt(1);
                String rowKey = rs.getString(2); // StringBinary format
                rowPosToKey.put(rowPos, Bytes.toString(Bytes.toBytesBinary(rowKey)));
            }
        }
    }

    private ResultSet readRow(Connection conn, int rowPosition) throws SQLException {
        String tableName = schemaBuilder.getEntityTableName();
        String query = String.format("select * FROM %s where ID1 = ? AND ID2 = ?", tableName);
        List<Object> pkCols = generatePKColumnValues(rowPosition);
        PreparedStatement ps = conn.prepareStatement(query);
        for (int i = 0; i < pkCols.size(); ++i) {
            ps.setObject(i + 1, pkCols.get(i));
        }
        return ps.executeQuery();
    }

}
