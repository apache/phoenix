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
package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class TableTTLIT extends BaseTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(TableTTLIT.class);
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

    public TableTTLIT(boolean multiCF, boolean columnEncoded,
            KeepDeletedCells keepDeletedCells, int versions, int ttl) {
        this.multiCF = multiCF;
        this.columnEncoded = columnEncoded;
        this.keepDeletedCells = keepDeletedCells;
        this.versions = versions;
        this.ttl = ttl;
    }
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(MAX_LOOKBACK_AGE));
        props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest(){
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
            + "keepDeletedCells={2}, versions={3}, ttl={4}")
    public static synchronized Collection<Object[]> data() {
            return Arrays.asList(new Object[][] {
                    { false, false, KeepDeletedCells.FALSE, 1, 100},
                    { false, false, KeepDeletedCells.TRUE, 5, 50},
                    { false, false, KeepDeletedCells.TTL, 1, 25},
                    { true, false, KeepDeletedCells.FALSE, 5, 50},
                    { true, false, KeepDeletedCells.TRUE, 1, 25},
                    { true, false, KeepDeletedCells.TTL, 5, 100} });
    }

    /**
     * This test creates two tables with the same schema. The same row is updated in a loop on
     * both tables with the same content. Each update changes one or more columns chosen
     * randomly with randomly generated values.
     *
     * After every upsert, all versions of the rows are retrieved from each table and compared.
     * The test also occasionally deletes the row from both tables and but compacts only the
     * first table during this test.
     *
     * Both tables are subject to masking during queries.
     *
     * This test expects that both tables return the same row content for the same row version.
     *
     * @throws Exception
     */

    @Test
    public void testMaskingAndCompaction() throws Exception {
        final int maxDeleteCounter = MAX_LOOKBACK_AGE;
        final int maxCompactionCounter = ttl / 2;
        final int maxFlushCounter = ttl;
        final int maxMaskingCounter = 2 * ttl;
        final int maxVerificationCounter = 2 * ttl;
        final byte[] rowKey = Bytes.toBytes("a");
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            createTable(tableName);
            String noCompactTableName = generateUniqueName();
            createTable(noCompactTableName);
            conn.createStatement().execute("Alter Table " + noCompactTableName
                    + " set \"phoenix.table.ttl.enabled\" = false");
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
                    injectEdge.incrementValue((ttl + MAX_LOOKBACK_AGE + 1) * 1000);
                    LOG.debug("Masking " + i + " current time: " + injectEdge.currentTime());
                    ResultSet rs = conn.createStatement().executeQuery(
                            "SELECT count(*) FROM " + tableName);
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(rs.getLong(1), 0);
                    rs = conn.createStatement().executeQuery(
                            "SELECT count(*) FROM " + noCompactTableName);
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
                if (verificationCounter-- >  0) {
                    continue;
                }
                verificationCounter = RAND.nextInt(maxVerificationCounter) + 1;
                compareRow(conn, tableName, noCompactTableName, "a", MAX_COLUMN_INDEX);
                long scn = injectEdge.currentTime() - MAX_LOOKBACK_AGE * 1000;
                long scnEnd = injectEdge.currentTime();
                long scnStart = Math.max(scn, startTime);
                for (scn = scnEnd; scn >= scnStart; scn -= 1000) {
                    // Compare all row versions using scn queries
                    Properties props = new Properties();
                    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
                    try (Connection scnConn = DriverManager.getConnection(url, props)) {
                        compareRow(scnConn, tableName, noCompactTableName, "a",
                                MAX_COLUMN_INDEX);
                    }
                }
            }
        }
    }

    @Test
    public void testMinorCompactionShouldNotRetainCellsWhenMaxLookbackIsDisabled()
            throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            createTable(tableName);
            conn.createStatement().execute("Alter Table " + tableName + " set \"phoenix.max.lookback.age.seconds\" = 0");
            conn.commit();
            final int flushCount = 10;
            byte[] row = Bytes.toBytes("a");
            int rowUpdateCounter = 0;
            injectEdge.setValue(System.currentTimeMillis());
            EnvironmentEdgeManager.injectEdge(injectEdge);
            injectEdge.incrementValue(1);
            for (int i = 0; i < flushCount; i++) {
                // Generate more row versions than the maximum cell versions for the table
                int updateCount = RAND.nextInt(10) + versions;
                rowUpdateCounter += updateCount;
                for (int j = 0; j < updateCount; j++) {
                    updateRow(conn, tableName, "a");
                    injectEdge.incrementValue(1);
                }
                flush(TableName.valueOf(tableName));
                injectEdge.incrementValue(1);
                // Flushes dump and retain all the cells to HFile.
                // Doing MAX_COLUMN_INDEX + 1 to account for empty cells
                assertEquals(TestUtil.getRawCellCount(conn, TableName.valueOf(tableName), row),
                        rowUpdateCounter * (MAX_COLUMN_INDEX + 1));
            }
            TestUtil.dumpTable(conn, TableName.valueOf(tableName));
            // Run one minor compaction (in case no minor compaction has happened yet)
            TestUtil.minorCompact(utility, TableName.valueOf(tableName));
            injectEdge.incrementValue(1);
            TestUtil.dumpTable(conn, TableName.valueOf(tableName));
            // For multi-CF table we retain all empty cells during minor compaction
            int retainedCellCount = (MAX_COLUMN_INDEX + 1) * versions + (multiCF ? rowUpdateCounter - versions : 0);
            assertEquals(retainedCellCount, TestUtil.getRawCellCount(conn, TableName.valueOf(tableName), Bytes.toBytes("a")));
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
    private void flush(TableName table) throws IOException {
        Admin admin = getUtility().getAdmin();
        admin.flush(table);
    }

    private void majorCompact(TableName table) throws Exception {
        TestUtil.majorCompact(getUtility(), table);
    }

    private void deleteRow(Connection conn, String tableName, String id) throws SQLException{
        String dml = "DELETE from " + tableName + " WHERE id = '" + id + "'";
        conn.createStatement().executeUpdate(dml);
        conn.commit();
    }
    private void updateColumn(Connection conn, String dataTableName, String id,
            int columnIndex, String value)
            throws SQLException {
        String upsertSql;
        if (value == null) {
            upsertSql = String.format("UPSERT INTO %s (id, %s) VALUES ('%s', null)",
                    dataTableName, "val" + columnIndex, id);
        } else {
            upsertSql = String.format("UPSERT INTO %s (id, %s) VALUES ('%s', '%s')",
                    dataTableName, "val" + columnIndex, id, value);
        }
        conn.createStatement().execute(upsertSql);
    }

    private void updateRow(Connection conn, String tableName1, String tableName2, String id)
            throws SQLException {

        int columnCount =  RAND.nextInt(MAX_COLUMN_INDEX) + 1;
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

    private void updateRow(Connection conn, String tableName, String id)
            throws SQLException {

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
        ResultSet rs1 = conn.createStatement().executeQuery(
                String.format(queryBuilder.toString(), tableName1));
        ResultSet rs2 = conn.createStatement().executeQuery(
                String.format(queryBuilder.toString(), tableName2));

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
                Assert.assertEquals("VAL" + i,  rs2.getString(i), rs1.getString(i));
            }
        }
    }

    private void createTable(String tableName) throws SQLException {
        try(Connection conn = DriverManager.getConnection(getUrl())) {
            String createSql;
            if (multiCF) {
                createSql = "create table " + tableName +
                        " (id varchar not null primary key, val1 varchar, " +
                        "a.val2 varchar, a.val3 varchar, a.val4 varchar, " +
                        "b.val5 varchar, a.val6 varchar, b.val7 varchar) " +
                        tableDDLOptions;
            }
            else {
                createSql = "create table " + tableName +
                        " (id varchar not null primary key, val1 varchar, " +
                        "val2 varchar, val3 varchar, val4 varchar, " +
                        "val5 varchar, val6 varchar, val7 varchar) " +
                        tableDDLOptions;
            }
            conn.createStatement().execute(createSql);
            conn.commit();
        }
    }
}
