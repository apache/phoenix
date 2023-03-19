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
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.query.BaseTest;
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

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class PhoenixTableTTLIT extends BaseTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(PhoenixTableTTLIT.class);
    private static final Random RAND = new Random(11);
    private static final int MAX_COLUMN_INDEX = 6;
    private static final int MAX_LOOKBACK_AGE = 8;
    private static final int TTL = 30;
    private String tableDDLOptions;
    private StringBuilder optionBuilder;
    ManualEnvironmentEdge injectEdge;
    private int versions;
    private final boolean multiCF;
    private final boolean columnEncoded;
    private final KeepDeletedCells keepDeletedCells;

    public PhoenixTableTTLIT(boolean multiCF, boolean columnEncoded, KeepDeletedCells keepDeletedCells) {
        this.multiCF = multiCF;
        this.columnEncoded = columnEncoded;
        this.keepDeletedCells = keepDeletedCells;
    }
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        props.put(BaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(MAX_LOOKBACK_AGE));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest(){
        EnvironmentEdgeManager.reset();
        optionBuilder = new StringBuilder();
        versions = 2;
        optionBuilder.append(" TTL=" + TTL);
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

    @Parameterized.Parameters(name = "TableTTLIT_multiCF={0}, columnEncoded={1}, keepDeletedCells={2}")
    public static synchronized Collection<Object[]> data() {
            return Arrays.asList(new Object[][] { { false, false, KeepDeletedCells.FALSE },
                    { false, false, KeepDeletedCells.TRUE }, { false, false, KeepDeletedCells.TTL },
                    { false, true, KeepDeletedCells.FALSE }, { false, true, KeepDeletedCells.TRUE },
                    { false, true, KeepDeletedCells.TTL }, { true, false, KeepDeletedCells.FALSE },
                    { true, false, KeepDeletedCells.TRUE }, { true, false, KeepDeletedCells.TTL },
                    { true, true, KeepDeletedCells.FALSE }, { true, true, KeepDeletedCells.TRUE },
                    { true, true, KeepDeletedCells.TTL} });
    }

    /**
     * This test creates two tables with the same schema. The same row is updated in a loop on
     * both tables with the same content. Each update changes one or more columns chosen
     * randomly with randomly generated values.
     *
     * After every upsert, all versions of the rows are retrieved from each table and compared.
     * The test also occasionally deletes the row from both tables and but compacts only the
     * first tables during this test.
     *
     * This test expects that both table returns the same row content for the same row version.
     *
     * @throws Exception
     */
    @Test
    public void testMaskingAndCompaction() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            createTable(tableName);
            String noCompactTableName = generateUniqueName();
            createTable(noCompactTableName);
            long startTime = System.currentTimeMillis() + 1000;
            startTime = (startTime/1000)*1000;
            EnvironmentEdgeManager.injectEdge(injectEdge);
            injectEdge.setValue(startTime);
            for (int i = 1; i < 200; i++) {
                updateRow(conn, tableName, noCompactTableName, "a");
                compareRow(conn, tableName, noCompactTableName, "a", MAX_COLUMN_INDEX);
                long scn = injectEdge.currentTime() - MAX_LOOKBACK_AGE * 1000;
                long scnEnd = injectEdge.currentTime() - 1000;
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
                if (i % 5 == 0) {
                    injectEdge.incrementValue(1000);
                    deleteRow(conn, tableName, "a");
                    deleteRow(conn, noCompactTableName, "a");
                }
                if (i % 28 == 0) {
                    injectEdge.incrementValue(1000);
                    flush(TableName.valueOf(tableName));
                    majorCompact(TableName.valueOf(tableName));
                }
                if (i % (TTL + 1) == 0) {
                    // Once in a while we update the last column that is not updated or checked
                    // regularly. Then we read this column from both tables after the TTL period.
                    // On the table that is compacted the column value would be masked and also
                    // removed by compaction. On the no compact table, the column value would be
                    // masked. So, both tables would return null value.
                    if (i != TTL + 1) {
                        compareRow(conn, tableName, noCompactTableName, "a", MAX_COLUMN_INDEX + 1);
                        ResultSet
                                rs =
                                conn.createStatement().executeQuery(
                                        "Select val" + (MAX_COLUMN_INDEX + 1) + " from "
                                                + noCompactTableName + " where id = 'a'");
                        if (rs.next()) {
                            Assert.assertTrue(rs.getString(1) == null);
                        }

                    }
                    updateColumn(conn, tableName, "a", MAX_COLUMN_INDEX + 1,
                            "invisible");
                    updateColumn(conn, noCompactTableName, "a", MAX_COLUMN_INDEX + 1,
                            "invisible");
                    conn.commit();
                }
                injectEdge.incrementValue(1000);
            }
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
            for (int i = 1; i <= maxColumnIndex; i++) {
                if (rs1.getString(i) != null) {
                    if (!rs1.getString(i).equals(rs2.getString(i))) {
                        LOG.debug("VAL" + i + " " + rs1.getString(i) + " : " + rs2.getString(i));
                    }
                } else if (rs2.getString(i) != null) {
                    LOG.debug("VAL" + i + " " + rs1.getString(i) + " : " + rs2.getString(i));
                }
                Assert.assertEquals(rs1.getString(i), rs2.getString(i));
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
                        "b.val5 varchar, a.val6 varchar, a.val7 varchar) " +
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
