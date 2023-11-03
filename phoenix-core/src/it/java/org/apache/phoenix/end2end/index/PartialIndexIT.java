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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class PartialIndexIT extends BaseTest {
    private final boolean local;
    private final boolean uncovered;
    private final boolean salted;

    public PartialIndexIT (boolean local, boolean uncovered, boolean salted) {
        this.local = local;
        this.uncovered = uncovered;
        this.salted = salted;
    }
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @After
    public void unsetFailForTesting() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        assertFalse("refCount leaked", refCountLeaked);
    }
    @Parameterized.Parameters(
            name = "local={0}, uncovered={1}, salted={2}")
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                // Partial local indexes are not supported currently.
                {false, false, true},
                {false, false, false},
                {false, true, false},
                {false, true, true}
        });
    }

    public static void assertPlan(PhoenixResultSet rs, String schemaName, String tableName) {
        PTable table = rs.getContext().getCurrentTable().getTable();
        assertTrue(table.getSchemaName().getString().equals(schemaName) &&
                table.getTableName().getString().equals(tableName));
    }

    private static void verifyIndex(String dataTableName, String indexTableName) throws Exception {
        IndexTool indexTool = IndexToolIT.runIndexTool(false, "", dataTableName,
                indexTableName, null, 0, IndexTool.IndexVerifyType.ONLY);

        assertEquals(0, indexTool.getJob().getCounters().
                findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
        assertEquals(0, indexTool.getJob().getCounters().
                findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
        assertEquals(0, indexTool.getJob().getCounters().
                findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
        assertEquals(0, indexTool.getJob().getCounters().
                findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).getValue());
        assertEquals(0, indexTool.getJob().getCounters().
                findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).getValue());
        assertEquals(0, indexTool.getJob().getCounters().
                findCounter(BEFORE_REBUILD_OLD_INDEX_ROW_COUNT).getValue());
        assertEquals(0, indexTool.getJob().getCounters().
                findCounter(BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT).getValue());

        IndexToolIT.runIndexTool(false, "", dataTableName,
                indexTableName, null, 0, IndexTool.IndexVerifyType.ONLY, "-fi");
        CounterGroup mrJobCounters = IndexToolIT.getMRJobCounters(indexTool);
        assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT.name()).getValue());
        assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT.name()).getValue());
        assertEquals(0,
                mrJobCounters.findCounter(BEFORE_REPAIR_EXTRA_UNVERIFIED_INDEX_ROW_COUNT.name()).getValue());
    }

    @Test
    public void testUnsupportedDDLs() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute(
                    "create table " + dataTableName + " (id varchar not null primary key, "
                            + "A integer, B integer, C double, D varchar)" + (salted ?
                            " SALT_BUCKETS=4" :
                            ""));
            String indexTableName = generateUniqueName();
            try {
                conn.createStatement().execute(
                        "CREATE " + (uncovered ? "UNCOVERED " : " ") + (local ? "LOCAL " : " ")
                                + "INDEX " + indexTableName + " on " + dataTableName + " (A) "
                                + (uncovered ? "" : "INCLUDE (B, C, D)") + " WHERE E > 50 ASYNC");
                Assert.fail();
            } catch (ColumnNotFoundException e) {
                // Expected
            }
            try {
                conn.createStatement().execute(
                        "CREATE " + (uncovered ? "UNCOVERED " : " ") + (local ? "LOCAL " : " ")
                                + "INDEX " + indexTableName + " on " + dataTableName + " (A) "
                                + (uncovered ? "" : "INCLUDE (B, C, D)")
                                + " WHERE A  < ANY (SELECT B FROM " + dataTableName + ")");
                Assert.fail();
            } catch (SQLException e) {
                // Index where clause cannot include a subquery
                assertTrue(e.getSQLState().equals("23101"));
            }
            try {
                conn.createStatement().execute(
                        "CREATE LOCAL INDEX " + indexTableName + " on " + dataTableName + " (A)"
                                + " WHERE A  > 0");
                Assert.fail();
            } catch (PhoenixParserException e) {
                // Local indexes are not supported yet
            }
        }
    }
    @Test
    public void testDDLWithAllDataTypes() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String fullTableName = String.format("%s.%s", "S", dataTableName);
            conn.createStatement().execute(
                    "create table " + fullTableName
                            + " (id varchar not null, kp varchar not null, "
                            + "A INTEGER, B UNSIGNED_INT, C BIGINT, D UNSIGNED_LONG, E TINYINT, "
                            + "F UNSIGNED_TINYINT, G SMALLINT, H UNSIGNED_SMALLINT, I FLOAT, "
                            + "J UNSIGNED_FLOAT, K DOUBLE, L UNSIGNED_DOUBLE, M DECIMAL, "
                            + "N BOOLEAN, O TIME, P DATE, Q TIMESTAMP, R UNSIGNED_TIME, "
                            + "S UNSIGNED_DATE, T UNSIGNED_TIMESTAMP, U CHAR(10), V BINARY(1024), "
                            + "W VARBINARY, Y INTEGER ARRAY, Z VARCHAR ARRAY[10], AA DATE ARRAY, "
                            + "AB TIMESTAMP ARRAY, AC UNSIGNED_TIME ARRAY, AD UNSIGNED_DATE ARRAY, "
                            + "AE UNSIGNED_TIMESTAMP ARRAY "
                            + "CONSTRAINT pk PRIMARY KEY (id,kp)) "
                            + "MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0" );
            String indexTableName = generateUniqueName();
            try {
                conn.createStatement().execute(
                        "CREATE " + (uncovered ? "UNCOVERED " : " ") + (local ? "LOCAL " : " ")
                                + "INDEX IF NOT EXISTS " + indexTableName + " on " + fullTableName
                                + " (kp,A) WHERE (kp  > '5')");
            } catch (PhoenixParserException e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }
    @Test
    public void testAtomicUpsert() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar not null primary key, " +
                    "A integer, B integer, C double, D varchar)" +
                    (salted ? " SALT_BUCKETS=4" : ""));
            String indexTableName = generateUniqueName();
            // Add rows to the data table before creating a partial index to test that the index
            // will be built correctly by IndexTool
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id1', 25, 2, 3.14, 'a')");

            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, A, D) values ('id2', 100, 'b')");
            conn.commit();
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") +
                    (local ? "LOCAL " : " ") +"INDEX "
                    + indexTableName + " on " + dataTableName + " (A) " +
                    (uncovered ? "" : "INCLUDE (B, C, D)") + " WHERE A > 50 ASYNC");

            IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);

            String selectSql = "SELECT  D from " + dataTableName + " WHERE A > 60";
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is used
            assertPlan((PhoenixResultSet) rs,  "", indexTableName);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertFalse(rs.next());

            selectSql = "SELECT  D from " + dataTableName + " WHERE A = 50";
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is not used
            assertPlan((PhoenixResultSet) rs,  "", dataTableName);

            // Add more rows to test the index write path
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id3', 50, 2, 9.5, 'c')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id4', 75, 2, 9.5, 'd')");
            conn.commit();

            // Verify that index table includes only the rows with A > 50
            selectSql = "SELECT * from " + indexTableName;
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(75, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
            assertFalse(rs.next());

            // Overwrite an existing row that satisfies the index WHERE clause using an atomic
            // upsert such that the new version of the row does not satisfy the index where clause
            // anymore. This should result in deleting the index row.
            String dml = "UPSERT INTO " + dataTableName + " values ('id2', 300, 2, 9.5, 'd') " +
                    "ON DUPLICATE KEY UPDATE A = 0";
            conn.createStatement().execute(dml);
            conn.commit();
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(75, rs.getInt(1));
            assertFalse(rs.next());

            // Retrieve update row from the data table and verify that the index table is not used
            selectSql = "SELECT  ID from " + dataTableName + " WHERE A = 0";
            rs = conn.createStatement().executeQuery(selectSql);
            assertPlan((PhoenixResultSet) rs,  "", dataTableName);
            assertTrue(rs.next());
            assertEquals("id2", rs.getString(1));

            // Test index verification and repair by IndexTool
            verifyIndex(dataTableName, indexTableName);

            try (Connection newConn = DriverManager.getConnection(getUrl())) {
                PTable indexTable = PhoenixRuntime.getTableNoCache(newConn, indexTableName);
                assertTrue(indexTable.getIndexWhere().equals("A > 50"));
            }
        }
    }

    @Test
    public void testComparisonOfColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar not null primary key, " +
                    "A integer, B integer, C double, D varchar) COLUMN_ENCODED_BYTES=0" +
                    (salted ? ", SALT_BUCKETS=4" : ""));
            String indexTableName = generateUniqueName();

            // Add rows to the data table before creating a partial index to test that the index
            // will be built correctly by IndexTool
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id1', 25, 2, 3.14, 'a')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, A, B, D) values ('id2', 100, 200, 'b')");
            conn.commit();
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") +
                    (local ? "LOCAL " : " ") +"INDEX "
                    + indexTableName + " on " + dataTableName + " (A) " +
                    (uncovered ? "" : "INCLUDE (B, C, D)") + " WHERE A > B ASYNC");
            conn.commit();
            IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);

            String selectSql = "SELECT D from " + dataTableName + " WHERE A > B and D is not NULL";

            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is used
            assertPlan((PhoenixResultSet) rs,  "", indexTableName);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());

            selectSql = "SELECT  D from " + dataTableName + " WHERE A > 100";
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is not used
            assertPlan((PhoenixResultSet) rs,  "", dataTableName);

            // Add more rows to test the index write path
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id3', 50, 300, 9.5, 'c')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id4', 75, 2, 9.5, 'd')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id4', 76, 2, 9.5, 'd')");
            conn.commit();

            // Verify that index table includes only the rows with A >  B
            selectSql = "SELECT * from " + indexTableName;
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(25, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(76, rs.getInt(1));
            assertFalse(rs.next());

            // Overwrite an existing row that satisfies the index WHERE clause such that
            // the new version of the row does not satisfy the index where clause anymore. This
            // should result in deleting the index row.
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (ID, B) values ('id1', 100)");
            conn.commit();
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(76, rs.getInt(1));
            assertFalse(rs.next());

            // Test index verification and repair by IndexTool
            verifyIndex(dataTableName, indexTableName);

            try (Connection newConn = DriverManager.getConnection(getUrl())) {
                PTable indexTable = PhoenixRuntime.getTableNoCache(newConn, indexTableName);
                assertTrue(indexTable.getIndexWhere().equals("A > B"));
            }
        }
    }

    @Test
    public void testIsNull() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar not null primary key, " +
                    "A integer, B integer, C double, D varchar)" +
                    (salted ? " SALT_BUCKETS=4" : ""));
            String indexTableName = generateUniqueName();

            // Add rows to the data table before creating a partial index to test that the index
            // will be built correctly by IndexTool
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id1', 70, 2, 3.14, 'a')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, A, D) values ('id2', 100, 'b')");
            conn.commit();
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") +
                    (local ? "LOCAL " : " ") +"INDEX "
                    + indexTableName + " on " + dataTableName + " (A) " +
                    (uncovered ? "" : "INCLUDE (B, C, D)") + " WHERE B IS NOT NULL AND " +
                    "C IS NOT NULL ASYNC");
            IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);

            String selectSql = "SELECT A, D from " + dataTableName +
                    " WHERE A > 60 AND B IS NOT NULL AND C IS NOT NULL";

            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is used
            assertPlan((PhoenixResultSet) rs,  "", indexTableName);
            assertTrue(rs.next());
            assertEquals(70, rs.getInt(1));
            assertEquals("a", rs.getString(2));
            assertFalse(rs.next());

            // Add more rows to test the index write path
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id3', 20, 2, 3.14, 'a')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id4', 90, 2, 3.14, 'a')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, A, D) values ('id5', 150, 'b')");
            conn.commit();

            // Verify that index table includes only the rows where B is not null
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(70, rs.getInt(1));
            assertEquals("a", rs.getString(2));
            assertTrue(rs.next());
            assertEquals(90, rs.getInt(1));
            assertEquals("a", rs.getString(2));
            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("SELECT Count(*) from " + dataTableName);
            // Verify that the index table is not used
            assertPlan((PhoenixResultSet) rs,  "", dataTableName);
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));

            // Overwrite an existing row that satisfies the index WHERE clause such that
            // the new version of the row does not satisfy the index where clause anymore. This
            // should result in deleting the index row.
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (ID, B) values ('id4', null)");
            conn.commit();
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(70, rs.getInt(1));
            assertEquals("a", rs.getString(2));
            assertFalse(rs.next());

            // Test index verification and repair by IndexTool
            verifyIndex(dataTableName, indexTableName);

            try (Connection newConn = DriverManager.getConnection(getUrl())) {
                PTable indexTable = PhoenixRuntime.getTableNoCache(newConn, indexTableName);
                assertTrue(indexTable.getIndexWhere().equals("(B IS NOT NULL  AND C IS NOT NULL )"));
            }
        }
    }

    @Test
    public void testLike() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar not null primary key, " +
                    "A integer, B integer, C double, D varchar)" +
                    (salted ? " SALT_BUCKETS=4" : ""));
            String indexTableName = generateUniqueName();

            // Add rows to the data table before creating a partial index to test that the index
            // will be built correctly by IndexTool
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id1', 70, 2, 3.14, 'abcdef')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, A, D) values ('id2', 100, 'bcdez')");
            conn.commit();
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") +
                    (local ? "LOCAL " : " ") +"INDEX "
                    + indexTableName + " on " + dataTableName + " (A) " +
                    (uncovered ? "" : "INCLUDE (B, C, D)") + " WHERE D like '%cde_' ASYNC");
            IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);

            String selectSql = "SELECT D from " + dataTableName +
                    " WHERE B is not NULL AND D like '%cde_'";
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is used
            assertPlan((PhoenixResultSet) rs,  "", indexTableName);
            assertTrue(rs.next());
            assertEquals("abcdef", rs.getString(1));
            assertFalse(rs.next());

            // Add more rows to test the index write path
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id3', 20, 2, 3.14, 'abcdegg')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id4', 10, 2, 3.14, 'aabecdeh')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, A, D) values ('id5', 150, 'bbbb')");
            conn.commit();

            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("aabecdeh", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("abcdef", rs.getString(1));
            assertFalse(rs.next());

            selectSql = "SELECT Count(*) from " + dataTableName;
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is not used
            assertPlan((PhoenixResultSet) rs,  "", dataTableName);
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));

            // Overwrite an existing row that satisfies the index WHERE clause such that
            // the new version of the row does not satisfy the index where clause anymore. This
            // should result in deleting the index row.
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, D) values ('id4',  'zzz')");
            conn.commit();
            selectSql = "SELECT D from " + dataTableName +
                    " WHERE B is not NULL AND D like '%cde_'";
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abcdef", rs.getString(1));
            assertFalse(rs.next());

            // Test index verification and repair by IndexTool
            verifyIndex(dataTableName, indexTableName);

            try (Connection newConn = DriverManager.getConnection(getUrl())) {
                PTable indexTable = PhoenixRuntime.getTableNoCache(newConn, indexTableName);
                assertTrue(indexTable.getIndexWhere().equals("D LIKE '%cde_'"));
            }
        }
    }

    @Test
    public void testPhoenixRowTimestamp() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute(
                    "create table " + dataTableName + " (id varchar not null primary key, "
                            + "A integer, B integer)" + (salted ?
                            " SALT_BUCKETS=4" :
                            ""));
            String indexTableName = generateUniqueName();

            // Add rows to the data table before creating a partial index to test that the index
            // will be built correctly by IndexTool
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " values ('id1', 70, 2)");
            conn.commit();
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " values ('id5', 0, 2)");
            conn.commit();
            Thread.sleep(10);
            Timestamp before = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
            String timeZoneID = Calendar.getInstance().getTimeZone().getID();
            conn.createStatement().execute(
                    "CREATE " + (uncovered ? "UNCOVERED " : " ") + (local ? "LOCAL " : " ") +
                            "INDEX " + indexTableName + " on " + dataTableName + " (A) " + (
                            uncovered ?
                                    "" :
                                    "INCLUDE (B)") + " WHERE PHOENIX_ROW_TIMESTAMP() < " +
                            "TO_DATE('" + before + "', 'yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')  ASYNC");
            IndexToolIT.runIndexTool(false, null, dataTableName, indexTableName);

            String selectSql = "SELECT A from " + dataTableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() < TO_DATE('" + before +
                    "', 'yyyy-MM-dd HH:mm:ss.SSS', '" + timeZoneID + "')";
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is used
            assertPlan((PhoenixResultSet) rs,  "", indexTableName);
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(70, rs.getInt(1));
            assertFalse(rs.next());

            // Add more rows to test the index write path
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " values ('id2', 20, 3)");
            conn.commit();
            conn.createStatement().execute(
                    "upsert into " + dataTableName + " values ('id3', 10, 4)");
            conn.commit();

            rs = conn.createStatement().executeQuery("SELECT Count(*) from " + dataTableName);
            // Verify that the index table is not used
            assertPlan((PhoenixResultSet) rs, "", dataTableName);
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));

            rs = conn.createStatement().executeQuery("SELECT Count(*) from " + indexTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            // Overwrite an existing row that satisfies the index WHERE clause such that
            // the new version of the row does not satisfy the index where clause anymore. This
            // should result in deleting the index row.
            conn.createStatement()
                    .execute("upsert into " + dataTableName + " values ('id1',  70, 2)");
            conn.commit();

            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is used
            assertPlan((PhoenixResultSet) rs,  "", indexTableName);
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertFalse(rs.next());

            // Test index verification and repair by IndexTool
            verifyIndex(dataTableName, indexTableName);
        }
    }

    @Test
    public void testPartialIndexPreferredOverFullIndex() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar not null primary key, " +
                    "A integer, B integer, C double, D varchar)" +
                    (salted ? " SALT_BUCKETS=4" : ""));
            conn.createStatement().execute("upsert into " + dataTableName +
                    " values ('id1', 10, 2, 3.14, 'a')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName +
                    " (id, A, D) values ('id2', 100, 'b')");
            conn.commit();
            String fullIndexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") +
                    (local ? "LOCAL " : " ") +"INDEX "
                    + fullIndexTableName + " on " + dataTableName + " (A) " +
                    (uncovered ? "" : "INCLUDE (B, C, D)") + " ASYNC");
            IndexToolIT.runIndexTool(false, null, dataTableName, fullIndexTableName);
            String partialIndexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") +
                    (local ? "LOCAL " : " ") +"INDEX "
                    + partialIndexTableName + " on " + dataTableName + " (A) " +
                    (uncovered ? "" : "INCLUDE (B, C, D)") + " WHERE A > 50 ASYNC");
            IndexToolIT.runIndexTool(false, null, dataTableName, partialIndexTableName);
            String selectSql = "SELECT  D from " + dataTableName + " WHERE A > 60";
            // Verify that the partial index table is used
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertPlan((PhoenixResultSet) rs,  "", partialIndexTableName);
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertFalse(rs.next());

            selectSql = "SELECT  D from " + dataTableName + " WHERE A < 50";
            // Verify that the full index table is used
            rs = conn.createStatement().executeQuery(selectSql);
            assertPlan((PhoenixResultSet) rs,  "", fullIndexTableName);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());
        }
    }
}
