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
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
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
            //explain plan verify to check if partial index is used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains(indexTableName));

            selectSql = "SELECT  D from " + dataTableName + " WHERE A = 50";
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is not used
            assertPlan((PhoenixResultSet) rs,  "", dataTableName);
            //explain plan verify to check if partial index is not used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertFalse(rs.getString(1).contains(indexTableName));

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
            //explain plan verify to check if partial index is not used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertFalse(rs.getString(1).contains(indexTableName));

            // Test index verification and repair by IndexTool
            verifyIndex(dataTableName, indexTableName);

            try (PhoenixConnection newConn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
                PTable indexTable = newConn.getTableNoCache(indexTableName);
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
            //explain plan verify to check if partial index is used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains(indexTableName));

            selectSql = "SELECT  D from " + dataTableName + " WHERE A > 100";
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that the index table is not used
            assertPlan((PhoenixResultSet) rs,  "", dataTableName);
            //explain plan verify to check if partial index is not used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertFalse(rs.getString(1).contains(indexTableName));

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

            try (PhoenixConnection newConn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
                PTable indexTable = newConn.getTableNoCache(indexTableName);
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
            //explain plan verify to check if partial index is used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains(indexTableName));

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
            //explain plan verify to check if partial index is not used
            rs = conn.createStatement().executeQuery("EXPLAIN " + "SELECT Count(*) from " + dataTableName);
            assertTrue(rs.next());
            assertFalse(rs.getString(1).contains(indexTableName));

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

            try (PhoenixConnection newConn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
                PTable indexTable = newConn.getTableNoCache(indexTableName);
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
            //explain plan verify to check if partial index is used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains(indexTableName));

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
            //explain plan verify to check if partial index is not used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertFalse(rs.getString(1).contains(indexTableName));

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

            try (PhoenixConnection newConn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
                PTable indexTable = newConn.getTableNoCache(indexTableName);
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
            //explain plan verify to check if partial index is used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains(indexTableName));

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
            //explain plan verify to check if partial index is not used
            rs = conn.createStatement().executeQuery("EXPLAIN " + "SELECT Count(*) from " + dataTableName);
            assertTrue(rs.next());
            assertFalse(rs.getString(1).contains(indexTableName));

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
            //explain plan verify to check if partial index is used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains(indexTableName));

            // Test index verification and repair by IndexTool
            verifyIndex(dataTableName, indexTableName);
        }
    }

    @Test
    public void testViewIndexes() throws Exception {
        String baseTableName =  generateUniqueName();
        String globalViewName = generateUniqueName();
        String globalViewIndexName =  generateUniqueName();
        String tenantViewName =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + baseTableName +
                    " (TENANT_ID CHAR(15) NOT NULL, KP CHAR(3) NOT NULL, PK2 DATE NOT NULL, "+
                    "PK3 INTEGER NOT NULL, KV1 VARCHAR, KV2 VARCHAR, KV3 CHAR(15) " +
                    "CONSTRAINT PK PRIMARY KEY(TENANT_ID, KP, PK2, PK3)) MULTI_TENANT=true" +
                    (salted ? ", SALT_BUCKETS=4" : ""));
            conn.createStatement().execute("CREATE VIEW " + globalViewName +
                    " AS SELECT * FROM " + baseTableName + " WHERE  KP = '001'");
            conn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") +
                    (local ? "LOCAL " : " ") + "INDEX " + globalViewIndexName + " on " +
                    globalViewName + " (PK3 DESC, KV3) " +
                    (uncovered ? "" : "INCLUDE (KV1)") + " WHERE KV3 IS NOT NULL ASYNC");

            String tenantId = "tenantId";
            Properties tenantProps = new Properties();
            tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            // Create a tenant specific view and view index
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
                tenantConn.createStatement().execute("CREATE VIEW " + tenantViewName + " AS SELECT * FROM " + globalViewName);
                String tenantViewIndexName =  generateUniqueName();
                tenantConn.createStatement().execute("CREATE " + (uncovered ? "UNCOVERED " : " ") +
                        (local ? "LOCAL " : " ") + "INDEX " + tenantViewIndexName + " on " +
                        tenantViewName + " (PK3) " +
                        (uncovered ? "" : "INCLUDE (KV1)") + " WHERE PK3 > 4");
                PreparedStatement
                        stmt = tenantConn.prepareStatement("UPSERT INTO  " + tenantViewName + " (PK2, PK3, KV1, KV3) VALUES (?, ?, ?, ?)");
                stmt.setDate(1, new Date(100));
                stmt.setInt(2, 1);
                stmt.setString(3, "KV1");
                stmt.setString(4, "KV3");
                stmt.executeUpdate();
                stmt.setDate(1, new Date(100));
                stmt.setInt(2, 2);
                stmt.setString(3, "KV4");
                stmt.setString(4, "KV5");
                stmt.executeUpdate();
                stmt.setDate(1, new Date(100));
                stmt.setInt(2, 3);
                stmt.setString(3, "KV6");
                stmt.setString(4, "KV7");
                stmt.executeUpdate();
                stmt.setDate(1, new Date(100));
                stmt.setInt(2, 4);
                stmt.setString(3, "KV8");
                stmt.setString(4, "KV9");
                stmt.executeUpdate();
                stmt.setDate(1, new Date(100));
                stmt.setInt(2, 5);
                stmt.setString(3, "KV10");
                stmt.setString(4, "KV11");
                stmt.executeUpdate();
                tenantConn.commit();

                // Verify that query uses the tenant view index
                ResultSet rs = tenantConn.createStatement().executeQuery("SELECT KV1 FROM  " +
                        tenantViewName + " WHERE PK3 = 5");
                assertPlan((PhoenixResultSet) rs,  "", tenantViewIndexName);
                assertTrue(rs.next());
                assertEquals("KV10", rs.getString(1));
                assertFalse(rs.next());

                // Verify that query does not use the tenant view index when the partial index
                // where clause does not contain the query where clause
                rs = tenantConn.createStatement().executeQuery("SELECT KV1 FROM  " +
                        tenantViewName + " WHERE PK3 = 4");
                assertPlan((PhoenixResultSet) rs,  "", tenantViewName);
                assertTrue(rs.next());
                assertEquals("KV8", rs.getString(1));
                assertFalse(rs.next());

                // Verify that the tenant view index has only one row
                rs = tenantConn.createStatement().executeQuery("SELECT Count(*) FROM  " +
                        tenantViewIndexName);
                assertPlan((PhoenixResultSet) rs,  "", tenantViewIndexName);
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }

            // Run the IndexTool MR job
            IndexToolIT.runIndexTool(false, "", globalViewName,
                    globalViewIndexName);
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
                // Verify that the query uses the global view index
                ResultSet rs = tenantConn.createStatement().executeQuery("SELECT KV1 FROM  " +
                        tenantViewName + " WHERE PK3 = 1 AND KV3 = 'KV3'");
                assertPlan((PhoenixResultSet) rs,  "", tenantViewName +
                        "#" + globalViewIndexName);
                assertTrue(rs.next());
                assertEquals("KV1", rs.getString(1));
                assertFalse(rs.next());
            }

            // Verify that the query uses the global view index
            ResultSet rs = conn.createStatement().executeQuery("SELECT KV1 FROM  " +
                    globalViewName + " WHERE PK3 = 1 AND KV3 = 'KV3'");
            assertPlan((PhoenixResultSet) rs,  "", globalViewIndexName);
            assertTrue(rs.next());
            assertEquals("KV1", rs.getString(1));
            assertFalse(rs.next());

            // Verify that the global view index has five rows
            rs = conn.createStatement().executeQuery("SELECT Count(*) FROM  " +
                    globalViewIndexName);
            assertPlan((PhoenixResultSet) rs,  "", globalViewIndexName);
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
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
            //explain plan verify to check if partial index is used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains(partialIndexTableName));

            selectSql = "SELECT  D from " + dataTableName + " WHERE A < 50";
            // Verify that the full index table is used
            rs = conn.createStatement().executeQuery(selectSql);
            assertPlan((PhoenixResultSet) rs,  "", fullIndexTableName);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());
            //explain plan verify to check if full index is used
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            assertTrue(rs.next());
            assertTrue(rs.getString(1).contains(fullIndexTableName));
        }
    }

    @Test
    public void testPartialIndexWithIndexHint() throws Exception {
        try(Connection conn = DriverManager.getConnection(getUrl());
            Statement stmt = conn.createStatement()) {
            String dataTableName = generateUniqueName();
            stmt.execute("create table " + dataTableName + " (id1 varchar not null, id2 integer not null, "
                    + "A integer constraint pk primary key (id1, id2))" + (salted ? " SALT_BUCKETS=4" : ""));
            stmt.execute("upsert into " + dataTableName + " values ('id11', 10, 1)");
            conn.commit();
            stmt.execute("upsert into " + dataTableName + " values ('id12', 100, 2)");
            conn.commit();
            String indexTableName = generateUniqueName();
            stmt.execute("CREATE " + (uncovered ? "UNCOVERED " : " ") +
                    (local ? "LOCAL " : " ") +"INDEX "
                    + indexTableName + " on " + dataTableName + " (id2, id1) " +
                    (uncovered ? "" : "INCLUDE (A)") + " WHERE id2 > 50");
            // Index hint provided and query plan using partial index is usable
            String selectSql = "SELECT  /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ "
                    + "A from " + dataTableName + " WHERE id2 = 100 AND id1 = 'id12'";
            ResultSet rs = stmt.executeQuery("EXPLAIN " + selectSql);
            String actualQueryPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(actualQueryPlan.contains("POINT LOOKUP ON 1 KEY OVER " + indexTableName));
            rs = stmt.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            // Index hint provided but query plan using partial index is not usable so, no data
            selectSql = "SELECT  /*+ INDEX(" + dataTableName + " " + indexTableName + ")*/ "
                    + "A from " + dataTableName + " WHERE id2 = 10 AND id1 = 'id11'";
            rs = stmt.executeQuery("EXPLAIN " + selectSql);
            actualQueryPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(actualQueryPlan.contains("POINT LOOKUP ON 1 KEY OVER " + indexTableName));
            rs = stmt.executeQuery(selectSql);
            assertFalse(rs.next());
            // No index hint so, use data table only as its point lookup
            selectSql = "SELECT A from " + dataTableName + " WHERE id2 = 10 AND id1 = 'id11'";
            rs = stmt.executeQuery("EXPLAIN " + selectSql);
            actualQueryPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(actualQueryPlan.contains("POINT LOOKUP ON 1 KEY OVER " + dataTableName));
            rs = stmt.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testPartialIndexOnTableWithCaseSensitiveColumns() throws Exception {
        try(Connection conn = DriverManager.getConnection(getUrl());
            PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class)) {
            String dataTableName = generateUniqueName();
            String indexName1 = generateUniqueName();
            String indexName2 = generateUniqueName();
            String indexName3 = generateUniqueName();

            stmt.execute("CREATE TABLE " + dataTableName
                    + " (\"hashKeY\" VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, \"CoL\" VARCHAR, \"coLUmn3\" VARCHAR)"
                    + (salted ? " SALT_BUCKETS=4" : ""));

            stmt.execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + (local ? "LOCAL " : " ")
                    + "INDEX " + indexName1 + " on " + dataTableName + " (v1) " +
                    (uncovered ? "" : "INCLUDE (\"CoL\", \"coLUmn3\")"));
            stmt.execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + (local ? "LOCAL " : " ")
                    + "INDEX " + indexName2 + " on " + dataTableName + " (\"CoL\") " +
                    (uncovered ? "" : "INCLUDE (v1, \"coLUmn3\")"));
            stmt.execute("CREATE " + (uncovered ? "UNCOVERED " : " ") + (local ? "LOCAL " : " ")
                    + "INDEX " + indexName3 + " on " + dataTableName + " (\"coLUmn3\") " +
                    (uncovered ? "" : "INCLUDE (\"CoL\", v1)"));

            stmt.execute("UPSERT INTO " + dataTableName + " VALUES ('a', 'b', 'c', 'd')");
            conn.commit();

            ResultSet rs = stmt.executeQuery("SELECT \"CoL\" FROM " + dataTableName + " WHERE v1='b'");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(indexName1, stmt.getQueryPlan().getTableRef().getTable().getTableName().toString());

            rs = stmt.executeQuery("SELECT v1 FROM " + dataTableName + " WHERE \"CoL\"='c'");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(indexName2, stmt.getQueryPlan().getTableRef().getTable().getTableName().toString());

            rs = stmt.executeQuery("SELECT \"CoL\" FROM " + dataTableName + " WHERE \"coLUmn3\"='d'");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(indexName3, stmt.getQueryPlan().getTableRef().getTable().getTableName().toString());
        }
    }
}
