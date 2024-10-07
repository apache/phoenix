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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class OnDuplicateKey2IT extends ParallelStatsDisabledIT {
    private final String indexDDL;
    private final String tableDDLOptions;

    private static final String[] INDEX_DDLS =
            new String[] {
                    "",
                    "create local index %s_IDX on %s(counter1) include (counter2)",
                    "create local index %s_IDX on %s(counter1, counter2)",
                    "create index %s_IDX on %s(counter1) include (counter2)",
                    "create index %s_IDX on %s(counter1, counter2)",
                    "create uncovered index %s_IDX on %s(counter1)",
                    "create uncovered index %s_IDX on %s(counter1, counter2)"};

    public OnDuplicateKey2IT(String indexDDL, boolean columnEncoded) {
        this.indexDDL = indexDDL;
        this.tableDDLOptions = columnEncoded ? "" : "COLUMN_ENCODED_BYTES=0";
    }

    @Parameters(name="OnDuplicateKey2IT_{index},columnEncoded={1}")
    public static synchronized Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        for (String indexDDL : INDEX_DDLS) {
            for (boolean columnEncoded : new boolean[]{ false, true }) {
                testCases.add(new Object[] { indexDDL, columnEncoded });
            }
        }
        return testCases;
    }

    private void createIndex(Connection conn, String tableName) throws SQLException {
        if (indexDDL == null || indexDDL.length() == 0) {
            return;
        }
        String ddl = String.format(indexDDL, tableName, tableName);
        conn.createStatement().execute(ddl);
    }

    @Test
    public void testIgnoreReturnValue() throws Exception {
        Assume.assumeTrue("Set correct result to RegionActionResult on hbase versions " +
                "2.4.18+, 2.5.9+, and 2.6.0+", isSetCorrectResultEnabledOnHBase());

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        String tableName = generateUniqueName();
        String ddl = " create table " + tableName + "(pk varchar primary key, counter1 bigint, counter2 bigint)";
        conn.createStatement().execute(ddl);
        createIndex(conn, tableName);
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES('a',10)");

        int actualReturnValue = conn.createStatement().executeUpdate(
                "UPSERT INTO " + tableName + " VALUES('a',0) ON DUPLICATE KEY IGNORE");
        assertEquals(0, actualReturnValue);

        conn.close();
    }

    @Test
    public void testReturnRowResult() throws Exception {
        Assume.assumeTrue("Set correct result to RegionActionResult on hbase versions " +
                "2.4.18+, 2.5.9+, and 2.6.0+", isSetCorrectResultEnabledOnHBase());

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String sample1 = getJsonString("json/sample_01.json");
        String sample2 = getJsonString("json/sample_02.json");
        BsonDocument bsonDocument1 = RawBsonDocument.parse(sample1);
        BsonDocument bsonDocument2 = RawBsonDocument.parse(sample2);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String tableName = generateUniqueName();
            String ddl = "CREATE TABLE " + tableName
                    + "(PK1 VARCHAR, PK2 DOUBLE NOT NULL, PK3 VARCHAR, COUNTER1 DOUBLE,"
                    + " COUNTER2 VARCHAR,"
                    + " COL3 BSON, COL4 INTEGER, CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3))";
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);

            String upsertSql = "UPSERT INTO " + tableName + " (PK1, PK2, PK3, COUNTER1, COL3, COL4)"
                    + " VALUES('pk000', -123.98, 'pk003', 1011.202, ?, 123) ON DUPLICATE KEY " +
                    "IGNORE";
            validateReturnedRowAfterUpsert(conn, upsertSql, tableName, 1011.202, null, true,
                    bsonDocument1, bsonDocument1, 123);

            upsertSql =
                    "UPSERT INTO " + tableName + " (PK1, PK2, PK3, COUNTER1) "
                            + "VALUES('pk000', -123.98, 'pk003', 0) ON DUPLICATE"
                            + " KEY IGNORE";
            validateReturnedRowAfterUpsert(conn, upsertSql, tableName, 1011.202, null, false,
                    null, bsonDocument1, 123);

            upsertSql =
                    "UPSERT INTO " + tableName
                            + " (PK1, PK2, PK3, COUNTER1, COUNTER2) VALUES('pk000', -123.98, "
                            + "'pk003', 234, 'col2_000')";
            validateReturnedRowAfterUpsert(conn, upsertSql, tableName, 234d, "col2_000", true,
                    null, bsonDocument1, 123);

            upsertSql = "UPSERT INTO " + tableName
                    + " (PK1, PK2, PK3) VALUES('pk000', -123.98, 'pk003') ON DUPLICATE KEY UPDATE "
                    + "COUNTER1 = CASE WHEN COUNTER1 < 2000 THEN COUNTER1 + 1999.99 ELSE COUNTER1"
                    + " END, "
                    + "COUNTER2 = CASE WHEN COUNTER2 = 'col2_000' THEN 'col2_001' ELSE COUNTER2 "
                    + "END, "
                    + "COL3 = ?, "
                    + "COL4 = 234";
            validateReturnedRowAfterUpsert(conn, upsertSql, tableName, 2233.99, "col2_001", true,
                    bsonDocument2, bsonDocument2, 234);

            upsertSql = "UPSERT INTO " + tableName
                    + " (PK1, PK2, PK3) VALUES('pk000', -123.98, 'pk003') ON DUPLICATE KEY UPDATE "
                    + "COUNTER1 = CASE WHEN COUNTER1 < 2000 THEN COUNTER1 + 1999.99 ELSE COUNTER1"
                    + " END,"
                    + "COUNTER2 = CASE WHEN COUNTER2 = 'col2_000' THEN 'col2_001' ELSE COUNTER2 "
                    + "END";
            validateReturnedRowAfterUpsert(conn, upsertSql, tableName, 2233.99, "col2_001", false
                    , null, bsonDocument2, 234);

            PhoenixPreparedStatement ps =
                    conn.prepareStatement(
                            "DELETE FROM " + tableName + " WHERE PK1 = ? AND PK2 " +
                                    "= ? AND PK3 = ?").unwrap(PhoenixPreparedStatement.class);
            ps.setString(1, "pk000");
            ps.setDouble(2, -123.98);
            ps.setString(3, "pk003");
            validateReturnedRowAfterDelete(conn, ps, tableName, 2233.99, "col2_001", true, true,
                    bsonDocument2, 234);
            validateReturnedRowAfterDelete(conn, ps, tableName, 2233.99, "col2_001", true, false,
                    bsonDocument2, 234);

            addRows(tableName, conn);

            ps = conn.prepareStatement(
                    "DELETE FROM " + tableName + " WHERE PK1 = ? AND PK2 = ?")
                    .unwrap(PhoenixPreparedStatement.class);
            ps.setString(1, "pk001");
            ps.setDouble(2, 122.34);
            validateReturnedRowAfterDelete(conn, ps, tableName, 2233.99, "col2_001", false, false,
                    bsonDocument2, 234);

            ps = conn.prepareStatement(
                            "DELETE FROM " + tableName).unwrap(PhoenixPreparedStatement.class);
            validateReturnedRowAfterDelete(conn, ps, tableName, 2233.99, "col2_001", false, false,
                    bsonDocument2, 234);

            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertFalse(rs.next());

            addRows(tableName, conn);

            ps = conn.prepareStatement(
                            "DELETE FROM " + tableName + " WHERE PK1 IN (?) AND PK2 IN (?) AND " +
                                    "PK3 IN (?, ?)")
                    .unwrap(PhoenixPreparedStatement.class);
            ps.setString(1, "pk001");
            ps.setDouble(2, 122.34);
            ps.setString(3, "pk004");
            ps.setString(4, "pk005");
            validateReturnedRowAfterDelete(conn, ps, tableName, 2233.99, "col2_001", false, false,
                    bsonDocument2, 234);
        }
    }

    private static void addRows(String tableName, Connection conn) throws SQLException {
        String upsertSql;
        upsertSql =
                "UPSERT INTO " + tableName
                        + " (PK1, PK2, PK3, COUNTER1, COUNTER2) VALUES('pk001', 122.34, "
                        + "'pk004', 23, 'col2_001')";
        conn.createStatement().execute(upsertSql);
        upsertSql =
                "UPSERT INTO " + tableName
                        + " (PK1, PK2, PK3, COUNTER1, COUNTER2) VALUES('pk001', 122.34, "
                        + "'pk005', 23, 'col2_001')";
        conn.createStatement().execute(upsertSql);
        upsertSql =
                "UPSERT INTO " + tableName
                        + " (PK1, PK2, PK3, COUNTER1, COUNTER2) VALUES('pk003', 122.34, "
                        + "'pk005', 23, 'col2_001')";
        conn.createStatement().execute(upsertSql);
    }

    private static void validateReturnedRowAfterDelete(Connection conn,
                                                       PhoenixPreparedStatement ps,
                                                       String tableName,
                                                       Double col1,
                                                       String col2,
                                                       boolean isSinglePointLookup,
                                                       boolean atomicDeleteSuccessful,
                                                       BsonDocument expectedDoc,
                                                       Integer col4)
            throws SQLException {
        final Pair<Integer, Tuple> resultPair = ps.executeUpdateReturnRow();
        ResultTuple result = (ResultTuple) resultPair.getSecond();

        PTable table = conn.unwrap(PhoenixConnection.class).getTable(tableName);

        if (!isSinglePointLookup) {
            assertNull(result.getResult());
            return;
        }
        Cell cell =
                result.getResult()
                        .getColumnLatestCell(table.getColumns().get(3).getFamilyName().getBytes(),
                                table.getColumns().get(3).getColumnQualifierBytes());
        if (!atomicDeleteSuccessful) {
            assertNull(cell);
            return;
        }

        validateReturnedRowResult(col1, col2, expectedDoc, col4, table, cell, result);
    }

    private static void validateReturnedRowResult(Double col1, String col2,
                                                  BsonDocument expectedDoc, Integer col4,
                                                  PTable table, Cell cell, ResultTuple result) {
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        table.getRowKeySchema().iterator(cell.getRowArray(), ptr, 1);
        assertEquals("pk000",
                PVarchar.INSTANCE.toObject(ptr.get(), ptr.getOffset(), ptr.getLength()));

        ptr = new ImmutableBytesPtr();
        table.getRowKeySchema().iterator(cell.getRowArray(), ptr, 2);
        assertEquals(-123.98,
                PDouble.INSTANCE.toObject(ptr.get(), ptr.getOffset(), ptr.getLength()));

        ptr = new ImmutableBytesPtr();
        table.getRowKeySchema().iterator(cell.getRowArray(), ptr, 3);
        assertEquals("pk003",
                PVarchar.INSTANCE.toObject(ptr.get(), ptr.getOffset(), ptr.getLength()));

        assertEquals(col1,
                PDouble.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(),
                        cell.getValueLength()));
        cell = result.getResult()
                .getColumnLatestCell(table.getColumns().get(4).getFamilyName().getBytes(),
                        table.getColumns().get(4).getColumnQualifierBytes());
        if (col2 != null) {
            assertEquals(col2,
                    PVarchar.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength()));
        } else {
            assertNull(cell);
        }

        cell = result.getResult()
                .getColumnLatestCell(table.getColumns().get(5).getFamilyName().getBytes(),
                        table.getColumns().get(5).getColumnQualifierBytes());
        if (expectedDoc != null) {
            assertEquals(expectedDoc,
                    PBson.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength()));
        } else {
            assertNull(cell);
        }

        cell = result.getResult()
                .getColumnLatestCell(table.getColumns().get(6).getFamilyName().getBytes(),
                        table.getColumns().get(6).getColumnQualifierBytes());
        if (col4 != null) {
            assertEquals(col4,
                    PInteger.INSTANCE.toObject(cell.getValueArray(), cell.getValueOffset(),
                            cell.getValueLength()));
        } else {
            assertNull(cell);
        }
    }

    private static void validateReturnedRowAfterUpsert(Connection conn,
                                                       String upsertSql,
                                                       String tableName,
                                                       Double col1,
                                                       String col2,
                                                       boolean success,
                                                       BsonDocument inputDoc,
                                                       BsonDocument expectedDoc,
                                                       Integer col4)
            throws SQLException {
        final Pair<Integer, Tuple> resultPair;
        if (inputDoc != null) {
            PhoenixPreparedStatement ps =
                    conn.prepareStatement(upsertSql).unwrap(PhoenixPreparedStatement.class);
            ps.setObject(1, inputDoc);
            resultPair = ps.executeUpdateReturnRow();
        } else {
            resultPair = conn.createStatement().unwrap(PhoenixStatement.class)
                    .executeUpdateReturnRow(upsertSql);
        }
        assertEquals(success ? 1 : 0, resultPair.getFirst().intValue());
        ResultTuple result = (ResultTuple) resultPair.getSecond();
        PTable table = conn.unwrap(PhoenixConnection.class).getTable(tableName);

        Cell cell =
                result.getResult()
                        .getColumnLatestCell(table.getColumns().get(3).getFamilyName().getBytes(),
                                table.getColumns().get(3).getColumnQualifierBytes());
        validateReturnedRowResult(col1, col2, expectedDoc, col4, table, cell, result);
    }

    @Test
    public void testReturnRowResultForMultiPointLookup() throws Exception {
        Assume.assumeTrue("Set correct result to RegionActionResult on hbase versions " +
                "2.4.18+, 2.5.9+, and 2.6.0+", isSetCorrectResultEnabledOnHBase());

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String tableName = generateUniqueName();
            String ddl = "CREATE TABLE " + tableName
                    + "(PK VARCHAR PRIMARY KEY, COL1 DOUBLE, COL2 VARCHAR, COUNTER1 "
                    + "DOUBLE, COUNTER2 VARCHAR)";
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);

            addRows2(tableName, conn);

            PhoenixPreparedStatement ps = conn.prepareStatement(
                            "DELETE FROM " + tableName + " WHERE PK IN (?, ?, ?)")
                    .unwrap(PhoenixPreparedStatement.class);
            ps.setString(1, "pk001");
            ps.setString(2, "pk002");
            ps.setString(3, "pk003");
            validateReturnedRowAfterDelete(conn, ps, tableName, 2233.99, "col2_001", false, false,
                    null, 234);

            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertFalse(rs.next());
        }
    }

    private static void addRows2(String tableName, Connection conn) throws SQLException {
        String upsertSql;
        upsertSql =
                "UPSERT INTO " + tableName
                        + " (PK, COL1, COL2, COUNTER1, COUNTER2) VALUES('pk001', 122.34, "
                        + "'pk004', 23, 'col2_001')";
        conn.createStatement().execute(upsertSql);
        upsertSql =
                "UPSERT INTO " + tableName
                        + " (PK, COL1, COL2, COUNTER1, COUNTER2) VALUES('pk002', 122.34, "
                        + "'pk005', 23, 'col2_001')";
        conn.createStatement().execute(upsertSql);
        upsertSql =
                "UPSERT INTO " + tableName
                        + " (PK, COL1, COL2, COUNTER1, COUNTER2) VALUES('pk003', 122.34, "
                        + "'pk005', 23, 'col2_001')";
        conn.createStatement().execute(upsertSql);
    }

    @Test
    public void testReturnRowResultWithoutAutoCommit() throws Exception {
        Assume.assumeTrue("Set correct result to RegionActionResult on hbase versions " +
                "2.4.18+, 2.5.9+, and 2.6.0+", isSetCorrectResultEnabledOnHBase());

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String sample1 = getJsonString("json/sample_01.json");
        BsonDocument bsonDocument1 = RawBsonDocument.parse(sample1);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = generateUniqueName();
            String ddl = "CREATE TABLE " + tableName
                    + "(PK1 VARCHAR, PK2 DOUBLE NOT NULL, PK3 VARCHAR, COUNTER1 DOUBLE,"
                    + " COUNTER2 VARCHAR,"
                    + " COL3 BSON, COL4 INTEGER, CONSTRAINT pk PRIMARY KEY(PK1, PK2, PK3))";
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);

            String upsertSql = "UPSERT INTO " + tableName + " (PK1, PK2, PK3, COUNTER1, COL3, COL4)"
                    + " VALUES('pk000', -123.98, 'pk003', 1011.202, ?, 123) ON DUPLICATE KEY " +
                    "IGNORE";
            validateReturnedRowAfterUpsert(conn, upsertSql, tableName, 1011.202, null, true,
                    bsonDocument1, bsonDocument1, 123);
            throw new RuntimeException("Should not reach here");
        } catch (SQLException e) {
            Assert.assertEquals(SQLExceptionCode.AUTO_COMMIT_NOT_ENABLED.getErrorCode(),
                    e.getErrorCode());
        }
    }

    @Test
    public void testColumnsTimestampUpdateWithAllCombinations() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);

            String ddl = "create table " + tableName + "(pk varchar primary key, " +
                    "counter1 integer, counter2 integer, counter3 smallint, counter4 bigint, " +
                    "counter5 varchar)" + tableDDLOptions;
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);
            String dml = String.format("UPSERT INTO %s VALUES('abc', 0, 10, 100, 1000, 'NONE')",
                    tableName);
            int actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            String dql = "SELECT * from " + tableName;
            ResultSet rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());

            List<Long> oldTimestamps = getAllColumnsLatestCellTimestamp(conn, tableName);

            dml = "UPSERT INTO " + tableName + " VALUES ('abc', 0, 10) ON DUPLICATE KEY UPDATE " +
                    // conditional update with different value
                    "counter1 = CASE WHEN counter1 < 1 THEN counter1 + 1 ELSE counter1 END, " +
                    // conditional update with same value in ELSE clause (will not update timestamp)
                    "counter2 = CASE WHEN counter2 < 10 THEN counter2 + 1 ELSE counter2 END, " +
                    // intentional update with different value
                    "counter3 = counter3 + 100, " +
                    // intentional update with same value
                    "counter4 = counter4";
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(10, rs.getInt("counter2"));
            assertEquals(200, rs.getInt("counter3"));
            assertEquals(1000, rs.getInt("counter4"));
            assertEquals("NONE", rs.getString("counter5"));
            assertFalse(rs.next());

            List<Long> newTimestamps = getAllColumnsLatestCellTimestamp(conn, tableName);

            assertEquals(6, oldTimestamps.size());
            assertEquals(6, newTimestamps.size());
            assertEquals(oldTimestamps.get(2), newTimestamps.get(2)); // counter2 NOT updated
            assertEquals(oldTimestamps.get(5), newTimestamps.get(5)); // counter5 NOT updated
            assertTrue(oldTimestamps.get(0) < newTimestamps.get(0)
                    && oldTimestamps.get(1) < newTimestamps.get(1)
                    && oldTimestamps.get(3) < newTimestamps.get(3)
                    && oldTimestamps.get(4) < newTimestamps.get(4)); // other columns updated
        }
    }

    @Test
    public void testColumnsTimestampUpdateWithOneConditionalUpdate() throws Exception {
        Assume.assumeTrue("Set correct result to RegionActionResult on hbase versions " +
                "2.4.18+, 2.5.9+, and 2.6.0+", isSetCorrectResultEnabledOnHBase());

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);

            String ddl = "create table " + tableName +
                    "(pk varchar primary key, counter1 bigint, counter2 bigint)" + tableDDLOptions;
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);

            String dml;
            dml = String.format("UPSERT INTO %s VALUES('abc', 0, 100)", tableName);
            int actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            String dql = "SELECT * from " + tableName;
            ResultSet rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());

            List<Long> timestampList0 = getAllColumnsLatestCellTimestamp(conn, tableName);

            // Case 1: timestamps update with different value in WHEN-THEN-clause
            dml = String.format("UPSERT INTO %s(pk, counter1, counter2) VALUES ('abc', 0, 10) " +
                            "ON DUPLICATE KEY UPDATE " +
                            "counter1 = CASE WHEN counter1 < 1 THEN counter1 + 1 ELSE counter1 END",
                    tableName);
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(100, rs.getInt("counter2"));
            assertFalse(rs.next());

            List<Long> timestampList1 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertTrue(timestampList1.get(0) > timestampList0.get(0)
                    && timestampList1.get(1) > timestampList0.get(1));

            // Case 2: timestamps NOT update with same value in ELSE-clause
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(0, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(100, rs.getInt("counter2"));
            assertFalse(rs.next());

            List<Long> timestampList2 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertEquals(timestampList1.get(0), timestampList2.get(0)); // empty column NOT updated
            assertEquals(timestampList1.get(1), timestampList2.get(1)); // counter1 NOT updated

            // Case 3: timestamps update with different value in ELSE-clause
            dml = String.format("UPSERT INTO %s(pk, counter1, counter2) VALUES ('abc', 0, 10) " +
                            "ON DUPLICATE KEY UPDATE " +
                            "counter1 = CASE WHEN counter1 < 1 THEN counter1 ELSE counter1 + 1 END",
                    tableName);
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(2, rs.getInt("counter1"));
            assertEquals(100, rs.getInt("counter2"));
            assertFalse(rs.next());

            List<Long> timestampList3 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertTrue(timestampList3.get(0) > timestampList2.get(0)
                    && timestampList3.get(1) > timestampList2.get(1));

            // Case 4: timestamps update with same value in WHEN-THEN-clause
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(100, rs.getInt("counter2"));
            assertFalse(rs.next());

            List<Long> timestampList4 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertTrue(timestampList4.get(0) > timestampList3.get(0)
                    && timestampList4.get(1) > timestampList3.get(1));
        }
    }

    @Test
    public void testColumnsTimestampUpdateWithOneConditionalValuesUpdate() throws Exception {
        Assume.assumeTrue("Set correct result to RegionActionResult on hbase versions " +
                "2.4.18+, 2.5.9+, and 2.6.0+", isSetCorrectResultEnabledOnHBase());

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);

            String ddl = "create table " + tableName +
                    "(pk varchar primary key, counter1 integer, counter2 integer)" + tableDDLOptions;
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);

            String dml = String.format("UPSERT INTO %s VALUES('abc', 1, 100)", tableName);
            int actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            List<Long> timestampList0 = getAllColumnsLatestCellTimestamp(conn, tableName);

            // Case 1: timestamps update with same value in WHEN-THEN-clause
            dml = String.format("UPSERT INTO %s(pk, counter1, counter2) VALUES ('abc', 0, 10) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "counter1 = CASE WHEN counter2 <= 100 THEN 1 ELSE 0 END", tableName);
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            String dql = "SELECT * from " + tableName;
            ResultSet rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(100, rs.getInt("counter2"));
            assertFalse(rs.next());

            List<Long> timestampList1 = getAllColumnsLatestCellTimestamp(conn, tableName);

            assertTrue(timestampList0.get(0) < timestampList1.get(0)
                    && timestampList0.get(1) < timestampList1.get(1)); // counter1 updated
            assertEquals(timestampList0.get(2), timestampList1.get(2)); // counter2 NOT updated

            // Case 2: timestamps NOT update with same value in ELSE-clause
            dml = String.format("UPSERT INTO %s(pk, counter1, counter2) VALUES ('abc', 0, 10) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "counter1 = CASE WHEN counter2 > 100 THEN 0 ELSE 1 END", tableName);
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(0, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(100, rs.getInt("counter2"));
            assertFalse(rs.next());

            List<Long> timestampList2 = getAllColumnsLatestCellTimestamp(conn, tableName);

            assertEquals(timestampList1.get(0), timestampList2.get(0));
            assertEquals(timestampList1.get(1), timestampList2.get(1));
            assertEquals(timestampList1.get(2), timestampList2.get(2));
        }
    }

    @Test
    public void testColumnsTimestampUpdateWithMultipleConditionalUpdate() throws Exception {
        Assume.assumeTrue("Set correct result to RegionActionResult on hbase versions " +
                "2.4.18+, 2.5.9+, and 2.6.0+", isSetCorrectResultEnabledOnHBase());

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            String ddl = "create table " + tableName +
                    "(pk varchar primary key, counter1 integer, counter2 integer, approval " +
                    "varchar)" + tableDDLOptions;
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);

            String dml;
            dml = String.format("UPSERT INTO %s VALUES('abc', 0, 9, 'NONE')", tableName);
            int actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            List<Long> timestampList0 = getAllColumnsLatestCellTimestamp(conn, tableName);

            // Case 1: all columns timestamps updated
            dml = String.format("UPSERT INTO %s(pk, counter1, counter2) VALUES ('abc', 0, 10) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "counter1 = CASE WHEN counter1 < 1 THEN 1 ELSE counter1 END," +
                    "counter2 = CASE WHEN counter2 < 11 THEN counter2 + 1 ELSE counter2 END," +
                    "approval = CASE WHEN counter2 < 10 THEN 'NONE' " +
                    "WHEN counter2 < 11 THEN 'MANAGER_APPROVAL' " +
                    "ELSE approval END", tableName);
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            String dql = "SELECT * from " + tableName;
            ResultSet rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(10, rs.getInt("counter2"));
            assertEquals("NONE", rs.getString("approval"));
            assertFalse(rs.next());

            List<Long> timestampList1 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertTrue(timestampList1.get(0) > timestampList0.get(0)
                    && timestampList1.get(1) > timestampList0.get(1)
                    && timestampList1.get(2) > timestampList0.get(2)
                    && timestampList1.get(3) > timestampList0.get(3));

            // Case 2: timestamps of counter2 and approval updated
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(11, rs.getInt("counter2"));
            assertEquals("MANAGER_APPROVAL", rs.getString("approval"));
            assertFalse(rs.next());

            List<Long> timestampList2 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertEquals(timestampList1.get(1), timestampList2.get(1)); // counter1 NOT updated
            assertTrue(timestampList2.get(0) > timestampList1.get(0)
                    && timestampList2.get(2) > timestampList1.get(2)
                    && timestampList2.get(3) > timestampList1.get(3));

            // Case 3: all timestamps NOT updated, including empty column
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(0, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(11, rs.getInt("counter2"));
            assertEquals("MANAGER_APPROVAL", rs.getString("approval"));
            assertFalse(rs.next());

            List<Long> timestampList3 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertEquals(timestampList2.get(0), timestampList3.get(0));
            assertEquals(timestampList2.get(1), timestampList3.get(1));
            assertEquals(timestampList2.get(2), timestampList3.get(2));
            assertEquals(timestampList2.get(3), timestampList3.get(3));
        }
    }

    @Test
    public void testColumnsTimestampUpdateWithIntentionalUpdate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);

            String ddl = "create table " + tableName +
                    "(pk varchar primary key, counter1 bigint, counter2 bigint)" + tableDDLOptions;
            conn.createStatement().execute(ddl);
            createIndex(conn, tableName);

            String dml;
            dml = String.format("UPSERT INTO %s VALUES('abc', 0, 100)", tableName);
            int actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            List<Long> timestampList0 = getAllColumnsLatestCellTimestamp(conn, tableName);

            // Case 1: different value of one column
            dml = String.format("UPSERT INTO %s(pk, counter1, counter2) VALUES ('abc', 0, 10) " +
                            "ON DUPLICATE KEY UPDATE counter1 = counter1 + 1",
                    tableName);
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            String dql = "SELECT * from " + tableName;
            ResultSet rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(100, rs.getInt("counter2"));
            assertFalse(rs.next());

            List<Long> timestampList1 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertEquals(timestampList0.get(2), timestampList1.get(2)); // counter2 NOT updated
            assertTrue(timestampList1.get(0) > timestampList0.get(0)
                    && timestampList1.get(1) > timestampList0.get(1)); // updated columns

            // Case 2: same value of one column will also be updated
            dml = String.format("UPSERT INTO %s(pk, counter1, counter2) VALUES ('abc', 0, 10) " +
                            "ON DUPLICATE KEY UPDATE counter1 = counter1",
                    tableName);
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(100, rs.getInt("counter2"));
            assertFalse(rs.next());

            List<Long> timestampList2 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertEquals(timestampList2.get(2), timestampList1.get(2)); // counter2 NOT updated
            assertTrue(timestampList2.get(0) > timestampList1.get(0)
                    && timestampList2.get(1) > timestampList1.get(1));

            // Case 3: same value of one column, different of the other
            dml = String.format("UPSERT INTO %s(pk, counter1, counter2) VALUES ('abc', 0, 10) " +
                            "ON DUPLICATE KEY UPDATE counter1 = counter1, counter2 = counter2 + 1",
                    tableName);
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(101, rs.getInt("counter2"));
            assertFalse(rs.next());

            List<Long> timestampList3 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertTrue(timestampList3.get(0) > timestampList2.get(0)
                    && timestampList3.get(1) > timestampList2.get(1)
                    && timestampList3.get(2) > timestampList2.get(2)); // counter2

            // Case 4: same values of all columns will also be updated
            dml = String.format("UPSERT INTO %s(pk, counter1, counter2) VALUES ('abc', 0, 10) " +
                            "ON DUPLICATE KEY UPDATE counter1 = counter1, counter2 = counter2",
                    tableName);
            actualReturnValue = conn.createStatement().executeUpdate(dml);
            assertEquals(1, actualReturnValue);

            rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(101, rs.getInt("counter2"));
            assertFalse(rs.next());

            List<Long> timestampList4 = getAllColumnsLatestCellTimestamp(conn, tableName);
            assertTrue(timestampList4.get(0) > timestampList3.get(0)
                    && timestampList4.get(1) > timestampList3.get(1)
                    && timestampList4.get(2) > timestampList3.get(2));
        }
    }

    @Test
    public void testBatchedUpsertOnDupKeyAutoCommit() throws Exception {
        testBatchedUpsertOnDupKey(true);
    }

    @Test
    public void testBatchedUpsertOnDupKeyNoAutoCommit() throws Exception {
        testBatchedUpsertOnDupKey(false);
    }

    private void testBatchedUpsertOnDupKey(boolean autocommit) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(autocommit);

            Statement stmt = conn.createStatement();

            stmt.execute("create table " + tableName + "(pk varchar primary key, " +
                    "counter1 integer, counter2 integer, approval varchar)");
            createIndex(conn, tableName);

            stmt.execute("UPSERT INTO " + tableName + " VALUES('a', 0, 10, 'NONE')");
            conn.commit();

            stmt.addBatch("UPSERT INTO " + tableName +
                    " (pk, counter1, counter2) VALUES ('a', 0, 10) ON DUPLICATE KEY IGNORE");
            stmt.addBatch("UPSERT INTO " + tableName +
                    " (pk, counter1, counter2) VALUES ('a', 0, 10) ON DUPLICATE KEY UPDATE" +
                    " counter1 = CASE WHEN counter1 < 1 THEN 1 ELSE counter1 END");

            stmt.addBatch("UPSERT INTO " + tableName +
                    " (pk, counter1, counter2) VALUES ('b', 0, 9) ON DUPLICATE KEY IGNORE");
            String dml =  "UPSERT INTO " + tableName +
                    " (pk, counter1, counter2) VALUES ('b', 0, 10) ON DUPLICATE KEY UPDATE" +
                    " counter2 = CASE WHEN counter2 < 11 THEN counter2 + 1 ELSE counter2 END," +
                    " approval = CASE WHEN counter2 < 10 THEN 'NONE'" +
                    " WHEN counter2 < 11 THEN 'MANAGER_APPROVAL'" +
                    " ELSE approval END";
            stmt.addBatch(dml);
            stmt.addBatch(dml);
            stmt.addBatch(dml);

            int[] actualReturnValues = stmt.executeBatch();
            int[] expectedReturnValues = new int[]{1, 1, 1, 1, 1, 1};
            if (!autocommit) {
                conn.commit();
            }
            assertArrayEquals(expectedReturnValues, actualReturnValues);

            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            assertEquals("a", rs.getString("pk"));
            assertEquals(1, rs.getInt("counter1"));
            assertEquals(10, rs.getInt("counter2"));
            assertEquals("NONE", rs.getString("approval"));
            assertTrue(rs.next());
            assertEquals("b", rs.getString("pk"));
            assertEquals(0, rs.getInt("counter1"));
            assertEquals(11, rs.getInt("counter2"));
            assertEquals("MANAGER_APPROVAL", rs.getString("approval"));
            assertFalse(rs.next());
        }
    }

    private long getEmptyKVLatestCellTimestamp(String tableName) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        PTable pTable = PhoenixRuntime.getTable(conn, tableName);
        byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(pTable).getFirst();
        return getColumnLatestCellTimestamp(tableName, emptyCQ);
    }

    private long getColumnLatestCellTimestamp(String tableName, byte[] cq) throws Exception {
        Scan scan = new Scan();
        try (org.apache.hadoop.hbase.client.Connection hconn =
                     ConnectionFactory.createConnection(config)) {
            Table table = hconn.getTable(TableName.valueOf(tableName));
            ResultScanner resultScanner = table.getScanner(scan);
            Result result = resultScanner.next();
            return result.getColumnLatestCell(
                    QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, cq).getTimestamp();
        }
    }

    private List<Long> getAllColumnsLatestCellTimestamp(Connection conn, String tableName) throws Exception {
        List<Long> timestampList = new ArrayList<>();
        PTable pTable = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, tableName));
        List<PColumn> columns = pTable.getColumns();

        // timestamp of the empty cell
        timestampList.add(getEmptyKVLatestCellTimestamp(tableName));
        // timestamps of all other columns
        for (int i = 1; i < columns.size(); i++) {
            byte[] cq = columns.get(i).getColumnQualifierBytes();
            timestampList.add(getColumnLatestCellTimestamp(tableName, cq));
        }
        return timestampList;
    }

    private boolean isSetCorrectResultEnabledOnHBase() {
        // true for HBase 2.4.18+, 2.5.9+, and 2.6.0+ versions, false otherwise
        String hbaseVersion = VersionInfo.getVersion();
        String[] versionArr = hbaseVersion.split("\\.");
        int majorVersion = Integer.parseInt(versionArr[0]);
        int minorVersion = Integer.parseInt(versionArr[1]);
        int patchVersion = Integer.parseInt(versionArr[2].split("-")[0]);
        if (majorVersion > 2) {
            return true;
        }
        if (majorVersion < 2) {
            return false;
        }
        if (minorVersion >= 6) {
            return true;
        }
        if (minorVersion < 4) {
            return false;
        }
        if (minorVersion == 4) {
            return patchVersion >= 18;
        }
        return patchVersion >= 9;
    }

    private static String getJsonString(String jsonFilePath) throws IOException {
        URL fileUrl = OnDuplicateKey2IT.class.getClassLoader().getResource(jsonFilePath);
        return FileUtils.readFileToString(new File(fileUrl.getFile()), Charset.defaultCharset());
    }
}
