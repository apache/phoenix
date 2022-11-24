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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class RowTimestampIT extends ParallelStatsDisabledIT {

    private final boolean mutable;
    private final String sortOrder;
    private final String tableDDLOptions;

    public RowTimestampIT(boolean mutable, boolean ascending) {
        StringBuilder optionBuilder = new StringBuilder("UPDATE_CACHE_FREQUENCY=600000");
        this.mutable = mutable;
        this.sortOrder = !ascending ? "DESC" : "";
        if (!mutable) {
            optionBuilder.append(", IMMUTABLE_ROWS=true");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    // name is used by failsafe as file name in reports
    @Parameters(name = "RowTimestampIT_mutable={0},ascending={1}")
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(
            new Boolean[][] { { false, false }, { false, true }, { true, false }, { true, true } });
    }

    @Test
    public void testUpsertingRowTimestampColSpecifiedWithTimestamp() throws Exception {
        upsertingRowTimestampColSpecified("TIMESTAMP");
    }

    @Test
    public void testUpsertingRowTimestampColSpecifiedWithDate() throws Exception {
        upsertingRowTimestampColSpecified("DATE");
    }

    private void upsertingRowTimestampColSpecified(String type) throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("CREATE TABLE IF NOT EXISTS " + tableName
                            + " (PK1 VARCHAR NOT NULL, PK2 " + type + " NOT NULL, KV1 VARCHAR, KV2 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2 "
                            + sortOrder + " ROW_TIMESTAMP)) " + tableDDLOptions);
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexName + " ON  "
                    + tableName + "  (PK2, KV1) INCLUDE (KV2)");
            if (mutable) {
                fail("Should not be able to create an index on a mutable table that has a ROW_TIMESTAMP column");
            }
        } catch (SQLException e) {
            if (mutable) {
                assertEquals(SQLExceptionCode.CANNOT_CREATE_INDEX_ON_MUTABLE_TABLE_WITH_ROWTIMESTAMP
                        .getErrorCode(),
                    e.getErrorCode());
            } else {
                throw e;
            }
        }
        Thread.sleep(1000);
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis();
        Date rowTimestampDate = new Date(rowTimestamp);
        Properties props = new Properties();
        long scn = rowTimestamp-500;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // The timestamp of the put will be the value of the row_timestamp column.
            PreparedStatement stmt =
                    conn.prepareStatement(
                        "UPSERT INTO  " + tableName + "  (PK1, PK2, KV1, KV2) VALUES (?, ?, ?, ?)");
            stmt.setString(1, "PK1");
            stmt.setDate(2, rowTimestampDate);
            stmt.setString(3, "KV1");
            stmt.setString(4, "KV2");
            stmt.executeUpdate();
            conn.commit();
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(rowTimestamp));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            // Verify that a connection with rowTimestamp  isn't able to see the data
            // inserted above. 
            PreparedStatement stmt =
                    conn.prepareStatement(
                        "SELECT * FROM  " + tableName + "  WHERE PK1 = ? AND PK2 = ?");
            stmt.setString(1, "PK1");
            stmt.setDate(2, rowTimestampDate);
            ResultSet rs = stmt.executeQuery();
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(tableName));
            assertFalse(rs.next());

            if (!mutable) {
                // Same holds when querying the index table too
                stmt = conn.prepareStatement("SELECT KV1 FROM  " + tableName + "  WHERE PK2 = ?");
                stmt.setDate(1, rowTimestampDate);
                rs = stmt.executeQuery();
                plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
                assertTrue(plan.getTableRef().getTable().getName().getString().equals(indexName));
                assertFalse(rs.next());
            }
        }

        // verify that the timestamp of the keyvalues matches the ROW_TIMESTAMP column value
        Scan scan = new Scan();
        byte[] emptyKVQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(true).getFirst();
        org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionFactory.createConnection(getUtility().getConfiguration());
        Table hTable = hbaseConn.getTable(TableName.valueOf(tableName));
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            long timeStamp = result.getColumnLatestCell(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, emptyKVQualifier).getTimestamp();
            assertEquals(rowTimestampDate.getTime(), timeStamp);
        }
        if (!mutable) {
            hTable = hbaseConn.getTable(TableName.valueOf(indexName));
             resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                long timeStamp = result.getColumnLatestCell(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, emptyKVQualifier).getTimestamp();
                assertEquals(rowTimestampDate.getTime(), timeStamp);
            }
        }

        // Verify now that if the connection is at an SCN beyond the rowtimestamp then we can indeed
        // see the data that we upserted above.
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(rowTimestamp + 1));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PreparedStatement stmt =
                    conn.prepareStatement(
                        "SELECT * FROM  " + tableName + "  WHERE PK1 = ? AND PK2 = ?");
            stmt.setString(1, "PK1");
            stmt.setDate(2, rowTimestampDate);
            ResultSet rs = stmt.executeQuery();
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(tableName));
            assertTrue(rs.next());
            assertEquals("PK1", rs.getString("PK1"));
            assertEquals(rowTimestampDate, rs.getDate("PK2"));
            assertEquals("KV1", rs.getString("KV1"));

            if (!mutable) {
                // Data visible when querying the index table too.
                stmt =
                        conn.prepareStatement(
                            "SELECT KV2 FROM  " + tableName + "  WHERE PK2 = ? AND KV1 = ?");
                stmt.setDate(1, rowTimestampDate);
                stmt.setString(2, "KV1");
                rs = stmt.executeQuery();
                plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
                assertTrue(plan.getTableRef().getTable().getName().getString().equals(indexName));
                assertTrue(rs.next());
                assertEquals("KV2", rs.getString("KV2"));
            }
        }
    }

    @Test
    public void testAutomaticallySettingRowTimestampWithTimestamp () throws Exception {
        automaticallySettingRowTimestampForImmutableTableAndIndexes("TIMESTAMP");
    }

    @Test
    public void testAutomaticallySettingRowTimestampWithDate () throws Exception {
        automaticallySettingRowTimestampForImmutableTableAndIndexes("DATE");
    }

    private void automaticallySettingRowTimestampForImmutableTableAndIndexes(String type) throws Exception {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("CREATE TABLE IF NOT EXISTS " + tableName
                            + " (PK1 VARCHAR NOT NULL, PK2 " + type + " NOT NULL, KV1 VARCHAR, KV2 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2 "
                            + sortOrder + " ROW_TIMESTAMP)) " + tableDDLOptions);
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexName + " ON  "
                    + tableName + "  (PK2, KV1) INCLUDE (KV2)");
            if (mutable) {
                fail("Should not be able to create an index on a mutable table that has a ROW_TIMESTAMP column");
            }
        } catch (SQLException e) {
            if (mutable) {
                assertEquals(SQLExceptionCode.CANNOT_CREATE_INDEX_ON_MUTABLE_TABLE_WITH_ROWTIMESTAMP
                        .getErrorCode(),
                    e.getErrorCode());
            } else {
                throw e;
            }
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Upsert values where row_timestamp column PK2 is not set and the column names are
            // specified
            // This should upsert data with the value for PK2 as the s
            PreparedStatement stmt =
                    conn.prepareStatement(
                        "UPSERT INTO  " + tableName + " (PK1, KV1, KV2) VALUES (?, ?, ?)");
            stmt.setString(1, "PK1");
            stmt.setString(2, "KV1");
            stmt.setString(3, "KV2");
            stmt.executeUpdate();
            conn.commit();
        }
        long endTime = EnvironmentEdgeManager.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Now query for data that was upserted above. If the row key was generated correctly
            // then we should be able to see
            // the data in this query.
            PreparedStatement stmt =
                    conn.prepareStatement("SELECT KV1, KV2, PK2 FROM " + tableName
                            + " WHERE PK1 = ? AND PK2 >= ? AND PK2 <= ? ");
            stmt.setString(1, "PK1");
            stmt.setDate(2, new Date(startTime));
            stmt.setDate(3, new Date(endTime));
            ResultSet rs = stmt.executeQuery();
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(tableName));
            assertTrue(rs.next());
            assertEquals("KV1", rs.getString(1));
            assertEquals("KV2", rs.getString(2));
            Date rowTimestampDate = rs.getDate(3);
            assertFalse(rs.next());
            
            // verify that the timestamp of the keyvalues matches the ROW_TIMESTAMP column value
            Scan scan = new Scan();
            byte[] emptyKVQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(true).getFirst();
            org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionFactory.createConnection(getUtility().getConfiguration());
            Table hTable = hbaseConn.getTable(TableName.valueOf(tableName));
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                long timeStamp = result.getColumnLatestCell(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, emptyKVQualifier).getTimestamp();
                assertEquals(rowTimestampDate.getTime(), timeStamp);
            }
            if (!mutable) {
                hTable = hbaseConn.getTable(TableName.valueOf(indexName));
                 resultScanner = hTable.getScanner(scan);
                for (Result result : resultScanner) {
                    long timeStamp = result.getColumnLatestCell(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, emptyKVQualifier).getTimestamp();
                    assertEquals(rowTimestampDate.getTime(), timeStamp);
                }
            }

            if (!mutable) {
                // Verify now that the data was correctly added to the immutable index too.
                stmt =
                        conn.prepareStatement(
                            "SELECT KV2 FROM " + tableName + " WHERE PK2 = ? AND KV1 = ?");
                stmt.setDate(1, rowTimestampDate);
                stmt.setString(2, "KV1");
                rs = stmt.executeQuery();
                plan = stmt.unwrap(PhoenixStatement.class).getQueryPlan();
                assertTrue(plan.getTableRef().getTable().getName().getString().equals(indexName));
                assertTrue(rs.next());
                assertEquals("KV2", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testComparisonOperatorsOnRowTimestampCol() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("CREATE TABLE IF NOT EXISTS " + tableName
                            + " (PK1 VARCHAR NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2 "
                            + sortOrder + " ROW_TIMESTAMP)) " + tableDDLOptions);
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(upsert);
            stmt.setString(1, "a");
            stmt.setDate(2, new Date(10));
            stmt.setString(3, "KV");
            stmt.executeUpdate();
            stmt.setString(1, "b");
            stmt.setDate(2, new Date(20));
            stmt.setString(3, "KV");
            stmt.executeUpdate();
            stmt.setString(1, "c");
            stmt.setDate(2, new Date(30));
            stmt.setString(3, "KV");
            stmt.executeUpdate();
            stmt.setString(1, "d");
            stmt.setDate(2, new Date(40));
            stmt.setString(3, "KV");
            stmt.executeUpdate();
            conn.commit();
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            assertNumRecords(3, "SELECT count(*) from " + tableName + " WHERE PK2 > ?", conn,
                new Date(10));
            assertNumRecords(1, "SELECT count(*) from " + tableName + " WHERE PK2 < ? AND PK2 > ?",
                conn, new Date(30), new Date(10));
            assertNumRecords(3,
                "SELECT count(*) from " + tableName + " WHERE PK2 <= ? AND PK2 >= ?", conn,
                new Date(30), new Date(10));
            assertNumRecords(2, "SELECT count(*) from " + tableName + " WHERE PK2 <= ? AND PK2 > ?",
                conn, new Date(30), new Date(10));
            assertNumRecords(2, "SELECT count(*) from " + tableName + " WHERE PK2 < ? AND PK2 >= ?",
                conn, new Date(30), new Date(10));
            assertNumRecords(0, "SELECT count(*) from " + tableName + " WHERE PK2 < ?", conn,
                new Date(10));
            assertNumRecords(4, "SELECT count(*) from " + tableName, conn);
        }
    }

    private void assertNumRecords(int count, String sql, Connection conn, Date... params)
            throws Exception {
        PreparedStatement stmt = conn.prepareStatement(sql);
        int counter = 1;
        for (Date param : params) {
            stmt.setDate(counter++, param);
        }
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(count, rs.getInt(1));
    }

    @Test
    public void testDisallowNegativeValuesForRowTsColumn() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("CREATE TABLE " + tableName + " (PK1 DATE NOT NULL PRIMARY KEY "
                            + sortOrder + " ROW_TIMESTAMP, KV1 VARCHAR) " + tableDDLOptions);
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Date d = new Date(-100);
            PreparedStatement stmt =
                    conn.prepareStatement(
                        "UPSERT INTO " + tableName + " VALUES (?, ?) ");
            stmt.setDate(1, d);
            stmt.setString(2, "KV1");
            stmt.executeUpdate();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), e.getErrorCode());
        }
    }
}
