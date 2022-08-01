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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class PhoenixRowTimestampFunctionIT extends ParallelStatsDisabledIT {
    private final boolean encoded;
    private final boolean optimized;
    private final String tableDDLOptions;
    private static final int NUM_ROWS = 5;
    private static final long TS_OFFSET = 120000;

    public PhoenixRowTimestampFunctionIT(QualifierEncodingScheme encoding,
            ImmutableStorageScheme storage) {
        StringBuilder optionBuilder = new StringBuilder();
        this.optimized = storage == ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS
                            ? true : false;
        // We cannot have non encoded column names if the storage type is single cell
        this.encoded = (encoding != QualifierEncodingScheme.NON_ENCODED_QUALIFIERS)
                ? true : (this.optimized) ? true : false;

        if (this.optimized && encoding == QualifierEncodingScheme.NON_ENCODED_QUALIFIERS) {
            optionBuilder.append(" COLUMN_ENCODED_BYTES = " + QualifierEncodingScheme.ONE_BYTE_QUALIFIERS.ordinal());
        } else {
            optionBuilder.append(" COLUMN_ENCODED_BYTES = " + encoding.ordinal());
        }
        optionBuilder.append(", IMMUTABLE_STORAGE_SCHEME = "+ storage.toString());
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Parameterized.Parameters(name = "encoding={0},storage={1}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayList();
        for (QualifierEncodingScheme encoding : QualifierEncodingScheme.values()) {
            for (ImmutableStorageScheme storage : ImmutableStorageScheme.values()) {
                list.add(new Object[]{encoding, storage});
            }
        }
        return list;
    }

    private void verifyHbaseAllRowsTimestamp(String tableName, ResultSet rs, int expectedRowCount)
            throws Exception {

        Scan scan = new Scan();
        byte[] emptyKVQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(this.encoded).getFirst();
        try (org.apache.hadoop.hbase.client.Connection hconn =
                ConnectionFactory.createConnection(config)) {
            Table table = hconn.getTable(TableName.valueOf(tableName));
            ResultScanner resultScanner = table.getScanner(scan);
            int rowCount = 0;
            while (rs.next()) {
                Result result = resultScanner.next();
                long timeStamp = result.getColumnLatestCell(
                        QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                        emptyKVQualifier).getTimestamp();
                assertEquals(rs.getDate(1).getTime(), timeStamp);
                ++rowCount;
            }
            assertEquals(expectedRowCount, rowCount);
        }
    }

    private void verifyHbaseRowTimestamp(String tableName, String rowKey, Date expectedTimestamp)
            throws Exception {

        byte[] emptyKVQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(this.encoded).getFirst();
        try (org.apache.hadoop.hbase.client.Connection hconn =
                ConnectionFactory.createConnection(config)) {
            Table table = hconn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytesBinary(rowKey));
            Result result = table.get(get);
            assertFalse(result.isEmpty());
            long timeStamp = result.getColumnLatestCell(
                    QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, emptyKVQualifier).getTimestamp();
            assertEquals(expectedTimestamp.getTime(), timeStamp);
        }
    }

    private String createTestData(long rowTimestamp, int numRows) throws Exception {
        String tableName =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create a test table.
            try (Statement stmt = conn.createStatement()) {
                String ddl = "CREATE TABLE IF NOT EXISTS " + tableName +
                        " (PK1 INTEGER NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR, KV2 VARCHAR" +
                        " CONSTRAINT PK PRIMARY KEY(PK1, PK2))" + this.tableDDLOptions;
                stmt.execute(ddl);
            }

            // Upsert data into the test table.
            String dml = "UPSERT INTO " + tableName + " (PK1, PK2, KV1, KV2) VALUES (?, ?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(dml)) {
                Date rowTimestampDate = new Date(rowTimestamp);
                int count = numRows;
                for (int id = 0; id < count; ++id) {
                    int idValue = id;
                    stmt.setInt(1, idValue);
                    stmt.setDate(2, rowTimestampDate);
                    stmt.setString(3, "KV1_" + idValue);
                    stmt.setString(4, "KV2_" + idValue);
                    stmt.executeUpdate();
                }
            }
            conn.commit();
        }
        return tableName;
    }

    @Test
    public void testRowTimestampDefault() throws Exception {
        if (encoded || optimized) return;
        String tableName =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE IF NOT EXISTS " + tableName
                    + " (PK INTEGER NOT NULL PRIMARY KEY, KV1 VARCHAR, KV2 VARCHAR)"
                    + this.tableDDLOptions;
            conn.createStatement().execute(ddl);

            String dml = "UPSERT INTO " + tableName + " (PK, KV1, KV2) VALUES (?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(dml)) {
                int count = NUM_ROWS;
                for (int id = 0; id < count; ++id) {
                    stmt.setInt(1, id);
                    stmt.setString(2, "KV1_" + id);
                    stmt.setString(3, "KV2_" + id);
                    stmt.executeUpdate();
                }
            } finally {
                conn.commit();
            }

            // verify row timestamp returned by the query matches the empty column cell timestamp
            String dql = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + tableName;
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(dql);
                verifyHbaseAllRowsTimestamp(tableName, rs, NUM_ROWS);
            }

            // update one row
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("UPSERT INTO " + tableName
                        + " (PK, KV1) VALUES (2, 'KV1_foo')");
            } finally {
                conn.commit();
            }

            // verify again after update
            // verify row timestamp returned by the query matches the empty column cell timestamp
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(dql);
                verifyHbaseAllRowsTimestamp(tableName, rs, NUM_ROWS);
            }

            dql = "SELECT ROWKEY_BYTES_STRING(), PHOENIX_ROW_TIMESTAMP() FROM " + tableName
                    + " WHERE PK >= 1 AND PK <=3 ";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(dql);
                while (rs.next()) {
                    verifyHbaseRowTimestamp(tableName,
                            rs.getString(1), rs.getDate(2));
                }
            }
        }
    }

    @Test
    public void testRowTimestampColumn() throws Exception {
        String tableName =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = "CREATE TABLE IF NOT EXISTS " + tableName
                    + " (PK1 INTEGER NOT NULL, PK2 DATE NOT NULL, KV1 VARCHAR, KV2 VARCHAR"
                    + " CONSTRAINT PK PRIMARY KEY(PK1, PK2 ROW_TIMESTAMP))" + this.tableDDLOptions;
            conn.createStatement().execute(ddl);

            String dml = "UPSERT INTO " + tableName + " (PK1, PK2, KV1, KV2) VALUES (?, ?, ?, ?)";

            long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis();
            Date rowTimestampDate = new Date(rowTimestamp);
            try (PreparedStatement stmt = conn.prepareStatement(dml)) {
                int count = NUM_ROWS;
                for (int id = 0; id < count; ++id) {
                    stmt.setInt(1, id);
                    stmt.setDate(2, rowTimestampDate);
                    stmt.setString(3, "KV1_" + id);
                    stmt.setString(4, "KV2_" + id);
                    stmt.executeUpdate();
                }
            } finally {
                conn.commit();
            }

            String dql = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + tableName;
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(dql);
                while (rs.next()) {
                    assertEquals(rs.getDate(1), rowTimestampDate);
                }
            }
        }
    }


    @Test
    // case: No rows should have the phoenix_row_timestamp() = date column
    // Since we used a future date for column PK2
    public void testRowTimestampFunctionAndEqualPredicate() throws Exception {
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() + TS_OFFSET;
        String tableName = createTestData(rowTimestamp, NUM_ROWS);
        // With phoenix_row_timestamp function only in projection
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() = PK2 ";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                assertFalse(rs.next());
                rs.close();
            }
        }

        // With phoenix_row_timestamp function and additional columns in projection
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql = "SELECT PHOENIX_ROW_TIMESTAMP(), KV1 FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() = PK2 ";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                assertFalse(rs.next());
                rs.close();
            }
        }
    }


    @Test
    // case: All rows selected should have the phoenix_row_timestamp() < date column
    // Since we used a future date for column PK2
    public void testRowTimestampFunctionOnlyWithLessThanPredicate() throws Exception {
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() + TS_OFFSET;
        String tableName = createTestData(rowTimestamp, NUM_ROWS);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() < PK2 ";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                int actualCount = 0;
                while(rs.next()) {
                    assertTrue(rs.getDate(1).before(new Date(rowTimestamp)));
                    actualCount++;
                }
                assertEquals(NUM_ROWS, actualCount);
                rs.close();
            }
        }
    }

    @Test
    // case: All rows selected should have the phoenix_row_timestamp() < date column
    // Since we used a future date for column PK2
    // Additional columns should return non null values.
    public void testRowTimestampFunctionAndAdditionalColsWithLessThanPredicate() throws Exception {
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() + TS_OFFSET;
        String tableName = createTestData(rowTimestamp, NUM_ROWS);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql = "SELECT PHOENIX_ROW_TIMESTAMP(), KV2 FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() < PK2 ";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                int actualCount = 0;
                while(rs.next()) {
                    assertTrue(rs.getDate(1).before(new Date(rowTimestamp)));
                    rs.getString(2);
                    assertFalse(rs.wasNull());
                    actualCount++;
                }
                assertEquals(NUM_ROWS, actualCount);
                rs.close();
            }
        }

    }

    @Test
    // case: All rows selected should have the phoenix_row_timestamp() > date column
    // Since we used a past date for column PK2
    public void testRowTimestampFunctionOnlyWithGreaterThanPredicate() throws Exception {
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() - TS_OFFSET;
        String tableName = createTestData(rowTimestamp, NUM_ROWS);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() > PK2 ";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                int actualCount = 0;
                while(rs.next()) {
                    assertTrue(rs.getDate(1).after(new Date(rowTimestamp)));
                    actualCount++;
                }
                assertEquals(NUM_ROWS, actualCount);
                rs.close();
            }
        }
    }

    @Test
    // case: All rows selected should have the phoenix_row_timestamp() > date column
    // Since we used a past date for column PK2
    // Additional columns should return non null values.
    public void testRowTimestampFunctionAndColsWithGreaterThanPredicate() throws Exception {
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() - TS_OFFSET;
        String tableName = createTestData(rowTimestamp, NUM_ROWS);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql = "SELECT PHOENIX_ROW_TIMESTAMP(), KV1 FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() > PK2 ";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                int actualCount = 0;
                while(rs.next()) {
                    assertTrue(rs.getDate(1).after(new Date(rowTimestamp)));
                    rs.getString(2);
                    assertFalse(rs.wasNull());
                    actualCount++;
                }
                assertEquals(NUM_ROWS, actualCount);
                rs.close();
            }
        }
    }

    @Test
    // case: All rows selected should have the phoenix_row_timestamp() > date column
    // Since we used a past date for column PK2
    // Projected columns should return non null and expected values.
    public void testSimpleSelectColsWithPhoenixRowTimestampPredicate() throws Exception {
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() - TS_OFFSET;
        String tableName = createTestData(rowTimestamp, NUM_ROWS);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql = "SELECT KV1 FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() > PK2 ";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                int actualCount = 0;
                while(rs.next()) {
                    String kv1Value = rs.getString(1);
                    assertFalse(rs.wasNull());
                    assertTrue(kv1Value.substring(0, "KV2_".length())
                            .compareToIgnoreCase("KV1_") == 0);
                    actualCount++;
                }
                assertEquals(NUM_ROWS, actualCount);
                rs.close();
            }
        }
    }

    @Test
    // case: Aggregate SQLs work with PhoenixRowTimestamp predicate.
    public void testSelectCountWithPhoenixRowTimestampPredicate() throws Exception {
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() - TS_OFFSET;
        String tableName = createTestData(rowTimestamp, NUM_ROWS);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql = "SELECT COUNT(*) FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() > PK2 ";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                while(rs.next()) {
                    int rowCount = rs.getInt(1);
                    assertFalse(rs.wasNull());
                    assertTrue(rowCount == NUM_ROWS);
                }
                rs.close();
            }
        }
    }

    @Test
    // case: Select with non primary keys in where clause.
    public void testSelectWithMultiplePredicates() throws Exception {
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() - TS_OFFSET;
        String tableName = createTestData(rowTimestamp, NUM_ROWS);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String sql = "SELECT COUNT(*) FROM " + tableName +
                    " WHERE PHOENIX_ROW_TIMESTAMP() > PK2 AND KV1 = 'KV1_1'";
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(sql);
                while(rs.next()) {
                    int rowCount = rs.getInt(1);
                    assertFalse(rs.wasNull());
                    assertTrue(rowCount == 1);
                }
                rs.close();
            }
        }
    }


    @Test
    // case: Comparision with TO_TIME()
    public void testTimestampComparePredicate() throws Exception {
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() - TS_OFFSET;
        String tableName = createTestData(rowTimestamp, NUM_ROWS);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT COUNT(*) FROM " + tableName +
                        " WHERE ((PHOENIX_ROW_TIMESTAMP() > PK2) AND " +
                        " (PHOENIX_ROW_TIMESTAMP() > TO_TIME('2005-10-01 14:03:22.559')))";
                ResultSet rs = stmt.executeQuery(sql);
                while(rs.next()) {
                    int rowCount = rs.getInt(1);
                    assertFalse(rs.wasNull());
                    assertTrue(rowCount == NUM_ROWS);
                }
                rs.close();
            }
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT COUNT(*) FROM " + tableName +
                        " WHERE ((PHOENIX_ROW_TIMESTAMP() > PK2) AND " +
                        " (PHOENIX_ROW_TIMESTAMP() < TO_TIME('2005-10-01 14:03:22.559')))";
                ResultSet rs = stmt.executeQuery(sql);
                while(rs.next()) {
                    int rowCount = rs.getInt(1);
                    assertFalse(rs.wasNull());
                    assertTrue(rowCount == 0);
                }
                rs.close();
            }

        }
    }

    @Test
    // case: PHOENIX_ROW_TIMESTAMP() in select clause when aggregating should fail.
    public void testPhoenixRowTimestampWhenAggShouldFail() throws Exception {
        // Do not need to run for all test combinations
        if (encoded || !optimized) {
            return;
        }
        long rowTimestamp = EnvironmentEdgeManager.currentTimeMillis() - TS_OFFSET;
        String tableName = createTestData(rowTimestamp, NUM_ROWS);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (Statement stmt = conn.createStatement()) {
                String sql = "SELECT PHOENIX_ROW_TIMESTAMP(), PK1, COUNT(*) FROM " + tableName +
                        " WHERE ((PHOENIX_ROW_TIMESTAMP() > PK2) AND " +
                        " (PHOENIX_ROW_TIMESTAMP() > TO_TIME('2005-10-01 14:03:22.559')))" +
                        " GROUP BY PHOENIX_ROW_TIMESTAMP(), PK1";
                try {
                    ResultSet rs = stmt.executeQuery(sql);
                    fail();
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("ERROR 1018 (42Y27"));
                }
            }
        }
    }

    @Test
    public void testPhoenixRowTimestampWithWildcard() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (pk1 integer not null primary key, x.v1 float, y.v2 float, z.v3 float)" + this.tableDDLOptions);
            conn.createStatement().execute("upsert into " + dataTableName + " values(rand() * 100000000, rand(), rand(), rand())");
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("SELECT v1 from " + dataTableName);
            assertTrue(rs.next());
            float v1 = rs.getFloat(1);
            rs = conn.createStatement().executeQuery("SELECT * from " + dataTableName + " order by phoenix_row_timestamp()");
            assertTrue(rs.next());
            System.out.println(v1);
            assertTrue(v1 == rs.getFloat(2));
        }
    }

}
