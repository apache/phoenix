/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class IndexIT extends ParallelStatsDisabledIT {

    private final boolean localIndex;
    private final boolean transactional;
    private final boolean mutable;
    private final String tableDDLOptions;

    public IndexIT(boolean localIndex, boolean mutable, boolean transactional, boolean columnEncoded) {
        this.localIndex = localIndex;
        this.transactional = transactional;
        this.mutable = mutable;
        StringBuilder optionBuilder = new StringBuilder();
        if (!columnEncoded) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        if (!mutable) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append("IMMUTABLE_ROWS=true");
            if (!columnEncoded) {
                optionBuilder.append(",IMMUTABLE_STORAGE_SCHEME="+PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
            }
        }
        if (transactional) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append(" TRANSACTIONAL=true ");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Parameters(name="IndexIT_localIndex={0},mutable={1},transactional={2},columnEncoded={3}") // name is used by failsafe as file name in reports
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false, false, false, false }, { false, false, false, true }, { false, false, true, false }, { false, false, true, true }, 
                { false, true, false, false }, { false, true, false, true }, { false, true, true, false }, { false, true, true, true }, 
                { true, false, false, false }, { true, false, false, true }, { true, false, true, false }, { true, false, true, true }, 
                { true, true, false, false }, { true, true, false, true }, { true, true, true, false }, { true, true, true, true } 
           });
    }

    @Test
    public void testIndexWithNullableFixedWithCols() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            BaseTest.populateTestTable(fullTableName);
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName
                    + " (char_col1 ASC, int_col1 ASC)"
                    + " INCLUDE (long_col1, long_col2)";
            stmt.execute(ddl);

            String query = "SELECT d.char_col1, int_col1 from " + fullTableName + " as d";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex) {
                assertEquals(
                        "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + tableName + " [1]\n" +
                                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                                "CLIENT MERGE SORT",
                                QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + indexName + "\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals("chara", rs.getString("char_col1"));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertFalse(rs.next());

            conn.createStatement().execute("DROP INDEX " + indexName + " ON " + fullTableName);

            query = "SELECT char_col1, int_col1 from " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());

            query = "SELECT char_col1, int_col1 from "+indexName;
            try{
                rs = conn.createStatement().executeQuery(query);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.TABLE_UNDEFINED.getErrorCode(), e.getErrorCode());
            }
        }
    }

    @Test
    public void testDeleteFromAllPKColumnIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            BaseTest.populateTestTable(fullTableName);
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName
                        + " (long_pk, varchar_pk)"
                        + " INCLUDE (long_col1, long_col2)";
            stmt.execute(ddl);

            ResultSet rs;

            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));

            String dml = "DELETE from " + fullTableName + " WHERE long_col2 = 4";
            assertEquals(1,conn.createStatement().executeUpdate(dml));
            conn.commit();

            String query = "SELECT /*+ NO_INDEX */ long_pk FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong(1));
            assertFalse(rs.next());

            query = "SELECT long_pk FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong(1));
            assertFalse(rs.next());

            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong(1));
            assertFalse(rs.next());

            conn.createStatement().execute("DROP INDEX " + indexName + " ON " + fullTableName);
        }
    }

    @Test
    public void testCreateIndexAfterUpsertStarted() throws Exception {
        testCreateIndexAfterUpsertStarted(false, 
                SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, generateUniqueName()),
                SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, generateUniqueName()));
    }

    @Test
    public void testCreateIndexAfterUpsertStartedTxnl() throws Exception {
        if (transactional) {
            testCreateIndexAfterUpsertStarted(true, 
                    SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, generateUniqueName()),
                    SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, generateUniqueName()));
        }
    }

    private void testCreateIndexAfterUpsertStarted(boolean readOwnWrites, String fullTableName, String fullIndexName) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn1 = DriverManager.getConnection(getUrl(), props)) {
            conn1.setAutoCommit(true);
            String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            Statement stmt1 = conn1.createStatement();
            stmt1.execute(ddl);
            BaseTest.populateTestTable(fullTableName);

            ResultSet rs;

            rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));

            try (Connection conn2 = DriverManager.getConnection(getUrl(), props)) {

                String upsert = "UPSERT INTO " + fullTableName
                        + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement pstmt2 = conn2.prepareStatement(upsert);
                pstmt2.setString(1, "varchar4");
                pstmt2.setString(2, "char4");
                pstmt2.setInt(3, 4);
                pstmt2.setLong(4, 4L);
                pstmt2.setBigDecimal(5, new BigDecimal(4.0));
                Date date = DateUtil.parseDate("2015-01-01 00:00:00");
                pstmt2.setDate(6, date);
                pstmt2.setString(7, "varchar_a");
                pstmt2.setString(8, "chara");
                pstmt2.setInt(9, 2);
                pstmt2.setLong(10, 2L);
                pstmt2.setBigDecimal(11, new BigDecimal(2.0));
                pstmt2.setDate(12, date);
                pstmt2.setString(13, "varchar_b");
                pstmt2.setString(14, "charb");
                pstmt2.setInt(15, 3);
                pstmt2.setLong(16, 3L);
                pstmt2.setBigDecimal(17, new BigDecimal(3.0));
                pstmt2.setDate(18, date);
                pstmt2.executeUpdate();

                if (readOwnWrites) {
                    String query = "SELECT long_pk FROM " + fullTableName + " WHERE long_pk=4";
                    rs = conn2.createStatement().executeQuery(query);
                    assertTrue(rs.next());
                    assertFalse(rs.next());
                }

                String indexName = SchemaUtil.getTableNameFromFullName(fullIndexName);
                ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName
                        + " (long_pk, varchar_pk)"
                        + " INCLUDE (long_col1, long_col2)";
                stmt1.execute(ddl);

                /*
                 * Commit upsert after index created through different connection.
                 * This forces conn2 (which doesn't know about the index yet) to update the metadata
                 * at commit time, recognize the new index, and generate the correct metadata (or index
                 * rows for immutable indexes).
                 *
                 * For transactional data, this is problematic because the index
                 * gets a timestamp *after* the commit timestamp of conn2 and thus won't be seen during
                 * the commit. Also, when the index is being built, the data hasn't yet been committed
                 * and thus won't be part of the initial index build (fixed by PHOENIX-2446).
                 */
                conn2.commit();

                stmt1 = conn1.createStatement();
                rs = stmt1.executeQuery("SELECT COUNT(*) FROM " + fullTableName);
                assertTrue(rs.next());
                assertEquals(4,rs.getInt(1));
                assertEquals(fullIndexName, stmt1.unwrap(PhoenixStatement.class).getQueryPlan().getTableRef().getTable().getName().getString());

                String query = "SELECT /*+ NO_INDEX */ long_pk FROM " + fullTableName;
                rs = conn1.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong(1));
                assertTrue(rs.next());
                assertEquals(2L, rs.getLong(1));
                assertTrue(rs.next());
                assertEquals(3L, rs.getLong(1));
                assertTrue(rs.next());
                assertEquals(4L, rs.getLong(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testDeleteFromNonPKColumnIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            BaseTest.populateTestTable(fullTableName);
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName
                        + " (long_col1, long_col2)"
                        + " INCLUDE (decimal_col1, decimal_col2)";
            stmt.execute(ddl);
        }
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            ResultSet rs;

            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));

            String dml = "DELETE from " + fullTableName + " WHERE long_col2 = 4";
            assertEquals(1,conn.createStatement().executeUpdate(dml));
            conn.commit();

            // query the data table
            String query = "SELECT /*+ NO_INDEX */ long_pk FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong(1));
            assertFalse(rs.next());

            // query the index table
            query = "SELECT long_pk FROM " + fullTableName + " ORDER BY long_col1";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong(1));
            assertFalse(rs.next());

            conn.createStatement().execute("DROP INDEX " + indexName + " ON " + fullTableName);
        }
    }

    @Test
    public void testGroupByCount() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            BaseTest.populateTestTable(fullTableName);
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName + " (int_col2)";
            stmt.execute(ddl);
            ResultSet rs;
            rs = conn.createStatement().executeQuery("SELECT int_col2, COUNT(*) FROM " + fullTableName + " GROUP BY int_col2");
            assertTrue(rs.next());
            assertEquals(1,rs.getInt(2));
        }
    }

    @Test
    public void testSelectDistinctOnTableWithSecondaryImmutableIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            BaseTest.populateTestTable(fullTableName);
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName + " (int_col2)";
            conn.createStatement().execute(ddl);
            ResultSet rs = conn.createStatement().executeQuery("SELECT distinct int_col2 FROM " + fullTableName + " where int_col2 > 0");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testInClauseWithIndexOnColumnOfUsignedIntType() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            BaseTest.populateTestTable(fullTableName);
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName + " (int_col1)";
            stmt.execute(ddl);
            ResultSet rs = conn.createStatement().executeQuery("SELECT int_col1 FROM " + fullTableName + " where int_col1 IN (1, 2, 3, 4)");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void createIndexOnTableWithSpecifiedDefaultCF() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            String ddl ="CREATE TABLE " + fullTableName
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) DEFAULT_COLUMN_FAMILY='A'" + (!tableDDLOptions.isEmpty() ? "," + tableDDLOptions : "");
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);

            query = "SELECT * FROM " + tableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            String options = localIndex ? "SALT_BUCKETS=10, MULTI_TENANT=true, IMMUTABLE_ROWS=true, DISABLE_WAL=true" : "";
            conn.createStatement().execute(
                    "CREATE INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2) " + options);
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            //check options set correctly on index
            TableName indexTableName = TableName.create(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
            NamedTableNode indexNode = NamedTableNode.create(null, indexTableName, null);
            ColumnResolver resolver = FromCompiler.getResolver(indexNode, conn.unwrap(PhoenixConnection.class));
            PTable indexTable = resolver.getTables().get(0).getTable();
            // Can't set IMMUTABLE_ROWS, MULTI_TENANT or DEFAULT_COLUMN_FAMILY_NAME on an index
            assertNull(indexTable.getDefaultFamilyName());
            assertFalse(indexTable.isMultiTenant());
            assertEquals(mutable, !indexTable.isImmutableRows()); // Should match table
            if(localIndex) {
                assertEquals(10, indexTable.getBucketNum().intValue());
                assertTrue(indexTable.isWALDisabled());
            }
        }
    }

    @Test
    public void testIndexWithNullableDateCol() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            Date date = new Date(System.currentTimeMillis());

            TestUtil.createMultiCFTestTable(conn, fullTableName, tableDDLOptions);
            populateMultiCFTestTable(fullTableName, date);
            String ddl = "CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (date_col)";
            conn.createStatement().execute(ddl);

            String query = "SELECT int_pk from " + fullTableName ;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName +" [1]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName + "\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());

            query = "SELECT date_col from " + fullTableName + " order by date_col" ;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName + " [1]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName + "\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(date, rs.getDate(1));
            assertTrue(rs.next());
            assertEquals(new Date(date.getTime() + MILLIS_IN_DAY), rs.getDate(1));
            assertTrue(rs.next());
            assertEquals(new Date(date.getTime() + 2 * MILLIS_IN_DAY), rs.getDate(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectAllAndAliasWithIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            String ddl = "CREATE TABLE " + fullTableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " + tableDDLOptions;
            conn.createStatement().execute(ddl);
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            ddl = "CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (v2 DESC) INCLUDE (v1)";
            conn.createStatement().execute(ddl);
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
            stmt.setString(1,"a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            stmt.setString(1,"b");
            stmt.setString(2, "y");
            stmt.setString(3, "2");
            stmt.execute();
            conn.commit();

            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex){
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName+" [1]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("b",rs.getString(1));
            assertEquals("y",rs.getString(2));
            assertEquals("2",rs.getString(3));
            assertEquals("b",rs.getString("k"));
            assertEquals("y",rs.getString("v1"));
            assertEquals("2",rs.getString("v2"));
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals("x",rs.getString(2));
            assertEquals("1",rs.getString(3));
            assertEquals("a",rs.getString("k"));
            assertEquals("x",rs.getString("v1"));
            assertEquals("1",rs.getString("v2"));
            assertFalse(rs.next());

            query = "SELECT v1 as foo FROM " + fullTableName + " WHERE v2 = '1' ORDER BY foo";
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex){
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " +fullTableName + " [1,~'1']\n" +
                        "    SERVER SORTED BY [\"V1\"]\n" +
                        "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " +fullIndexName + " [~'1']\n" +
                        "    SERVER SORTED BY [\"V1\"]\n" +
                        "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("x",rs.getString(1));
            assertEquals("x",rs.getString("foo"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectCF() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            String ddl = "CREATE TABLE " + fullTableName + " (k VARCHAR NOT NULL PRIMARY KEY, a.v1 VARCHAR, a.v2 VARCHAR, b.v1 VARCHAR) " + tableDDLOptions;
            conn.createStatement().execute(ddl);
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            ddl = "CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (v2 DESC) INCLUDE (a.v1)";
            conn.createStatement().execute(ddl);
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?,?)");
            stmt.setString(1,"a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.setString(4, "A");
            stmt.execute();
            stmt.setString(1,"b");
            stmt.setString(2, "y");
            stmt.setString(3, "2");
            stmt.setString(4, "B");
            stmt.execute();
            conn.commit();

            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullTableName, QueryUtil.getExplainPlan(rs));

            query = "SELECT a.* FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName+" [1]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
            }
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("y",rs.getString(1));
            assertEquals("2",rs.getString(2));
            assertEquals("y",rs.getString("v1"));
            assertEquals("2",rs.getString("v2"));
            assertTrue(rs.next());
            assertEquals("x",rs.getString(1));
            assertEquals("1",rs.getString(2));
            assertEquals("x",rs.getString("v1"));
            assertEquals("1",rs.getString("v2"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertAfterIndexDrop() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            // make sure that the tables are empty, but reachable
            conn.createStatement().execute(
                    "CREATE TABLE " + fullTableName
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)" + tableDDLOptions);
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            conn.createStatement().execute(
                    "CREATE " + (localIndex ? "LOCAL " : "") + "INDEX " + indexName + " ON " + fullTableName + " (v1, v2)");
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            // load some data into the table
            PreparedStatement stmt =
                    conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            conn.commit();

            // make sure the index is working as expected
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("1", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());

            String ddl = "DROP INDEX " + indexName + " ON " + fullTableName;
            stmt = conn.prepareStatement(ddl);
            stmt.execute();

            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k, v1) VALUES(?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "y");
            stmt.execute();
            conn.commit();

            query = "SELECT * FROM " + fullTableName;

            // check that the data table matches as expected
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMultipleUpdatesAcrossRegions() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);

        String testTable = fullTableName+"_MULTIPLE_UPDATES";
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            // make sure that the tables are empty, but reachable
            conn.createStatement().execute(
                    "CREATE TABLE " + testTable
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) "
                    + (!tableDDLOptions.isEmpty() ? tableDDLOptions : "") + " SPLIT ON ('b')");
            query = "SELECT * FROM " + testTable;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            conn.createStatement().execute(
                    "CREATE " + (localIndex ? "LOCAL " : "") + "INDEX " + indexName + " ON " + testTable + " (v1, v2)");
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            // load some data into the table
            PreparedStatement stmt =
                    conn.prepareStatement("UPSERT INTO " + testTable + " VALUES(?,?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            stmt.setString(1, "b");
            stmt.setString(2, "y");
            stmt.setString(3, "2");
            stmt.execute();
            stmt.setString(1, "c");
            stmt.setString(2, "z");
            stmt.setString(3, "3");
            stmt.execute();
            conn.commit();

            query = "SELECT /*+ NO_INDEX */ * FROM " + testTable;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("x", rs.getString(2));
            assertEquals("1", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("2", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals("z", rs.getString(2));
            assertEquals("3", rs.getString(3));
            assertFalse(rs.next());
            
            // make sure the index is working as expected
            query = "SELECT * FROM " + testTable;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 2-WAY RANGE SCAN OVER " + testTable+" [1]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT",
                        QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName + "\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY",
                        QueryUtil.getExplainPlan(rs));
            }

            // check that the data table matches as expected
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("x", rs.getString(2));
            assertEquals("1", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("2", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals("z", rs.getString(2));
            assertEquals("3", rs.getString(3));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testIndexWithCaseSensitiveCols() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            conn.createStatement().execute("CREATE TABLE " + fullTableName + " (k VARCHAR NOT NULL PRIMARY KEY, \"V1\" VARCHAR, \"v2\" VARCHAR)"+tableDDLOptions);
            query = "SELECT * FROM "+fullTableName;
            rs = conn.createStatement().executeQuery(query);
            long ts = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null,fullTableName)).getTimeStamp();
            assertFalse(rs.next());
            conn.createStatement().execute(
                    "CREATE " + (localIndex ? "LOCAL " : "") + "INDEX " + indexName + " ON " + fullTableName + "(\"v2\") INCLUDE (\"V1\")");
            query = "SELECT * FROM "+fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
            stmt.setString(1,"a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            stmt.setString(1,"b");
            stmt.setString(2, "y");
            stmt.setString(3, "2");
            stmt.execute();
            conn.commit();

            query = "SELECT * FROM " + fullTableName + " WHERE \"v2\" = '1'";
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex){
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName + " [1,'1']\n"
                        + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullIndexName + " ['1']", QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals("x",rs.getString(2));
            assertEquals("1",rs.getString(3));
            assertEquals("a",rs.getString("k"));
            assertEquals("x",rs.getString("V1"));
            assertEquals("1",rs.getString("v2"));
            assertFalse(rs.next());

            query = "SELECT \"V1\", \"V1\" as foo1, \"v2\" as foo, \"v2\" as \"Foo1\", \"v2\" FROM " + fullTableName + " ORDER BY foo";
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex){
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName + " [1]\nCLIENT MERGE SORT",
                        QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER "+fullIndexName, QueryUtil.getExplainPlan(rs));
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("x",rs.getString(1));
            assertEquals("x",rs.getString("V1"));
            assertEquals("x",rs.getString(2));
            assertEquals("x",rs.getString("foo1"));
            assertEquals("1",rs.getString(3));
            assertEquals("1",rs.getString("Foo"));
            assertEquals("1",rs.getString(4));
            assertEquals("1",rs.getString("Foo1"));
            assertEquals("1",rs.getString(5));
            assertEquals("1",rs.getString("v2"));
            assertTrue(rs.next());
            assertEquals("y",rs.getString(1));
            assertEquals("y",rs.getString("V1"));
            assertEquals("y",rs.getString(2));
            assertEquals("y",rs.getString("foo1"));
            assertEquals("2",rs.getString(3));
            assertEquals("2",rs.getString("Foo"));
            assertEquals("2",rs.getString(4));
            assertEquals("2",rs.getString("Foo1"));
            assertEquals("2",rs.getString(5));
            assertEquals("2",rs.getString("v2"));
            assertFalse(rs.next());

            assertNoIndexDeletes(conn, ts, fullIndexName);
        }
    }

    private void assertNoIndexDeletes(Connection conn, long minTimestamp, String fullIndexName) throws IOException, SQLException {
        if (!this.mutable) {
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            PTable index = pconn.getTable(new PTableKey(null, fullIndexName));
            byte[] physicalIndexTable = index.getPhysicalName().getBytes();
            try (HTableInterface hIndex = pconn.getQueryServices().getTable(physicalIndexTable)) {
                Scan scan = new Scan();
                scan.setRaw(true);
                if (this.transactional) {
                    minTimestamp = TransactionUtil.convertToNanoseconds(minTimestamp);
                }
                scan.setTimeRange(minTimestamp, HConstants.LATEST_TIMESTAMP);
                ResultScanner scanner = hIndex.getScanner(scan);
                Result result;
                while ((result = scanner.next()) != null) {
                    CellScanner cellScanner = result.cellScanner();
                    while (cellScanner.advance()) {
                        Cell current = cellScanner.current();
                        assertEquals (KeyValue.Type.Put.getCode(), current.getTypeByte());
                    }
                }
            };
        }
    }

    @Test
    public void testInFilterOnIndexedTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            String ddl = "CREATE TABLE " + fullTableName +"  (PK1 CHAR(2) NOT NULL PRIMARY KEY, CF1.COL1 BIGINT) " + tableDDLOptions;
            conn.createStatement().execute(ddl);
            ddl = "CREATE " + (localIndex ? "LOCAL " : "") + "INDEX " + indexName + " ON " + fullTableName + "(COL1)";
            conn.createStatement().execute(ddl);

            query = "SELECT COUNT(COL1) FROM " + fullTableName +" WHERE COL1 IN (1,25,50,75,100)";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
        }
    }

    @Test
    public void testIndexWithDecimalCol() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            Date date = new Date(System.currentTimeMillis());

            TestUtil.createMultiCFTestTable(conn, fullTableName, tableDDLOptions);
            populateMultiCFTestTable(fullTableName, date);
            String ddl = null;
            ddl = "CREATE " + (localIndex ? "LOCAL " : "") + "INDEX " + indexName + " ON " + fullTableName + " (decimal_pk) INCLUDE (decimal_col1, decimal_col2)";
            conn.createStatement().execute(ddl);

            query = "SELECT decimal_pk, decimal_col1, decimal_col2 from " + fullTableName ;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName+" [1]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
            }

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(new BigDecimal("1.1"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("2.1"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("3.1"), rs.getBigDecimal(3));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("2.2"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("3.2"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("4.2"), rs.getBigDecimal(3));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("3.3"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("4.3"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("5.3"), rs.getBigDecimal(3));
            assertFalse(rs.next());
        }
    }

    /**
     * Ensure that HTD contains table priorities correctly.
     */
    @Test
    public void testTableDescriptorPriority() throws SQLException, IOException {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        // Check system tables priorities.
        try (HBaseAdmin admin = driver.getConnectionQueryServices(null, null).getAdmin(); 
                Connection c = DriverManager.getConnection(getUrl())) {
            ResultSet rs = c.getMetaData().getTables("", 
                    PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA, 
                    null, 
                    new String[] {PTableType.SYSTEM.toString()});
            ReadOnlyProps p = c.unwrap(PhoenixConnection.class).getQueryServices().getProps();
            while (rs.next()) {
                String schemaName = rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM);
                String tName = rs.getString(PhoenixDatabaseMetaData.TABLE_NAME);
                org.apache.hadoop.hbase.TableName hbaseTableName = SchemaUtil.getPhysicalTableName(SchemaUtil.getTableName(schemaName, tName), p);
                HTableDescriptor htd = admin.getTableDescriptor(hbaseTableName);
                String val = htd.getValue("PRIORITY");
                assertNotNull("PRIORITY is not set for table:" + htd, val);
                assertTrue(Integer.parseInt(val)
                        >= PhoenixRpcSchedulerFactory.getMetadataPriority(config));
            }
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
                conn.setAutoCommit(false);
                Statement stmt = conn.createStatement();
                stmt.execute(ddl);
                BaseTest.populateTestTable(fullTableName);
                ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName
                        + " ON " + fullTableName + " (long_col1, long_col2)"
                        + " INCLUDE (decimal_col1, decimal_col2)";
                stmt.execute(ddl);
            }

            HTableDescriptor dataTable = admin.getTableDescriptor(
                    org.apache.hadoop.hbase.TableName.valueOf(fullTableName));
            String val = dataTable.getValue("PRIORITY");
            assertTrue(val == null || Integer.parseInt(val) < HConstants.HIGH_QOS);

            if (!localIndex && mutable) {
                HTableDescriptor indexTable = admin.getTableDescriptor(
                        org.apache.hadoop.hbase.TableName.valueOf(indexName));
                val = indexTable.getValue("PRIORITY");
                assertNotNull("PRIORITY is not set for table:" + indexTable, val);
                assertTrue(Integer.parseInt(val) >= PhoenixRpcSchedulerFactory.getIndexPriority(config));
            }
        }
    }

    @Test
    public void testQueryBackToDataTableWithDescPKColumn() throws SQLException {
        doTestQueryBackToDataTableWithDescPKColumn(true);
        doTestQueryBackToDataTableWithDescPKColumn(false);
    }

    private void doTestQueryBackToDataTableWithDescPKColumn(boolean isSecondPKDesc) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            // create data table and index table
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            String ddl = "CREATE TABLE " + fullTableName + "(p1 integer not null, p2 integer not null, " +
                    " a integer, b integer CONSTRAINT PK PRIMARY KEY ";
            if (isSecondPKDesc) {
                ddl += "(p1, p2 desc))";
            } else {
                ddl += "(p1 desc, p2))";
            }
            stmt.executeUpdate(ddl);
            ddl = "CREATE "+ (localIndex ? "LOCAL " : "") + " INDEX " + fullIndexName + " on " + fullTableName + "(a)";
            stmt.executeUpdate(ddl);

            // upsert a single row
            String upsert = "UPSERT INTO " + fullTableName + " VALUES(1,2,3,4)";
            stmt.executeUpdate(upsert);

            // try select with index
            // a = 3, should hit index table, but we select column B, so it will query back to data table
            String query = "SELECT /*+index(" + fullTableName + " " + fullIndexName + "*/ b from " + fullTableName +
                    " WHERE a = 3";
            ResultSet rs = stmt.executeQuery(query);
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
            assertFalse(rs.next());
            rs.close();
            stmt.close();
        }
    }

}
