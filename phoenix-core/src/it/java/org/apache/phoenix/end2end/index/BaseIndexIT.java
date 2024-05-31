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
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

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
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.end2end.CreateTableIT;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.TransactionUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class BaseIndexIT extends ParallelStatsDisabledIT {
    private static final Random RAND = new Random();

    private final boolean localIndex;
    private final boolean uncovered;
    private final boolean transactional;
    private final TransactionFactory.Provider transactionProvider;
    private final boolean mutable;
    private final boolean columnEncoded;
    private final String tableDDLOptions;

    protected BaseIndexIT(boolean localIndex, boolean uncovered, boolean mutable, String transactionProvider, boolean columnEncoded) {
        this.localIndex = localIndex;
        this.uncovered = uncovered;
        this.mutable = mutable;
        this.columnEncoded = columnEncoded;
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
        transactional = transactionProvider != null;
        if (transactional) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append(" TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + transactionProvider + "'");
            this.transactionProvider = TransactionFactory.Provider.valueOf(transactionProvider);
        } else {
            this.transactionProvider = null;
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Test
    public void testIndexWithNullableFixedWithCols() throws Exception {
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
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName
                    + " (char_col1 ASC, int_col1 ASC)"
                    + (uncovered ? "" : " INCLUDE (long_col1, long_col2)");
            stmt.execute(ddl);

            String query = "SELECT d.char_col1, int_col1 from " + fullTableName + " as d";

            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            if (!uncovered) {
                // Optimizer would not select the uncovered index for this query
                assertEquals(columnEncoded ? "SERVER FILTER BY FIRST KEY ONLY" :
                                "SERVER FILTER BY EMPTY COLUMN ONLY",
                        explainPlanAttributes.getServerWhereFilter());
            }

            if (localIndex) {
                assertEquals(fullIndexName + "(" + fullTableName + ")",
                        explainPlanAttributes.getTableName());
                assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
            } else if (!uncovered) {
                assertEquals(fullIndexName, explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
            }

            ResultSet rs = conn.createStatement().executeQuery(query);
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

            query = "SELECT char_col1, int_col1 from "+fullIndexName;
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
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName
                        + " (long_pk, varchar_pk)"
                        + (uncovered ? "" : " INCLUDE (long_col1, long_col2)");
            stmt.execute(ddl);

            ResultSet rs;

            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(3,rs.getInt(1));

            String dml = "DELETE from " + fullTableName + " WHERE long_col2 = 4";
            assertEquals(1,conn.createStatement().executeUpdate(dml));
            assertNoClientSideIndexMutations(conn);
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

    private void assertNoClientSideIndexMutations(Connection conn) throws SQLException {
        Iterator<Pair<byte[],List<Cell>>> iterator = PhoenixRuntime.getUncommittedDataIterator(conn);
        if (iterator.hasNext()) {
            byte[] tableName = iterator.next().getFirst(); // skip data table mutations
            PTable table = conn.unwrap(PhoenixConnection.class).getTable(Bytes.toString(tableName));
            boolean clientSideUpdate = (!localIndex || (transactional && table.getTransactionProvider().getTransactionProvider().isUnsupported(Feature.MAINTAIN_LOCAL_INDEX_ON_SERVER))) 
                    && (!mutable || transactional);
            if (!clientSideUpdate) {
                assertTrue(table.getType() == PTableType.TABLE); // should be data table
            }
            boolean hasIndexData = iterator.hasNext();
            // global immutable and global transactional tables are processed client side
            assertEquals(clientSideUpdate, hasIndexData); 
        }
    }

    @Test
    public void testCreateIndexAfterUpsertStarted() throws Exception {
        testCreateIndexAfterUpsertStarted(transactional, 
                SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, generateUniqueName()),
                SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, generateUniqueName()));
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
                ddl = "CREATE " + (localIndex ? "LOCAL" : "") + (uncovered ? "UNCOVERED" : "")
                        + " INDEX " + indexName + " ON " + fullTableName
                        + " (long_pk, varchar_pk)"
                        + (uncovered ? "" : " INCLUDE (long_col1, long_col2)");
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
        catch (Exception e) {
            if (!transactional) {
                fail("Should not fail for non-transactional tables");
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
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName
                        + " (long_col1, long_col2)"
                        + (uncovered ? "" : " INCLUDE (decimal_col1, decimal_col2)");
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
            assertNoClientSideIndexMutations(conn);
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
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName + " (int_col2)";
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
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName + " (int_col2)";
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
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName + " (int_col1)";
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

            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            String options = localIndex ? "SALT_BUCKETS=10, MULTI_TENANT=true, IMMUTABLE_ROWS=true, DISABLE_WAL=true" : "";
            conn.createStatement().execute(
                    "CREATE " + (uncovered ? "UNCOVERED" : "") + " INDEX " + indexName
                            + " ON " + fullTableName + " (v1)"
                            + (uncovered ? " " : "INCLUDE (v2) ") + options);
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
            String ddl = "CREATE " + (localIndex ? " LOCAL " : "")
                    + (uncovered ? "UNCOVERED" : "") + " INDEX " + indexName
                    + " ON " + fullTableName + " (date_col)";
            conn.createStatement().execute(ddl);

            String query = "SELECT" + (uncovered ? " /*+ INDEX(" + fullTableName + " "
                    + indexName + ")*/ " : " ") + "int_pk from " + fullTableName;
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals(columnEncoded ? "SERVER FILTER BY FIRST KEY ONLY" :
                            "SERVER FILTER BY EMPTY COLUMN ONLY",
                    explainPlanAttributes.getServerWhereFilter());
            if (localIndex) {
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName + "(" + fullTableName + ")",
                    explainPlanAttributes.getTableName());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
                assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
            } else {
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName,
                    explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }

            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());

            query = "SELECT date_col from " + fullTableName + " order by date_col" ;
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals(columnEncoded ? "SERVER FILTER BY FIRST KEY ONLY" :
                            "SERVER FILTER BY EMPTY COLUMN ONLY",
                    explainPlanAttributes.getServerWhereFilter());
            if (localIndex) {
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName + "(" + fullTableName + ")",
                        explainPlanAttributes.getTableName());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
                assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
            } else {
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName, explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
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

            ddl = "CREATE " + (localIndex ? " LOCAL " : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName + " (v2 DESC)"
                    + (uncovered ? "" : "INCLUDE (v1)");
            conn.createStatement().execute(ddl);
            query = "SELECT" + (uncovered ? " /*+ INDEX(" + fullTableName + " "
                    + indexName + ")*/ * FROM " + fullTableName : " * FROM " + fullIndexName);
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

            query = "SELECT" + (uncovered ? " /*+ INDEX(" + fullTableName + " "
                    + indexName + ")*/" : "") + " * FROM " + fullTableName;
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            if (localIndex) {
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName + "(" + fullTableName + ")",
                    explainPlanAttributes.getTableName());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
                assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
            } else {
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName,
                    explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
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
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals("[\"V1\"]", explainPlanAttributes.getServerSortedBy());
            assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());
            if (localIndex) {
                assertEquals(fullIndexName + "(" + fullTableName + ")",
                    explainPlanAttributes.getTableName());
            } else {
                assertEquals(fullIndexName,
                    explainPlanAttributes.getTableName());
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
            ddl = "CREATE " + (localIndex ? " LOCAL " : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName + " (v2 DESC)"
                    + (uncovered ? "" : "INCLUDE (a.v1)");
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
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(fullTableName, explainPlanAttributes.getTableName());

            query = "SELECT" + (uncovered ? " /*+ INDEX(" + fullTableName + " "
                    + indexName + ")*/ " : " ") + "a.* FROM " + fullTableName;
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            if(localIndex) {
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName + "(" + fullTableName + ")",
                    explainPlanAttributes.getTableName());
                assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName,
                    explainPlanAttributes.getTableName());
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
                    "CREATE " + (localIndex ? "LOCAL " : "") + (uncovered ? "UNCOVERED" : "")
                            + " INDEX " + indexName + " ON " + fullTableName + " (v1, v2)");
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
            conn.createStatement().execute(ddl);

            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k, v1) VALUES(?,?)");
            if (mutable) {
	            stmt.setString(1, "a");
	            stmt.setString(2, "y");
	            stmt.execute();
	            conn.commit();
            }
            stmt.setString(1, "b");
            stmt.setString(2, "x");
            stmt.execute();
            conn.commit();
	            
            // the index table is one row
            HTable table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(fullTableName.getBytes());
            ResultScanner resultScanner = table.getScanner(new Scan());
            for (Result result : resultScanner) {
            	System.out.println(result);
            }
            resultScanner.close();
            table.close();

            query = "SELECT * FROM " + fullTableName;

            // check that the data table matches as expected
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals(mutable ? "y" : "x", rs.getString(2));
            assertEquals("1", rs.getString(3));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals("x", rs.getString(2));
            assertNull(rs.getString(3));
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
                    "CREATE " + (localIndex ? "LOCAL " : "") + (uncovered ? "UNCOVERED" : "")
                            + " INDEX " + indexName + " ON " + testTable + " (v1, v2)");
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
            query = "SELECT" + (uncovered ? " /*+ INDEX(" + testTable + " "
                    + indexName + ")*/ " : " ") + "* FROM "+ testTable;
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals(columnEncoded ? "SERVER FILTER BY FIRST KEY ONLY" :
                            "SERVER FILTER BY EMPTY COLUMN ONLY",
                    explainPlanAttributes.getServerWhereFilter());
            if (localIndex) {
                assertEquals("PARALLEL 2-WAY",
                    explainPlanAttributes.getIteratorTypeAndScanSize());
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName + "(" + testTable + ")",
                        explainPlanAttributes.getTableName());
                assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("PARALLEL 1-WAY",
                    explainPlanAttributes.getIteratorTypeAndScanSize());
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName,
                    explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
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
                    "CREATE " + (localIndex ? "LOCAL " : "") + (uncovered ? "UNCOVERED " : " ")
                            + "INDEX " + indexName + " ON " + fullTableName + "(\"v2\")"
                    + (uncovered ? "" : " INCLUDE (\"V1\")"));
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
            if (!transactional) {
                conn.commit();
            }

            query = "SELECT * FROM " + fullTableName + " WHERE \"v2\" = '1'";
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            if (localIndex) {
                assertEquals(fullIndexName + "(" + fullTableName + ")",
                    explainPlanAttributes.getTableName());
                assertEquals(" [1,'1']", explainPlanAttributes.getKeyRanges());
            } else {
                assertEquals(fullIndexName,
                    explainPlanAttributes.getTableName());
                assertEquals(" ['1']", explainPlanAttributes.getKeyRanges());
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

            // Shadow cells shouldn't exist yet since commit hasn't happened
            if (transactional && transactionProvider == TransactionFactory.Provider.OMID) {
                assertShadowCellsDoNotExist(conn, fullTableName, fullIndexName);
            }

            conn.commit();

            // Confirm shadow cells exist after commit
            if (transactional && transactionProvider == TransactionFactory.Provider.OMID) {
                assertShadowCellsExist(conn, fullTableName, fullIndexName);
            }

            query = "SELECT \"V1\", \"V1\" as foo1, \"v2\" as foo, \"v2\" as \"Foo1\", \"v2\" FROM " + fullTableName + " ORDER BY foo";
            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            if (localIndex) {
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName + "(" + fullTableName + ")",
                    explainPlanAttributes.getTableName());
                assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName,
                    explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
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
            try (Table hIndex = pconn.getQueryServices().getTable(physicalIndexTable)) {
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
                        assertTrue(CellUtil.isPut(current));
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
            ddl = "CREATE " + (localIndex ? "LOCAL " : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName + "(COL1)";
            conn.createStatement().execute(ddl);

            query = "SELECT COUNT(COL1) FROM " + fullTableName +" WHERE COL1 IN (1,25,50,75,100)";
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
        }
    }

    @Test
    public void testIndexWithDecimalColServerSideUpsert() throws Exception {
        testIndexWithDecimalCol(true);
    }
    
    @Test
    public void testIndexWithDecimalColClientSideUpsert() throws Exception {
        testIndexWithDecimalCol(false);
    }
    
    private void testIndexWithDecimalCol(boolean enableServerSideUpsert) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        props.setProperty(QueryServices.ENABLE_SERVER_UPSERT_SELECT, Boolean.toString(enableServerSideUpsert));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            Date date = new Date(System.currentTimeMillis());

            TestUtil.createMultiCFTestTable(conn, fullTableName, tableDDLOptions);
            populateMultiCFTestTable(fullTableName, date);
            String ddl = null;
            ddl = "CREATE " + (localIndex ? "LOCAL " : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName + " (decimal_pk)"
                    + (uncovered ? "" : "INCLUDE (decimal_col1, decimal_col2)");
            conn.createStatement().execute(ddl);
            
            if (transactional && transactionProvider == TransactionFactory.Provider.OMID) {
                assertShadowCellsExist(conn, fullTableName, fullIndexName);
            }

            query = "SELECT" + (uncovered ? " /*+ INDEX(" + fullTableName + " " + indexName
                    + ")*/ " : " ") + "decimal_pk, decimal_col1, decimal_col2 from "
                    + fullTableName ;
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            if (localIndex) {
                assertEquals("RANGE SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName + "(" + fullTableName + ")",
                    explainPlanAttributes.getTableName());
                assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(fullIndexName,
                    explainPlanAttributes.getTableName());
                assertNull(explainPlanAttributes.getClientSortAlgo());
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

    private static void assertShadowCellsDoNotExist(Connection conn, String fullTableName, String fullIndexName) 
            throws Exception {
        assertShadowCells(conn, fullTableName, fullIndexName, false);
    }
    
    private static void assertShadowCellsExist(Connection conn, String fullTableName, String fullIndexName)
            throws Exception {
        assertShadowCells(conn, fullTableName, fullIndexName, true);
    }
    
    private static void assertShadowCells(Connection conn, String fullTableName, String fullIndexName, boolean exists) 
        throws Exception {
        PTable ptable = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, fullTableName));
        int nTableKVColumns = ptable.getColumns().size() - ptable.getPKColumns().size();
        Table hTable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName));
        ResultScanner tableScanner = hTable.getScanner(new Scan());
        Result tableResult;
        PTable pindex = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, fullIndexName));
        int nIndexKVColumns = pindex.getColumns().size() - pindex.getPKColumns().size();
        Table hIndex = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullIndexName));
        ResultScanner indexScanner = hIndex.getScanner(new Scan());
        Result indexResult;
        while ((indexResult = indexScanner.next()) != null) {
            int nColumns = 0;
            CellScanner scanner = indexResult.cellScanner();
            while (scanner.advance()) {
                nColumns++;
            }
            assertEquals(exists, nColumns > nIndexKVColumns * 2);
            assertNotNull(tableResult = tableScanner.next());
            nColumns = 0;
            scanner = tableResult.cellScanner();
            while (scanner.advance()) {
                nColumns++;
            }
            assertEquals(exists, nColumns > nTableKVColumns * 2);
        }
        assertNull(tableScanner.next());
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
            ddl = "CREATE "+ (localIndex ? "LOCAL " : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " on " + fullTableName + "(a)";
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

    @Test
    public void testReturnedTimestamp() throws Exception {
        String tenantId = getOrganizationId();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String indexName = generateUniqueName();
            String tableName =
                    initATableValues(generateUniqueName(), tenantId, getDefaultSplits(tenantId),
                        new Date(System.currentTimeMillis()), null, getUrl(), tableDDLOptions);
            String ddl = "CREATE "+ (localIndex ? "LOCAL " : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " on " + tableName + "(A_STRING)"
            + (uncovered ? "" : " INCLUDE (B_STRING)");
            conn.createStatement().executeUpdate(ddl);
            String query = "SELECT ENTITY_ID,A_STRING,B_STRING FROM " + tableName + " WHERE organization_id=? and entity_id=?";

            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);

            long currentTime = EnvironmentEdgeManager.currentTimeMillis();
            String entityId = mutable ? ROW5 : Integer.toString(Math.abs(RAND.nextInt() % 1000000000));
            PreparedStatement ddlStatement = conn.prepareStatement("UPSERT INTO " + tableName + "(ORGANIZATION_ID, ENTITY_ID,A_STRING) VALUES('" + tenantId + "',?,?)");
            ddlStatement.setString(1, entityId);
            ddlStatement.setString(2, Integer.toString(Math.abs(RAND.nextInt() % 1000000000)));
            ddlStatement.executeUpdate();
            conn.commit();
 
            statement.setString(2, entityId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(rs.unwrap(PhoenixResultSet.class).getCurrentRow().getValue(0).getTimestamp() >= currentTime);
            assertEquals(rs.getString(1).trim(), entityId);
            assertFalse(rs.next());

            currentTime = EnvironmentEdgeManager.currentTimeMillis();
            entityId = mutable ? ROW5 : Integer.toString(Math.abs(RAND.nextInt() % 1000000000));
            ddlStatement = conn.prepareStatement("UPSERT INTO " + tableName + "(ORGANIZATION_ID, ENTITY_ID,B_STRING) VALUES('" + tenantId + "',?,?)");
            ddlStatement.setString(1, entityId);
            ddlStatement.setString(2, Integer.toString(Math.abs(RAND.nextInt() % 1000000000)));
            ddlStatement.executeUpdate();
            conn.commit();
            
            statement.setString(2, entityId);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(rs.unwrap(PhoenixResultSet.class).getCurrentRow().getValue(0).getTimestamp() >= currentTime);
            assertEquals(rs.getString(1).trim(), entityId);
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }

    /**
     * Tests that we add LAST_DDL_TIMESTAMP when we create an index and we update LAST_DDL_TIMESTAMP when we update
     * an index.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampOnIndexes() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        long startTs = EnvironmentEdgeManager.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName
                    + " (long_col1, long_col2)"
                    + (uncovered ? "" : " INCLUDE (decimal_col1, decimal_col2)");
            stmt.execute(ddl);
            TestUtil.waitForIndexState(conn, fullIndexName, PIndexState.ACTIVE);

            long activeIndexLastDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(fullIndexName, startTs, conn);
            Thread.sleep(1);
            // Disable an index. This should change the LAST_DDL_TIMESTAMP.
            String disableIndexDDL = "ALTER INDEX " + indexName + " ON " +  TestUtil.DEFAULT_SCHEMA_NAME +
                    QueryConstants.NAME_SEPARATOR + tableName + " DISABLE";
            stmt.execute(disableIndexDDL);
            TestUtil.waitForIndexState(conn, fullIndexName, PIndexState.DISABLE);
            CreateTableIT.verifyLastDDLTimestamp(fullIndexName, activeIndexLastDDLTimestamp  + 1, conn);
        }
    }

    /**
     * Tests that we add LAST_DDL_TIMESTAMP when we create an async index
     * and we update LAST_DDL_TIMESTAMP when we update an index.
     * For ASYNC indexes, the state goes from BUILDING --> ACTIVE --> DISABLE
     * Verify that LAST_DDL_TIMESTAMP changes at every step.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampOnAsyncIndexes() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);

        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        long startTs = EnvironmentEdgeManager.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            ddl = "CREATE " + (localIndex ? "LOCAL" : "") + (uncovered ? "UNCOVERED" : "")
                    + " INDEX " + indexName + " ON " + fullTableName
                    + " (long_col1, long_col2)"
                    + (uncovered ? " ASYNC" : " INCLUDE (decimal_col1, decimal_col2) ASYNC");
            stmt.execute(ddl);
            TestUtil.waitForIndexState(conn, fullIndexName, PIndexState.BUILDING);
            long buildingIndexLastDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(fullIndexName, startTs, conn);
            Thread.sleep(1);

            // run the index MR job.
            IndexToolIT.runIndexTool(false, TestUtil.DEFAULT_SCHEMA_NAME, tableName, indexName);
            TestUtil.waitForIndexState(conn, fullIndexName, PIndexState.ACTIVE);
            long activeIndexLastDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(
                    fullIndexName, buildingIndexLastDDLTimestamp + 1, conn);
            Thread.sleep(1);

            // Disable an index. This should change the LAST_DDL_TIMESTAMP.
            String disableIndexDDL = "ALTER INDEX " + indexName + " ON " +  TestUtil.DEFAULT_SCHEMA_NAME +
                    QueryConstants.NAME_SEPARATOR + tableName + " DISABLE";
            stmt.execute(disableIndexDDL);
            TestUtil.waitForIndexState(conn, fullIndexName, PIndexState.DISABLE);
            CreateTableIT.verifyLastDDLTimestamp(fullIndexName, activeIndexLastDDLTimestamp + 1, conn);
        }
    }

    @Test
    public void testSelectUncoveredWithCoveredFieldSimple() throws Exception {
        assumeFalse(localIndex);
        assumeFalse(uncovered);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl =
                    "CREATE TABLE " + fullTableName
                            + " (v1 integer, k integer primary key, v2 integer, v3 integer, v4 integer) "
                            + tableDDLOptions;
            String upsert = "UPSERT INTO " + fullTableName + " values (1, 2, 3, 4, 5)";
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            stmt.execute(upsert);
            conn.commit();
            ddl =
                    "CREATE " + (uncovered ? "UNCOVERED" : "") + " INDEX " + indexName + " ON "
                            + fullTableName + " (v2) include(v3)";
            conn.createStatement().execute(ddl);
            ResultSet rs =
                    conn.createStatement().executeQuery("SELECT /*+ INDEX(" + fullTableName + " "
                            + indexName + ")*/ * FROM " + fullTableName + " where v2=3 and v3=4");
            // We're only testing the projector
            assertTrue(rs.next());
            assertEquals("1", rs.getString("v1"));
            assertEquals("2", rs.getString("k"));
            assertEquals("3", rs.getString("v2"));
            assertEquals("4", rs.getString("v3"));
            assertEquals("5", rs.getString("v4"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSelectUncoveredWithCoveredField() throws Exception {
        assumeFalse(localIndex);
        assumeFalse(uncovered);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);


        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            String ddl =
                    "CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            stmt.execute(ddl);
            BaseTest.populateTestTable(fullTableName);
            conn.commit();
            ddl =
                    "CREATE " + (uncovered ? "UNCOVERED" : "") + " INDEX " + indexName + " ON "
                            + fullTableName + " ( int_col1 ASC)"
                            + " INCLUDE (long_col1, long_col2)"
            ;
            stmt.execute(ddl);

            String query;
            ResultSet rs;

            for (String columns : Arrays.asList(new String[] { "*",
                    "varchar_pk,char_pk,int_pk,long_pk,decimal_pk,date_pk,a.varchar_col1,a.char_col1,a.int_col1,a.long_col1,a.decimal_col1,a.date1,b.varchar_col2,b.char_col2,b.int_col2,b.long_col2,b.decimal_col2,b.date2",
                    "varchar_pk,char_pk,int_pk,long_pk,decimal_pk,date_pk,varchar_col1,char_col1,int_col1,long_col1,decimal_col1,date1,varchar_col2,char_col2,int_col2,long_col2,decimal_col2,date2", })) {

                query =
                        "SELECT /*+ INDEX(" + fullTableName + " " + indexName + ")*/ " + columns
                                + " from " + fullTableName + " where int_col1=2 and long_col1=2";
                rs = stmt.executeQuery("Explain " + query);
                String explainPlan = QueryUtil.getExplainPlan(rs);
                assertEquals("bad plan with columns:" + columns,
                    "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullIndexName + " [2]\n"
                            + "    SERVER MERGE [A.VARCHAR_COL1, A.CHAR_COL1, A.DECIMAL_COL1, A.DATE1, B.VARCHAR_COL2, B.CHAR_COL2, B.INT_COL2, B.DECIMAL_COL2, B.DATE2]\n"
                            + "    SERVER FILTER BY A.\"LONG_COL1\" = 2",
                    explainPlan);
                rs = stmt.executeQuery(query);
                assertTrue(rs.next());
                // Test the projector thoroughly
                assertEquals("varchar1", rs.getString("varchar_pk"));
                assertEquals("char1", rs.getString("char_pk"));
                assertEquals("1", rs.getString("int_pk"));
                assertEquals("1", rs.getString("long_pk"));
                assertEquals("1", rs.getString("decimal_pk"));
                assertEquals("2015-01-01 00:00:00.000", rs.getString("date_pk"));
                assertEquals("varchar_a", rs.getString("varchar_col1"));
                assertEquals("chara", rs.getString("char_col1"));
                assertEquals("2", rs.getString("int_col1"));
                assertEquals("2", rs.getString("long_col1"));
                assertEquals("2", rs.getString("decimal_col1"));
                assertEquals("2015-01-01 00:00:00.000", rs.getString("date1"));
                assertEquals("varchar_b", rs.getString("varchar_col2"));
                assertEquals("charb", rs.getString("char_col2"));
                assertEquals("3", rs.getString("int_col2"));
                assertEquals("3", rs.getString("long_col2"));
                assertEquals("3", rs.getString("decimal_col2"));
                assertEquals("2015-01-01 00:00:00.000", rs.getString("date2"));
                assertFalse(rs.next());
            }
        }
    }
}
