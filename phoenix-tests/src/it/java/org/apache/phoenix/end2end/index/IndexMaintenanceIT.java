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

import static org.apache.phoenix.query.QueryConstants.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class IndexMaintenanceIT extends ParallelStatsDisabledIT {

    @Test
    public void testImmutableIndexCreateAndUpdate() throws Exception {
        helpTestCreateAndUpdate(false, false);
    }

    @Test
    public void testImmutableLocalIndexCreateAndUpdate() throws Exception {
        helpTestCreateAndUpdate(false, true);
    }

    @Test
    public void testMutableIndexCreateAndUpdate() throws Exception {
        helpTestCreateAndUpdate(true, false);
    }

    @Test
    public void testMutableLocalIndexCreateAndUpdate() throws Exception {
        helpTestCreateAndUpdate(true, true);
    }

    /**
     * Adds a row to the index data table
     * 
     * @param i
     *            row number
     */
    private void insertRow(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setString(1, "varchar" + String.valueOf(i));
        stmt.setString(2, "char" + String.valueOf(i));
        stmt.setInt(3, i);
        stmt.setLong(4, i);
        stmt.setBigDecimal(5, new BigDecimal(i*0.5d));
        Date date = new Date(DateUtil.parseDate("2015-01-01 00:00:00").getTime() + (i - 1) * MILLIS_IN_DAY);
        stmt.setDate(6, date);
        stmt.setString(7, "a.varchar" + String.valueOf(i));
        stmt.setString(8, "a.char" + String.valueOf(i));
        stmt.setInt(9, i);
        stmt.setLong(10, i);
        stmt.setBigDecimal(11, new BigDecimal(i*0.5d));
        stmt.setDate(12, date);
        stmt.setString(13, "b.varchar" + String.valueOf(i));
        stmt.setString(14, "b.char" + String.valueOf(i));
        stmt.setInt(15, i);
        stmt.setLong(16, i);
        stmt.setBigDecimal(17, new BigDecimal(i*0.5d));
        stmt.setDate(18, date);
        stmt.executeUpdate();
    }

    private void verifyResult(ResultSet rs, int i) throws SQLException {
        assertTrue(rs.next());
        assertEquals("VARCHAR" + String.valueOf(i) + "_" + StringUtils.rightPad("CHAR" + String.valueOf(i), 10, ' ')
                + "_A.VARCHAR" + String.valueOf(i) + "_" + StringUtils.rightPad("B.CHAR" + String.valueOf(i), 10, ' '),
                rs.getString(1));
        assertEquals(i * 3, rs.getInt(2));
        Date date = new Date(DateUtil.parseDate("2015-01-01 00:00:00").getTime() + (i) * MILLIS_IN_DAY);
        assertEquals(date, rs.getDate(3));
        assertEquals(date, rs.getDate(4));
        assertEquals(date, rs.getDate(5));
        assertEquals("varchar" + String.valueOf(i), rs.getString(6));
        assertEquals("char" + String.valueOf(i), rs.getString(7));
        assertEquals(i, rs.getInt(8));
        assertEquals(i, rs.getLong(9));
        assertEquals(i*0.5d, rs.getDouble(10), 0.000001);
        assertEquals(i, rs.getLong(11));
        assertEquals(i, rs.getLong(12));
    }

    private void createDataTable(Connection conn, String dataTableName, String tableProps) throws SQLException {
        String tableDDL = "create table " + dataTableName + TestUtil.TEST_TABLE_SCHEMA + tableProps;
        conn.createStatement().execute(tableDDL);
    }
    
    private void helpTestCreateAndUpdate(boolean mutable, boolean localIndex) throws Exception {
        String dataTableName = generateUniqueName();
        String fullDataTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + dataTableName;
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(false);
            createDataTable(conn, fullDataTableName, mutable ? "" : "IMMUTABLE_ROWS=true");
            populateDataTable(conn, fullDataTableName);

            // create an expression index
            String ddl = "CREATE "
                    + (localIndex ? "LOCAL" : "")
                    + " INDEX " + indexName + " ON "
                    + fullDataTableName
                    + " ((UPPER(varchar_pk) || '_' || UPPER(char_pk) || '_' || UPPER(varchar_col1) || '_' || UPPER(b.char_col2)),"
                    + " (decimal_pk+int_pk+decimal_col2+int_col1)," + " date_pk+1, date1+1, date2+1 )"
                    + " INCLUDE (long_col1, long_col2)";
            conn.createStatement().execute(ddl);

            // run select query with expression in WHERE clause
            String whereSql = "SELECT long_col1, long_col2 from "
                    + fullDataTableName
                    + " WHERE UPPER(varchar_pk) || '_' || UPPER(char_pk) || '_' || UPPER(varchar_col1) || '_' || UPPER(b.char_col2) = ?"
                    + " AND decimal_pk+int_pk+decimal_col2+int_col1=?"
                    // since a.date1 and b.date2 are NULLABLE and date is fixed width, these expressions are stored as
                    // DECIMAL in the index (which is not fixed width)
                    + " AND date_pk+1=? AND date1+1=? AND date2+1=?";
            PreparedStatement stmt = conn.prepareStatement(whereSql);
            stmt.setString(1, "VARCHAR1_CHAR1     _A.VARCHAR1_B.CHAR1   ");
            stmt.setInt(2, 3);
            Date date = DateUtil.parseDate("2015-01-02 00:00:00");
            stmt.setDate(3, date);
            stmt.setDate(4, date);
            stmt.setDate(5, date);

            // verify that the query does a range scan on the index table
            ResultSet rs = stmt.executeQuery("EXPLAIN " + whereSql);
            assertEquals(
                    localIndex ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER INDEX_TEST."
                            + indexName + "(" + fullDataTableName + ")"
                            + " [1,'VARCHAR1_CHAR1     _A.VARCHAR1_B.CHAR1   ',3,'2015-01-02 00:00:00.000',1,420,156,800,000,1,420,156,800,000]\nCLIENT MERGE SORT"
                            : "CLIENT PARALLEL 1-WAY RANGE SCAN OVER INDEX_TEST." + indexName + " ['VARCHAR1_CHAR1     _A.VARCHAR1_B.CHAR1   ',3,'2015-01-02 00:00:00.000',1,420,156,800,000,1,420,156,800,000]",
                    QueryUtil.getExplainPlan(rs));

            // verify that the correct results are returned
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());

            // verify all rows in data table are present in index table
            String indexSelectSql = "SELECT UPPER(varchar_pk) || '_' || UPPER(char_pk) || '_' || UPPER(varchar_col1) || '_' || UPPER(b.char_col2), "
                    + "decimal_pk+int_pk+decimal_col2+int_col1, "
                    + "date_pk+1, date1+1, date2+1, "
                    + "varchar_pk, char_pk, int_pk, long_pk, decimal_pk, "
                    + "long_col1, long_col2 "
                    + "from "
                    + fullDataTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + indexSelectSql);
            assertEquals(localIndex ?
                            "CLIENT PARALLEL 1-WAY RANGE SCAN OVER INDEX_TEST." + indexName +
                                    "(" + fullDataTableName + ") [1]\nCLIENT MERGE SORT"
                            : "CLIENT PARALLEL 1-WAY FULL SCAN OVER INDEX_TEST." + indexName,
                    QueryUtil.getExplainPlan(rs));
            rs = conn.createStatement().executeQuery(indexSelectSql);
            verifyResult(rs, 1);
            verifyResult(rs, 2);

            // Insert two more rows to the index data table
            String upsert = "UPSERT INTO " + fullDataTableName
                    + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            stmt = conn.prepareStatement(upsert);
            insertRow(stmt, 3);
            insertRow(stmt, 4);
            conn.commit();

            rs = conn.createStatement().executeQuery(indexSelectSql);
            verifyResult(rs, 1);
            verifyResult(rs, 2);
            // verify that two rows added after index was created were also added to
            // the index table
            verifyResult(rs, 3);
            verifyResult(rs, 4);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testMutableIndexUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        String fullDataTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + dataTableName;
        String indexName = generateUniqueName();
    	helpTestUpdate(fullDataTableName, indexName, false);
    }

    @Test
    public void testMutableLocalIndexUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        String fullDataTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + dataTableName;
        String indexName = generateUniqueName();
        helpTestUpdate(fullDataTableName, indexName, true);
    }
    
    private void helpTestUpdate(String fullDataTableName, String indexName, boolean localIndex) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(false);
            createDataTable(conn, fullDataTableName, "");
            populateDataTable(conn, fullDataTableName);

            // create an expression index
            String ddl = "CREATE "
                    + (localIndex ? "LOCAL" : "")
                    + " INDEX " + indexName + " ON "
                    + fullDataTableName
                    + " ((UPPER(varchar_pk) || '_' || UPPER(char_pk) || '_' || UPPER(varchar_col1) || '_' || UPPER(char_col2)),"
                    + " (decimal_pk+int_pk+decimal_col2+int_col1)," + " date_pk+1, date1+1, date2+1 )"
                    + " INCLUDE (long_col1, long_col2)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();

            // update index pk column and covered column
            String upsert = "UPSERT INTO "
                    + fullDataTableName
                    + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk, varchar_col1, long_col1) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

            stmt = conn.prepareStatement(upsert);
            stmt.setString(1, "varchar1");
            stmt.setString(2, "char1");
            stmt.setInt(3, 1);
            stmt.setLong(4, 1l);
            stmt.setBigDecimal(5, new BigDecimal(0.5));
            stmt.setDate(6, DateUtil.parseDate("2015-01-01 00:00:00"));
            stmt.setString(7, "a.varchar_updated");
            stmt.setLong(8, 101);
            stmt.executeUpdate();
            conn.commit();

            // verify only one row was updated in the data table
            String selectSql = "UPPER(varchar_pk) || '_' || UPPER(char_pk) || '_' || UPPER(varchar_col1) || '_' || UPPER(char_col2), long_col1 from "
                    + fullDataTableName;
            ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ " + selectSql);
            assertTrue(rs.next());
            assertEquals("VARCHAR1_CHAR1     _A.VARCHAR_UPDATED_B.CHAR1   ", rs.getString(1));
            assertEquals(101, rs.getLong(2));
            assertTrue(rs.next());
            assertEquals("VARCHAR2_CHAR2     _A.VARCHAR2_B.CHAR2   ", rs.getString(1));
            assertEquals(2, rs.getLong(2));
            assertFalse(rs.next());

            // verify that the rows in the index table are also updated
            rs = conn.createStatement().executeQuery("SELECT " + selectSql);
            assertTrue(rs.next());
            assertEquals("VARCHAR1_CHAR1     _A.VARCHAR_UPDATED_B.CHAR1   ", rs.getString(1));
            assertEquals(101, rs.getLong(2));
            assertTrue(rs.next());
            assertEquals("VARCHAR2_CHAR2     _A.VARCHAR2_B.CHAR2   ", rs.getString(1));
            assertEquals(2, rs.getLong(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    private void populateDataTable(Connection conn, String dataTable) throws SQLException {
        String upsert = "UPSERT INTO " + dataTable
                + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement stmt1 = conn.prepareStatement(upsert);
        // insert two rows
        insertRow(stmt1, 1);
        insertRow(stmt1, 2);
        conn.commit();
    }

    @Test
    public void testDeleteIndexedExpressionImmutableIndex() throws Exception {
        helpTestDeleteIndexedExpression(false, false);
    }

    @Test
    public void testDeleteIndexedExpressionImmutableLocalIndex() throws Exception {
        helpTestDeleteIndexedExpression(false, true);
    }

    @Test
    public void testDeleteIndexedExpressionMutableIndex() throws Exception {
        helpTestDeleteIndexedExpression(true, false);
    }

    @Test
    public void testDeleteIndexedExpressionMutableLocalIndex() throws Exception {
        helpTestDeleteIndexedExpression(true, true);
    }

    protected void helpTestDeleteIndexedExpression(boolean mutable, boolean localIndex) throws Exception {
        String dataTableName = generateUniqueName();
        String fullDataTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + dataTableName;
        String indexName = generateUniqueName();
        String fullIndexTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(false);
            createDataTable(conn, fullDataTableName, mutable ? "" : "IMMUTABLE_ROWS=true");
            populateDataTable(conn, fullDataTableName);
            String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullDataTableName
                    + " (2*long_col2)";
            conn.createStatement().execute(ddl);

            ResultSet rs;
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullDataTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            conn.setAutoCommit(true);
            conn.createStatement().execute("DELETE from " + fullDataTableName + " WHERE long_col2 = 2");

            if (!mutable) {
                conn.createStatement().execute("DELETE from " + fullDataTableName + " WHERE 2*long_col2 = 4");
            }

            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullDataTableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexTableName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDeleteCoveredColImmutableIndex() throws Exception {
        helpTestDeleteCoveredCol(false, false);
    }

    @Test
    public void testDeleteCoveredColImmutableLocalIndex() throws Exception {
        helpTestDeleteCoveredCol(false, true);
    }

    @Test
    public void testDeleteCoveredColMutableIndex() throws Exception {
        helpTestDeleteCoveredCol(true, false);
    }

    @Test
    public void testDeleteCoveredColMutableLocalIndex() throws Exception {
        helpTestDeleteCoveredCol(true, true);
    }

    protected void helpTestDeleteCoveredCol(boolean mutable, boolean localIndex) throws Exception {
        String dataTableName = generateUniqueName();
        String fullDataTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + dataTableName;
        String indexName = generateUniqueName();
        String fullIndexTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(false);
            createDataTable(conn, fullDataTableName, mutable ? "" : "IMMUTABLE_ROWS=true");
            populateDataTable(conn, fullDataTableName);
            String ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullDataTableName
                    + " (long_pk, varchar_pk, 1+long_pk, UPPER(varchar_pk) )" + " INCLUDE (long_col1, long_col2)";
            conn.createStatement().execute(ddl);

            ResultSet rs;
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullDataTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            
            String sql = "SELECT LONG_COL1 from " + fullDataTableName + " WHERE LONG_COL2 = 2";
            rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertFalse(rs.next());
            
            String dml = "DELETE from " + fullDataTableName + " WHERE long_col2 = 2";
            assertEquals(1, conn.createStatement().executeUpdate(dml));
            conn.commit();

            String query = "SELECT /*+ NO_INDEX */ long_pk, varchar_pk, 1+long_pk, UPPER(varchar_pk) FROM "
                    + fullDataTableName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertEquals("varchar1", rs.getString(2));
            assertEquals(2L, rs.getLong(3));
            assertEquals("VARCHAR1", rs.getString(4));
            assertFalse(rs.next());

            query = "SELECT long_pk, varchar_pk, 1+long_pk, UPPER(varchar_pk) FROM " + fullDataTableName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1L, rs.getLong(1));
            assertEquals("varchar1", rs.getString(2));
            assertEquals(2L, rs.getLong(3));
            assertEquals("VARCHAR1", rs.getString(4));
            assertFalse(rs.next());

            query = "SELECT * FROM " + fullIndexTableName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());

            assertEquals(1L, rs.getLong(1));
            assertEquals("varchar1", rs.getString(2));
            assertEquals(2L, rs.getLong(3));
            assertEquals("VARCHAR1", rs.getString(4));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

}
