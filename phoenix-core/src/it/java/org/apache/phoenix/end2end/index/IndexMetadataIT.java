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

import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class IndexMetadataIT extends ParallelStatsDisabledIT {

	private enum Order {ASC, DESC};
	
    private static void assertIndexInfoMetadata(ResultSet rs, String schemaName, String dataTableName, String indexName, int colPos, String colName, Order order) throws SQLException {
        assertTrue(rs.next());
        assertEquals(null,rs.getString(1));
        assertEquals(schemaName, rs.getString(2));
        assertEquals(dataTableName, rs.getString(3));
        assertEquals(Boolean.TRUE, rs.getBoolean(4));
        assertEquals(null,rs.getString(5));
        assertEquals(indexName, rs.getString(6));
        assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
        assertEquals(colPos, rs.getShort(8));
        assertEquals(colName, rs.getString(9));
        assertEquals(order == Order.ASC ? "A" : order == Order.DESC ? "D" : null, rs.getString(10));
        assertEquals(0,rs.getInt(11));
        assertTrue(rs.wasNull());
        assertEquals(0,rs.getInt(12));
        assertTrue(rs.wasNull());
        assertEquals(null,rs.getString(13));
    }

    private static void assertIndexInfoMetadata(ResultSet rs, String schemaName, String dataTableName, String indexName, int colPos, String colName, Order order, int type) throws SQLException {
        assertTrue(rs.next());
        assertEquals(null,rs.getString(1));
        assertEquals(schemaName, rs.getString(2));
        assertEquals(dataTableName, rs.getString(3));
        assertEquals(Boolean.TRUE, rs.getBoolean(4));
        assertEquals(null,rs.getString(5));
        assertEquals(indexName, rs.getString(6));
        assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort(7));
        assertEquals(colPos, rs.getShort(8));
        assertEquals(colName, rs.getString(9));
        assertEquals(order == Order.ASC ? "A" : order == Order.DESC ? "D" : null, rs.getString(10));
        assertEquals(0,rs.getInt(11));
        assertTrue(rs.wasNull());
        assertEquals(0,rs.getInt(12));
        assertTrue(rs.wasNull());
        assertEquals(null,rs.getString(13));
        assertEquals(type,rs.getInt(14));
    }
	
    private static void assertActiveIndex(Connection conn, String schemaName, String tableName) throws SQLException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        conn.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName).next(); // client side cache will update
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName)).getIndexMaintainers(ptr, pconn);
        assertTrue(ptr.getLength() > 0);
    }
    
    private static void assertNoActiveIndex(Connection conn, String schemaName, String tableName) throws SQLException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        conn.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName).next(); // client side cache will update
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName)).getIndexMaintainers(ptr, pconn);
        assertTrue(ptr.getLength() == 0);
    }
    
    @Test
    public void testIndexCreateDrop() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String indexDataTable = generateUniqueName();
        String fullIndexDataTable = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
        String indexName = generateUniqueName();
        try {
            String tableDDL = "create table " + fullIndexDataTable + TestUtil.TEST_TABLE_SCHEMA;
            conn.createStatement().execute(tableDDL);
            String ddl = "CREATE INDEX " + indexName + " ON " + fullIndexDataTable
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (int_col1, int_col2)";
            conn.createStatement().execute(ddl);
            
            // Verify the metadata for index is correct.
            ResultSet rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 3, ":INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 6, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 7, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 8, ":DATE_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 9, "A:INT_COL1", null);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 10, "B:INT_COL2", null);
            assertFalse(rs.next());
            
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), StringUtil.escapeLike(indexName ), new String[] {PTableType.INDEX.getValue().getString() });
            assertTrue(rs.next());
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));

            // Verify that there is a row inserted into the data table for the index table.
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName );
            assertTrue(rs.next());
            assertEquals(indexName , rs.getString(1));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);
            
            ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " UNUSABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals(indexName , rs.getString(3));
            assertEquals(PIndexState.INACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);

            ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " USABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals(indexName , rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);

            ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " DISABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals(indexName , rs.getString(3));
            assertEquals(PIndexState.DISABLE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertNoActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);

            try {
                ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " USABLE";
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode(), e.getErrorCode());
            }
            try {
                ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " UNUSABLE";
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode(), e.getErrorCode());
            }
            
            ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " REBUILD";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals(indexName , rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);
            
            ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " REBUILD ASYNC";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals(indexName , rs.getString(3));
            assertEquals(PIndexState.BUILDING.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());

            ddl = "DROP INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
            conn.createStatement().execute(ddl);
            
            assertNoActiveIndex(conn, INDEX_DATA_SCHEMA, indexDataTable);

           // Assert the rows for index table is completely removed.
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
            assertFalse(rs.next());
            
            // Assert the row in the original data table is removed.
            // Verify that there is a row inserted into the data table for the index table.
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName );
            assertFalse(rs.next());
            
            // Create another two indexes, and drops the table, verifies the indexes are dropped as well.
            ddl = "CREATE INDEX " + indexName + "1 ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (int_col1, int_col2)";
            conn.createStatement().execute(ddl);
            
            ddl = "CREATE INDEX " + indexName + "2 ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (long_pk, int_col2)";
            conn.createStatement().execute(ddl);
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1", 3, ":INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1", 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1", 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1", 6, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1", 7, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1", 8, ":DATE_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1", 9, "A:INT_COL1", null);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1", 10, "B:INT_COL2", null);

            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2", 3, ":INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2", 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2", 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2", 6, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2", 7, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2", 8, ":DATE_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2", 9, "B:INT_COL2", null);
            assertFalse(rs.next());
            
            // Create another table in the same schema
            String diffTableNameInSameSchema = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + "2";
            conn.createStatement().execute("CREATE TABLE " + diffTableNameInSameSchema + "(k INTEGER PRIMARY KEY)");
            try {
                conn.createStatement().execute("DROP INDEX " + indexName + "1 ON " + diffTableNameInSameSchema);
                fail("Should have realized index " + indexName + "1 is not on the table");
            } catch (TableNotFoundException ignore) {
                
            }
            ddl = "DROP TABLE " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
            conn.createStatement().execute(ddl);
            
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
            assertFalse(rs.next());
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName + "1");
            assertFalse(rs.next());
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName + "2");
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIndexDefinitionWithNullableFixedWidthColInPK() throws Exception {
    	// If we have nullable fixed width column in the PK, we convert those types into a compatible variable type
    	// column. The definition is defined in IndexUtil.getIndexColumnDataType.
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String indexDataTable = generateUniqueName();
        String indexName = generateUniqueName();
        conn.setAutoCommit(false);
        try {
            String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
            conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + "IMMUTABLE_ROWS=true");
            String ddl = "CREATE INDEX " + indexName + " ON " + fullTableName
                    + " (char_col1 ASC, int_col2 ASC, long_col2 DESC)"
                    + " INCLUDE (int_col1)";
            conn.createStatement().execute(ddl);
            
            // Verify the CHAR, INT and LONG are converted to right type.
            ResultSet rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 1, "A:CHAR_COL1", Order.ASC, Types.VARCHAR);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 2, "B:INT_COL2", Order.ASC, Types.DECIMAL);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 3, "B:LONG_COL2", Order.DESC, Types.DECIMAL);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 6, ":INT_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 7, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 8, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 9, ":DATE_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, indexDataTable, indexName , 10, "A:INT_COL1", null);
            assertFalse(rs.next());
            
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName );
            assertTrue(rs.next());
            assertEquals(indexName , rs.getString(1));
            assertFalse(rs.next());
            
            ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " UNUSABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), indexName , new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals(indexName , rs.getString(3));
            assertEquals(PIndexState.INACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            ddl = "DROP INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
            conn.createStatement().execute(ddl);
            
            // Assert the rows for index table is completely removed.
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
            assertFalse(rs.next());
            
            // Assert the row in the original data table is removed.
            // Verify that there is a row inserted into the data table for the index table.
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, indexDataTable, indexName );
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAlterIndexWithLowerCaseName() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String indexName = "\"lowerCaseIndex\"";
        String indexDataTable = generateUniqueName();
        try {
            String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
            conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + "IMMUTABLE_ROWS=true");
            String ddl = "CREATE INDEX " + indexName + " ON " + fullTableName
                    + " (char_col1 ASC, int_col2 ASC, long_col2 DESC)"
                    + " INCLUDE (int_col1)";
            conn.createStatement().execute(ddl);

            ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable + " UNUSABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            ResultSet rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "lowerCaseIndex", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("lowerCaseIndex", rs.getString(3));
            
            ddl = "DROP INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
            conn.createStatement().execute(ddl);
            
            // Assert the rows for index table is completely removed.
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, indexDataTable, false, false);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testIndexDefinitionWithRepeatedColumns() throws Exception {
    	// Test index creation when the columns is included in both the PRIMARY and INCLUDE section. Test de-duplication.
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String indexDataTable = generateUniqueName();
        String indexName = generateUniqueName();
        try {
            String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + indexDataTable;
            conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + "IMMUTABLE_ROWS=true");
            String ddl = "CREATE INDEX " + indexName + " ON " + fullTableName
            		+ " (a.int_col1, a.long_col1, b.int_col2, b.long_col2)"
            		+ " INCLUDE(int_col1, int_col2)";
            conn.createStatement().execute(ddl);
            fail("Should have caught exception.");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_EXIST_IN_DEF.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTableWithSameColumnNames() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "create table test_table (char_pk varchar not null,"
        		+ " int_col integer, long_col integer, int_col integer"
        		+ " constraint pk primary key (char_pk))";
            conn.createStatement().execute(ddl);
            fail("Should have caught exception");
        } catch (ColumnAlreadyExistsException e) {
            assertEquals(SQLExceptionCode.COLUMN_EXIST_IN_DEF.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTableWithSameColumnNamesWithFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "create table test_table (char_pk varchar not null,"
        		+ " a.int_col integer, a.long_col integer,"
        		+ " a.int_col integer, b.long_col integer"
        		+ " constraint pk primary key (char_pk))";
            conn.createStatement().execute(ddl);
            fail("Should have caught exception");
        } catch (ColumnAlreadyExistsException e) {
            assertEquals(SQLExceptionCode.COLUMN_EXIST_IN_DEF.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testIndexDefinitionWithSameColumnNamesInTwoFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String testTable = generateUniqueName();
        String indexName = generateUniqueName();
        String ddl = "create table " + testTable  + " (char_pk varchar not null,"
        		+ " a.int_col integer, a.long_col integer,"
        		+ " b.int_col integer, b.long_col integer"
        		+ " constraint pk primary key (char_pk))";
        conn.createStatement().execute(ddl);
        
        ddl = "CREATE INDEX " + indexName + "1 ON " + testTable  + " (a.int_col, b.int_col)";
        conn.createStatement().execute(ddl);
        try {
            ddl = "CREATE INDEX " + indexName + "2 ON " + testTable  + " (int_col)";
            conn.createStatement().execute(ddl);
            fail("Should have caught exception");
        } catch (AmbiguousColumnException e) {
            assertEquals(SQLExceptionCode.AMBIGUOUS_COLUMN.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testBinaryNonnullableIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String testTable = generateUniqueName();
        String indexName = generateUniqueName();
        try {
            String ddl =
                    "CREATE TABLE " + testTable  + " ( "
                    + "v1 BINARY(64) NOT NULL, "
                    + "v2 VARCHAR, "
                    + "v3 BINARY(64), "
                    + "v4 VARCHAR "
                    + "CONSTRAINT PK PRIMARY KEY (v1))";
            conn.createStatement().execute(ddl);
            conn.commit();

            try {
                conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + testTable  + " (v3) INCLUDE (v4)");
                fail("Should have seen SQLExceptionCode.VARBINARY_IN_ROW_KEY");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.VARBINARY_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
            }

            try {
                conn.createStatement().execute("CREATE INDEX " + indexName + "3 ON " + testTable  + " (v2, v3) INCLUDE (v4)");
                fail("Should have seen SQLExceptionCode.VARBINARY_IN_ROW_KEY");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.VARBINARY_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
            }
            conn.createStatement().execute("CREATE INDEX " + indexName + "4 ON " + testTable  + " (v4) INCLUDE (v2)");
            conn.commit();

            conn.createStatement().execute("CREATE INDEX varbinLastInRow ON " + testTable  + " (v1, v3)");
            conn.commit();

            conn.createStatement().execute( "CREATE INDEX " + indexName + "5 ON " + testTable  + " (v2) INCLUDE (v4, v3, v1)");
            conn.commit();

            conn.createStatement().executeQuery(
                "select v1,v2,v3,v4 FROM " + testTable  + " where v2 = 'abc' and v3 != 'a'");


        } finally {
            conn.close();
        }
    }

    @Test
    public void testAsyncCreatedDate() throws Exception {
        Date d0 = new Date(System.currentTimeMillis());
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String testTable = generateUniqueName();


        String ddl = "create table " + testTable  + " (k varchar primary key, v1 varchar, v2 varchar, v3 varchar)";
        conn.createStatement().execute(ddl);
        String indexName = "ASYNCIND_" + generateUniqueName();
        
        ddl = "CREATE INDEX " + indexName + "1 ON " + testTable  + " (v1) ASYNC";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX " + indexName + "2 ON " + testTable  + " (v2) ASYNC";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX " + indexName + "3 ON " + testTable  + " (v3)";
        conn.createStatement().execute(ddl);
        
        ResultSet rs = conn.createStatement().executeQuery(
            "select table_name, " + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + " " +
            "from system.catalog (" + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + " " + PDate.INSTANCE.getSqlTypeName() + ") " +
            "where " + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE + " is not null and table_name like 'ASYNCIND_%' " +
            "order by " + PhoenixDatabaseMetaData.ASYNC_CREATED_DATE
        );
        assertTrue(rs.next());
        assertEquals(indexName + "1", rs.getString(1));
        Date d1 = rs.getDate(2);
        assertTrue(d1.after(d0));
        assertTrue(rs.next());
        assertEquals(indexName + "2", rs.getString(1));
        Date d2 = rs.getDate(2);
        assertTrue(d2.after(d1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testAsyncRebuildTimestamp() throws Exception {
        long startTimestamp = System.currentTimeMillis();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String testTable = generateUniqueName();


        String ddl = "create table " + testTable  + " (k varchar primary key, v1 varchar, v2 varchar, v3 varchar)";
        Statement stmt = conn.createStatement();
        stmt.execute(ddl);
        String indexName = "R_ASYNCIND_" + generateUniqueName();
        
        ddl = "CREATE INDEX " + indexName + "1 ON " + testTable  + " (v1) ";
        stmt.execute(ddl);
        ddl = "CREATE INDEX " + indexName + "2 ON " + testTable  + " (v2) ";
        stmt.execute(ddl);
        ddl = "CREATE INDEX " + indexName + "3 ON " + testTable  + " (v3)";
        stmt.execute(ddl);
        conn.createStatement().execute("ALTER INDEX "+indexName+"1 ON " + testTable +" DISABLE ");
        conn.createStatement().execute("ALTER INDEX "+indexName+"2 ON " + testTable +" REBUILD ");
        conn.createStatement().execute("ALTER INDEX "+indexName+"3 ON " + testTable +" REBUILD ASYNC");
        
        ResultSet rs = conn.createStatement().executeQuery(
            "select table_name, " + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " " +
            "from system.catalog (" + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " " + PLong.INSTANCE.getSqlTypeName() + ") " +
            "where " + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " !=0 and table_name like 'R_ASYNCIND_%' " +
            "order by table_name");
        assertTrue(rs.next());
        assertEquals(indexName + "3", rs.getString(1));
        long asyncTimestamp = rs.getLong(2);
		assertTrue("Async timestamp is recent timestamp", asyncTimestamp > startTimestamp);
        PTable table = PhoenixRuntime.getTable(conn, indexName+"3");
        assertEquals(table.getTimeStamp(), asyncTimestamp);
        assertFalse(rs.next());
        conn.createStatement().execute("ALTER INDEX "+indexName+"3 ON " + testTable +" DISABLE");
        rs = conn.createStatement().executeQuery(
                "select table_name, " + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " " +
                "from system.catalog (" + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " " + PLong.INSTANCE.getSqlTypeName() + ") " +
                "where " + PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " !=0 and table_name like 'ASYNCIND_%' " +
                "order by table_name" );
        assertFalse(rs.next());
    }
}
