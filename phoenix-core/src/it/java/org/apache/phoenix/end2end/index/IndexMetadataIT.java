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
import static org.apache.phoenix.util.TestUtil.INDEX_DATA_TABLE;
import static org.apache.phoenix.util.TestUtil.MUTABLE_INDEX_DATA_TABLE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class IndexMetadataIT extends BaseHBaseManagedTimeIT {

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
        pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), fullTableName)).getIndexMaintainers(ptr, pconn);
        assertTrue(ptr.getLength() > 0);
    }
    
    private static void assertNoActiveIndex(Connection conn, String schemaName, String tableName) throws SQLException {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        conn.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName).next(); // client side cache will update
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), fullTableName)).getIndexMaintainers(ptr, pconn);
        assertTrue(ptr.getLength() == 0);
    }
    
    @Test
    public void testIndexCreateDrop() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            ensureTableCreated(getUrl(), MUTABLE_INDEX_DATA_TABLE);
            String ddl = "CREATE INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (int_col1, int_col2)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            // Verify the metadata for index is correct.
            ResultSet rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 3, ":INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 6, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 7, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 8, ":DATE_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 9, "A:INT_COL1", null);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX", 10, "B:INT_COL2", null);
            assertFalse(rs.next());
            
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), StringUtil.escapeLike("IDX"), new String[] {PTableType.INDEX.getValue().getString() });
            assertTrue(rs.next());
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));

            // Verify that there is a row inserted into the data table for the index table.
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX");
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(1));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);
            
            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " UNUSABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.INACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);

            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " USABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);

            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " DISABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.DISABLE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertNoActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);

            try {
                ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " USABLE";
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode(), e.getErrorCode());
            }
            try {
                ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " UNUSABLE";
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode(), e.getErrorCode());
            }
            
            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + " REBUILD";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            assertActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);

            ddl = "DROP INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE;
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            assertNoActiveIndex(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE);

           // Assert the rows for index table is completely removed.
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, false, false);
            assertFalse(rs.next());
            
            // Assert the row in the original data table is removed.
            // Verify that there is a row inserted into the data table for the index table.
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX");
            assertFalse(rs.next());
            
            // Create another two indexes, and drops the table, verifies the indexes are dropped as well.
            ddl = "CREATE INDEX IDX1 ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (int_col1, int_col2)";
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            ddl = "CREATE INDEX IDX2 ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE
                    + " (varchar_col1 ASC, varchar_col2 ASC, int_pk DESC)"
                    + " INCLUDE (long_pk, int_col2)";
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 3, ":INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 6, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 7, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 8, ":DATE_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 9, "A:INT_COL1", null);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1", 10, "B:INT_COL2", null);

            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 1, "A:VARCHAR_COL1", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 2, "B:VARCHAR_COL2", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 3, ":INT_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 6, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 7, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 8, ":DATE_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2", 9, "B:INT_COL2", null);
            assertFalse(rs.next());
            
            // Create another table in the same schema
            String diffTableNameInSameSchema = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + "2";
            conn.createStatement().execute("CREATE TABLE " + diffTableNameInSameSchema + "(k INTEGER PRIMARY KEY)");
            try {
                conn.createStatement().execute("DROP INDEX IDX1 ON " + diffTableNameInSameSchema);
                fail("Should have realized index IDX1 is not on the table");
            } catch (TableNotFoundException ignore) {
                
            }
            ddl = "DROP TABLE " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE;
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, false, false);
            assertFalse(rs.next());
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX1");
            assertFalse(rs.next());
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, MUTABLE_INDEX_DATA_TABLE, "IDX2");
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
        conn.setAutoCommit(false);
        try {
            ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
            String ddl = "CREATE INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " (char_col1 ASC, int_col2 ASC, long_col2 DESC)"
                    + " INCLUDE (int_col1)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            // Verify the CHAR, INT and LONG are converted to right type.
            ResultSet rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, false, false);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 1, "A:CHAR_COL1", Order.ASC, Types.VARCHAR);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 2, "B:INT_COL2", Order.ASC, Types.DECIMAL);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 3, "B:LONG_COL2", Order.DESC, Types.DECIMAL);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 4, ":VARCHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 5, ":CHAR_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 6, ":INT_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 7, ":LONG_PK", Order.DESC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 8, ":DECIMAL_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 9, ":DATE_PK", Order.ASC);
            assertIndexInfoMetadata(rs, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX", 10, "A:INT_COL1", null);
            assertFalse(rs.next());
            
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX");
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(1));
            assertFalse(rs.next());
            
            ddl = "ALTER INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE + " UNUSABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "IDX", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("IDX", rs.getString(3));
            assertEquals(PIndexState.INACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());
            
            ddl = "DROP INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            // Assert the rows for index table is completely removed.
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, false, false);
            assertFalse(rs.next());
            
            // Assert the row in the original data table is removed.
            // Verify that there is a row inserted into the data table for the index table.
            rs = IndexTestUtil.readDataTableIndexRow(conn, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, "IDX");
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
        try {
            ensureTableCreated(getUrl(), INDEX_DATA_TABLE);
            String ddl = "CREATE INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
                    + " (char_col1 ASC, int_col2 ASC, long_col2 DESC)"
                    + " INCLUDE (int_col1)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();

            ddl = "ALTER INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE + " UNUSABLE";
            conn.createStatement().execute(ddl);
            // Verify the metadata for index is correct.
            ResultSet rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(INDEX_DATA_SCHEMA), "lowerCaseIndex", new String[] {PTableType.INDEX.toString()});
            assertTrue(rs.next());
            assertEquals("lowerCaseIndex", rs.getString(3));
            
            ddl = "DROP INDEX " + indexName + " ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE;
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
            
            // Assert the rows for index table is completely removed.
            rs = conn.getMetaData().getIndexInfo(null, INDEX_DATA_SCHEMA, INDEX_DATA_TABLE, false, false);
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
        try {
            ensureTableCreated(getUrl(), TestUtil.INDEX_DATA_TABLE);
            String ddl = "CREATE INDEX IDX ON " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE
            		+ " (a.int_col1, a.long_col1, b.int_col2, b.long_col2)"
            		+ " INCLUDE(int_col1, int_col2)";
            PreparedStatement stmt = conn.prepareStatement(ddl);
            stmt.execute();
            fail("Should have caught exception.");
        } catch (SQLException e) {
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
        String ddl = "create table test_table (char_pk varchar not null,"
        		+ " a.int_col integer, a.long_col integer,"
        		+ " b.int_col integer, b.long_col integer"
        		+ " constraint pk primary key (char_pk))";
        PreparedStatement stmt = conn.prepareStatement(ddl);
        stmt.execute();
        
        ddl = "CREATE INDEX IDX1 ON test_table (a.int_col, b.int_col)";
        stmt = conn.prepareStatement(ddl);
        stmt.execute();
        try {
            ddl = "CREATE INDEX IDX2 ON test_table (int_col)";
            stmt = conn.prepareStatement(ddl);
            stmt.execute();
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
        try {
            String ddl =
                    "CREATE TABLE test_table ( "
                    + "v1 BINARY(64) NOT NULL, "
                    + "v2 VARCHAR, "
                    + "v3 BINARY(64), "
                    + "v4 VARCHAR "
                    + "CONSTRAINT PK PRIMARY KEY (v1))";
            conn.createStatement().execute(ddl);
            conn.commit();

            try {
                conn.createStatement().execute("CREATE INDEX idx ON test_table (v3) INCLUDE (v4)");
                fail("Should have seen SQLExceptionCode.VARBINARY_IN_ROW_KEY");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.VARBINARY_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
            }

            try {
                conn.createStatement().execute("CREATE INDEX idx3 ON test_table (v2, v3) INCLUDE (v4)");
                fail("Should have seen SQLExceptionCode.VARBINARY_IN_ROW_KEY");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.VARBINARY_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
            }
            conn.createStatement().execute("CREATE INDEX idx4 ON test_table (v4) INCLUDE (v2)");
            conn.commit();

            conn.createStatement().execute("CREATE INDEX varbinLastInRow ON test_table (v1, v3)");
            conn.commit();

            conn.createStatement().execute( "CREATE INDEX idx5 ON test_table (v2) INCLUDE (v4, v3, v1)");
            conn.commit();

            conn.createStatement().executeQuery(
                "select v1,v2,v3,v4 FROM test_table where v2 = 'abc' and v3 != 'a'");


        } finally {
            conn.close();
        }
    }
}
