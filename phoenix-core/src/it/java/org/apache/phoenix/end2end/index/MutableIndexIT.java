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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.IndexScrutiny;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.primitives.Doubles;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class MutableIndexIT extends ParallelStatsDisabledIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(MutableIndexIT.class);
    protected final boolean localIndex;
    private final String tableDDLOptions;
    
    public MutableIndexIT(Boolean localIndex, String txProvider, Boolean columnEncoded) {
        this.localIndex = localIndex;
        StringBuilder optionBuilder = new StringBuilder();
        if (txProvider != null) {
            optionBuilder.append("TRANSACTIONAL=true," + PhoenixDatabaseMetaData.TRANSACTION_PROVIDER + "='" + txProvider + "'");
        }
        if (!columnEncoded) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }
    
    private static Connection getConnection(Properties props) throws SQLException {
        props.setProperty(QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, Integer.toString(1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        return conn;
    }
    
    private static Connection getConnection() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        return getConnection(props);
    }
    
    @Parameters(name="MutableIndexIT_localIndex={0},transactionProvider={1},columnEncoded={2}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Object[]> data() {
        return TestUtil.filterTxParamData(Arrays.asList(new Object[][] { 
                { false, null, false }, { false, null, true },
                { false, "TEPHRA", false }, { false, "TEPHRA", true },
                { false, "OMID", false },
                { true, null, false }, { true, null, true },
                { true, "TEPHRA", false }, { true, "TEPHRA", true },
                }),1);
    }
    
    @Test
    public void testCoveredColumnUpdates() throws Exception {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            String tableName = "TBL_" + generateUniqueName();
            String indexName = "IDX_" + generateUniqueName();
            String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
            String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);

            TestUtil.createMultiCFTestTable(conn, fullTableName, tableDDLOptions);
            populateMultiCFTestTable(fullTableName);
            conn.createStatement().execute("CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName 
                    + " (char_col1 ASC, int_col1 ASC) INCLUDE (long_col1, long_col2)");
            
            String query = "SELECT char_col1, int_col1, long_col2 from " + fullTableName;
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName +" [1]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(4L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName
                    + "(varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2) SELECT varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2*2 FROM "
                    + fullTableName + " WHERE long_col2=?");
            stmt.setLong(1,4L);
            assertEquals(1,stmt.executeUpdate());
            conn.commit();

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(8L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName
                    + "(varchar_pk, char_pk, int_pk, long_pk , decimal_pk, long_col2) SELECT varchar_pk, char_pk, int_pk, long_pk , decimal_pk, CAST(null AS BIGINT) FROM "
                    + fullTableName + " WHERE long_col2=?");
            stmt.setLong(1,3L);
            assertEquals(1,stmt.executeUpdate());
            conn.commit();
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(0, rs.getLong(3));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals(8L, rs.getLong(3));
            assertTrue(rs.next());
            assertEquals("chara", rs.getString(1));
            assertEquals(4, rs.getInt(2));
            assertEquals(5L, rs.getLong(3));
            assertFalse(rs.next());
            if(localIndex) {
                query = "SELECT b.* from " + fullTableName + " where int_col1 = 4 AND char_col1 = 'chara'";
                rs = conn.createStatement().executeQuery("EXPLAIN " + query);
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName +" [1,'chara',4]\n" +
                        "    SERVER MERGE [B.VARCHAR_COL2, B.CHAR_COL2, B.INT_COL2, B.DECIMAL_COL2, B.DATE_COL]\n" +
                        "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
                rs = conn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals("varchar_b", rs.getString(1));
                assertEquals("charb", rs.getString(2));
                assertEquals(5, rs.getInt(3));
                assertEquals(5, rs.getLong(4));
                assertFalse(rs.next());
                
            }
        } 
    }
    
    @Test
    public void testUpsertIntoViewOnTableWithIndex() throws Exception {
        String baseTable = generateUniqueName();
        String view = generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE IF NOT EXISTS " + baseTable + 
                    " (ID VARCHAR PRIMARY KEY, V1 VARCHAR)";
            conn.createStatement().execute(baseTableDDL);

            // Create an Index on the base table
            String tableIndex = generateUniqueName() + "_IDX";
            conn.createStatement().execute("CREATE INDEX " + tableIndex + 
                " ON " + baseTable + " (V1)");

            // Create a view on the base table
            String viewDDL = "CREATE VIEW IF NOT EXISTS " + view 
                    + " (V2 INTEGER) AS SELECT * FROM " + baseTable
                    + " WHERE ID='a'";
            conn.createStatement().execute(viewDDL);

            String upsert = "UPSERT INTO " + view + " (ID, V1, V2) "
                    + "VALUES ('a' ,'ab', 7)";
            conn.createStatement().executeUpdate(upsert);
            conn.commit();
            
            ResultSet rs = conn.createStatement().executeQuery("SELECT ID, V1 from " + baseTable);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("ab", rs.getString(2));         
        }
    }
    
    @Test
    public void testCoveredColumns() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = getConnection()) {

            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            conn.createStatement().execute("CREATE TABLE " + fullTableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)" + tableDDLOptions);
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            
            conn.createStatement().execute("CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
    
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
            stmt.setString(1,"a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("x",rs.getString(1));
            assertEquals("a",rs.getString(2));
            assertEquals("1",rs.getString(3));
            assertFalse(rs.next());
    
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k,v2) VALUES(?,?)");
            stmt.setString(1,"a");
            stmt.setString(2, null);
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("x",rs.getString(1));
            assertEquals("a",rs.getString(2));
            assertNull(rs.getString(3));
            assertFalse(rs.next());
    
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName+" [1]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
            }
    
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals("x",rs.getString(2));
            assertNull(rs.getString(3));
            assertFalse(rs.next());
    
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k,v2) VALUES(?,?)");
            stmt.setString(1,"a");
            stmt.setString(2,"3");
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName + " [1]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals("x",rs.getString(2));
            assertEquals("3",rs.getString(3));
            assertFalse(rs.next());
    
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k,v2) VALUES(?,?)");
            stmt.setString(1,"a");
            stmt.setString(2,"4");
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName+" [1]\nCLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));            
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName, QueryUtil.getExplainPlan(rs));
            }
            
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals("x",rs.getString(2));
            assertEquals("4",rs.getString(3));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testCompoundIndexKey() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            String query;
            ResultSet rs;
            // make sure that the tables are empty, but reachable
            conn.createStatement().execute("CREATE TABLE " + fullTableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)" + tableDDLOptions);
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
            conn.createStatement().execute("CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (v1, v2)");
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
    
            // load some data into the table
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
            stmt.setString(1,"a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("x",rs.getString(1));
            assertEquals("1",rs.getString(2));
            assertEquals("a",rs.getString(3));
            assertFalse(rs.next());
    
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
            stmt.setString(1,"a");
            stmt.setString(2, "y");
            stmt.setString(3, null);
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("y",rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals("a",rs.getString(3));
            assertFalse(rs.next());
    
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if (localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName+" [1]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n"
                        + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
            } else {
                assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + fullIndexName + "\n"
                           + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
            }
            //make sure the data table looks like what we expect
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a",rs.getString(1));
            assertEquals("y",rs.getString(2));
            assertNull(rs.getString(3));
            assertFalse(rs.next());
            
            // Upsert new row with null leading index column
            stmt.setString(1,"b");
            stmt.setString(2, null);
            stmt.setString(3, "3");
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(null,rs.getString(1));
            assertEquals("3",rs.getString(2));
            assertEquals("b",rs.getString(3));
            assertTrue(rs.next());
            assertEquals("y",rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals("a",rs.getString(3));
            assertFalse(rs.next());
    
            // Update row with null leading index column to have a value
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?)");
            stmt.setString(1,"b");
            stmt.setString(2, "z");
            stmt.execute();
            conn.commit();
            
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("y",rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals("a",rs.getString(3));
            assertTrue(rs.next());
            assertEquals("z",rs.getString(1));
            assertEquals("3",rs.getString(2));
            assertEquals("b",rs.getString(3));
            assertFalse(rs.next());
        }

    }
    
    /**
     * There was a case where if there were multiple updates to a single row in the same batch, the
     * index wouldn't be updated correctly as each element of the batch was evaluated with the state
     * previous to the batch, rather than with the rest of the batch. This meant you could do a put
     * and a delete on a row in the same batch and the index result would contain the current + put
     * and current + delete, but not current + put + delete.
     * @throws Exception on failure
     */
    @Test
    public void testMultipleUpdatesToSingleRow() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = getConnection()) {
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
    
            conn.createStatement().execute("CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (v1, v2)");
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
          
            // do multiple updates to the same row, in the same batch
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k, v1) VALUES(?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "y");
            stmt.execute();
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + "(k,v2) VALUES(?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, null);
            stmt.execute();
            conn.commit();
        
            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("y", rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
        
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            if(localIndex) {
                assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + fullTableName+" [1]\n"
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
            assertEquals("y", rs.getString(2));
            assertNull(rs.getString(3));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testUpsertingNullForIndexedColumns() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        String testTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName + "_" + System.currentTimeMillis());
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            ResultSet rs;
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + testTableName + "(v1 VARCHAR PRIMARY KEY, v2 DOUBLE, v3 VARCHAR) "+tableDDLOptions);
            stmt.execute("CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + testTableName + "  (v2) INCLUDE(v3)");
            
            //create a row with value null for indexed column v2
            stmt.executeUpdate("upsert into " + testTableName + " values('cc1', null, 'abc')");
            conn.commit();
            
            //assert values in index table 
            rs = stmt.executeQuery("select * from " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(0, Doubles.compare(0, rs.getDouble(1)));
            assertTrue(rs.wasNull());
            assertEquals("cc1", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertFalse(rs.next());
            
            //assert values in data table
            rs = stmt.executeQuery("select v1, v2, v3 from " + testTableName);
            assertTrue(rs.next());
            assertEquals("cc1", rs.getString(1));
            assertEquals(0, Doubles.compare(0, rs.getDouble(2)));
            assertTrue(rs.wasNull());
            assertEquals("abc", rs.getString(3));
            assertFalse(rs.next());
            
            //update the previously null value for indexed column v2 to a non-null value 1.23
            stmt.executeUpdate("upsert into " + testTableName + " values('cc1', 1.23, 'abc')");
            conn.commit();
            
            //assert values in data table
            rs = stmt.executeQuery("select /*+ NO_INDEX */ v1, v2, v3 from " + testTableName);
            assertTrue(rs.next());
            assertEquals("cc1", rs.getString(1));
            assertEquals(0, Doubles.compare(1.23, rs.getDouble(2)));
            assertEquals("abc", rs.getString(3));
            assertFalse(rs.next());
            
            //assert values in index table 
            rs = stmt.executeQuery("select * from " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(0, Doubles.compare(1.23, rs.getDouble(1)));
            assertEquals("cc1", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertFalse(rs.next());
            
            //update the value for indexed column v2 back to null
            stmt.executeUpdate("upsert into " + testTableName + " values('cc1', null, 'abc')");
            conn.commit();
            
            //assert values in index table 
            rs = stmt.executeQuery("select * from " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(0, Doubles.compare(0, rs.getDouble(1)));
            assertTrue(rs.wasNull());
            assertEquals("cc1", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertFalse(rs.next());
            
            //assert values in data table
            rs = stmt.executeQuery("select v1, v2, v3 from " + testTableName);
            assertTrue(rs.next());
            assertEquals("cc1", rs.getString(1));
            assertEquals(0, Doubles.compare(0, rs.getDouble(2)));
            assertEquals("abc", rs.getString(3));
            assertFalse(rs.next());
        } 
    }
    
    
    private void assertImmutableRows(Connection conn, String fullTableName, boolean expectedValue) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        assertEquals(expectedValue, pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName)).isImmutableRows());
    }
    
    @Test
    public void testAlterTableWithImmutability() throws Exception {
        if (localIndex)
            return;

        String query;
        ResultSet rs;
        String tableName = "TBL_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);

        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            conn.createStatement().execute(
                "CREATE TABLE " + fullTableName +" (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) " + tableDDLOptions);
            
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());
    
            assertImmutableRows(conn,fullTableName, false);
            conn.createStatement().execute("ALTER TABLE " + fullTableName +" SET IMMUTABLE_ROWS=true");
            assertImmutableRows(conn,fullTableName, true);
            
            
            conn.createStatement().execute("ALTER TABLE " + fullTableName +" SET immutable_rows=false");
            assertImmutableRows(conn,fullTableName, false);
        }
    }

    private void createBaseTable(Connection conn, String tableName, String splits) throws SQLException {
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "k3 INTEGER,\n" +
                "v1 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n"
                        + (tableDDLOptions!=null?tableDDLOptions:"") + (splits != null ? (" split on " + splits) : "");
        conn.createStatement().execute(ddl);
    }
    
  @Test
  public void testTenantSpecificConnection() throws Exception {
      String tableName = "TBL_" + generateUniqueName();
      String indexName = "IDX_" + generateUniqueName();
      String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
      Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
      try (Connection conn = getConnection()) {
          conn.setAutoCommit(false);
          // create data table
          conn.createStatement().execute(
              "CREATE TABLE IF NOT EXISTS " + fullTableName + 
              "(TENANT_ID CHAR(15) NOT NULL,"+
              "TYPE VARCHAR(25),"+
              "ENTITY_ID CHAR(15) NOT NULL,"+
              "CONSTRAINT PK_CONSTRAINT PRIMARY KEY (TENANT_ID, ENTITY_ID)) MULTI_TENANT=TRUE "
              + (!tableDDLOptions.isEmpty() ? "," + tableDDLOptions : "") );
          // create index
          conn.createStatement().execute("CREATE " + (localIndex ? " LOCAL " : "") + " INDEX IF NOT EXISTS " + indexName + " ON " + fullTableName + " (ENTITY_ID, TYPE)");
          // upsert rows
          String dml = "UPSERT INTO " + fullTableName + " (ENTITY_ID, TYPE) VALUES ( ?, ?)";
          props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "tenant1");
          // connection is tenant-specific
          try (Connection tenantConn = getConnection(props)) {
              // upsert one row
              upsertRow(dml, tenantConn, 0);
              tenantConn.commit();
              ResultSet rs = tenantConn.createStatement().executeQuery("SELECT ENTITY_ID FROM " + fullTableName + " ORDER BY TYPE LIMIT 5");
              assertTrue(rs.next());
              // upsert two rows which ends up using the tenant cache
              upsertRow(dml, tenantConn, 1);
              upsertRow(dml, tenantConn, 2);
              tenantConn.commit();
          }
      }
  }

  @Test
  public void testUpsertingDeletedRowShouldGiveProperDataWithIndexes() throws Exception {
      testUpsertingDeletedRowShouldGiveProperDataWithIndexes(false);
  }

  @Test
  public void testUpsertingDeletedRowShouldGiveProperDataWithMultiCFIndexes() throws Exception {
      testUpsertingDeletedRowShouldGiveProperDataWithIndexes(true);
  }

  private void testUpsertingDeletedRowShouldGiveProperDataWithIndexes(boolean multiCf) throws Exception {
      String tableName = "TBL_" + generateUniqueName();
      String indexName = "IDX_" + generateUniqueName();
      String columnFamily1 = "cf1";
      String columnFamily2 = "cf2";
      String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
      String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
      try (Connection conn = getConnection()) {
            conn.createStatement().execute(
                "create table " + fullTableName + " (id integer primary key, "
                        + (multiCf ? columnFamily1 + "." : "") + "f float, "
                        + (multiCf ? columnFamily2 + "." : "") + "s varchar)" + tableDDLOptions);
            conn.createStatement().execute(
                "create " + (localIndex ? "LOCAL" : "") + " index " + indexName + " on " + fullTableName + " ("
                        + (multiCf ? columnFamily1 + "." : "") + "f) include ("+(multiCf ? columnFamily2 + "." : "") +"s)");
            conn.createStatement().execute(
                "upsert into " + fullTableName + " values (1, 0.5, 'foo')");
          conn.commit();
          conn.createStatement().execute("delete from  " + fullTableName + " where id = 1");
          conn.commit();
            conn.createStatement().execute(
                "upsert into  " + fullTableName + " values (1, 0.5, 'foo')");
          conn.commit();
          ResultSet rs = conn.createStatement().executeQuery("select * from "+fullIndexName);
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(2));
          assertEquals(0.5F, rs.getFloat(1), 0.0);
          assertEquals("foo", rs.getString(3));
      } 
  }

    @Test
    public void testUpsertingDeletedRowWithNullCoveredColumn() throws Exception {
        testUpsertingDeletedRowWithNullCoveredColumn(false);
    }

    @Test
    public void testUpsertingDeletedRowWithNullCoveredColumnMultiCfs() throws Exception {
        testUpsertingDeletedRowWithNullCoveredColumn(true);
    }

    public void testUpsertingDeletedRowWithNullCoveredColumn(boolean multiCf) throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String columnFamily1 = "cf1";
        String columnFamily2 = "cf2";
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        try (Connection conn = getConnection()) {
            conn.createStatement()
                    .execute("create table " + fullTableName + " (id integer primary key, "
                            + (multiCf ? columnFamily1 + "." : "") + "f varchar, "
                            + (multiCf ? columnFamily2 + "." : "") + "s varchar)"
                            + tableDDLOptions);
            conn.createStatement()
                    .execute("create " + (localIndex ? "LOCAL" : "") + " index " + indexName
                            + " on " + fullTableName + " (" + (multiCf ? columnFamily1 + "." : "")
                            + "f) include (" + (multiCf ? columnFamily2 + "." : "") + "s)");
            conn.createStatement()
                    .execute("upsert into " + fullTableName + " values (1, 'foo', 'bar')");
            conn.commit();
            conn.createStatement().execute("delete from  " + fullTableName + " where id = 1");
            conn.commit();
            conn.createStatement()
                    .execute("upsert into  " + fullTableName + " values (1, null, 'bar')");
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("select * from " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(2));
            assertEquals(null, rs.getString(1));
            assertEquals("bar", rs.getString(3));
        }
    }

  /**
   * PHOENIX-4988
   * Test updating only a non-indexed column after two successive deletes to an indexed row
   */
  @Test
  public void testUpdateNonIndexedColumn() throws Exception {
      String tableName = "TBL_" + generateUniqueName();
      String indexName = "IDX_" + generateUniqueName();
      String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
      String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
      try (Connection conn = getConnection()) {
          conn.setAutoCommit(false);
          conn.createStatement().execute("CREATE TABLE " + fullTableName + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " + tableDDLOptions);
          conn.createStatement().execute("CREATE " + (localIndex ? " LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (v2)");
          conn.createStatement().executeUpdate("UPSERT INTO " + fullTableName + "(k,v1,v2) VALUES ('testKey','v1_1','v2_1')");
          conn.commit();
          conn.createStatement().executeUpdate("DELETE FROM " + fullTableName);
          conn.commit();
          conn.createStatement().executeUpdate("UPSERT INTO " + fullTableName + "(k,v1,v2) VALUES ('testKey','v1_2','v2_2')");
          conn.commit();
          conn.createStatement().executeUpdate("DELETE FROM " + fullTableName);
          conn.commit();
          conn.createStatement().executeUpdate("UPSERT INTO " + fullTableName + "(k,v1) VALUES ('testKey','v1_3')");
          conn.commit();
          IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
          // PHOENIX-4980
          // When there is a flush after a data table update of non-indexed columns, the
          // index gets out of sync on the next write
          getUtility().getHBaseAdmin().flush(TableName.valueOf(fullTableName));
          conn.createStatement().executeUpdate("UPSERT INTO " + fullTableName + "(k,v1,v2) VALUES ('testKey','v1_4','v2_3')");
          conn.commit();
          IndexScrutiny.scrutinizeIndex(conn, fullTableName, fullIndexName);
      }
  }
    private void setupForDeleteCount(Connection conn, String schemaName, String dataTableName,
        String indexTableName1, String indexTableName2) throws SQLException {

        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

        conn.createStatement().execute("CREATE TABLE " + dataTableFullName
            + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) "
            + this.tableDDLOptions);

        if (indexTableName1 != null) {
            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName1, dataTableFullName));
        }

        if (indexTableName2 != null) {
            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL2) INCLUDE (VAL1)", indexTableName2, dataTableFullName));
        }

        PreparedStatement dataPreparedStatement =
            conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
        for (int i = 1; i <= 10; i++) {
            dataPreparedStatement.setInt(1, i);
            dataPreparedStatement.setInt(2, i + 1);
            dataPreparedStatement.setInt(3, i * 2);
            dataPreparedStatement.execute();
        }
        conn.commit();
    }

    @Test
    public void testDeleteCount_PK() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = "IND_" + generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            setupForDeleteCount(conn, schemaName, dataTableName, indexTableName, null);

            PreparedStatement deleteStmt =
                conn.prepareStatement("DELETE FROM " + dataTableFullName + " WHERE ID > 5");
            assertEquals(5, deleteStmt.executeUpdate());
            conn.commit();
        }
    }

    @Test
    public void testDeleteCount_nonPK() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName1 = "IND_" + generateUniqueName();
        String indexTableName2 = "IND_" + generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            setupForDeleteCount(conn, schemaName, dataTableName, indexTableName1, indexTableName2);

            PreparedStatement deleteStmt =
                conn.prepareStatement("DELETE FROM " + dataTableFullName + " WHERE VAL1 > 6");
            assertEquals(5, deleteStmt.executeUpdate());
            conn.commit();
        }
    }

    @Test
    public void testDeleteCount_limit() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName1 = "IND_" + generateUniqueName();
        String indexTableName2 = "IND_" + generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            setupForDeleteCount(conn, schemaName, dataTableName, indexTableName1, indexTableName2);

            PreparedStatement deleteStmt =
                conn.prepareStatement("DELETE FROM " + dataTableFullName + " WHERE VAL1 > 6 LIMIT 3");
            assertEquals(3, deleteStmt.executeUpdate());
            conn.commit();
        }
    }

    @Test
    public void testDeleteCount_index() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String indexTableName = "IND_" + generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            setupForDeleteCount(conn, schemaName, dataTableName, indexTableName, null);

            PreparedStatement deleteStmt =
                conn.prepareStatement("DELETE FROM " + indexTableFullName + " WHERE \"0:VAL1\" > 6");
            assertEquals(5, deleteStmt.executeUpdate());
            conn.commit();
        }
    }

private void upsertRow(String dml, Connection tenantConn, int i) throws SQLException {
    PreparedStatement stmt = tenantConn.prepareStatement(dml);
      stmt.setString(1, "00000000000000" + String.valueOf(i));
      stmt.setString(2, String.valueOf(i));
      assertEquals(1,stmt.executeUpdate());
}
}
