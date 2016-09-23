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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class SaltedIndexIT extends ParallelStatsDisabledIT {
    private static final int TABLE_SPLITS = 3;
    private static final int INDEX_SPLITS = 4;
    
    private static Connection getConnection() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, Integer.toString(1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        return conn;
    }
    
    private static void makeImmutableAndDeleteData(String tableName, String fullTableName) throws Exception {
        Connection conn = getConnection();
        try {
            conn.setAutoCommit(true);
            conn.createStatement().execute("DELETE FROM " + fullTableName);
            conn.createStatement().execute("ALTER TABLE " + fullTableName + " SET IMMUTABLE_ROWS=true");
            conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName).next();
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            assertTrue(pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName)).isImmutableRows());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testMutableTableIndexMaintanenceSaltedSalted() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        testMutableTableIndexMaintanence(tableName, fullTableName, indexName, fullIndexName, TABLE_SPLITS, INDEX_SPLITS);
        makeImmutableAndDeleteData(tableName, fullTableName);
        testMutableTableIndexMaintanence(tableName, fullTableName, indexName, fullIndexName, TABLE_SPLITS, INDEX_SPLITS);
    }

    @Test
    public void testMutableTableIndexMaintanenceSalted() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        testMutableTableIndexMaintanence(tableName, fullTableName, indexName, fullIndexName, null, INDEX_SPLITS);
        makeImmutableAndDeleteData(tableName, fullTableName);
        testMutableTableIndexMaintanence(tableName, fullTableName, indexName, fullIndexName, null, INDEX_SPLITS);
    }

    @Test
    public void testMutableTableIndexMaintanenceUnsalted() throws Exception {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        testMutableTableIndexMaintanence(tableName, fullTableName, indexName, fullIndexName, null, null);
        makeImmutableAndDeleteData(tableName, fullTableName);
        testMutableTableIndexMaintanence(tableName, fullTableName, indexName, fullIndexName, null, null);
    }

    private void testMutableTableIndexMaintanence(String dataTableName , String dataTableFullName, String indexTableName, String indexTableFullName, Integer tableSaltBuckets, Integer indexSaltBuckets) throws Exception {
        String query;
        ResultSet rs;
        
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS " + dataTableFullName + " (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR)  " +  (tableSaltBuckets == null ? "" : " SALT_BUCKETS=" + tableSaltBuckets));
        query = "SELECT * FROM " + dataTableFullName;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());
        
        conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexTableName + " ON " + dataTableFullName + " (v DESC)" + (indexSaltBuckets == null ? "" : " SALT_BUCKETS=" + indexSaltBuckets));
        query = "SELECT * FROM " + indexTableFullName;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?)");
        stmt.setString(1,"a");
        stmt.setString(2, "x");
        stmt.execute();
        stmt.setString(1,"b");
        stmt.setString(2, "y");
        stmt.execute();
        conn.commit();

        query = "SELECT * FROM " + indexTableFullName;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("y",rs.getString(1));
        assertEquals("b",rs.getString(2));
        assertTrue(rs.next());
        assertEquals("x",rs.getString(1));
        assertEquals("a",rs.getString(2));
        assertFalse(rs.next());

        query = "SELECT k,v FROM " + dataTableFullName + " WHERE v = 'y'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertFalse(rs.next());
        
        String expectedPlan;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        expectedPlan = indexSaltBuckets == null ? 
             "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + indexTableFullName + " [~'y']\n" +
             "    SERVER FILTER BY FIRST KEY ONLY" :
            ("CLIENT PARALLEL 4-WAY RANGE SCAN OVER " + indexTableFullName + " [0,~'y'] - ["+(indexSaltBuckets.intValue()-1)+",~'y']\n" +

             "    SERVER FILTER BY FIRST KEY ONLY\n" +
             "CLIENT MERGE SORT");
        assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));

        // Will use index, so rows returned in DESC order.
        // This is not a bug, though, because we can
        // return in any order.
        query = "SELECT k,v FROM " + dataTableFullName + " WHERE v >= 'x'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        expectedPlan = indexSaltBuckets == null ? 
            "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + indexTableFullName + " [*] - [~'x']\n"
          + "    SERVER FILTER BY FIRST KEY ONLY" :
            ("CLIENT PARALLEL 4-WAY RANGE SCAN OVER " + indexTableFullName + " [0,*] - ["+(indexSaltBuckets.intValue()-1)+",~'x']\n"

           + "    SERVER FILTER BY FIRST KEY ONLY\n" +
             "CLIENT MERGE SORT");
        assertEquals(expectedPlan,QueryUtil.getExplainPlan(rs));
        
        // Use data table, since point lookup trumps order by
        query = "SELECT k,v FROM " + dataTableFullName + " WHERE k = 'a' ORDER BY v";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        expectedPlan = tableSaltBuckets == null ? 
                "CLIENT PARALLEL 1-WAY POINT LOOKUP ON 1 KEY OVER " + dataTableFullName + "\n" +
                "    SERVER SORTED BY [V]\n" + 
                "CLIENT MERGE SORT" :
                    "CLIENT PARALLEL 1-WAY POINT LOOKUP ON 1 KEY OVER " + dataTableFullName + "\n" +
                    "    SERVER SORTED BY [V]\n" + 
                    "CLIENT MERGE SORT";
        String explainPlan2 = QueryUtil.getExplainPlan(rs);
        assertEquals(expectedPlan,explainPlan2);
        
        // Will use data table now, since there's a LIMIT clause and
        // we're able to optimize out the ORDER BY, unless the data
        // table is salted.
        query = "SELECT k,v FROM " + dataTableFullName + " WHERE v >= 'x' ORDER BY k LIMIT 2";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("x",rs.getString(2));
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals("y",rs.getString(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        expectedPlan = tableSaltBuckets == null ? 
             "CLIENT PARALLEL 1-WAY FULL SCAN OVER " + dataTableFullName + "\n" +
             "    SERVER FILTER BY V >= 'x'\n" + 
             "    SERVER 2 ROW LIMIT\n" + 
             "CLIENT 2 ROW LIMIT" :
                 "CLIENT PARALLEL 3-WAY FULL SCAN OVER " + dataTableFullName + "\n" +
                 "    SERVER FILTER BY V >= 'x'\n" + 
                 "    SERVER 2 ROW LIMIT\n" + 
                 "CLIENT MERGE SORT\n" + 
                 "CLIENT 2 ROW LIMIT";
        String explainPlan = QueryUtil.getExplainPlan(rs);
        assertEquals(expectedPlan,explainPlan);
    }
}
