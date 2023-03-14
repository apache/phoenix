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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.exception.PhoenixParserException;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsEnabledTest.class)
public class QueryWithTableSampleIT extends ParallelStatsEnabledIT {
    private String tableName;
    private String joinedTableName;
    
    @Before
    public void generateTableNames() {
        tableName = "T_" + generateUniqueName();
        joinedTableName = "T_" + generateUniqueName();
    }
        
    @Test(expected=PhoenixParserException.class)
    public void testSingleQueryWrongSyntax() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "SELECT i1, i2 FROM " + tableName +" tablesample 15 ";

            ResultSet rs = conn.createStatement().executeQuery(query);
        } finally {
            conn.close();
        }
    }
    
    @Test(expected=PhoenixParserException.class)
    public void testSingleQueryWrongSamplingRate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "SELECT i1, i2 FROM " + tableName +" tablesample (175) ";

            ResultSet rs = conn.createStatement().executeQuery(query);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSingleQueryZeroSamplingRate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "SELECT i1, i2 FROM " + tableName +" tablesample (0) ";
            ResultSet rs = conn.createStatement().executeQuery(query);                        
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSingleQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "SELECT i1, i2 FROM " + tableName +" tablesample (45) ";
            ResultSet rs = conn.createStatement().executeQuery(query);
            
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(200, rs.getInt(2));
            
            assertTrue(rs.next());
            assertEquals(6, rs.getInt(1));
            assertEquals(600, rs.getInt(2));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSingleQueryWithWhereClause() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "SELECT i1, i2 FROM " + tableName +" tablesample (22) where i2>=300 and i1<14 LIMIT 4 ";
            ResultSet rs = conn.createStatement().executeQuery(query);
            
            assertTrue(rs.next());
            assertEquals(8, rs.getInt(1));
            
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            
            assertTrue(rs.next());
            assertEquals(12, rs.getInt(1));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSingleQueryWithAggregator() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "SELECT count(i1) FROM " + tableName +" tablesample (22) where i2>=3000 or i1<2 ";
            ResultSet rs = conn.createStatement().executeQuery(query);
            
            assertTrue(rs.next());
            assertEquals(14, rs.getInt(1));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSingleQueryWithUnion() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "SELECT * FROM " + tableName +" tablesample (100) where i1<2 union all SELECT * FROM " + tableName +" tablesample (2) where i2<6000";
            ResultSet rs = conn.createStatement().executeQuery(query);
            
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(44, rs.getInt(1));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSingleQueryWithSubQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "SELECT count(*) FROM (SELECT * FROM " + tableName +" tablesample (50))";
            ResultSet rs = conn.createStatement().executeQuery(query);
            
            assertTrue(rs.next());
            assertEquals(50, rs.getInt(1));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSingleQueryWithJoins() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "SELECT count(*) FROM " + tableName +" as A tablesample (45), "+joinedTableName+" as B tablesample (75) where A.i1=B.i1";
            ResultSet rs = conn.createStatement().executeQuery(query);

            assertTrue(rs.next());
            assertEquals(31, rs.getInt(1));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testExplainSingleQuery() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            prepareTableWithValues(conn, 100);
            String query = "SELECT i1, i2 FROM " + tableName + " tablesample (45) ";
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(0.45D, explainPlanAttributes.getSamplingRate(), 0D);
            assertEquals(tableName, explainPlanAttributes.getTableName());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
        }
    }
    
    @Test
    public void testExplainSingleQueryWithUnion() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "EXPLAIN SELECT * FROM " + tableName +" tablesample (100) where i1<2 union all SELECT * FROM " + tableName +" tablesample (2) where i2<6000";
            ResultSet rs = conn.createStatement().executeQuery(query);
            
            assertEquals("UNION ALL OVER 2 QUERIES\n" +
						"    CLIENT PARALLEL 1-WAY 1.0-SAMPLED RANGE SCAN OVER " + tableName+" [*] - [2]\n"+
						"        SERVER FILTER BY FIRST KEY ONLY\n" +
						"    CLIENT PARALLEL 1-WAY 0.02-SAMPLED FULL SCAN OVER " + tableName + "\n" +
						"        SERVER FILTER BY FIRST KEY ONLY AND I2 < 6000",QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testExplainSingleQueryWithJoins() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            prepareTableWithValues(conn, 100);
            String query = "EXPLAIN SELECT count(*) FROM " + tableName +" as A tablesample (45), "+joinedTableName+" as B tablesample (75) where A.i1=B.i1";
            System.out.println(query);
            ResultSet rs = conn.createStatement().executeQuery(query);
            
            assertEquals("CLIENT PARALLEL 1-WAY 0.45-SAMPLED FULL SCAN OVER " + tableName + "\n" +
						"    SERVER FILTER BY FIRST KEY ONLY\n" +
						"    SERVER AGGREGATE INTO SINGLE ROW\n" +
						"    PARALLEL INNER-JOIN TABLE 0 (SKIP MERGE)\n" +
						"        CLIENT PARALLEL 1-WAY 0.75-SAMPLED FULL SCAN OVER " + joinedTableName + "\n" +
						"            SERVER FILTER BY FIRST KEY ONLY\n" +
						"    DYNAMIC SERVER FILTER BY A.I1 IN (B.I1)",QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    /**
     * Prepare tables with stats updated. format of first table such as
     *  i1, i2
     *   1, 100
     *   2, 200
     *   3, 300
     *   ...
     *   
     * @param conn
     * @param nRows
     * @throws Exception
     */
    final private void prepareTableWithValues(final Connection conn, final int nRows) throws Exception {
        conn.createStatement().execute("create table " + tableName + "\n" +
                "   (i1 integer not null, i2 integer not null\n" +
                "    CONSTRAINT pk PRIMARY KEY (i1,i2))");
        
        final PreparedStatement stmt = conn.prepareStatement(
            "upsert into " + tableName + 
            " VALUES (?, ?)");
        for (int i = 0; i < nRows; i++) {
            stmt.setInt(1, i);
            stmt.setInt(2, i*100);
            stmt.execute();
        }
        conn.commit();
        
        conn.createStatement().execute("UPDATE STATISTICS " + tableName);
        
        //Prepare for second table
        conn.createStatement().execute("create table " + joinedTableName + "\n" +
                "   (i1 integer not null, i2 integer not null\n" +
                "    CONSTRAINT pk PRIMARY KEY (i1,i2))");
        
        final PreparedStatement stmt2 = conn.prepareStatement(
            "upsert into " + joinedTableName + 
            " VALUES (?, ?)");
        for (int i = 0; i < nRows; i++) {
            stmt2.setInt(1, i);
            stmt2.setInt(2, i*-100);
            stmt2.execute();
        }
        conn.commit();
        conn.createStatement().execute("UPDATE STATISTICS " + joinedTableName);
    }
}
