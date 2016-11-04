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
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

public class TenantSpecificViewIndexCompileTest extends BaseConnectionlessQueryTest {

    @Test
    public void testOrderByOptimizedOut() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t(t_id VARCHAR NOT NULL, k1 VARCHAR, k2 VARCHAR, v1 VARCHAR," +
        		" CONSTRAINT pk PRIMARY KEY(t_id, k1, k2)) multi_tenant=true");
        
        String tenantId = "me";
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM t WHERE k1 = 'a'");
        conn.createStatement().execute("CREATE INDEX i1 ON v(v2) INCLUDE(v1)");
        
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT v1,v2 FROM v WHERE v2 > 'a' ORDER BY v2");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _IDX_T [-32768,'me','a'] - [-32768,'me',*]",
                QueryUtil.getExplainPlan(rs));
    }

    @Test
    public void testOrderByOptimizedOutWithoutPredicateInView() throws Exception {

        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t(t_id CHAR(15) NOT NULL, k1 CHAR(3) NOT NULL, k2 CHAR(15) NOT NULL, k3 DATE NOT NULL, v1 VARCHAR," +
                " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2, k3)) multi_tenant=true");
        conn.createStatement().execute("CREATE VIEW v1  AS SELECT * FROM t");

        conn = createTenantSpecificConnection();
        
        // Query without predicate ordered by full row key
        String sql = "SELECT * FROM v1 ORDER BY k1, k2, k3";
        String expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789']"; 
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);

        // Predicate with valid partial PK
        sql = "SELECT * FROM v1 WHERE k1 = 'xyz' ORDER BY k1, k2, k3";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz']";
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);
        
        sql = "SELECT * FROM v1 WHERE k1 > 'xyz' ORDER BY k1, k2, k3";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xy{'] - ['tenant123456789',*]";
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);

        String datePredicate = createStaticDate();
        sql = "SELECT * FROM v1 WHERE k1 = 'xyz' AND k2 = '123456789012345' AND k3 < TO_DATE('" + datePredicate + "') ORDER BY k1, k2, k3";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz','123456789012345',*] - ['tenant123456789','xyz','123456789012345','2015-01-01 08:00:00.000']";
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);

        
        // Predicate without valid partial PK
        sql = "SELECT * FROM v1 WHERE k2 < 'abcde1234567890' ORDER BY k1, k2, k3";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789']\n" +
                "    SERVER FILTER BY K2 < 'abcde1234567890'";
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);
    }
    
    @Test
    public void testOrderByOptimizedOutWithPredicateInView() throws Exception {
        // Arrange
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t(t_id CHAR(15) NOT NULL, k1 CHAR(3) NOT NULL, k2 CHAR(15) NOT NULL, k3 DATE NOT NULL, v1 VARCHAR," +
                " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2, k3)) multi_tenant=true");
        conn.createStatement().execute("CREATE VIEW v1  AS SELECT * FROM t WHERE k1 = 'xyz'");
        conn = createTenantSpecificConnection();

        // Query without predicate ordered by full row key
        String sql = "SELECT * FROM v1 ORDER BY k2, k3";
        String expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz']"; 
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);
        
        // Query without predicate ordered by full row key, but without column view predicate
        sql = "SELECT * FROM v1 ORDER BY k2, k3";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz']"; 
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);
        
        // Predicate with valid partial PK
        sql = "SELECT * FROM v1 WHERE k1 = 'xyz' ORDER BY k2, k3";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz']";
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);

        sql = "SELECT * FROM v1 WHERE k2 < 'abcde1234567890' ORDER BY k2, k3";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz',*] - ['tenant123456789','xyz','abcde1234567890']";
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);

        // Predicate with full PK
        String datePredicate = createStaticDate();
        sql = "SELECT * FROM v1 WHERE k2 = '123456789012345' AND k3 < TO_DATE('" + datePredicate + "') ORDER BY k2, k3";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz','123456789012345',*] - ['tenant123456789','xyz','123456789012345','2015-01-01 08:00:00.000']";
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);

        
        // Predicate with valid partial PK
        sql = "SELECT * FROM v1 WHERE k3 < TO_DATE('" + datePredicate + "') ORDER BY k2, k3";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz']\n" +
                "    SERVER FILTER BY K3 < DATE '" + datePredicate + "'";
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);
    }

    @Test
    public void testOrderByOptimizedOutWithMultiplePredicatesInView() throws Exception {
        // Arrange
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t(t_id CHAR(15) NOT NULL, k1 CHAR(3) NOT NULL, k2 CHAR(5) NOT NULL, k3 DATE NOT NULL, v1 VARCHAR," +
                " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2, k3 DESC)) multi_tenant=true");
        conn.createStatement().execute("CREATE VIEW v1  AS SELECT * FROM t WHERE k1 = 'xyz' AND k2='abcde'");
        conn = createTenantSpecificConnection();

        // Query without predicate ordered by full row key
        String sql = "SELECT * FROM v1 ORDER BY k3 DESC";
        String expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz','abcde']"; 
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);
        
        // Query without predicate ordered by full row key, but without column view predicate
        sql = "SELECT * FROM v1 ORDER BY k3 DESC";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz','abcde']"; 
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);

        // Query with predicate ordered by full row key
        sql = "SELECT * FROM v1 WHERE k3 < TO_DATE('" + createStaticDate() + "') ORDER BY k3 DESC";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['tenant123456789','xyz','abcde',~'2015-01-01 07:59:59.999'] - ['tenant123456789','xyz','abcde',*]";
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);

        // Query with predicate ordered by full row key with date in reverse order
        sql = "SELECT * FROM v1 WHERE k3 < TO_DATE('" + createStaticDate() + "') ORDER BY k3";
        expectedExplainOutput = "CLIENT PARALLEL 1-WAY REVERSE RANGE SCAN OVER T ['tenant123456789','xyz','abcde',~'2015-01-01 07:59:59.999'] - ['tenant123456789','xyz','abcde',*]";
        assertExplainPlanIsCorrect(conn, sql, expectedExplainOutput);
        assertOrderByHasBeenOptimizedOut(conn, sql);

    }


    @Test
    public void testViewConstantsOptimizedOut() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t(t_id VARCHAR NOT NULL, k1 VARCHAR, k2 VARCHAR, v1 VARCHAR," +
                " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2)) multi_tenant=true");
        
        String tenantId = "me";
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM t WHERE k2 = 'a'");
        conn.createStatement().execute("CREATE INDEX i1 ON v(v2)");
        
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT v2 FROM v WHERE v2 > 'a' and k2 = 'a' ORDER BY v2,k2");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _IDX_T [-32768,'me','a'] - [-32768,'me',*]\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY",
                QueryUtil.getExplainPlan(rs));
        
        // Won't use index b/c v1 is not in index, but should optimize out k2 still from the order by
        // K2 will still be referenced in the filter, as these are automatically tacked on to the where clause.
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT v1 FROM v WHERE v2 > 'a' ORDER BY k2");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['me']\n" + 
                "    SERVER FILTER BY (V2 > 'a' AND K2 = 'a')",
                QueryUtil.getExplainPlan(rs));

        // If we match K2 against a constant not equal to it's view constant, we should get a degenerate plan
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT v1 FROM v WHERE v2 > 'a' and k2='b' ORDER BY k2");
        assertEquals("DEGENERATE SCAN OVER V",
                QueryUtil.getExplainPlan(rs));
    }

    @Test
    public void testViewConstantsOptimizedOutOnReadOnlyView() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t(t_id VARCHAR NOT NULL, k1 VARCHAR, k2 VARCHAR, v1 VARCHAR," +
                " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2)) multi_tenant=true");
        
        String tenantId = "me";
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM t WHERE k2 = 'a'");
        conn.createStatement().execute("CREATE VIEW v2(v3 VARCHAR) AS SELECT * FROM v WHERE k1 > 'a'");
        conn.createStatement().execute("CREATE INDEX i2 ON v2(v3) include(v2)");
        
        // Confirm that a read-only view on an updatable view still optimizes out the read-only parts of the updatable view
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT v2 FROM v2 WHERE v3 > 'a' and k2 = 'a' ORDER BY v3,k2");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _IDX_T [-32768,'me','a'] - [-32768,'me',*]",
                QueryUtil.getExplainPlan(rs));
    }
    
    //-----------------------------------------------------------------
    // Private Helper Methods
    //-----------------------------------------------------------------
    private Connection createTenantSpecificConnection() throws SQLException {
        Connection conn;
        Properties props = new Properties();
        String tenantId = "tenant123456789";
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
        conn = DriverManager.getConnection(getUrl(), props);
        return conn;
    }
    
    
    private void assertExplainPlanIsCorrect(Connection conn, String sql,
            String expectedExplainOutput) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + sql);
        assertEquals(expectedExplainOutput, QueryUtil.getExplainPlan(rs));
    }

    private void assertOrderByHasBeenOptimizedOut(Connection conn, String sql) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(sql);
        QueryPlan plan = PhoenixRuntime.getOptimizedQueryPlan(stmt);
        assertEquals(0, plan.getOrderBy().getOrderByExpressions().size());
    }
    
    /**
     * Returns the default String representation of 1/1/2015 00:00:00
     */
    private String createStaticDate() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DAY_OF_YEAR, 1);
        cal.set(Calendar.YEAR, 2015);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        return DateUtil.DEFAULT_DATE_FORMATTER.format(cal.getTime());
    }
    
}
