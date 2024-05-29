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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsEnabledTest.class)
public class TenantSpecificTablesDMLIT extends BaseTenantSpecificTablesIT {
	
	@Test
	public void testSelectWithLimit() throws Exception {
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, PropertiesUtil.deepCopy(TEST_PROPERTIES));
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + TENANT_TABLE_NAME + " LIMIT 100");
		while(rs.next()) {}
	}

    @Test
    public void testPointLookupOnBaseTable() throws Exception {
        final String tableName = "T_" + generateUniqueName();
        final String viewName = "V_" + generateUniqueName();
        final String tenantId = "tenant1";
        final String kp = "abc";
        String ddl = String.format("CREATE TABLE %s (ORG_ID CHAR(15) NOT NULL, KP CHAR(3) NOT NULL, V1 INTEGER, V2 VARCHAR," +
                "CONSTRAINT PK PRIMARY KEY(ORG_ID, KP)) MULTI_TENANT=true", tableName);
        int nRows = 16;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            try (Connection tconn = DriverManager.getConnection(getUrl(), props)) {
                ddl = String.format("CREATE VIEW %s (PK1 VARCHAR PRIMARY KEY, V3 VARCHAR)" +
                        " AS SELECT * FROM %s WHERE KP='%s'", viewName, tableName, kp);
                tconn.createStatement().execute(ddl);
                tconn.commit();
                // upsert through the tenant
                try(PreparedStatement ps = tconn.prepareStatement(
                        String.format("UPSERT INTO %s(V1,V2,PK1,V3) VALUES (?, ?, ?, ?)", viewName))) {
                    for (int i = 0; i < nRows; i++) {
                        ps.setInt(1, i); // V1
                        ps.setString(2, "v2"); // V2
                        ps.setString(3, "pk_" + i); // PK1
                        ps.setString(4, "v3"); // V3
                        ps.executeUpdate();
                    }
                    tconn.commit();
                }
            }
            // Do a point lookup on the base table
            String dql = String.format("SELECT * FROM %s where org_id='%s' AND kp='%s' LIMIT 1",
                    tableName, tenantId, kp);
            try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
                PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
                String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
                assertTrue(explainPlan.contains("POINT LOOKUP ON 1 KEY"));
                assertTrue(rs.next());
                assertEquals(tenantId, rs.getString(1));
                assertEquals(kp, rs.getString(2));
            }
            dql = String.format("SELECT count(*) FROM %s where org_id='%s' AND kp='%s'",
                    tableName, tenantId, kp);
            try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
                PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
                String explainPlan = QueryUtil.getExplainPlan(prs.getUnderlyingIterator());
                assertTrue(explainPlan.contains("POINT LOOKUP ON 1 KEY"));
                assertTrue(rs.next());
                assertEquals(nRows, rs.getInt(1));
            }
        }
    }
	
    @Test
    public void testBasicUpsertSelect() throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, PropertiesUtil.deepCopy(TEST_PROPERTIES));
        try {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (1, 'Cheap Sunglasses')");
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (2, 'Viva Las Vegas')");
            conn.commit();
            analyzeTable(conn, TENANT_TABLE_NAME);
            ResultSet rs = conn.createStatement().executeQuery("select tenant_col from " + TENANT_TABLE_NAME + " where id = 1");
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals("Cheap Sunglasses", rs.getString(1));
            assertFalse("Expected 1 row in result set", rs.next());
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testBasicUpsertSelect2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn1 = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL2, TENANT_TABLE_DDL);
        Connection conn2 = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL2, props);
        try {
            conn1.setAutoCommit(false);
            conn1.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " values ('me','" + TENANT_TYPE_ID + "',1,'Cheap Sunglasses')");
            conn1.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " values ('you','" + TENANT_TYPE_ID +"',2,'Viva Las Vegas')");
            conn1.commit();
            analyzeTable(conn1, TENANT_TABLE_NAME);
            
            conn2.setAutoCommit(true);
            conn2.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " values ('them','" + TENANT_TYPE_ID + "',1,'Long Hair')");
            conn2.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " values ('us','" + TENANT_TYPE_ID + "',2,'Black Hat')");
            ResultSet rs = conn1.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME + " where id = 1");
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals(1, rs.getInt(3));
            assertEquals("Cheap Sunglasses", rs.getString(4));
            assertFalse("Expected 1 row in result set", rs.next());
            analyzeTable(conn2, TENANT_TABLE_NAME);

            rs = conn2.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME + " where id = 2");
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals(2, rs.getInt(3));
            assertEquals("Black Hat", rs.getString(4));
            assertFalse("Expected 1 row in result set", rs.next());
            analyzeTable(conn1, TENANT_TABLE_NAME);
            
            conn2.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " select * from " + TENANT_TABLE_NAME );
            conn2.commit();
            
            rs = conn2.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME);
            assertTrue("Expected row in result set", rs.next());
            assertEquals(1, rs.getInt(3));
            assertEquals("Long Hair", rs.getString(4));
            assertTrue("Expected row in result set", rs.next());
            assertEquals(2, rs.getInt(3));
            assertEquals("Black Hat", rs.getString(4));
            assertFalse("Expected 2 rows total", rs.next());
            
            conn2.setAutoCommit(true);;
            conn2.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " select 'all', tenant_type_id, id, 'Big ' || tenant_col from " + TENANT_TABLE_NAME );

            analyzeTable(conn2, TENANT_TABLE_NAME);
            rs = conn2.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME);
            assertTrue("Expected row in result set", rs.next());
            assertEquals("all", rs.getString(1));
            assertEquals(TENANT_TYPE_ID, rs.getString(2));
            assertEquals(1, rs.getInt(3));
            assertEquals("Big Long Hair", rs.getString(4));
            assertTrue("Expected row in result set", rs.next());
            assertEquals("all", rs.getString(1));
            assertEquals(TENANT_TYPE_ID, rs.getString(2));
            assertEquals(2, rs.getInt(3));
            assertEquals("Big Black Hat", rs.getString(4));
            assertFalse("Expected 2 rows total", rs.next());
            rs = conn1.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME);
            assertTrue("Expected row row in result set", rs.next());
            assertEquals(1, rs.getInt(3));
            assertEquals("Cheap Sunglasses", rs.getString(4));
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals(2, rs.getInt(3));
            assertEquals("Viva Las Vegas", rs.getString(4));
            
            List<KeyRange> splits = getAllSplits(conn1, TENANT_TABLE_NAME);
            assertEquals(3, splits.size());
        }
        finally {
            conn1.close();
            conn2.close();
        }
    }
    
    @Test
    public void testJoinWithGlobalTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table foo (k INTEGER NOT NULL PRIMARY KEY)");

        conn.createStatement().execute("upsert into foo(k) values(1)");
        conn.commit();

        conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (1, 'Cheap Sunglasses')");
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (2, 'Viva Las Vegas')");
            conn.commit();
            
            analyzeTable(conn, TENANT_TABLE_NAME);
            ResultSet rs = conn.createStatement().executeQuery("select tenant_col from " + TENANT_TABLE_NAME + " join foo on k=id");
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals("Cheap Sunglasses", rs.getString(1));
            assertFalse("Expected 1 row in result set", rs.next());
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelectOnlySeesTenantData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
            
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            ResultSet rs = conn.createStatement().executeQuery("select \"user\" from " + TENANT_TABLE_NAME);
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals("Billy Gibbons", rs.getString(1));
            assertFalse("Expected 1 row in result set", rs.next());
            
            rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            analyzeTable(conn, PARENT_TABLE_NAME);
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse("Expected 1 row in result set", rs.next());
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDeleteOnlyDeletesTenantData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
            
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("delete from " + TENANT_TABLE_NAME);
            assertEquals("Expected 1 row have been deleted", 1, count);
            ResultSet rs = conn.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME);
            assertFalse("Expected no rows in result set", rs.next());
            
            conn = DriverManager.getConnection(getUrl(), props);
            analyzeTable(conn, PARENT_TABLE_NAME);
            rs = conn.createStatement().executeQuery("select count(*) from " + PARENT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDeleteWhenImmutableIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
            
            Connection tsConn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            tsConn.setAutoCommit(true);
            tsConn.createStatement().executeUpdate("create index idx1 on " + TENANT_TABLE_NAME + "(\"user\")");
            int count = tsConn.createStatement().executeUpdate("delete from " + TENANT_TABLE_NAME + " where \"user\"='Billy Gibbons'");
            assertEquals("Expected 1 row have been deleted", 1, count);
            ResultSet rs = tsConn.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME);
            assertFalse("Expected no rows in result set", rs.next());
            tsConn.close();
            
            analyzeTable(conn, PARENT_TABLE_NAME);
            rs = conn.createStatement().executeQuery("select count(*) from " + PARENT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDeleteOnlyDeletesTenantDataWithNoTenantTypeId() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, \"user\") values ('AC/DC', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, \"user\") values ('" + TENANT_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, \"user\") values ('" + TENANT_ID + "', 2, 'Billy Gibbons')");
            
            Connection tsConn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            tsConn.setAutoCommit(true);
            int count = tsConn.createStatement().executeUpdate("delete from " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID);
            assertEquals("Expected 2 rows have been deleted", 2, count);
            ResultSet rs = tsConn.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID);
            assertFalse("Expected no rows in result set", rs.next());
            
            rs = conn.createStatement().executeQuery("select count(*) from " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID);
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDeleteAllTenantTableData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Connection tsConn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
            
            analyzeTable(tsConn, PARENT_TABLE_NAME);
            tsConn.createStatement().execute("delete from " + TENANT_TABLE_NAME);
            tsConn.commit();
            
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + PARENT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
        finally {
            if (conn != null) conn.close();
            if (tsConn != null) tsConn.close();
        }
    }
    
    @Test
    public void testDropTenantTableDeletesNoData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Connection tsConn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, \"user\") values ('AC/DC', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, \"user\") values ('" + TENANT_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, \"user\") values ('" + TENANT_ID + "', 2, 'Billy Gibbons')");
            
            tsConn.createStatement().execute("drop view " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID);
            
            analyzeTable(conn, PARENT_TABLE_NAME_NO_TENANT_TYPE_ID);
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID);
            rs.next();
            assertEquals(3, rs.getInt(1));
        }
        finally {
            if (conn != null) conn.close();
            if (tsConn != null) tsConn.close();
        }
    }
    
    @Test
    public void testUpsertSelectOnlyUpsertsTenantData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Connection tsConn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('AC/DC', 'aaa', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', 'def', 2, 'Billy Gibbons')");
            
            analyzeTable(tsConn, TENANT_TABLE_NAME);
            int count = tsConn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + "(id, \"user\") select id+100, \"user\" from " + TENANT_TABLE_NAME);
            tsConn.commit();
            assertEquals("Expected 1 row to have been inserted", 1, count);
            
            ResultSet rs = tsConn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
        finally {
            if (conn != null) conn.close();
            if (tsConn != null) tsConn.close();
        }
    }
    
    @Test
    public void testUpsertSelectOnlyUpsertsTenantDataWithDifferentTenantTable() throws Exception {
        String anotherTableName = "V_" + generateUniqueName();
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW " + anotherTableName + " ( " + 
            "tenant_col VARCHAR) AS SELECT * FROM " + PARENT_TABLE_NAME + " WHERE tenant_type_id = 'def'");
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Connection tsConn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('AC/DC', 'aaa', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, \"user\") values ('" + TENANT_ID + "', 'def', 2, 'Billy Gibbons')");
            
            analyzeTable(tsConn, TENANT_TABLE_NAME);
            tsConn.setAutoCommit(true);
            int count = tsConn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + "(id, \"user\")"
                    + "select id+100, \"user\" from " + anotherTableName + " where id=2");
            assertEquals("Expected 1 row to have been inserted", 1, count);
            ResultSet rs = tsConn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
        finally {
            if (conn != null) conn.close();
            if (tsConn != null) tsConn.close();
        }
    }
    
    @Test
    public void testUpsertValuesOnlyUpsertsTenantData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            int count = conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, \"user\") values (1, 'Bon Scott')");
            conn.commit();
            assertEquals("Expected 1 row to have been inserted", 1, count);
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testBaseTableCanBeUsedInStatementsInMultitenantConnections() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            ResultSet rs = conn.createStatement().executeQuery("select * from " + PARENT_TABLE_NAME);
            assertFalse(rs.next());
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_type_id, id, \"user\") values ('" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.commit();
            analyzeTable(conn, PARENT_TABLE_NAME);
            rs = conn.createStatement().executeQuery("select \"user\" from " + PARENT_TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(rs.getString(1),"Billy Gibbons");
            assertFalse(rs.next());
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testTenantTableCannotBeUsedInStatementsInNonMultitenantConnections() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            try {
                conn.createStatement().execute("select * from " + TENANT_TABLE_NAME);
                fail();
            }
            catch (TableNotFoundException expected) {};   
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testUpsertValuesUsingViewWithNoWhereClause() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID + " (id) values (0)");
            conn.commit();
            ResultSet rs = conn.createStatement().executeQuery("select id from " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID);
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
