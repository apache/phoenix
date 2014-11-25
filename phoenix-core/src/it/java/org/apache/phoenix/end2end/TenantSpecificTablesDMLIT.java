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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class TenantSpecificTablesDMLIT extends BaseTenantSpecificTablesIT {
	
	@Test
	public void testSelectWithLimit() throws Exception {
		Connection conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + TENANT_TABLE_NAME + " LIMIT 100");
		while(rs.next()) {}
	}
	
    @Test
    public void testBasicUpsertSelect() throws Exception {
        Connection conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        try {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (1, 'Cheap Sunglasses')");
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (2, 'Viva Las Vegas')");
            conn.commit();
            conn.close();
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            analyzeTable(conn, TENANT_TABLE_NAME);
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
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
        Connection conn1 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL2, TENANT_TABLE_DDL, null, nextTimestamp());
        Connection conn2 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL2);
        try {
            conn1.setAutoCommit(false);
            conn1.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " values ('me','" + TENANT_TYPE_ID + "',1,'Cheap Sunglasses')");
            conn1.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " values ('you','" + TENANT_TYPE_ID +"',2,'Viva Las Vegas')");
            conn1.commit();
            conn1 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            analyzeTable(conn1, TENANT_TABLE_NAME);
            conn1 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            
            conn2.setAutoCommit(true);
            conn2.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " values ('them','" + TENANT_TYPE_ID + "',1,'Long Hair')");
            conn2.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " values ('us','" + TENANT_TYPE_ID + "',2,'Black Hat')");
            conn2.close();
            conn1.close();
            conn1 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            ResultSet rs = conn1.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME + " where id = 1");
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals(1, rs.getInt(3));
            assertEquals("Cheap Sunglasses", rs.getString(4));
            assertFalse("Expected 1 row in result set", rs.next());
            conn2 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL2);
            analyzeTable(conn2, TENANT_TABLE_NAME);
            conn2 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL2);

            rs = conn2.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME + " where id = 2");
            assertTrue("Expected 1 row in result set", rs.next());
            assertEquals(2, rs.getInt(3));
            assertEquals("Black Hat", rs.getString(4));
            assertFalse("Expected 1 row in result set", rs.next());
            conn2.close();
            conn1 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            analyzeTable(conn1, TENANT_TABLE_NAME);
            conn1.close();
            
            conn2 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL2);
            conn2.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " select * from " + TENANT_TABLE_NAME );
            conn2.commit();
            conn2.close();
            
            conn2 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL2);
            rs = conn2.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME);
            assertTrue("Expected row in result set", rs.next());
            assertEquals(1, rs.getInt(3));
            assertEquals("Long Hair", rs.getString(4));
            assertTrue("Expected row in result set", rs.next());
            assertEquals(2, rs.getInt(3));
            assertEquals("Black Hat", rs.getString(4));
            assertFalse("Expected 2 rows total", rs.next());
            conn2.close();
            
            conn2 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL2);
            conn2.setAutoCommit(true);;
            conn2.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " select 'all', tenant_type_id, id, 'Big ' || tenant_col from " + TENANT_TABLE_NAME );
            conn2.close();

            conn2 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL2);
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
            conn2.close();
            conn1 = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
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
    
    private Connection nextConnection(String url) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        return DriverManager.getConnection(url, props);
    }
    
    @Test
    public void testJoinWithGlobalTable() throws Exception {
        Connection conn = nextConnection(getUrl());
        conn.createStatement().execute("create table foo (k INTEGER NOT NULL PRIMARY KEY)");
        conn.close();

        conn = nextConnection(getUrl());
        conn.createStatement().execute("upsert into foo(k) values(1)");
        conn.commit();
        conn.close();

        conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        try {
            conn.setAutoCommit(false);
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (1, 'Cheap Sunglasses')");
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (2, 'Viva Las Vegas')");
            conn.commit();
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            analyzeTable(conn, TENANT_TABLE_NAME);
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
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
        Connection conn = nextConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.close();
            
            conn = nextConnection(getUrl());
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            ResultSet rs = conn.createStatement().executeQuery("select user from " + TENANT_TABLE_NAME);
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
        Connection conn = nextConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.close();
            
            conn = nextConnection(getUrl());
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("delete from " + TENANT_TABLE_NAME);
            assertEquals("Expected 1 row have been deleted", 1, count);
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            ResultSet rs = conn.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME);
            assertFalse("Expected no rows in result set", rs.next());
            conn.close();
            
            conn = nextConnection(getUrl());
            analyzeTable(conn, PARENT_TABLE_NAME);
            conn = nextConnection(getUrl());
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
        Connection conn = nextConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.close();
            
            conn = nextConnection(getUrl());
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.createStatement().executeUpdate("create index idx1 on " + TENANT_TABLE_NAME + "(user)");
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("delete from " + TENANT_TABLE_NAME + " where user='Billy Gibbons'");
            assertEquals("Expected 1 row have been deleted", 1, count);
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            ResultSet rs = conn.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME);
            assertFalse("Expected no rows in result set", rs.next());
            conn.close();
            
            conn = nextConnection(getUrl());
            analyzeTable(conn, PARENT_TABLE_NAME);
            conn = nextConnection(getUrl());
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
        Connection conn = nextConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID);
            conn.close();
            
            conn = nextConnection(getUrl());
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, user) values ('AC/DC', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, user) values ('" + TENANT_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, user) values ('" + TENANT_ID + "', 2, 'Billy Gibbons')");
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("delete from " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID);
            assertEquals("Expected 2 rows have been deleted", 2, count);
            conn.close();
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            ResultSet rs = conn.createStatement().executeQuery("select * from " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID);
            assertFalse("Expected no rows in result set", rs.next());
            conn.close();
            
            conn = nextConnection(getUrl());
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
        Connection conn = nextConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.close();
            
            conn = nextConnection(getUrl());
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'abc', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 1, 'Billy Gibbons')");
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            analyzeTable(conn, PARENT_TABLE_NAME);
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.createStatement().execute("delete from " + TENANT_TABLE_NAME);
            conn.commit();
            conn.close();
            
            conn = nextConnection(getUrl());
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + PARENT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDropTenantTableDeletesNoData() throws Exception {
        Connection conn = nextConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID);
            conn.close();
            
            conn = nextConnection(getUrl());
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, user) values ('AC/DC', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, user) values ('" + TENANT_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " (tenant_id, id, user) values ('" + TENANT_ID + "', 2, 'Billy Gibbons')");
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.createStatement().execute("drop view " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID);
            conn.close();
            
            conn = nextConnection(getUrl());
            analyzeTable(conn, PARENT_TABLE_NAME_NO_TENANT_TYPE_ID);
            conn = nextConnection(getUrl());
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID);
            rs.next();
            assertEquals(3, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testUpsertSelectOnlyUpsertsTenantData() throws Exception {
        Connection conn = nextConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.close();
            
            conn = nextConnection(getUrl());
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'aaa', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 2, 'Billy Gibbons')");
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            analyzeTable(conn, TENANT_TABLE_NAME);
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + "(id, user) select id+100, user from " + TENANT_TABLE_NAME);
            assertEquals("Expected 1 row to have been inserted", 1, count);
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testUpsertSelectOnlyUpsertsTenantDataWithDifferentTenantTable() throws Exception {
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW ANOTHER_TENANT_TABLE ( " + 
            "tenant_col VARCHAR) AS SELECT * FROM " + PARENT_TABLE_NAME + " WHERE tenant_type_id = 'def'", null, nextTimestamp(), false);
        
        Connection conn = nextConnection(getUrl());
        try {
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("delete from " + PARENT_TABLE_NAME);
            conn.close();
            
            conn = nextConnection(getUrl());
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('AC/DC', 'aaa', 1, 'Bon Scott')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', '" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_id, tenant_type_id, id, user) values ('" + TENANT_ID + "', 'def', 2, 'Billy Gibbons')");
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            analyzeTable(conn, TENANT_TABLE_NAME);
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + "(id, user) select id+100, user from ANOTHER_TENANT_TABLE where id=2");
            assertEquals("Expected 1 row to have been inserted", 1, count);
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testUpsertValuesOnlyUpsertsTenantData() throws Exception {
        Connection conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        try {
            conn.setAutoCommit(true);
            int count = conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, user) values (1, 'Bon Scott')");
            assertEquals("Expected 1 row to have been inserted", 1, count);
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
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
        Connection conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        try {
            ResultSet rs = conn.createStatement().executeQuery("select * from " + PARENT_TABLE_NAME);
            assertFalse(rs.next());
            conn.close();
            
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            conn.setAutoCommit(true);
            conn.createStatement().executeUpdate("upsert into " + PARENT_TABLE_NAME + " (tenant_type_id, id, user) values ('" + TENANT_TYPE_ID + "', 1, 'Billy Gibbons')");
            conn.close();
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            analyzeTable(conn, PARENT_TABLE_NAME);
            conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
            rs = conn.createStatement().executeQuery("select user from " + PARENT_TABLE_NAME);
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
        Connection conn = nextConnection(getUrl());
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
        Connection conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        conn.setAutoCommit(true);
        conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID + " (id) values (0)");
        conn.close();
        
        conn = nextConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL);
        ResultSet rs = conn.createStatement().executeQuery("select id from " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID);
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }
}
