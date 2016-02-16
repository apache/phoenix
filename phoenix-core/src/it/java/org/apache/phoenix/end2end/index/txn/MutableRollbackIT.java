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
package org.apache.phoenix.end2end.index.txn;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class MutableRollbackIT extends BaseHBaseManagedTimeIT {
	
	private final boolean localIndex;
	private String tableName1;
    private String indexName1;
    private String fullTableName1;
    private String tableName2;
    private String indexName2;
    private String fullTableName2;

	public MutableRollbackIT(boolean localIndex) {
		this.localIndex = localIndex;
		this.tableName1 = TestUtil.DEFAULT_DATA_TABLE_NAME + "_1_";
        this.indexName1 = "IDX1";
        this.fullTableName1 = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName1);
        this.tableName2 = TestUtil.DEFAULT_DATA_TABLE_NAME + "_2_";
        this.indexName2 = "IDX2";
        this.fullTableName2 = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName2);
	}
	
	@BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.DEFAULT_TABLE_ISTRANSACTIONAL_ATTRIB, Boolean.toString(true));
        props.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
	
	@Parameters(name="localIndex = {0}")
    public static Collection<Boolean> data() {
        return Arrays.asList(new Boolean[] {     
                 false, true  
           });
    }
	
    public void testRollbackOfUncommittedExistingKeyValueIndexUpdate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName1 + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            stmt.execute("CREATE TABLE " + fullTableName2 + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX " + indexName1 + " ON " + fullTableName1 + " (v1) INCLUDE(v2)");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX " + indexName2 + " ON " + fullTableName2 + " (v1) INCLUDE(v2)");
            
            stmt.executeUpdate("upsert into " + fullTableName1 + " values('x', 'y', 'a')");
            conn.commit();
            
            //assert rows exists in fullTableName1
            ResultSet rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert rows exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert no rows exists in fullTableName2
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName2);
            assertFalse(rs.next());
            
            //assert no rows exists in indexName2
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName2 + ")*/ k, v1 from " + fullTableName2);
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into " + fullTableName1 + " values('x', 'y', 'b')");
            stmt.executeUpdate("upsert into " + fullTableName2 + " values('a', 'b', 'c')");
            
            //assert new covered column value 
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("b", rs.getString(3));
            assertFalse(rs.next());
            
            //assert new covered column value 
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("b", rs.getString(3));
            assertFalse(rs.next());
            
            //assert rows exists in fullTableName2
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName2);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("b", rs.getString(2));
            assertEquals("c", rs.getString(3));
            assertFalse(rs.next());
            
            //assert rows exists in " + fullTableName2 + " index table
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName2 + ")*/ k, v1 from " + fullTableName2);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("b", rs.getString(2));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert original row exists in fullTableName1
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert original row exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert no rows exists in fullTableName2
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName2);
            assertFalse(rs.next());
            
            //assert no rows exists in indexName2
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName2 + ")*/ k, v1 from " + fullTableName2);
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into " + fullTableName1 + " values('x', 'z', 'a')");
            stmt.executeUpdate("upsert into " + fullTableName2 + " values('a', 'b', 'c')");
            conn.commit();

            assertDataAndIndexRows(stmt);
            stmt.executeUpdate("delete from " + fullTableName1 + " where  k='x'");
            stmt.executeUpdate("delete from " + fullTableName2 + " where  v1='b'");
            
            //assert no rows exists in fullTableName1
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName1);
            assertFalse(rs.next());
            //assert no rows exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1 + " ORDER BY v1");
            assertFalse(rs.next());

            //assert no rows exists in fullTableName2
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName2);
            assertFalse(rs.next());
            //assert no rows exists in indexName2
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName2 + ")*/ k, v1 from " + fullTableName2);
            assertFalse(rs.next());
            
            conn.rollback();
            assertDataAndIndexRows(stmt);
        } finally {
            conn.close();
        }
    }

	@Test
    public void testRollbackOfUncommittedExistingRowKeyIndexUpdate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName1 + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            stmt.execute("CREATE TABLE " + fullTableName2 + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX " + indexName1 + " ON " + fullTableName1 + " (v1, k)");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX " + indexName2 + " ON " + fullTableName2 + " (v1, k)");
            
            stmt.executeUpdate("upsert into " + fullTableName1 + " values('x', 'y', 'a')");
            conn.commit();
            
            //assert rows exists in " + fullTableName1 + " 
            ResultSet rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert rows exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertFalse(rs.next());
            
            //assert no rows exists in fullTableName2
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName2);
            assertFalse(rs.next());
            
            //assert no rows exists in indexName2
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName2 + ")*/ k, v1 from " + fullTableName2);
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into " + fullTableName1 + " values('x', 'z', 'a')");
            stmt.executeUpdate("upsert into " + fullTableName2 + " values('a', 'b', 'c')");
            
            assertDataAndIndexRows(stmt);
            
            conn.rollback();
            
            //assert original row exists in fullTableName1
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert original row exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert no rows exists in fullTableName2
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName2);
            assertFalse(rs.next());
            
            //assert no rows exists in indexName2
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName2);
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into " + fullTableName1 + " values('x', 'z', 'a')");
            stmt.executeUpdate("upsert into " + fullTableName2 + " values('a', 'b', 'c')");
            conn.commit();

            assertDataAndIndexRows(stmt);
            stmt.executeUpdate("delete from " + fullTableName1 + " where  k='x'");
            stmt.executeUpdate("delete from " + fullTableName2 + " where  v1='b'");
            
            //assert no rows exists in fullTableName1
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertFalse(rs.next());
            //assert no rows exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1);
            assertFalse(rs.next());

            //assert no rows exists in fullTableName2
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName2);
            assertFalse(rs.next());
            //assert no rows exists in indexName2
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName2 + ")*/ k, v1 from " + fullTableName2);
            assertFalse(rs.next());
            
            conn.rollback();
            assertDataAndIndexRows(stmt);

        } finally {
            conn.close();
        }
    }
	
    private void assertDataAndIndexRows(Statement stmt) throws SQLException, IOException {
        ResultSet rs;
        //assert new covered row key value exists in fullTableName1
        rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName1);
        assertTrue(rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals("z", rs.getString(2));
        assertEquals("a", rs.getString(3));
        assertFalse(rs.next());
        
        //assert new covered row key value exists in indexName1
        rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1, v2 from " + fullTableName1);
        assertTrue(rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals("z", rs.getString(2));
        assertEquals("a", rs.getString(3));
        assertFalse(rs.next());
        
        //assert rows exists in fullTableName2
        rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName2);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("b", rs.getString(2));
        assertEquals("c", rs.getString(3));
        assertFalse(rs.next());
        
        //assert rows exists in " + fullTableName2 + " index table
        rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName2);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("b", rs.getString(2));
        assertFalse(rs.next());
    }
    
    @Test
    public void testMultiRollbackOfUncommittedExistingRowKeyIndexUpdate() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName1 + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX " + indexName1 + " ON " + fullTableName1 + " (v1, k)");
            
            stmt.executeUpdate("upsert into " + fullTableName1 + " values('x', 'yyyy', 'a')");
            conn.commit();
            
            //assert rows exists in " + fullTableName1 + " 
            ResultSet rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert rows exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1 + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into " + fullTableName1 + " values('x', 'zzz', 'a')");
            
            //assert new covered row key value exists in fullTableName1
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("zzz", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert new covered row key value exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1 + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("zzz", rs.getString(2));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert original row exists in fullTableName1
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert original row exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1 + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into " + fullTableName1 + " values('x', 'zz', 'a')");
            
            //assert new covered row key value exists in fullTableName1
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("zz", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert new covered row key value exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1 + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("zz", rs.getString(2));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert original row exists in fullTableName1
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert original row exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1 + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("yyyy", rs.getString(2));
            assertFalse(rs.next());
                        
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCheckpointAndRollback() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName1 + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            stmt.execute("CREATE "+(localIndex? " LOCAL " : "")+"INDEX " + indexName1 + " ON " + fullTableName1 + " (v1)");
            stmt.executeUpdate("upsert into " + fullTableName1 + " values('x', 'a', 'a')");
            conn.commit();
            
            stmt.executeUpdate("upsert into " + fullTableName1 + "(k,v1) SELECT k,v1||'a' FROM " + fullTableName1);
            ResultSet rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("aa", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1 + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("aa", rs.getString(2));
            assertFalse(rs.next());
            
            stmt.executeUpdate("upsert into " + fullTableName1 + "(k,v1) SELECT k,v1||'a' FROM " + fullTableName1);
            
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("aaa", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1 + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("aaa", rs.getString(2));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert original row exists in fullTableName1
            rs = stmt.executeQuery("select k, v1, v2 from " + fullTableName1);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert original row exists in indexName1
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName1 + ")*/ k, v1 from " + fullTableName1 + " ORDER BY v1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertFalse(rs.next());

        } finally {
            conn.close();
        }
    }
}
