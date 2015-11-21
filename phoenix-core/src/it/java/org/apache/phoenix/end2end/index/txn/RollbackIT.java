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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
public class RollbackIT extends BaseHBaseManagedTimeIT {
	
	private final boolean localIndex;
	private final boolean mutable;
	private final String tableName;
    private final String indexName;
    private final String fullTableName;

	public RollbackIT(boolean localIndex, boolean mutable) {
		this.localIndex = localIndex;
		this.mutable = mutable;
		this.tableName = TestUtil.DEFAULT_DATA_TABLE_NAME;
		this.indexName = "IDX";
		this.fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
	}
	
	@BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.DEFAULT_TRANSACTIONAL_ATTRIB, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
	
	@Parameters(name="localIndex = {0} , mutable = {1}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {     
                 { false, false }, { false, true },
                 { true, false }, { true, true }  
           });
    }
    
    @Test
    public void testRollbackOfUncommittedKeyValueIndexInsert() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"+(!mutable? " IMMUTABLE_ROWS=true" : ""));
            stmt.execute("CREATE "+(localIndex? "LOCAL " : "")+"INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE(v2)");
            
            stmt.executeUpdate("upsert into " + fullTableName + " values('x', 'y', 'a')");
            
            //assert values in data table
            ResultSet rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert values in index table
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName + ")*/ k, v1, v2  from " + fullTableName);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert values in data table
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName);
            assertFalse(rs.next());
            
            //assert values in index table
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName + ")*/ k, v1, v2 from " + fullTableName);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRollbackOfUncommittedRowKeyIndexInsert() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE " + fullTableName + "(k VARCHAR, v1 VARCHAR, v2 VARCHAR, CONSTRAINT pk PRIMARY KEY (v1, v2))"+(!mutable? " IMMUTABLE_ROWS=true" : ""));
            stmt.execute("CREATE "+(localIndex? "LOCAL " : "")+"INDEX " + indexName + " ON " + fullTableName + "(v1, k)");
            
            stmt.executeUpdate("upsert into " + fullTableName + " values('x', 'y', 'a')");

            ResultSet rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName);
            
            //assert values in data table
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            //assert values in index table
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName + ")*/ k, v1 from " + fullTableName);
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert values in data table
            rs = stmt.executeQuery("select /*+ NO_INDEX */ k, v1, v2 from " + fullTableName);
            assertFalse(rs.next());
            
            //assert values in index table
            rs = stmt.executeQuery("select /*+ INDEX(" + indexName + ")*/ k, v1 from " + fullTableName);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
}

