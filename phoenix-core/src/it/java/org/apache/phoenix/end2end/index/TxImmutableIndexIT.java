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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.Shadower;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TxImmutableIndexIT extends ImmutableIndexIT {
    
    @BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Don't split intra region so we can more easily know that the n-way parallelization is for the explain plan
        // Forces server cache to be used
        props.put(QueryServices.DEFAULT_TRANSACTIONAL_ATTRIB, Boolean.toString(true));
        // We need this b/c we don't allow a transactional table to be created if the underlying
        // HBase table already exists (since we don't know if it was transactional before).
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testRollbackOfUncommittedKeyValueIndexChange() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE DEMO(v1 VARCHAR PRIMARY KEY, v2 VARCHAR, v3 VARCHAR) IMMUTABLE_ROWS=true");
            stmt.execute("CREATE INDEX DEMO_idx ON DEMO (v2) INCLUDE(v3)");
            
            stmt.executeUpdate("upsert into DEMO values('x', 'y', 'a')");
            
            //assert values in data table
            ResultSet rs = stmt.executeQuery("select v1, v2, v3 from DEMO");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert values in data table
            rs = stmt.executeQuery("select v1, v2, v3 from DEMO");
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testRollbackOfUncommittedRowKeyIndexChange() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE DEMO(v1 VARCHAR, v2 VARCHAR, v3 VARCHAR, CONSTRAINT pk PRIMARY KEY (v1, v2)) IMMUTABLE_ROWS=true");
            stmt.execute("CREATE INDEX DEMO_idx ON DEMO (v2, v1)");
            
            stmt.executeUpdate("upsert into DEMO values('x', 'y', 'a')");
            
            //assert values in data table
            ResultSet rs = stmt.executeQuery("select v1, v2, v3 from DEMO");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertEquals("a", rs.getString(3));
            assertFalse(rs.next());
            
            conn.rollback();
            
            //assert values in data table
            rs = stmt.executeQuery("select v1, v2, v3 from DEMO");
            assertFalse(rs.next());
            
        } finally {
            conn.close();
        }
    }
}
