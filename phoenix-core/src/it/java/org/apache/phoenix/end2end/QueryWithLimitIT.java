/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.KEYONLY_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


public class QueryWithLimitIT extends BaseOwnClusterHBaseManagedTimeIT {

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(50));
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(1));
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.SEQUENCE_SALT_BUCKETS_ATTRIB, Integer.toString(0)); // Prevents RejectedExecutionException when deleting sequences
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testQueryWithLimitAndStats() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            ensureTableCreated(getUrl(),KEYONLY_NAME);
            initTableValues(conn, 100);
            
            String query = "SELECT i1 FROM KEYONLY LIMIT 1";
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals("CLIENT SERIAL 1-WAY FULL SCAN OVER KEYONLY\n" + 
                    "    SERVER FILTER BY FIRST KEY ONLY\n" + 
                    "    SERVER 1 ROW LIMIT\n" + 
                    "CLIENT 1 ROW LIMIT", QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testQueryWithoutLimitFails() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        ensureTableCreated(getUrl(),KEYONLY_NAME);
        initTableValues(conn, 100);
        conn.createStatement().execute("UPDATE STATISTICS " + KEYONLY_NAME);
        
        String query = "SELECT i1 FROM KEYONLY";
        try {
            ResultSet rs = conn.createStatement().executeQuery(query);
            rs.next();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getCause() instanceof RejectedExecutionException);
        }
        conn.close();
    }
    
    protected static void initTableValues(Connection conn, int nRows) throws Exception {
        PreparedStatement stmt = conn.prepareStatement(
            "upsert into " +
            "KEYONLY VALUES (?, ?)");
        for (int i = 0; i < nRows; i++) {
            stmt.setInt(1, i);
            stmt.setInt(2, i+1);
            stmt.execute();
        }
        
        conn.commit();
    }

}
