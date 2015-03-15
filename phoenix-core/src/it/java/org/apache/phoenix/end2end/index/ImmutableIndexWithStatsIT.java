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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseOwnClusterHBaseManagedTimeIT;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


public class ImmutableIndexWithStatsIT extends BaseOwnClusterHBaseManagedTimeIT {
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(5);
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(1));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.THREAD_POOL_SIZE_ATTRIB, Integer.toString(4));
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(500));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
   
    @Test
    public void testIndexCreationDeadlockWithStats() throws Exception {
        String query;
        ResultSet rs;
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String tableName = TestUtil.DEFAULT_DATA_TABLE_FULL_NAME;
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) IMMUTABLE_ROWS=TRUE");
        query = "SELECT * FROM " + tableName;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?)");
        for (int i=0; i<6;i++) {
	        stmt.setString(1,"k" + i);
	        stmt.setString(2, "v" + i );
	        stmt.execute();
        }
        conn.commit();
        
        conn.createStatement().execute("UPDATE STATISTICS " + tableName);
        query = "SELECT COUNT(*) FROM " + tableName;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertTrue(QueryUtil.getExplainPlan(rs).startsWith("CLIENT 7-CHUNK PARALLEL 1-WAY FULL SCAN"));

        String indexName = TestUtil.DEFAULT_INDEX_TABLE_NAME;
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + " (v)");
        
        query = "SELECT * FROM " + indexName;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
    }
}
