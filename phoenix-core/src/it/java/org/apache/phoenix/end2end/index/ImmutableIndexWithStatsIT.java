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
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;


public class ImmutableIndexWithStatsIT extends ParallelStatsEnabledIT {
    
    @Test
    public void testIndexCreationDeadlockWithStats() throws Exception {
        String query;
        ResultSet rs;
        
        String tableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR) IMMUTABLE_ROWS=TRUE");
        query = "SELECT * FROM " + tableName;
        rs = conn.createStatement().executeQuery(query);
        assertFalse(rs.next());

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?)");
        for (int i=0; i<6;i++) {
	        stmt.setString(1, "kkkkkkkkkk" + i);
	        stmt.setString(2, "vvvvvvvvvv" + i );
	        stmt.execute();
        }
        conn.commit();
        
        conn.createStatement().execute("UPDATE STATISTICS " + tableName);
        query = "SELECT COUNT(*) FROM " + tableName;
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertTrue(QueryUtil.getExplainPlan(rs).startsWith("CLIENT PARALLEL 1-WAY FULL SCAN"));

        String indexName = "I_" + generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + " (v)");
        
        query = "SELECT * FROM " + indexName;
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
    }
}
