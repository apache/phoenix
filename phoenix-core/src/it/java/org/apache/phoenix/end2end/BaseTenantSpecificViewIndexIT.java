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

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

public class BaseTenantSpecificViewIndexIT extends BaseHBaseManagedTimeIT {
    
    public static final String TENANT1_ID = "tenant1";
    public static final String TENANT2_ID = "tenant2";
    
    protected Set<Pair<String, String>> tenantViewsToDelete = newHashSet();
    
    protected void testUpdatableView(Integer saltBuckets) throws Exception {
        createBaseTable("t", saltBuckets);
        Connection conn = createTenantConnection(TENANT1_ID);
        try {
            createAndPopulateTenantView(conn, TENANT1_ID, "t", "");
            createAndVerifyIndex(conn, saltBuckets, TENANT1_ID, "");
            verifyViewData(conn, "");
        } finally {
            try { conn.close();} catch (Exception ignored) {}
        }
    }
    
    protected void testUpdatableViewsWithSameNameDifferentTenants(Integer saltBuckets) throws Exception {
        createBaseTable("t", saltBuckets);
        Connection conn1 = createTenantConnection(TENANT1_ID);
        Connection conn2 = createTenantConnection(TENANT2_ID);
        try {
            String prefixForTenant1Data = "TI";
            String prefixForTenant2Data = "TII";
            
            // tenant views with same name for two different tables
            createAndPopulateTenantView(conn1, TENANT1_ID, "t", prefixForTenant1Data);
            createAndPopulateTenantView(conn2, TENANT2_ID, "t", prefixForTenant2Data);
            
            createAndVerifyIndex(conn1, saltBuckets, TENANT1_ID, prefixForTenant1Data);
            createAndVerifyIndex(conn2, saltBuckets, TENANT2_ID, prefixForTenant2Data);
            
            verifyViewData(conn1, prefixForTenant1Data);
            verifyViewData(conn2, prefixForTenant2Data);
        } finally {
            try { conn1.close();} catch (Exception ignored) {}
            try { conn2.close();} catch (Exception ignored) {}
        }
    }
    
    private void createBaseTable(String tableName, Integer saltBuckets) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "v1 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n" +
                "multi_tenant=true" + (saltBuckets == null ? "" : (",salt_buckets="+saltBuckets));
        conn.createStatement().execute(ddl);
        conn.close();
    }
    
    private void createAndPopulateTenantView(Connection conn, String tenantId, String baseTable, String valuePrefix) throws SQLException {
        String ddl = "CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM " + baseTable + " WHERE k1 = 1";
        conn.createStatement().execute(ddl);
        tenantViewsToDelete.add(new Pair<String, String>(tenantId, "v"));
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO v(k2,v1,v2) VALUES(" + i + ",'" + valuePrefix + "v1-" + (i%5) + "','" + valuePrefix + "v2-" + (i%2) + "')");
        }
        conn.commit();
    }
    
    private void createAndVerifyIndex(Connection conn, Integer saltBuckets, String tenantId, String valuePrefix) throws SQLException {
        conn.createStatement().execute("CREATE INDEX i ON v(v2)");
        conn.createStatement().execute("UPSERT INTO v(k2,v1,v2) VALUES (-1, 'blah', 'superblah')"); // sanity check that we can upsert after index is there
        conn.commit();
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT k1, k2, v2 FROM v WHERE v2='" + valuePrefix + "v2-1'");
        assertEquals(saltBuckets == null ? 
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER _IDX_T ['" + tenantId + "',-32768,'" + valuePrefix + "v2-1']" :
                "CLIENT PARALLEL 4-WAY SKIP SCAN ON 3 KEYS OVER _IDX_T [0,'" + tenantId + "',-32768,'" + valuePrefix + "v2-1'] - [2,'" + tenantId + "',-32768,'" + valuePrefix + "v2-1']\n" + 
                "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
    }
    
    private Connection createTenantConnection(String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), props);
    }
    
    private void verifyViewData(Connection conn, String valuePrefix) throws SQLException {
        String query = "SELECT k1, k2, v2 FROM v WHERE v2='" + valuePrefix + "v2-1'";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertEquals(valuePrefix + "v2-1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(3, rs.getInt(2));
        assertEquals(valuePrefix + "v2-1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(5, rs.getInt(2));
        assertEquals(valuePrefix + "v2-1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(7, rs.getInt(2));
        assertEquals(valuePrefix + "v2-1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
        assertEquals(valuePrefix + "v2-1", rs.getString(3));
        assertFalse(rs.next());
    }
}
