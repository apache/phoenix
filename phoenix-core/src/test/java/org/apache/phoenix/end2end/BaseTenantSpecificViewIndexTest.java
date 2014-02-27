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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

public class BaseTenantSpecificViewIndexTest extends BaseHBaseManagedTimeTest {
    protected void testUpdatableView(Integer saltBuckets) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE t (t_id VARCHAR NOT NULL,\n" +
        		"k1 INTEGER NOT NULL,\n" +
        		"k2 INTEGER NOT NULL,\n" +
        		"v1 VARCHAR,\n" +
        		"CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n" +
        		"multi_tenant=true" + (saltBuckets == null ? "" : (",salt_buckets="+saltBuckets));
        conn.createStatement().execute(ddl);
        conn.close();
        
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "me");
        conn = DriverManager.getConnection(getUrl(), props);
        ddl = "CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM t WHERE k1 = 1";
        conn.createStatement().execute(ddl);
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO v(k2,v1,v2) VALUES(" + i + ",'v1-" + (i%5) + "','v2-" + (i%2) + "')");
        }
        conn.commit();
        
        conn.createStatement().execute("CREATE INDEX i ON v(v2)");
        
        String query = "SELECT k1, k2, v2 FROM v WHERE v2='v2-1'";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertEquals("v2-1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(3, rs.getInt(2));
        assertEquals("v2-1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(5, rs.getInt(2));
        assertEquals("v2-1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(7, rs.getInt(2));
        assertEquals("v2-1", rs.getString(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
        assertEquals("v2-1", rs.getString(3));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals(saltBuckets == null ? 
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER I ['me',-32768,'v2-1']" :
                "CLIENT PARALLEL 4-WAY SKIP SCAN ON 3 KEYS OVER I [0,'me',-32768,'v2-1'] - [2,'me',-32768,'v2-1']\n" + 
                "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
    }
}
