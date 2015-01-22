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
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

public class TenantSpecificViewIndexCompileTest extends BaseConnectionlessQueryTest {

    @Test
    public void testOrderByOptimizedOut() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t(t_id VARCHAR NOT NULL, k1 VARCHAR, k2 VARCHAR, v1 VARCHAR," +
        		" CONSTRAINT pk PRIMARY KEY(t_id, k1, k2)) multi_tenant=true");
        
        String tenantId = "me";
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM t WHERE k1 = 'a'");
        conn.createStatement().execute("CREATE INDEX i1 ON v(v2) INCLUDE(v1)");
        
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT v1,v2 FROM v WHERE v2 > 'a' ORDER BY v2");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _IDX_T ['me',-32768,'a'] - ['me',-32768,*]",
                QueryUtil.getExplainPlan(rs));
    }

    @Test
    public void testViewConstantsOptimizedOut() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t(t_id VARCHAR NOT NULL, k1 VARCHAR, k2 VARCHAR, v1 VARCHAR," +
                " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2)) multi_tenant=true");
        
        String tenantId = "me";
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM t WHERE k2 = 'a'");
        conn.createStatement().execute("CREATE INDEX i1 ON v(v2)");
        
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT v2 FROM v WHERE v2 > 'a' and k2 = 'a' ORDER BY v2,k2");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _IDX_T ['me',-32766,'a'] - ['me',-32766,*]\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY",
                QueryUtil.getExplainPlan(rs));
        
        // Won't use index b/c v1 is not in index, but should optimize out k2 still from the order by
        // K2 will still be referenced in the filter, as these are automatically tacked on to the where clause.
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT v1 FROM v WHERE v2 > 'a' ORDER BY k2");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER T ['me']\n" + 
                "    SERVER FILTER BY (V2 > 'a' AND K2 = 'a')",
                QueryUtil.getExplainPlan(rs));

        // If we match K2 against a constant not equal to it's view constant, we should get a degenerate plan
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT v1 FROM v WHERE v2 > 'a' and k2='b' ORDER BY k2");
        assertEquals("DEGENERATE SCAN OVER V",
                QueryUtil.getExplainPlan(rs));
    }

    @Test
    public void testViewConstantsOptimizedOutOnReadOnlyView() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t(t_id VARCHAR NOT NULL, k1 VARCHAR, k2 VARCHAR, v1 VARCHAR," +
                " CONSTRAINT pk PRIMARY KEY(t_id, k1, k2)) multi_tenant=true");
        
        String tenantId = "me";
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId); // connection is tenant-specific
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE VIEW v(v2 VARCHAR) AS SELECT * FROM t WHERE k2 = 'a'");
        conn.createStatement().execute("CREATE VIEW v2(v3 VARCHAR) AS SELECT * FROM v WHERE k1 > 'a'");
        conn.createStatement().execute("CREATE INDEX i2 ON v2(v3) include(v2)");
        
        // Confirm that a read-only view on an updatable view still optimizes out the read-only parts of the updatable view
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT v2 FROM v2 WHERE v3 > 'a' and k2 = 'a' ORDER BY v3,k2");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _IDX_T ['me',-32767,'a'] - ['me',-32767,*]",
                QueryUtil.getExplainPlan(rs));
    }
}
