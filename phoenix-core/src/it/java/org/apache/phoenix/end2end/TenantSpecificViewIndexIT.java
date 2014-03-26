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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

public class TenantSpecificViewIndexIT extends BaseTenantSpecificViewIndexIT {
	
    @Test
    public void testUpdatableView() throws Exception {
        testUpdatableView(null);
    }

    @Test
    public void testUpdatableViewsWithSameNameDifferentTenants() throws Exception {
        testUpdatableViewsWithSameNameDifferentTenants(null);
    }

    @Test
    public void testMultiCFViewIndex() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE MT_BASE (PK1 VARCHAR not null, PK2 VARCHAR not null, "
                + "MYCF1.COL1 varchar,MYCF2.COL2 varchar "
                + "CONSTRAINT pk PRIMARY KEY(PK1,PK2)) MULTI_TENANT=true";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO MT_BASE values ('a','b','c','d')");
        conn.commit();
        
        ResultSet rs = conn.createStatement().executeQuery("select * from mt_base where (pk1,pk2) IN (('a','b'),('b','b'))");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("b",rs.getString(2));
        assertFalse(rs.next());
        
        conn.close();
        String tenantId = "a";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        conn = DriverManager.getConnection(getUrl(),props);
        conn.createStatement().execute("CREATE VIEW acme AS SELECT * FROM MT_BASE");
        rs = conn.createStatement().executeQuery("select * from acme");
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals("c",rs.getString(2));
        assertEquals("d",rs.getString(3));
        assertFalse(rs.next());
        conn.createStatement().execute("UPSERT INTO acme VALUES ('e','f','g')");
        conn.commit();
        conn.createStatement().execute("create index idx_acme on acme (COL1)");
        rs = conn.createStatement().executeQuery("select * from acme");
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertEquals("c",rs.getString(2));
        assertEquals("d",rs.getString(3));
        assertTrue(rs.next());
        assertEquals("e",rs.getString(1));
        assertEquals("f",rs.getString(2));
        assertEquals("g",rs.getString(3));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("explain select * from acme");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER MT_BASE ['a']",QueryUtil.getExplainPlan(rs));

        rs = conn.createStatement().executeQuery("select pk2,col1 from acme where col1='f'");
        assertTrue(rs.next());
        assertEquals("e",rs.getString(1));
        assertEquals("f",rs.getString(2));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("explain select pk2,col1 from acme where col1='f'");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _IDX_MT_BASE ['a',-32768,'f']",QueryUtil.getExplainPlan(rs));
        
        try {
            // Cannot reference tenant_id column in tenant specific connection
            conn.createStatement().executeQuery("select * from mt_base where (pk1,pk2) IN (('a','b'),('b','b'))");
            fail();
        } catch (ColumnNotFoundException e) {
        }
        
        // This is ok, though
        rs = conn.createStatement().executeQuery("select * from mt_base where pk2 IN ('b','e')");
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertTrue(rs.next());
        assertEquals("e",rs.getString(1));
        assertFalse(rs.next());
        
        rs = conn.createStatement().executeQuery("select * from acme where pk2 IN ('b','e')");
        assertTrue(rs.next());
        assertEquals("b",rs.getString(1));
        assertTrue(rs.next());
        assertEquals("e",rs.getString(1));
        assertFalse(rs.next());
        
    }
}
