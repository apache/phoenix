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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;

import com.google.common.collect.Maps;

public class BaseViewIT extends BaseHBaseManagedTimeIT {

    @BeforeClass 
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Don't split intra region so we can more easily know that the n-way parallelization is for the explain plan
        props.put(QueryServices.MAX_INTRA_REGION_PARALLELIZATION_ATTRIB, Integer.toString(1));
        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }

    protected void testUpdatableViewWithIndex(Integer saltBuckets) throws Exception {
        testUpdatableView(saltBuckets);
        testUpdatableViewIndex(saltBuckets);
    }

    protected void testUpdatableView(Integer saltBuckets) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE t (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, k3 DECIMAL, s VARCHAR CONSTRAINT pk PRIMARY KEY (k1, k2, k3))" + (saltBuckets == null ? "" : (" SALT_BUCKETS="+saltBuckets));
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW v AS SELECT * FROM t WHERE k1 = 1";
        conn.createStatement().execute(ddl);
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO t VALUES(" + (i % 4) + "," + (i+100) + "," + (i > 5 ? 2 : 1) + ")");
        }
        conn.commit();
        ResultSet rs;
        
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM t");
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM v");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM v");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(101, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(105, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(109, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());

        conn.createStatement().execute("UPSERT INTO v(k2,S,k3) VALUES(120,'foo',50.0)");
        conn.createStatement().execute("UPSERT INTO v(k2,S,k3) VALUES(121,'bar',51.0)");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT k1, k2 FROM v WHERE k2 >= 120");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(120, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(121, rs.getInt(2));
        assertFalse(rs.next());
    }

    protected void testUpdatableViewIndex(Integer saltBuckets) throws Exception {
        ResultSet rs;
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE INDEX i1 on v(k3) include (s)");
        String query = "SELECT k1, k2, k3, s FROM v WHERE k3 = 51.0";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(121, rs.getInt(2));
        assertTrue(BigDecimal.valueOf(51.0).compareTo(rs.getBigDecimal(3))==0);
        assertEquals("bar", rs.getString(4));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals(saltBuckets == null
                ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER _IDX_T [" + Short.MIN_VALUE + ",51]"
                : "CLIENT PARALLEL " + saltBuckets + "-WAY SKIP SCAN ON 3 KEYS OVER _IDX_T [0," + Short.MIN_VALUE + ",51] - [2," + Short.MIN_VALUE + ",51]\nCLIENT MERGE SORT",
            QueryUtil.getExplainPlan(rs));

        conn.createStatement().execute("CREATE INDEX i2 on v(s)");
        query = "SELECT k1, k2, s FROM v WHERE s = 'foo'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(120, rs.getInt(2));
        assertEquals("foo", rs.getString(3));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        assertEquals(saltBuckets == null
                ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER _IDX_T [" + (Short.MIN_VALUE+1) + ",'foo']"
                : "CLIENT PARALLEL " + saltBuckets + "-WAY SKIP SCAN ON 3 KEYS OVER _IDX_T [0," + (Short.MIN_VALUE+1) + ",'foo'] - [2," + (Short.MIN_VALUE+1) + ",'foo']\nCLIENT MERGE SORT",
            QueryUtil.getExplainPlan(rs));
    }


}
