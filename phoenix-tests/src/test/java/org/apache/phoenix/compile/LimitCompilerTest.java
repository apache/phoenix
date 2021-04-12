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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class LimitCompilerTest extends BaseConnectionlessQueryTest {
    
    private static QueryPlan compileStatement(String query, List<Object> binds) throws SQLException {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        TestUtil.bindParams(pstmt, binds);
        return pstmt.compileQuery();
    }
    
    @Test
    public void testLimit() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' limit 5";
        List<Object> binds = Collections.emptyList();
        QueryPlan plan = compileStatement(query, binds);
        Scan scan = plan.getContext().getScan();
        
        assertNull(scan.getFilter());
        assertArrayEquals(PVarchar.INSTANCE.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
        assertEquals(plan.getLimit(),Integer.valueOf(5));
    }

    @Test
    public void testNoLimit() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "'";
        List<Object> binds = Collections.emptyList();
        QueryPlan plan = compileStatement(query, binds);
        Scan scan = plan.getContext().getScan();

        assertNull(scan.getFilter());
        assertNull(plan.getLimit());
        assertArrayEquals(PVarchar.INSTANCE.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
    }
    
    @Test
    public void testBoundLimit() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' limit ?";
        List<Object> binds = Arrays.<Object>asList(5);
        QueryPlan plan = compileStatement(query, binds);
        Scan scan = plan.getContext().getScan();

        assertNull(scan.getFilter());
        assertArrayEquals(PVarchar.INSTANCE.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PVarchar.INSTANCE.toBytes(tenantId)), scan.getStopRow());
        assertEquals(plan.getLimit(),Integer.valueOf(5));
    }

    @Test
    public void testTypeMismatchBoundLimit() throws SQLException {
        String query = "select * from atable limit ?";
        List<Object> binds = Arrays.<Object>asList("foo");
        try {
            compileStatement(query, binds);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }

    @Test
    public void testNegativeBoundLimit() throws SQLException {
        String query = "select * from atable limit ?";
        List<Object> binds = Arrays.<Object>asList(-1);
        QueryPlan plan = compileStatement(query, binds);
        assertNull(plan.getLimit());
    }

    @Test
    public void testBindTypeMismatch() throws SQLException {
        Long tenantId = Long.valueOf(0);
        String keyPrefix = "002";
        List<Object> binds = Arrays.<Object>asList(tenantId,keyPrefix);
        String query = "select * from atable where organization_id=? and substr(entity_id,1,3)=?";
        try {
            compileStatement(query, binds);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 203 (22005): Type mismatch."));
        }
    }
}
