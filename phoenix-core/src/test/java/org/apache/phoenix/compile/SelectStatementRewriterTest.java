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
import static org.apache.phoenix.util.TestUtil.and;
import static org.apache.phoenix.util.TestUtil.constantComparison;
import static org.apache.phoenix.util.TestUtil.multiKVFilter;
import static org.apache.phoenix.util.TestUtil.singleKVFilter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;



public class SelectStatementRewriterTest extends BaseConnectionlessQueryTest {
    private static Filter compileStatement(String query) throws SQLException {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        QueryPlan plan = pstmt.compileQuery();
        return plan.getContext().getScan().getFilter();
    }

    
    @Test
    public void testCollapseAnd() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0";
        Filter filter = compileStatement(query);
        assertEquals(
                singleKVFilter(constantComparison(
                    CompareOp.EQUAL,
                    A_INTEGER,
                    0)),
                filter);
    }
    
    @Test
    public void testLHSLiteralCollapseAnd() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where '" + tenantId + "'=organization_id and 0=a_integer";
        Filter filter = compileStatement(query);
        assertEquals(
                singleKVFilter(constantComparison(
                    CompareOp.EQUAL,
                    A_INTEGER,
                    0)),
                filter);
    }
    
    @Test
    public void testRewriteAnd() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0 and a_string='foo'";
        Filter filter = compileStatement(query);
        assertEquals(
                multiKVFilter(and(
                        constantComparison(
                            CompareOp.EQUAL,
                            A_INTEGER, 0),
                        constantComparison(
                            CompareOp.EQUAL,
                            A_STRING, "foo")
                    )),
                filter);
    }

    @Test
    public void testCollapseWhere() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(organization_id,1,3)='foo' LIMIT 2";
        Filter filter = compileStatement(query);
        assertNull(filter);
    }

    @Test
    public void testNoCollapse() throws SQLException {
        String query = "select * from atable where a_integer=0 and a_string='foo'";
        Filter filter = compileStatement(query);
        assertEquals(
                multiKVFilter(and(
                        constantComparison(
                            CompareOp.EQUAL,
                            A_INTEGER, 0),
                        constantComparison(
                            CompareOp.EQUAL,
                            A_STRING, "foo")
                    )),
                filter);
    }
}
