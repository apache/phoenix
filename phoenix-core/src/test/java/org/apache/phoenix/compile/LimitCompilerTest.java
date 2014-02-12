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
import org.junit.Test;

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.ByteUtil;


public class LimitCompilerTest extends BaseConnectionlessQueryTest {
    
    private static Integer compileStatement(String query, List<Object> binds, Scan scan) throws SQLException {
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        statement = StatementNormalizer.normalize(statement, resolver);
        StatementContext context = new StatementContext(new PhoenixStatement(pconn), resolver, binds, scan);

        Integer limit = LimitCompiler.compile(context, statement);
        GroupBy groupBy = GroupByCompiler.compile(context, statement);
        statement = HavingCompiler.rewrite(context, statement, groupBy);
        HavingCompiler.compile(context, statement, groupBy);
        Expression where = WhereCompiler.compile(context, statement);
        assertNull(where);
        return limit;
    }
    
    @Test
    public void testLimit() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' limit 5";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        Integer limit = compileStatement(query, binds, scan);

        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
        assertEquals(limit,Integer.valueOf(5));
    }

    @Test
    public void testNoLimit() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "'";
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        Integer limit = compileStatement(query, binds, scan);

        assertNull(limit);
        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
    }
    
    @Test
    public void testBoundLimit() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' limit ?";
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(5);
        Integer limit = compileStatement(query, binds, scan);

        assertArrayEquals(PDataType.VARCHAR.toBytes(tenantId), scan.getStartRow());
        assertArrayEquals(ByteUtil.nextKey(PDataType.VARCHAR.toBytes(tenantId)), scan.getStopRow());
        assertEquals(limit,Integer.valueOf(5));
    }

    @Test
    public void testTypeMismatchBoundLimit() throws SQLException {
        String query = "select * from atable limit ?";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList("foo");
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(new PhoenixStatement(pconn), resolver, binds, scan);
        try {
            LimitCompiler.compile(context, statement);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }
    }

    @Test
    public void testNegativeBoundLimit() throws SQLException {
        String query = "select * from atable limit ?";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        List<Object> binds = Arrays.<Object>asList(-1);
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(new PhoenixStatement(pconn), resolver, binds, scan);
        assertNull(LimitCompiler.compile(context, statement));
    }

    @Test
    public void testBindTypeMismatch() throws SQLException {
        Long tenantId = Long.valueOf(0);
        String keyPrefix = "002";
        List<Object> binds = Arrays.<Object>asList(tenantId,keyPrefix);
        String query = "select * from atable where organization_id=? and substr(entity_id,1,3)=?";
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        StatementContext context = new StatementContext(new PhoenixStatement(pconn), resolver, binds, scan);
        try {
            WhereCompiler.compile(context, statement);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 203 (22005): Type mismatch."));
        }
    }
}
