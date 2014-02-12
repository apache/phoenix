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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.Test;

import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;



public class SelectStatementRewriterTest extends BaseConnectionlessQueryTest {
    private static Expression compileStatement(String query) throws SQLException {
        Scan scan = new Scan();
        List<Object> binds = Collections.emptyList();
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        statement = StatementNormalizer.normalize(statement, resolver);
        StatementContext context = new StatementContext(new PhoenixStatement(pconn), resolver, binds, scan);
        Expression whereClause = WhereCompiler.compile(context, statement);
        return WhereOptimizer.pushKeyExpressionsToScan(context, statement, whereClause);
    }
    
    @Test
    public void testCollapseAnd() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0";
        Expression where = compileStatement(query);
        assertTrue(where instanceof ComparisonExpression);
        ComparisonExpression child = (ComparisonExpression)where;
        assertEquals(CompareOp.EQUAL, child.getFilterOp());
        assertTrue(child.getChildren().get(0) instanceof KeyValueColumnExpression);
        assertTrue(child.getChildren().get(1) instanceof LiteralExpression);
    }
    
    @Test
    public void testLHSLiteralCollapseAnd() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where '" + tenantId + "'=organization_id and 0=a_integer";
        Expression where = compileStatement(query);
        assertTrue(where instanceof ComparisonExpression);
        ComparisonExpression child = (ComparisonExpression)where;
        assertEquals(CompareOp.EQUAL, child.getFilterOp());
        assertTrue(child.getChildren().get(0) instanceof KeyValueColumnExpression);
        assertTrue(child.getChildren().get(1) instanceof LiteralExpression);
    }
    
    @Test
    public void testRewriteAnd() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and a_integer=0 and a_string='foo'";
        Expression where = compileStatement(query);
        
        assertTrue(where instanceof AndExpression);
        assertTrue(where.getChildren().size() == 2);
        assertTrue(where.getChildren().get(0) instanceof ComparisonExpression);
        assertEquals(CompareOp.EQUAL, ((ComparisonExpression)where.getChildren().get(0)).getFilterOp());
        assertTrue(where.getChildren().get(1) instanceof ComparisonExpression);
        assertEquals(CompareOp.EQUAL, ((ComparisonExpression)where.getChildren().get(1)).getFilterOp());
    }

    @Test
    public void testCollapseWhere() throws SQLException {
        String tenantId = "000000000000001";
        String query = "select * from atable where organization_id='" + tenantId + "' and substr(organization_id,1,3)='foo' LIMIT 2";
        Expression where = compileStatement(query);
        assertNull(where);
    }

    @Test
    public void testNoCollapse() throws SQLException {
        String query = "select * from atable where a_integer=0 and a_string='foo'";
        Expression where = compileStatement(query);
        assertEquals(2, where.getChildren().size());
        assertTrue(where.getChildren().get(0) instanceof ComparisonExpression);
        assertEquals(CompareOp.EQUAL, ((ComparisonExpression)where.getChildren().get(0)).getFilterOp());
        assertTrue(where.getChildren().get(1) instanceof ComparisonExpression);
        assertEquals(CompareOp.EQUAL, ((ComparisonExpression)where.getChildren().get(1)).getFilterOp());
    }
}
