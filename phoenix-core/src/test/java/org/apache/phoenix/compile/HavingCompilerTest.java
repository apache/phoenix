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
import static org.apache.phoenix.util.TestUtil.or;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.expression.function.RoundDateExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.junit.Test;


public class HavingCompilerTest extends BaseConnectionlessQueryTest {
    private static StatementContext context;
    
    private static class Expressions {
        private Expression whereClause;
        private Expression havingClause;
        
        private Expressions(Expression whereClause, Expression havingClause) {
            this.whereClause = whereClause;
            this.havingClause = havingClause;
        }
    }
    
    private static Expressions compileStatement(String query, List<Object> binds) throws SQLException {
        SQLParser parser = new SQLParser(query);
        SelectStatement statement = parser.parseQuery();
        Scan scan = new Scan();
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        ColumnResolver resolver = FromCompiler.getResolver(statement, pconn);
        statement = StatementNormalizer.normalize(statement, resolver);
        context = new StatementContext(new PhoenixStatement(pconn), resolver, binds, scan);

        GroupBy groupBy = GroupByCompiler.compile(context, statement);
        // Optimize the HAVING clause by finding any group by expressions that can be moved
        // to the WHERE clause
        statement = HavingCompiler.rewrite(context, statement, groupBy);
        Expression having = HavingCompiler.compile(context, statement, groupBy);
        Expression where = WhereCompiler.compile(context, statement);
        where = WhereOptimizer.pushKeyExpressionsToScan(context, statement, where);
        return new Expressions(where,having);
    }
    
    @Test
    public void testHavingToWhere() throws SQLException {
        String query = "select count(1) from atable group by a_string having a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"foo");
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testHavingFuncToWhere() throws SQLException {
        // TODO: confirm that this is a valid optimization
        String query = "select count(1) from atable group by a_date having round(a_date, 'hour') > ?";
        Date date = new Date(System.currentTimeMillis());
        List<Object> binds = Arrays.<Object>asList(date);
        Expressions expressions = compileStatement(query,binds);
        Expression w = constantComparison(CompareOp.GREATER, RoundDateExpression.create(Arrays.asList(BaseConnectionlessQueryTest.A_DATE,LiteralExpression.newConstant("hour"),LiteralExpression.newConstant(1))), date);
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testHavingToAndWhere() throws SQLException {
        String query = "select count(1) from atable where b_string > 'bar' group by a_string having a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = and(constantComparison(CompareOp.GREATER, BaseConnectionlessQueryTest.B_STRING,"bar"),constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"foo"));
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }

    
    @Test
    public void testAndHavingToAndWhere() throws SQLException {
        String query = "select count(1) from atable where b_string > 'bar' group by a_string having count(1) >= 1 and a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression h = constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(),1L);
        Expression w = and(constantComparison(CompareOp.GREATER, BaseConnectionlessQueryTest.B_STRING,"bar"),constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"foo"));
        assertEquals(w, expressions.whereClause);
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testAndHavingToWhere() throws SQLException {
        String query = "select count(1) from atable group by a_string having count(1) >= 1 and a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression h = constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(),1L);
        Expression w = constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"foo");
        assertEquals(w, expressions.whereClause);
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testAggFuncInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string having count(a_string) >= 1";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression h = constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(Arrays.asList(BaseConnectionlessQueryTest.A_STRING)),1L);
        assertTrue(LiteralExpression.isTrue(expressions.whereClause));
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testOrAggFuncInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string having count(1) >= 1 or a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        PColumn aCol = BaseConnectionlessQueryTest.ATABLE.getColumn("A_STRING");
        Expression h = or(
                constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(),1L),
                constantComparison(CompareOp.EQUAL, 
                        new RowKeyColumnExpression(aCol, // a_string comes from group by key in this case
                                new RowKeyValueAccessor(Arrays.<PColumn>asList(aCol), 0)),"foo"));
        assertTrue(LiteralExpression.isTrue(expressions.whereClause));
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testAndAggColsInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string,b_string having a_string = 'a' and b_string = 'b'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = and(constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"a"),constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.B_STRING,"b"));
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testOrAggColsInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string,b_string having a_string = 'a' or b_string = 'b'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = or(constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.A_STRING,"a"),constantComparison(CompareOp.EQUAL, BaseConnectionlessQueryTest.B_STRING,"b"));
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testNonAggColInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string having b_string = 'bar'";
        List<Object> binds = Collections.emptyList();
        try {
            compileStatement(query,binds);
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1019 (42Y26): Only aggregate maybe used in the HAVING clause."));
        }
    }
}
