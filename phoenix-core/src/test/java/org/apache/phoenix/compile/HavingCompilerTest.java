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

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.expression.function.RoundDateExpression;
import org.apache.phoenix.filter.BooleanExpressionFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class HavingCompilerTest extends BaseConnectionlessQueryTest {
    private static class Expressions {
        private Expression whereClause;
        private Expression havingClause;
        
        private Expressions(Expression whereClause, Expression havingClause) {
            this.whereClause = whereClause;
            this.havingClause = havingClause;
        }
    }
    
    private static Expressions compileStatement(String query, List<Object> binds) throws SQLException {
        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        TestUtil.bindParams(pstmt, binds);
        QueryPlan plan = pstmt.compileQuery();
        assertTrue(plan instanceof AggregatePlan);
        Filter filter = plan.getContext().getScan().getFilter();
        assertTrue(filter == null || filter instanceof BooleanExpressionFilter);
        BooleanExpressionFilter boolFilter = (BooleanExpressionFilter)filter;
        AggregatePlan aggPlan = (AggregatePlan)plan;
        return new Expressions(boolFilter == null ? null : boolFilter.getExpression(), aggPlan.getHaving());
    }
    
    @Test
    public void testHavingToWhere() throws SQLException {
        String query = "select count(1) from atable group by a_string having a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = constantComparison(CompareOp.EQUAL, A_STRING,"foo");
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
        Expression w = constantComparison(CompareOp.GREATER, RoundDateExpression.create(Arrays.asList(A_DATE,LiteralExpression.newConstant("hour"),LiteralExpression.newConstant(1))), date);
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testHavingToAndWhere() throws SQLException {
        String query = "select count(1) from atable where b_string > 'bar' group by a_string having a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = and(constantComparison(CompareOp.GREATER, B_STRING,"bar"),constantComparison(CompareOp.EQUAL, A_STRING,"foo"));
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }

    
    @Test
    public void testAndHavingToAndWhere() throws SQLException {
        String query = "select count(1) from atable where b_string > 'bar' group by a_string having count(1) >= 1 and a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression h = constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(),1L);
        Expression w = and(constantComparison(CompareOp.GREATER, B_STRING,"bar"),constantComparison(CompareOp.EQUAL, A_STRING,"foo"));
        assertEquals(w, expressions.whereClause);
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testAndHavingToWhere() throws SQLException {
        String query = "select count(1) from atable group by a_string having count(1) >= 1 and a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression h = constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(),1L);
        Expression w = constantComparison(CompareOp.EQUAL, A_STRING,"foo");
        assertEquals(w, expressions.whereClause);
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testInListHavingToWhere() throws SQLException {
        String query = "select count(1) from atable group by a_string having a_string in ('foo', 'bar')";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = TestUtil.in(A_STRING,"foo","bar");
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testAggFuncInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string having count(a_string) >= 1";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression h = constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(Arrays.asList(A_STRING)),1L);
        assertNull(expressions.whereClause);
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testOrAggFuncInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string having count(1) >= 1 or a_string = 'foo'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        PColumn aCol = ATABLE.getColumn("A_STRING");
        Expression h = or(
                constantComparison(CompareOp.GREATER_OR_EQUAL, new CountAggregateFunction(),1L),
                constantComparison(CompareOp.EQUAL, 
                        new RowKeyColumnExpression(aCol, // a_string comes from group by key in this case
                                new RowKeyValueAccessor(Arrays.<PColumn>asList(aCol), 0)),"foo"));
        assertNull(expressions.whereClause);
        assertEquals(h, expressions.havingClause);
    }
    
    @Test
    public void testAndAggColsInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string,b_string having a_string = 'a' and b_string = 'b'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = and(constantComparison(CompareOp.EQUAL, A_STRING,"a"),constantComparison(CompareOp.EQUAL, B_STRING,"b"));
        assertEquals(w, expressions.whereClause);
        assertNull(expressions.havingClause);
    }
    
    @Test
    public void testOrAggColsInHaving() throws SQLException {
        String query = "select count(1) from atable group by a_string,b_string having a_string = 'a' or b_string = 'b'";
        List<Object> binds = Collections.emptyList();
        Expressions expressions = compileStatement(query,binds);
        Expression w = or(constantComparison(CompareOp.EQUAL, A_STRING,"a"),constantComparison(CompareOp.EQUAL, B_STRING,"b"));
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
