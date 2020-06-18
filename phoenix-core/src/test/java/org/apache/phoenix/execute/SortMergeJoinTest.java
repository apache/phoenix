/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.execute;

import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.DefaultParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.JoinTableNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Test;
import org.mockito.Mockito;
import static org.junit.Assert.assertTrue;


public class SortMergeJoinTest {


    @Test
    public void testOptimizeSemiJoinForSortMergeJoinBug5956() throws SQLException, InterruptedException {
        // mock for SortMergeJoinPlan
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        PhoenixConnection phoenixConnection = Mockito.mock(PhoenixConnection.class);
        when(statementContext.getConnection()).thenReturn(phoenixConnection);
        ConnectionQueryServices connectionQueryServices = Mockito.mock(ConnectionQueryServices.class);
        when(connectionQueryServices.getProps()).thenReturn(ReadOnlyProps.EMPTY_PROPS);
        when(phoenixConnection.getQueryServices()).thenReturn(connectionQueryServices);

        List<Expression> expressions = new ArrayList<Expression>();
        Pair<List<Expression>,List<Expression>> lhsAndRhsJoinExpressions = Pair.newPair(expressions, expressions);
        Pair<List<OrderByNode>, List<OrderByNode>> lhsAndRhsOrderByNodes = Pair.<List<OrderByNode>, List<OrderByNode>> newPair(
                new ArrayList<OrderByNode>(),
                new ArrayList<OrderByNode>());

        //test semi join rhs is null
        JoinTableNode.JoinType joinType = JoinTableNode.JoinType.Semi;
        ResultIterator lhsResultIterator = Mockito.mock(ResultIterator.class);
        Tuple tuple = Mockito.mock(Tuple.class);
        when(lhsResultIterator.next()).thenReturn(tuple);
        QueryPlan lhsQueryPlan = Mockito.mock(QueryPlan.class);
        when(lhsQueryPlan.iterator(
                DefaultParallelScanGrouper.getInstance())).thenReturn(lhsResultIterator);

        QueryPlan rhsQueryPlan = Mockito.mock(QueryPlan.class);
        ResultIterator rhsResultIterator = Mockito.mock(ResultIterator.class);
        when(rhsResultIterator.next()).thenReturn(null);
        when(rhsQueryPlan.iterator(
                DefaultParallelScanGrouper.getInstance())).thenReturn(rhsResultIterator);

        SortMergeJoinPlan sortMergeJoinPlan = new SortMergeJoinPlan(
                statementContext,
                null,
                null,
                joinType,
                lhsQueryPlan,
                rhsQueryPlan,
                lhsAndRhsJoinExpressions,
                expressions,
                null,
                null,
                null,
                0,
                true,
                lhsAndRhsOrderByNodes);
        SortMergeJoinPlan.SemiAntiJoinIterator semiAntiJoinIterator =
                (SortMergeJoinPlan.SemiAntiJoinIterator)sortMergeJoinPlan.iterator();
        Tuple resultTuple = semiAntiJoinIterator.next();
        assertTrue(resultTuple == null);
        assertTrue(semiAntiJoinIterator.isEnd());

        //test semi join lhs is null
        joinType = JoinTableNode.JoinType.Semi;
        lhsResultIterator = Mockito.mock(ResultIterator.class);
        when(lhsResultIterator.next()).thenReturn(null);
        lhsQueryPlan = Mockito.mock(QueryPlan.class);
        when(lhsQueryPlan.iterator(
                DefaultParallelScanGrouper.getInstance())).thenReturn(lhsResultIterator);

        rhsQueryPlan = Mockito.mock(QueryPlan.class);
        rhsResultIterator = Mockito.mock(ResultIterator.class);
        tuple = Mockito.mock(Tuple.class);
        when(rhsResultIterator.next()).thenReturn(tuple);
        when(rhsQueryPlan.iterator(
                DefaultParallelScanGrouper.getInstance())).thenReturn(rhsResultIterator);

        sortMergeJoinPlan = new SortMergeJoinPlan(
                statementContext,
                null,
                null,
                joinType,
                lhsQueryPlan,
                rhsQueryPlan,
                lhsAndRhsJoinExpressions,
                expressions,
                null,
                null,
                null,
                0,
                true,
                lhsAndRhsOrderByNodes);
        semiAntiJoinIterator = (SortMergeJoinPlan.SemiAntiJoinIterator)sortMergeJoinPlan.iterator();
        resultTuple = semiAntiJoinIterator.next();
        assertTrue(resultTuple == null);
        assertTrue(semiAntiJoinIterator.isEnd());

        //test anti join lhs is null
        joinType = JoinTableNode.JoinType.Anti;
        lhsResultIterator = Mockito.mock(ResultIterator.class);
        when(lhsResultIterator.next()).thenReturn(null);
        lhsQueryPlan = Mockito.mock(QueryPlan.class);
        when(lhsQueryPlan.iterator(
                DefaultParallelScanGrouper.getInstance())).thenReturn(lhsResultIterator);

        rhsQueryPlan = Mockito.mock(QueryPlan.class);
        rhsResultIterator = Mockito.mock(ResultIterator.class);
        tuple = Mockito.mock(Tuple.class);
        when(rhsResultIterator.next()).thenReturn(tuple);
        when(rhsQueryPlan.iterator(
                DefaultParallelScanGrouper.getInstance())).thenReturn(rhsResultIterator);

        sortMergeJoinPlan = new SortMergeJoinPlan(
                statementContext,
                null,
                null,
                joinType,
                lhsQueryPlan,
                rhsQueryPlan,
                lhsAndRhsJoinExpressions,
                expressions,
                null,
                null,
                null,
                0,
                true,
                lhsAndRhsOrderByNodes);
        semiAntiJoinIterator = (SortMergeJoinPlan.SemiAntiJoinIterator)sortMergeJoinPlan.iterator();
        resultTuple = semiAntiJoinIterator.next();
        assertTrue(resultTuple == null);
        assertTrue(semiAntiJoinIterator.isEnd());
    }
}
