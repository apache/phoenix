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
package org.apache.phoenix.execute;

import static org.apache.phoenix.query.QueryConstants.VALUE_COLUMN_FAMILY;
import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.JoinCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.TupleProjectionCompiler;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.CorrelateVariableFieldAccessExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.EncodedCQCounter;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CorrelatePlanTest {
    
    private static final StatementContext CONTEXT;
    static {
        try {
            PhoenixConnection connection = DriverManager.getConnection(JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS).unwrap(PhoenixConnection.class);
            PhoenixStatement stmt = new PhoenixStatement(connection);
            ColumnResolver resolver = FromCompiler.getResolverForQuery(SelectStatement.SELECT_ONE, connection);
            CONTEXT = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static final Object[][] LEFT_RELATION = new Object[][] {
            {1, "1"},
            {2, "2"},
            {3, "3"},
            {4, "4"},
            {5, "5"},
    };
    
    private static final Object[][] RIGHT_RELATION = new Object[][] {
            {"2", 20},
            {"2", 40},
            {"5", 50},
            {"6", 60},
            {"5", 100},
            {"1", 10},
            {"3", 30},
    };        
    
    @Test
    public void testCorrelatePlanWithInnerJoinType() throws SQLException {
        Object[][] expected = new Object[][] {
                {1, "1", "1", 10},
                {2, "2", "2", 20},
                {2, "2", "2", 40},
                {3, "3", "3", 30},
                {5, "5", "5", 50},
                {5, "5", "5", 100},
        };
        testCorrelatePlan(LEFT_RELATION, RIGHT_RELATION, 1, 0, JoinType.Inner, expected);
    }
    
    @Test
    public void testCorrelatePlanWithLeftJoinType() throws SQLException {
        Object[][] expected = new Object[][] {
                {1, "1", "1", 10},
                {2, "2", "2", 20},
                {2, "2", "2", 40},
                {3, "3", "3", 30},
                {4, "4", null, null},
                {5, "5", "5", 50},
                {5, "5", "5", 100},
        };
        testCorrelatePlan(LEFT_RELATION, RIGHT_RELATION, 1, 0, JoinType.Left, expected);
    }
    
    @Test
    public void testCorrelatePlanWithSemiJoinType() throws SQLException {
        Object[][] expected = new Object[][] {
                {1, "1"},
                {2, "2"},
                {3, "3"},
                {5, "5"},
        };
        testCorrelatePlan(LEFT_RELATION, RIGHT_RELATION, 1, 0, JoinType.Semi, expected);
    }
    
    @Test
    public void testCorrelatePlanWithAntiJoinType() throws SQLException {
        Object[][] expected = new Object[][] {
                {4, "4"},
        };
        testCorrelatePlan(LEFT_RELATION, RIGHT_RELATION, 1, 0, JoinType.Anti, expected);
    }
    
    @Test
    public void testCorrelatePlanWithSingleValueOnly() throws SQLException {
        Object[][] expected = new Object[][] {
                {1, "1", "1", 10},
                {2, "2", "2", 20},
                {2, "2", "2", 40},
        };
        try {
            testCorrelatePlan(LEFT_RELATION, RIGHT_RELATION, 1, 0, JoinType.Inner, expected);
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SINGLE_ROW_SUBQUERY_RETURNS_MULTIPLE_ROWS.getErrorCode(), e.getErrorCode());
        }
        
        Object[][] rightRelation = new Object[][] {
                {"2", 20},
                {"6", 60},
                {"5", 100},
                {"1", 10},
        };        
        expected = new Object[][] {
                {1, "1", "1", 10},
                {2, "2", "2", 20},
                {5, "5", "5", 100},
        };
        testCorrelatePlan(LEFT_RELATION, rightRelation, 1, 0, JoinType.Inner, expected);
    }
    
    @Test
    public void testCorrelatePlanWithSingleValueOnlyAndOffset() throws SQLException {
        Integer offset = 1;
        Object[][] rightRelation = new Object[][] {
                {"6", 60},
                {"2", 20},
                {"5", 100},
                {"1", 10},
        };
        Object[][] expected = new Object[][] {
                {2, "2", "2", 20},
                {5, "5", "5", 100},
        };
        testCorrelatePlan(LEFT_RELATION, rightRelation, 1, 0, JoinType.Inner, expected, offset);
    }

    private void testCorrelatePlan(Object[][] leftRelation, Object[][] rightRelation, int leftCorrelColumn, int rightCorrelColumn, JoinType type, Object[][] expectedResult) throws SQLException {
        testCorrelatePlan(leftRelation, rightRelation, leftCorrelColumn, rightCorrelColumn, type, expectedResult, null);
    }

    private void testCorrelatePlan(Object[][] leftRelation, Object[][] rightRelation, int leftCorrelColumn,
            int rightCorrelColumn, JoinType type, Object[][] expectedResult, Integer offset) throws SQLException {
        TableRef leftTable = createProjectedTableFromLiterals(leftRelation[0]);
        TableRef rightTable = createProjectedTableFromLiterals(rightRelation[0]);
        String varName = "$cor0";
        RuntimeContext runtimeContext = new RuntimeContextImpl();
        runtimeContext.defineCorrelateVariable(varName, leftTable);
        QueryPlan leftPlan = newLiteralResultIterationPlan(leftRelation, offset);
        QueryPlan rightPlan = newLiteralResultIterationPlan(rightRelation, offset);
        Expression columnExpr = new ColumnRef(rightTable, rightCorrelColumn).newColumnExpression();
        Expression fieldAccess = new CorrelateVariableFieldAccessExpression(runtimeContext, varName, new ColumnRef(leftTable, leftCorrelColumn).newColumnExpression());
        Expression filter = ComparisonExpression.create(CompareOp.EQUAL, Arrays.asList(columnExpr, fieldAccess), CONTEXT.getTempPtr(), false);
        rightPlan = new ClientScanPlan(CONTEXT, SelectStatement.SELECT_ONE, rightTable, RowProjector.EMPTY_PROJECTOR,
                null, null, filter, OrderBy.EMPTY_ORDER_BY, rightPlan);
        PTable joinedTable = JoinCompiler.joinProjectedTables(leftTable.getTable(), rightTable.getTable(), type);
        CorrelatePlan correlatePlan = new CorrelatePlan(leftPlan, rightPlan, varName, type, false, runtimeContext, joinedTable, leftTable.getTable(), rightTable.getTable(), leftTable.getTable().getColumns().size());
        ResultIterator iter = correlatePlan.iterator();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        for (Object[] row : expectedResult) {
            Tuple next = iter.next();
            assertNotNull(next);
            for (int i = 0; i < row.length; i++) {
                PColumn column = joinedTable.getColumns().get(i);
                boolean eval = new ProjectedColumnExpression(column, joinedTable, column.getName().getString()).evaluate(next, ptr);
                Object o = eval ? column.getDataType().toObject(ptr) : null;
                assertEquals(row[i], o);
            }
        }
    }

    private QueryPlan newLiteralResultIterationPlan(Object[][] rows, Integer offset) {
        List<Tuple> tuples = Lists.newArrayList();
        Tuple baseTuple = new SingleKeyValueTuple(KeyValue.LOWESTKEY);
        for (Object[] row : rows) {
            Expression[] exprs = new Expression[row.length];
            for (int i = 0; i < row.length; i++) {
                exprs[i] = LiteralExpression.newConstant(row[i]);
            }
            TupleProjector projector = new TupleProjector(exprs);
            tuples.add(projector.projectResults(baseTuple));
        }
        
        return new LiteralResultIterationPlan(tuples, CONTEXT, SelectStatement.SELECT_ONE, TableRef.EMPTY_TABLE_REF,
                RowProjector.EMPTY_PROJECTOR, null, offset, OrderBy.EMPTY_ORDER_BY, null);
    }


    private TableRef createProjectedTableFromLiterals(Object[] row) {
        List<PColumn> columns = Lists.<PColumn>newArrayList();
        for (int i = 0; i < row.length; i++) {
            String name = ParseNodeFactory.createTempAlias();
            Expression expr = LiteralExpression.newConstant(row[i]);
            PName colName = PNameFactory.newName(name);
            columns.add(new PColumnImpl(PNameFactory.newName(name), PNameFactory.newName(VALUE_COLUMN_FAMILY),
                    expr.getDataType(), expr.getMaxLength(), expr.getScale(), expr.isNullable(),
                    i, expr.getSortOrder(), null, null, false, name, false, false, colName.getBytes()));
        }
        try {
            PTable pTable = PTableImpl.makePTable(null, PName.EMPTY_NAME, PName.EMPTY_NAME,
                    PTableType.SUBQUERY, null, MetaDataProtocol.MIN_TABLE_TIMESTAMP, PTable.INITIAL_SEQ_NUM,
                    null, null, columns, null, null, Collections.<PTable>emptyList(),
                    false, Collections.<PName>emptyList(), null, null, false, false, false, null,
                    null, null, true, false, 0, 0L, Boolean.FALSE, null, false, ImmutableStorageScheme.ONE_CELL_PER_COLUMN, QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, EncodedCQCounter.NULL_COUNTER);
            TableRef sourceTable = new TableRef(pTable);
            List<ColumnRef> sourceColumnRefs = Lists.<ColumnRef> newArrayList();
            for (PColumn column : sourceTable.getTable().getColumns()) {
                sourceColumnRefs.add(new ColumnRef(sourceTable, column.getPosition()));
            }
        
            return new TableRef(TupleProjectionCompiler.createProjectedTable(sourceTable, sourceColumnRefs, false));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }        
    }
}
