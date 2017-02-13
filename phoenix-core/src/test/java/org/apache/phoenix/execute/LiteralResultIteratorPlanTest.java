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
import static org.junit.Assert.assertNull;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.TupleProjectionCompiler;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
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
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.junit.Test;

import com.google.common.collect.Lists;

public class LiteralResultIteratorPlanTest {

    private static final StatementContext CONTEXT;

    static {
        try {
            PhoenixConnection connection = DriverManager
                    .getConnection(JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS)
                    .unwrap(PhoenixConnection.class);
            PhoenixStatement stmt = new PhoenixStatement(connection);
            ColumnResolver resolver = FromCompiler.getResolverForQuery(SelectStatement.SELECT_ONE, connection);
            CONTEXT = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Object[][] RELATION = new Object[][] {
        {"2", 20},
        {"2", 40},
        {"5", 50},
        {"6", 60},
        {"5", 100},
        {"1", 10},
        {"3", 30},
 };
    PTable table = createProjectedTableFromLiterals(RELATION[0]).getTable();

    @Test
    public void testLiteralResultIteratorPlanWithOffset() throws SQLException {
        Object[][] expected = new Object[][] {
            {"2", 40},
            {"5", 50},
            {"6", 60},
            {"5", 100},
            {"1", 10},
            {"3", 30},
        };
        testLiteralResultIteratorPlan(expected, 1, null);
    }

    @Test
    public void testLiteralResultIteratorPlanWithLimit() throws SQLException {
        Object[][] expected = new Object[][] {
            {"2", 20},
            {"2", 40},
            {"5", 50},
            {"6", 60},
        };
        testLiteralResultIteratorPlan(expected, null, 4);
    }

    @Test
    public void testLiteralResultIteratorPlanWithLimitAndOffset() throws SQLException {
        Object[][] expected = new Object[][] {
            {"5", 50},
            {"6", 60},
            {"5", 100},
            {"1", 10},
            };
        testLiteralResultIteratorPlan(expected, 2, 4);
    }

    private void testLiteralResultIteratorPlan(Object[][] expectedResult, Integer offset, Integer limit)
            throws SQLException {

        QueryPlan plan = newLiteralResultIterationPlan(offset, limit);
        ResultIterator iter = plan.iterator();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        for (Object[] row : expectedResult) {
            Tuple next = iter.next();
            assertNotNull(next);
            for (int i = 0; i < row.length; i++) {
                PColumn column = table.getColumns().get(i);
                boolean eval = new ProjectedColumnExpression(column, table, column.getName().getString()).evaluate(next,
                        ptr);
                Object o = eval ? column.getDataType().toObject(ptr) : null;
                assertEquals(row[i], o);
            }
        }
        assertNull(iter.next());
    }

    private QueryPlan newLiteralResultIterationPlan(Integer offset, Integer limit) {
        List<Tuple> tuples = Lists.newArrayList();

        Tuple baseTuple = new SingleKeyValueTuple(KeyValue.LOWESTKEY);
        for (Object[] row : RELATION) {
            Expression[] exprs = new Expression[row.length];
            for (int i = 0; i < row.length; i++) {
                exprs[i] = LiteralExpression.newConstant(row[i]);
            }
            TupleProjector projector = new TupleProjector(exprs);
            tuples.add(projector.projectResults(baseTuple));
        }

        return new LiteralResultIterationPlan(tuples, CONTEXT, SelectStatement.SELECT_ONE, TableRef.EMPTY_TABLE_REF,
                RowProjector.EMPTY_PROJECTOR, limit, offset, OrderBy.EMPTY_ORDER_BY, null);
    }

    private TableRef createProjectedTableFromLiterals(Object[] row) {
        List<PColumn> columns = Lists.<PColumn> newArrayList();
        for (int i = 0; i < row.length; i++) {
            String name = ParseNodeFactory.createTempAlias();
            Expression expr = LiteralExpression.newConstant(row[i]);
            PName colName = PNameFactory.newName(name);
            columns.add(new PColumnImpl(PNameFactory.newName(name),
                    PNameFactory.newName(VALUE_COLUMN_FAMILY), expr.getDataType(), expr.getMaxLength(),
                    expr.getScale(), expr.isNullable(), i, expr.getSortOrder(), null, null, false, name, false, false, colName.getBytes()));
        }
        try {
            PTable pTable = PTableImpl.makePTable(null, PName.EMPTY_NAME, PName.EMPTY_NAME, PTableType.SUBQUERY, null,
                    MetaDataProtocol.MIN_TABLE_TIMESTAMP, PTable.INITIAL_SEQ_NUM, null, null, columns, null, null,
                    Collections.<PTable> emptyList(), false, Collections.<PName> emptyList(), null, null, false, false,
                    false, null, null, null, true, false, 0, 0L, false, null, false, ImmutableStorageScheme.ONE_CELL_PER_COLUMN, QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, EncodedCQCounter.NULL_COUNTER);
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
