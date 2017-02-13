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
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.KeyValueUtil;
import org.junit.Test;

import com.google.common.collect.Lists;

@SuppressWarnings("rawtypes")
public class UnnestArrayPlanTest {
    
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
    
    @Test 
    public void testUnnestIntegerArrays() throws Exception {
        testUnnestArrays(PIntegerArray.INSTANCE, Arrays.asList(new Object[] {1, 10}, new Object[] {2, 20}), false);
    }
    
    @Test 
    public void testUnnestIntegerArraysWithOrdinality() throws Exception {
        testUnnestArrays(PIntegerArray.INSTANCE, Arrays.asList(new Object[] {1, 10}, new Object[] {2, 20}), true);
    }
    
    @Test 
    public void testUnnestVarcharArrays() throws Exception {
        testUnnestArrays(PVarcharArray.INSTANCE, Arrays.asList(new Object[] {"1", "10"}, new Object[] {"2", "20"}), false);
    }
    
    @Test 
    public void testUnnestVarcharArraysWithOrdinality() throws Exception {
        testUnnestArrays(PVarcharArray.INSTANCE, Arrays.asList(new Object[] {"1", "10"}, new Object[] {"2", "20"}), true);
    }
    
    @Test 
    public void testUnnestEmptyArrays() throws Exception {
        testUnnestArrays(PIntegerArray.INSTANCE, Arrays.asList(new Object[] {1, 10}, new Object[]{}, new Object[] {2, 20}), false);
    }
    
    @Test 
    public void testUnnestEmptyArraysWithOrdinality() throws Exception {
        testUnnestArrays(PIntegerArray.INSTANCE, Arrays.asList(new Object[] {1, 10}, new Object[]{}, new Object[] {2, 20}), true);
    }
    
    private void testUnnestArrays(PArrayDataType arrayType, List<Object[]> arrays, boolean withOrdinality) throws Exception {
        PDataType baseType = PDataType.fromTypeId(arrayType.getSqlType() - PDataType.ARRAY_TYPE_BASE);
        List<Tuple> tuples = toTuples(arrayType, arrays);
		LiteralResultIterationPlan subPlan = new LiteralResultIterationPlan(tuples, CONTEXT, SelectStatement.SELECT_ONE,
				TableRef.EMPTY_TABLE_REF, RowProjector.EMPTY_PROJECTOR, null, null, OrderBy.EMPTY_ORDER_BY, null);
        LiteralExpression dummy = LiteralExpression.newConstant(null, arrayType);
        RowKeyValueAccessor accessor = new RowKeyValueAccessor(Arrays.asList(dummy), 0);
        UnnestArrayPlan plan = new UnnestArrayPlan(subPlan, new RowKeyColumnExpression(dummy, accessor), withOrdinality);
        PName colName = PNameFactory.newName("ELEM");
        PColumn elemColumn = new PColumnImpl(PNameFactory.newName("ELEM"), PNameFactory.newName(VALUE_COLUMN_FAMILY), baseType, null, null, true, 0, SortOrder.getDefault(), null, null, false, "", false, false, colName.getBytes());
        colName = PNameFactory.newName("IDX");
        PColumn indexColumn = withOrdinality ? new PColumnImpl(colName, PNameFactory.newName(VALUE_COLUMN_FAMILY), PInteger.INSTANCE, null, null, true, 0, SortOrder.getDefault(), null, null, false, "", false, false, colName.getBytes()) : null;
        List<PColumn> columns = withOrdinality ? Arrays.asList(elemColumn, indexColumn) : Arrays.asList(elemColumn);
        ProjectedColumnExpression elemExpr = new ProjectedColumnExpression(elemColumn, columns, 0, elemColumn.getName().getString());
        ProjectedColumnExpression indexExpr = withOrdinality ? new ProjectedColumnExpression(indexColumn, columns, 1, indexColumn.getName().getString()) : null;
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ResultIterator iterator = plan.iterator();
        for (Object[] o : flatten(arrays)) {
            Tuple tuple = iterator.next();
            assertNotNull(tuple);
            assertTrue(elemExpr.evaluate(tuple, ptr));
            Object elem = baseType.toObject(ptr);
            assertEquals(o[0], elem);
            if (withOrdinality) {
                assertTrue(indexExpr.evaluate(tuple, ptr));
                Object index = PInteger.INSTANCE.toObject(ptr);
                assertEquals(o[1], index);                
            }
        }
        assertNull(iterator.next());
    }
    
    private List<Object[]> flatten(List<Object[]> arrays) {
        List<Object[]> ret = Lists.newArrayList();
        for (Object[] array : arrays) {
            for (int i = 0; i < array.length; i++) {
                ret.add(new Object[] {array[i], i + 1});
            }
        }
        return ret;
    }
    
    private List<Tuple> toTuples(PArrayDataType arrayType, List<Object[]> arrays) {
        List<Tuple> tuples = Lists.newArrayListWithExpectedSize(arrays.size());
        PDataType baseType = PDataType.fromTypeId(arrayType.getSqlType() - PDataType.ARRAY_TYPE_BASE);
        for (Object[] array : arrays) {
            PhoenixArray pArray = new PhoenixArray(baseType, array);
            byte[] bytes = arrayType.toBytes(pArray);            
            tuples.add(new SingleKeyValueTuple(KeyValueUtil.newKeyValue(bytes, 0, bytes.length, bytes, 0, 0, bytes, 0, 0, 0, bytes, 0, 0)));
        }
        
        return tuples;
    }
}
