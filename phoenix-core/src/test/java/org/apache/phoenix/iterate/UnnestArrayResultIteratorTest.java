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
package org.apache.phoenix.iterate;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.OrderByCompiler;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.LiteralResultIterationPlan;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBinaryArray;
import org.apache.phoenix.schema.types.PBooleanArray;
import org.apache.phoenix.schema.types.PCharArray;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDateArray;
import org.apache.phoenix.schema.types.PDoubleArray;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PLongArray;
import org.apache.phoenix.schema.types.PTinyintArray;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.UnnestArrayRowCounter;
import org.junit.Test;

public class UnnestArrayResultIteratorTest {
    private static final StatementContext CONTEXT;
    static {
        try {
            PhoenixConnection
                    connection = DriverManager
                    .getConnection(JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS).unwrap(PhoenixConnection.class);
            PhoenixStatement stmt = new PhoenixStatement(connection);
            ColumnResolver
                    resolver = FromCompiler
                    .getResolverForQuery(SelectStatement.SELECT_ONE, connection);
            CONTEXT = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSingleRowUnnestIntegerArray() throws SQLException {
        List<List<Object[]>> arrays = Arrays.asList(Arrays.asList(new Object[] {1,2,3},new Object[] {4,5,6}));
        PArrayDataType[] pArrayDataTypes = {PIntegerArray.INSTANCE};
        testUnnestColumns(pArrayDataTypes, arrays);
    }

    @Test
    public void testSingleRowUnnestDoubleArray() throws SQLException {
        List<List<Object[]>> arrays = Arrays.asList(Arrays.asList(new Object[] {1d,2d,3d},new Object[] {4d,5d,6d}));
        PArrayDataType[] pArrayDataTypes = {PDoubleArray.INSTANCE};
        testUnnestColumns(pArrayDataTypes, arrays);
    }

    @Test
    public void testSingleRowUnnestVarcharArray() throws SQLException {
        List<List<Object[]>> arrays = Arrays.asList(Arrays.asList(new Object[] {"abc","def","jkl"},new Object[] {"mno","pqr"}));
        PArrayDataType[] pArrayDataTypes = {PVarcharArray.INSTANCE};
        testUnnestColumns(pArrayDataTypes, arrays);
    }

    @Test
    public void testSingleRowUnnestDateArray() throws SQLException {
        List<List<Object[]>> arrays = Arrays.asList(
                Arrays.asList(new Object[] {new Date(21312352),new Date(2131223452),new Date(213452352)},new Object[] {new Date(21345232),new Date(2134523),new Date(21312987),new Date(21234590)}));
        PArrayDataType[] pArrayDataTypes = {PDateArray.INSTANCE};
        testUnnestColumns(pArrayDataTypes, arrays);
    }

    @Test
    public void testSingleRowUnnestTinyintArray()throws SQLException {
        List<List<Object[]>> arrays = Arrays.asList(Arrays.asList(new Object[] {(byte)10,(byte)20,(byte)30},new Object[] {(byte)12}, new Object[]{(byte)45,(byte)32}));
        PArrayDataType[] pArrayDataTypes = {PTinyintArray.INSTANCE};
        testUnnestColumns(pArrayDataTypes, arrays);

    }

    @Test
    public void testSingleRowUnnestLongArray()throws SQLException {
        List<List<Object[]>> arrays = Arrays.asList(Arrays.asList(new Object[] {45l,20l,30l},new Object[] {12l,41l}, new Object[]{45l,32l}));
        PArrayDataType[] pArrayDataTypes = {PLongArray.INSTANCE};
        testUnnestColumns(pArrayDataTypes, arrays);
    }

    @Test
    public void testTwoRowUnnestDoubleLong() throws SQLException{
        List<List<Object[]>> arrays = Arrays.asList( Arrays.asList(new Object[] {1d,2d,3d},new Object[] {4d,5d,6d},new Object[] {4d}),
                Arrays.asList(new Object[] {45l,20l,30l},new Object[] {12l,41l}, new Object[]{45l,32l}));
        PArrayDataType[] pArrayDataTypes = {PDoubleArray.INSTANCE,PLongArray.INSTANCE};
        testUnnestColumns(pArrayDataTypes, arrays);
    }

    @Test
    public void testThreeRowUnnestDoubleLongDate() throws SQLException{
        List<List<Object[]>> arrays = Arrays.asList( Arrays.asList(new Object[] {1d,2d,3d},new Object[] {4d,5d,6d},new Object[] {4d}),
                Arrays.asList(new Object[] {45l,20l,30l},new Object[] {12l,41l}, new Object[]{45l,32l}),
                Arrays.asList(new Object[] {new Date(1234),new Date(12341234),new Date(83435325), new Date(21341234)},new Object[] {new Date(656)},
                        new Object[] {new Date(57567567), new Date(454)}));
        PArrayDataType[] pArrayDataTypes = {PDoubleArray.INSTANCE,PLongArray.INSTANCE, PDateArray.INSTANCE};
        testUnnestColumns(pArrayDataTypes, arrays);
    }

    @Test
    public void testUnnestDoubleLongWithEmptyArray() throws SQLException{
        List<List<Object[]>> arrays = Arrays.asList( Arrays.asList(new Object[] {1d,2d,3d},new Object[] {4d,5d,6d},new Object[] {4d}),
                Arrays.asList(new Object[] {45l,20l,30l},new Object[] {12l,41l}, null));
        PArrayDataType[] pArrayDataTypes = {PDoubleArray.INSTANCE,PLongArray.INSTANCE};
        testUnnestColumns(pArrayDataTypes, arrays);
    }

    private void testUnnestColumns(PArrayDataType[] arrayDataTypes, List<List<Object[]>> arrays)
            throws SQLException {
        // ARRANGE
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        List<Tuple> tuples = getArrayTuples(arrayDataTypes,arrays);
        ResultIterator delegate = new LiteralResultIterationPlan(tuples,CONTEXT,SelectStatement.SELECT_ONE,
                TableRef.EMPTY_TABLE_REF, RowProjector.EMPTY_PROJECTOR,null,null,
                OrderByCompiler.OrderBy.EMPTY_ORDER_BY,null).iterator();
        PDataType[] baseTypes = new PDataType[arrayDataTypes.length];
        List<ColumnExpression> unnestKVRefs = new ArrayList<>(arrayDataTypes.length);
        ArrayElemRefExpression[] unnestKVExprs = new ArrayElemRefExpression[arrayDataTypes.length];
        ColumnExpression[] exprs = new ColumnExpression[arrayDataTypes.length];
        for(int i = 0; i < arrayDataTypes.length; i++) {
            baseTypes[i] = PDataType.fromTypeId(arrayDataTypes[i].getSqlType() - PDataType.ARRAY_TYPE_BASE);
            KeyValueColumnExpression unnestKVRef = getKVRef("test", i, arrayDataTypes[i]);
            unnestKVRefs.add(unnestKVRef);
            unnestKVExprs[i]= new ArrayElemRefExpression(Arrays.asList(unnestKVRef));
            exprs[i] = getUnnestKVRef(i,baseTypes[i]);
        }
        UnnestArrayResultIterator iterator = new UnnestArrayResultIterator(delegate,unnestKVRefs,unnestKVExprs,ptr);


        //ACT && ASSERT
        int[] arrLength =  new int[arrayDataTypes.length];
        int[] indexes = new int[arrayDataTypes.length];
        List<List<Object>> o = new ArrayList<>();
        for(int i = 0; i < arrayDataTypes.length;i++){
            List<Object> column = new ArrayList<>();
            o.add(column);
        }
        int totalNumRows = 0;
        for(int j = 0; j< arrays.get(0).size();j++) {
            int numRows = 1;

            for (int i = 0; i < arrayDataTypes.length; i++) {
                if(arrays.get(i).get(j) != null) {
                    arrLength[i] = arrays.get(i).get(j).length;
                }else{
                    arrLength[i] = 0;
                }
                indexes[i] = 0;
                numRows *= arrLength[i];
            }
            if(numRows>0) {
                getRows(o, arrays, j, arrLength, indexes, numRows);
            }
            totalNumRows += numRows;
        }

        for(int i = 0; i< totalNumRows; i++) {
            Tuple tuple = iterator.next();
            assertNotNull(tuple);
            for(int j = 0; j< arrayDataTypes.length; j++) {
                assertTrue(exprs[j].evaluate(tuple, ptr));
                Object elem = baseTypes[j].toObject(ptr);
                assertEquals(o.get(j).get(i), elem);
            }
        }
        assertNull(iterator.next());
    }

    private void getRows(List<List<Object>> o, List<List<Object[]>> arrays,int rowNumber,int[] arrayLength, int[] indexes, int numRows){
        for(int i = 0; i< numRows; i++){
            for(int j = 0; j < arrays.size(); j++){
                o.get(j).add(arrays.get(j).get(rowNumber)[indexes[j]]);
            }
            UnnestArrayRowCounter.count(arrayLength,indexes);
        }

    }

    private KeyValueColumnExpression getKVRef(String cf, int index,PArrayDataType type){
        PName cfName = PNameFactory.newName(cf);
        PName cqName = PNameFactory.newName("array");
        byte[] cq_byte = PTable.QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(index);
        PColumnImpl pCol = new PColumnImpl(cqName,cfName,type,null,null,true,0,
                SortOrder.getDefault(),null,null,false,"",
                false,false,cq_byte,HConstants.LATEST_TIMESTAMP);
        return new KeyValueColumnExpression(pCol);
    }

    private KeyValueColumnExpression getUnnestKVRef(int index, PDataType type){
        PName cqName = PNameFactory.newName("unnest_"+index);
        byte[] cq =  PTable.QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(QueryConstants.UNNEST_VALUE_COLUMN_QUALIFIER_BASE + index);
        PColumnImpl pCol = new PColumnImpl(cqName, PNameFactory.newName(QueryConstants.RESERVED_COLUMN_FAMILY),
                type,null,null,true,0,SortOrder.getDefault(),null,null,false,"",false,false,
                PTable.QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(QueryConstants.UNNEST_VALUE_COLUMN_QUALIFIER_BASE+index), HConstants.LATEST_TIMESTAMP);
        return new KeyValueColumnExpression(pCol);
    }

    private List<Tuple> getArrayTuples(PArrayDataType[] arrayDataTypes, List<List<Object[]>> arrays){
        int length = arrays.get(0).size();
        for(List<Object[]> array: arrays){
            if(array.size()!=length){
                throw new RuntimeException("Arrays must be of the same length");
            }
        }
        List<Tuple> tuples = new ArrayList<>(length);
        byte[] cf = PNameFactory.newName("test").getBytes();
        for(int i = 0; i< length; i++) {
            List<Cell> cells = new ArrayList<>(arrays.size());
            for(int j = 0; j < arrays.size();j++) {
                if(arrays.get(j).get(i) != null) {
                    PDataType baseType = PDataType.fromTypeId(arrayDataTypes[j].getSqlType() - PDataType.ARRAY_TYPE_BASE);
                    PhoenixArray pArray = new PhoenixArray(baseType, arrays.get(j).get(i));
                    byte[] cq = PTable.QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(j);
                    byte[] value = arrayDataTypes[j].toBytes(pArray);
                    byte[] key = "dummy key".getBytes();
                    cells.add(PhoenixKeyValueUtil.newKeyValue(key, cf, cq, 0, value, 0, value.length));
                }
            }
            Collections.sort(cells, new CellComparatorImpl());
            tuples.add(new MultiKeyValueTuple(cells));
        }
        return tuples;
    }
}
