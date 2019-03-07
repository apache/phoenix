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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.UnnestArrayRowCounter;

public class UnnestArrayResultIterator extends DelegateResultIterator {
    private final List<ColumnExpression> unnestArrayKVRefs;
    private final ArrayElemRefExpression[] unnestArrayExprs;
    private final ImmutableBytesWritable ptr;
    private Tuple currentRow;
    private int rowIndex;
    private int totalNumRow;
    private int[] arrayLengths;
    private int[] arrayIndexes;
    private PDataType[] dataTypes;

    public UnnestArrayResultIterator(ResultIterator delegate, List<ColumnExpression> unnestArrayKVRefs, ArrayElemRefExpression[] unnestArrayExprs,final ImmutableBytesWritable ptr){
        super(delegate);
        this.unnestArrayKVRefs = unnestArrayKVRefs;
        this.unnestArrayExprs = unnestArrayExprs;
        this.ptr = ptr;
        this.rowIndex = 0;
        this.totalNumRow = 0;
        arrayLengths = new int[unnestArrayExprs.length];
        arrayIndexes = new int[unnestArrayExprs.length];
        dataTypes = new PDataType[unnestArrayExprs.length];
        for(int i = 0; i < unnestArrayExprs.length; i++){
            dataTypes[i] = PDataType.fromTypeId(
                    this.unnestArrayKVRefs.get(i).getDataType().getSqlType() - PDataType.ARRAY_TYPE_BASE);
            arrayLengths[i] = 0;
            arrayIndexes[i] = 0;
        }

    }

    @Override public Tuple next() throws SQLException {
        while(rowIndex >= totalNumRow) {
            currentRow = super.next();
            if(currentRow == null){
                return null;
            }
            rowIndex = 0;
            initRow(currentRow);
        }
        Tuple unnestedRow = getUnnestedRow(currentRow);
        rowIndex++;
        return unnestedRow;

    }
    private void initRow(Tuple tuple){
        totalNumRow = 1;
        for(int i = 0; i < unnestArrayExprs.length; i++){
            if(unnestArrayKVRefs.get(i).evaluate(tuple, ptr)){
                arrayLengths[i] = PArrayDataType.getArrayLength(ptr, dataTypes[i], unnestArrayKVRefs
                        .get(i).getMaxLength());
                arrayIndexes[i] = 0;
                totalNumRow *= arrayLengths[i];
            }else{
                rowIndex = 0;
                totalNumRow = 0;
                return;
            }
        }
    }

    private Tuple getUnnestedRow(Tuple tuple){
        Cell rowKv = tuple.getValue(0);
        List<Cell> newRow = new ArrayList<>(tuple.size() + unnestArrayExprs.length);
        //Copy current column
        for(int i = 0; i < currentRow.size(); i++){
            Cell cell = tuple.getValue(i);
            Cell cellCopy = PhoenixKeyValueUtil.newKeyValue(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength(),
                    cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength(),
                    cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength(),
                    cell.getTimestamp(), cell.getValueArray(), cell.getValueOffset(),cell.getValueLength(),cell.getType());
            newRow.add(cellCopy);

        }
        //Add the unnested column
        for(int i = 0; i < unnestArrayExprs.length; i++){
            unnestArrayExprs[i].setIndex(arrayIndexes[i] + 1);
            if(unnestArrayExprs[i].evaluate(tuple,ptr)){
                byte[] value = ptr.copyBytes();
                byte[] cq = PTable.QualifierEncodingScheme.FOUR_BYTE_QUALIFIERS.encode(QueryConstants.UNNEST_VALUE_COLUMN_QUALIFIER_BASE + i);
                KeyValue
                        keyValue =
                        new KeyValue(rowKv.getRowArray(), rowKv.getRowOffset(), rowKv.getRowLength(),
                                QueryConstants.RESERVED_COLUMN_FAMILY_BYTES, 0,
                                QueryConstants.RESERVED_COLUMN_FAMILY_BYTES.length, cq, 0,
                                cq.length, HConstants.LATEST_TIMESTAMP, KeyValue.Type.codeToType(rowKv.getTypeByte()),
                                value, 0, value.length);

                newRow.add(keyValue);
            }
        }
        UnnestArrayRowCounter.count(arrayLengths,arrayIndexes);
        Collections.sort(newRow, new CellComparatorImpl());
        return new MultiKeyValueTuple(newRow);
    }


    @Override public void explain(List<String> planSteps) {
        planSteps.add("Client UNNEST");
    }

    @Override public void close() throws SQLException {
        getDelegate().close();
    }
}
