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
package org.apache.phoenix.expression.function;

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBinaryArray;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinaryArray;

@FunctionParseNode.BuiltInFunction(name = ArrayConcatFunction.NAME, args = {
        @FunctionParseNode.Argument(allowedTypes = {PBinaryArray.class, PVarbinaryArray.class}),
        @FunctionParseNode.Argument(allowedTypes = {PBinaryArray.class, PVarbinaryArray.class})})
public class ArrayConcatFunction extends ArrayModifierFunction {

    public static final String NAME = "ARRAY_CAT";

    public ArrayConcatFunction() {
    }

    public ArrayConcatFunction(List<Expression> children) throws TypeMismatchException {
        super(children);
    }


    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

        if (!getLHSExpr().evaluate(tuple, ptr)|| ptr.getLength() == 0){
            return false;
        }
        boolean isLHSRowKeyOrderOptimized = PArrayDataType.isRowKeyOrderOptimized(getLHSExpr().getDataType(), getLHSExpr().getSortOrder(), ptr);

        SortOrder sortOrder = getRHSExpr().getSortOrder();
        int actualLengthOfArray1 = Math.abs(PArrayDataType.getArrayLength(ptr, getLHSBaseType(), getLHSExpr().getMaxLength()));
        int lengthArray1 = ptr.getLength();
        int offsetArray1 = ptr.getOffset();
        byte[] array1Bytes = ptr.get();
        if (!getRHSExpr().evaluate(tuple, ptr)|| ptr.getLength() == 0){
            sortOrder = getLHSExpr().getSortOrder();
            ptr.set(array1Bytes, offsetArray1, lengthArray1);
            return true;
        }

        checkSizeCompatibility(ptr, sortOrder, getLHSExpr(), getLHSExpr().getDataType(), getRHSExpr(),getRHSExpr().getDataType());

        // FIXME: calling version of coerceBytes that takes into account the separator used by LHS
        // If the RHS does not have the same separator, it'll be coerced to use it. It's unclear
        // if we should do the same for all classes derived from the base class.
        // Coerce RHS to LHS type
        getLHSExpr().getDataType().coerceBytes(ptr, null, getRHSExpr().getDataType(), getRHSExpr().getMaxLength(),
                getRHSExpr().getScale(), getRHSExpr().getSortOrder(), getLHSExpr().getMaxLength(),
                getLHSExpr().getScale(), getLHSExpr().getSortOrder(), isLHSRowKeyOrderOptimized);
        return modifierFunction(ptr, lengthArray1, offsetArray1, array1Bytes, getLHSBaseType(), actualLengthOfArray1, getMaxLength(), getLHSExpr());
    }

    @Override
    protected boolean modifierFunction(ImmutableBytesWritable ptr, int len, int offset,
                                       byte[] array1Bytes, PDataType baseDataType, int actualLengthOfArray1, Integer maxLength,
                                       Expression array1Exp) {
        int actualLengthOfArray2 = Math.abs(PArrayDataType.getArrayLength(ptr, baseDataType, array1Exp.getMaxLength()));
        // FIXME: concatArrays will be fine if it's copying the separator bytes, including the terminating bytes.
        return PArrayDataType.concatArrays(ptr, len, offset, array1Bytes, baseDataType, actualLengthOfArray1, actualLengthOfArray2);
    }

    @Override
    public String getName() {
        return NAME;
    }

}
