/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.function;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;

@FunctionParseNode.BuiltInFunction(name = ArrayFillFunction.NAME, args = {
        @FunctionParseNode.Argument(allowedTypes = {PVarbinary.class}),
        @FunctionParseNode.Argument(allowedTypes = {PInteger.class})})
public class ArrayFillFunction extends ScalarFunction {

    public static final String NAME = "ARRAY_FILL";

    public ArrayFillFunction() {
    }

    public ArrayFillFunction(List<Expression> children) throws TypeMismatchException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getElementExpr().evaluate(tuple, ptr)) {
            return false;
        }
        Object element = getElementExpr().getDataType().toObject(ptr, getElementExpr().getSortOrder(), getElementExpr().getMaxLength(), getElementExpr().getScale());
        if (!getLengthExpr().evaluate(tuple, ptr) || ptr.getLength() == 0) {
            return false;
        }
        int length = (Integer) getLengthExpr().getDataType().toObject(ptr, getLengthExpr().getSortOrder(), getLengthExpr().getMaxLength(), getLengthExpr().getScale());
        if (length <= 0) {
            throw new IllegalArgumentException("Array length should be greater than 0");
        }
        Object[] elements = new Object[length];
        Arrays.fill(elements, element);
        PhoenixArray array = PDataType.instantiatePhoenixArray(getElementExpr().getDataType(), elements);
        //When max length of a char array is not the max length of the element passed in
        if (getElementExpr().getDataType().isFixedWidth() && getMaxLength() != null && getMaxLength() != array.getMaxLength()) {
            array = new PhoenixArray(array, getMaxLength());
        }
        ptr.set(((PArrayDataType) getDataType()).toBytes(array, getElementExpr().getDataType(), getElementExpr().getSortOrder()));
        return true;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public PDataType getDataType() {
        return PArrayDataType.fromTypeId(PDataType.ARRAY_TYPE_BASE + getElementExpr().getDataType().getSqlType());
    }

    @Override
    public Integer getMaxLength() {
        return getElementExpr().getDataType().getByteSize() == null ? getElementExpr().getMaxLength() : null;
    }

    @Override
    public SortOrder getSortOrder() {
        return children.get(0).getSortOrder();
    }

    public Expression getElementExpr() {
        return children.get(0);
    }

    public Expression getLengthExpr() {
        return children.get(1);
    }
}
