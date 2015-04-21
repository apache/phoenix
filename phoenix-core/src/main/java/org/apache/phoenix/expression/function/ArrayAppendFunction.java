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
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.schema.tuple.Tuple;

@FunctionParseNode.BuiltInFunction(name = ArrayAppendFunction.NAME, args = {
        @FunctionParseNode.Argument(allowedTypes = {PBinaryArray.class,
                PVarbinaryArray.class}),
        @FunctionParseNode.Argument(allowedTypes = {PVarbinary.class}, defaultValue = "null")})
public class ArrayAppendFunction extends ScalarFunction {

    public static final String NAME = "ARRAY_APPEND";

    public ArrayAppendFunction() {
    }

    public ArrayAppendFunction(List<Expression> children) throws TypeMismatchException {
        super(children);

        if (getDataType() != null && !(getElementExpr() instanceof LiteralExpression && getElementExpr().isNullable()) && !getElementDataType().isCoercibleTo(getBaseType())) {
            throw TypeMismatchException.newException(getBaseType(), getElementDataType());
        }

        // If the base type of an element is fixed width, make sure the element being appended will fit
        if (getDataType() != null && getElementExpr().getDataType().getByteSize() == null && getElementDataType() != null && getBaseType().isFixedWidth() && getElementDataType().isFixedWidth() && getArrayExpr().getMaxLength() != null &&
                getElementExpr().getMaxLength() != null && getElementExpr().getMaxLength() > getArrayExpr().getMaxLength()) {
            throw new DataExceedsCapacityException("");
        }
        // If the base type has a scale, make sure the element being appended has a scale less than or equal to it
        if (getDataType() != null && getArrayExpr().getScale() != null && getElementExpr().getScale() != null &&
                getElementExpr().getScale() > getArrayExpr().getScale()) {
            throw new DataExceedsCapacityException(getBaseType(), getArrayExpr().getMaxLength(), getArrayExpr().getScale());
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

        if (!getArrayExpr().evaluate(tuple, ptr)) {
            return false;
        } else if (ptr.getLength() == 0) {
            return true;
        }
        int arrayLength = PArrayDataType.getArrayLength(ptr, getBaseType(), getArrayExpr().getMaxLength());

        int length = ptr.getLength();
        int offset = ptr.getOffset();
        byte[] arrayBytes = ptr.get();

        if (!getElementExpr().evaluate(tuple, ptr) || ptr.getLength() == 0) {
            ptr.set(arrayBytes, offset, length);
            return true;
        }

        if (!getBaseType().isSizeCompatible(ptr, null, getElementDataType(), getElementExpr().getMaxLength(), getElementExpr().getScale(), getArrayExpr().getMaxLength(), getArrayExpr().getScale())) {
            throw new DataExceedsCapacityException("");
        }

        getBaseType().coerceBytes(ptr, null, getElementDataType(), getElementExpr().getMaxLength(), getElementExpr().getScale(), getElementExpr().getSortOrder(), getArrayExpr().getMaxLength(), getArrayExpr().getScale(), getArrayExpr().getSortOrder());

        return PArrayDataType.appendItemToArray(ptr, length, offset, arrayBytes, getBaseType(), arrayLength, getMaxLength(), getArrayExpr().getSortOrder());
    }

    @Override
    public PDataType getDataType() {
        return children.get(0).getDataType();
    }

    @Override
    public Integer getMaxLength() {
        return this.children.get(0).getMaxLength();
    }

    @Override
    public SortOrder getSortOrder() {
        return getChildren().get(0).getSortOrder();
    }

    @Override
    public String getName() {
        return NAME;
    }

    public Expression getArrayExpr() {
        return getChildren().get(0);
    }

    public Expression getElementExpr() {
        return getChildren().get(1);
    }

    public PDataType getBaseType() {
        return PDataType.arrayBaseType(getArrayExpr().getDataType());
    }

    public PDataType getElementDataType() {
        return getElementExpr().getDataType();
    }


}
