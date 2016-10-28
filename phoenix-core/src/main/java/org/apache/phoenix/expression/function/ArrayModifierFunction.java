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
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PDataType;

public abstract class ArrayModifierFunction extends ScalarFunction {

    public ArrayModifierFunction() {
    }

    public ArrayModifierFunction(List<Expression> children) throws TypeMismatchException {
        super(children);
        Expression arrayExpr = null;
        PDataType baseDataType = null;
        Expression otherExpr = null;
        PDataType otherExpressionType = null;
        if (getLHSExpr().getDataType().isArrayType()) {
            arrayExpr = getLHSExpr();
            baseDataType = getLHSBaseType();
            otherExpr = getRHSExpr();
            otherExpressionType = getRHSBaseType();
        } else {
            arrayExpr = getRHSExpr();
            baseDataType = getRHSBaseType();
            otherExpr = getLHSExpr();
            otherExpressionType = getLHSBaseType();
        }
        if (getDataType() != null && !(otherExpr instanceof LiteralExpression && otherExpr.isNullable()) && !otherExpressionType.isCoercibleTo(baseDataType)) {
            throw TypeMismatchException.newException(baseDataType, otherExpressionType);
        }

        // If the base type of an element is fixed width, make sure the element
        // being appended will fit
        if (getDataType() != null && otherExpressionType.getByteSize() == null
                && otherExpressionType != null && baseDataType.isFixedWidth()
                && otherExpressionType.isFixedWidth() && arrayExpr.getMaxLength() != null
                && otherExpr.getMaxLength() != null
                && otherExpr.getMaxLength() > arrayExpr.getMaxLength()) {
            throw new DataExceedsCapacityException("Values are not size compatible");
        }
        // If the base type has a scale, make sure the element being appended has a
        // scale less than or equal to it
        if (getDataType() != null && arrayExpr.getScale() != null && otherExpr.getScale() != null
                && otherExpr.getScale() > arrayExpr.getScale()) {
            throw new DataExceedsCapacityException(baseDataType, arrayExpr.getMaxLength(),
                    arrayExpr.getScale());
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arrayExpr = null;
        PDataType baseDataType = null;
        Expression otherExpr = null;
        PDataType otherExpressionType = null;
        if (getLHSExpr().getDataType().isArrayType()) {
            arrayExpr = getLHSExpr();
            baseDataType = getLHSBaseType();
            otherExpr = getRHSExpr();
            otherExpressionType = getRHSBaseType();
        } else {
            arrayExpr = getRHSExpr();
            baseDataType = getRHSBaseType();
            otherExpr = getLHSExpr();
            otherExpressionType = getLHSBaseType();
        }
        if (!arrayExpr.evaluate(tuple, ptr)) {
            return false;
        } else if (ptr.getLength() == 0) {
            return true;
        }
        int arrayLength = PArrayDataType.getArrayLength(ptr, baseDataType, arrayExpr.getMaxLength());

        int length = ptr.getLength();
        int offset = ptr.getOffset();
        byte[] arrayBytes = ptr.get();

        otherExpr.evaluate(tuple, ptr);

        checkSizeCompatibility(ptr, otherExpr.getSortOrder(), arrayExpr, baseDataType, otherExpr, otherExpressionType);
        coerceBytes(ptr, arrayExpr, baseDataType, otherExpr, otherExpressionType);
        return modifierFunction(ptr, length, offset, arrayBytes, baseDataType, arrayLength, getMaxLength(),
                arrayExpr);
    }

    // Override this method for various function implementations
    protected boolean modifierFunction(ImmutableBytesWritable ptr, int len, int offset,
                                       byte[] arrayBytes, PDataType baseDataType, int arrayLength, Integer maxLength,
                                       Expression arrayExp) {
        return false;
    }

    protected void checkSizeCompatibility(ImmutableBytesWritable ptr, SortOrder sortOrder, Expression arrayExpr,
                                          PDataType baseDataType, Expression otherExpr, PDataType otherExpressionType) {
        if (!baseDataType.isSizeCompatible(ptr, null, otherExpressionType,
                sortOrder, otherExpr.getMaxLength(), otherExpr.getScale(),
                arrayExpr.getMaxLength(), arrayExpr.getScale())) {
            throw new DataExceedsCapacityException("Values are not size compatible");
        }
    }


    protected void coerceBytes(ImmutableBytesWritable ptr, Expression arrayExpr,
                               PDataType baseDataType, Expression otherExpr, PDataType otherExpressionType) {
        baseDataType.coerceBytes(ptr, null, otherExpressionType, otherExpr.getMaxLength(),
                otherExpr.getScale(), otherExpr.getSortOrder(), arrayExpr.getMaxLength(),
                arrayExpr.getScale(), arrayExpr.getSortOrder());
    }

    public Expression getRHSExpr() {
        return this.children.get(1);
    }

    public Expression getLHSExpr() {
        return this.children.get(0);
    }

    public PDataType getLHSBaseType() {
        if (getLHSExpr().getDataType().isArrayType()) {
            return PDataType.arrayBaseType(getLHSExpr().getDataType());
        } else {
            return getLHSExpr().getDataType();
        }
    }

    public PDataType getRHSBaseType() {
        if (getRHSExpr().getDataType().isArrayType()) {
            return PDataType.arrayBaseType(getRHSExpr().getDataType());
        } else {
            return getRHSExpr().getDataType();
        }
    }

    @Override
    public PDataType getDataType() {
        if (getLHSExpr().getDataType().isArrayType()) {
            return getLHSExpr().getDataType();
        } else {
            return getRHSExpr().getDataType();
        }
    }


    @Override
    public Integer getMaxLength() {
        if (getLHSExpr().getDataType().isArrayType()) {
            return getLHSExpr().getMaxLength();
        } else {
            return getRHSExpr().getMaxLength();
        }
    }

    @Override
    public SortOrder getSortOrder() {
        if (getLHSExpr().getDataType().isArrayType()) {
            return getLHSExpr().getSortOrder();
        } else {
            return getRHSExpr().getSortOrder();
        }
    }

}
