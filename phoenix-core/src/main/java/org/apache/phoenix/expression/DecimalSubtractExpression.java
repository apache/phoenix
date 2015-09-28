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
package org.apache.phoenix.expression;

import java.math.BigDecimal;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.DataExceedsCapacityException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.util.NumberUtil;


/**
 * 
 * Subtract expression implementation
 *
 * 
 * @since 0.1
 */
public class DecimalSubtractExpression extends SubtractExpression {
    private Integer maxLength;
    private Integer scale;

    public DecimalSubtractExpression() {
    }

    public DecimalSubtractExpression(List<Expression> children) {
        super(children);
        Expression firstChild = children.get(0);
        maxLength = getPrecision(firstChild);
        scale = getScale(firstChild);
        for (int i=1; i<children.size(); i++) {
            Expression childExpr = children.get(i);
            maxLength = getPrecision(maxLength, getPrecision(childExpr), scale, getScale(childExpr));
            scale = getScale(maxLength, getPrecision(childExpr), scale, getScale(childExpr));
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        BigDecimal result = null;
        for (int i=0; i<children.size(); i++) {
            Expression childExpr = children.get(i);
            if (!childExpr.evaluate(tuple, ptr)) { 
                return false;
            }
            if (ptr.getLength() == 0) {
                return true;
            }
            
            PDataType childType = childExpr.getDataType();
            boolean isDate = childType.isCoercibleTo(PDate.INSTANCE);
            SortOrder childSortOrder = childExpr.getSortOrder();
            BigDecimal bd = isDate ?
                    BigDecimal.valueOf(childType.getCodec().decodeLong(ptr, childSortOrder)) :
                    (BigDecimal) PDecimal.INSTANCE.toObject(ptr, childType, childSortOrder);
            
            if (result == null) {
                result = bd;
            } else {
                result = result.subtract(bd);
                /*
                 * Special case for date subtraction - note that only first two expression may be dates.
                 * We need to convert the date to a unit of "days" because that's what sql expects.
                 */
                if (isDate) {
                    result = result.divide(BD_MILLIS_IN_DAY, PDataType.DEFAULT_MATH_CONTEXT);
                }
            }
        }
        if (maxLength != null || scale != null) {
            result = NumberUtil.setDecimalWidthAndScale(result, maxLength, scale);
        }
        if (result == null) {
            throw new DataExceedsCapacityException(PDecimal.INSTANCE, maxLength, scale);
        }
        ptr.set(PDecimal.INSTANCE.toBytes(result));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDecimal.INSTANCE;
    }

    @Override
    public Integer getScale() {
        return scale;
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }

    @Override
    public ArithmeticExpression clone(List<Expression> children) {
        return new DecimalSubtractExpression(children);
    }
}
