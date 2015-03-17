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
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.util.NumberUtil;


public class DecimalMultiplyExpression extends MultiplyExpression {

    public DecimalMultiplyExpression() {
    }

    public DecimalMultiplyExpression(List<Expression> children) {
        super(children);
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
            
            PDataType childType = children.get(i).getDataType();
            SortOrder childSortOrder = children.get(i).getSortOrder();
            BigDecimal bd = (BigDecimal) PDecimal.INSTANCE.toObject(ptr, childType, childSortOrder);
            
            if (result == null) {
                result = bd;
            } else {
                result = result.multiply(bd);
            }
        }
        if (getMaxLength() != null || getScale() != null) {
            result = NumberUtil.setDecimalWidthAndScale(result, getMaxLength(), getScale());
        }
        if (result == null) {
            throw new DataExceedsCapacityException(PDecimal.INSTANCE, getMaxLength(), getScale());
        }
        ptr.set(PDecimal.INSTANCE.toBytes(result));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDecimal.INSTANCE;
    }

    @Override
    public ArithmeticExpression clone(List<Expression> children) {
        return new DecimalMultiplyExpression(children);
    }
}
