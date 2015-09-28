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

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;

public class DoubleDivideExpression extends DivideExpression {

    public DoubleDivideExpression() {
    }

    public DoubleDivideExpression(List<Expression> children) {
        super(children);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        double result = 0.0;
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            if (!child.evaluate(tuple, ptr)) {
                return false;
            }
            if (ptr.getLength() == 0) {
                return true;
            }
            double childvalue = child.getDataType().getCodec()
                    .decodeDouble(ptr, child.getSortOrder());
            if (!Double.isNaN(childvalue)
                    && childvalue != Double.NEGATIVE_INFINITY
                    && childvalue != Double.POSITIVE_INFINITY) {
                if (i == 0) {
                    result = childvalue;
                } else {
                    result /= childvalue;
                }
            } else {
                return false;
            }
        }
        byte[] resultPtr = new byte[getDataType().getByteSize()];
        getDataType().getCodec().encodeDouble(result, resultPtr, 0);
        ptr.set(resultPtr);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDouble.INSTANCE;
    }

    @Override
    public ArithmeticExpression clone(List<Expression> children) {
        return new DoubleDivideExpression(children);
    }

}
