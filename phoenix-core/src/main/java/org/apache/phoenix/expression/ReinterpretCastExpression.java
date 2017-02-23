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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTime;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedDate;
import org.apache.phoenix.schema.types.PUnsignedTime;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PDataType.PDataCodec;
import org.apache.phoenix.util.DateUtil;

import com.google.common.collect.ImmutableList;


public class ReinterpretCastExpression extends BaseSingleExpression {
    private PDataType toType;
    private PDataCodec inputCodec;
    
    public ReinterpretCastExpression() {
    }

    public static Expression create(Expression expression, PDataType toType) throws SQLException {
        final PDataType sourceType = expression.getDataType();
        if (sourceType == PDate.INSTANCE
                || sourceType == PUnsignedDate.INSTANCE
                || sourceType == PTime.INSTANCE
                || sourceType == PUnsignedTime.INSTANCE) {
            if (toType != PInteger.INSTANCE
                    && toType != PLong.INSTANCE) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TYPE_MISMATCH)
                        .setMessage("Reinterpret cast cannot covert " + sourceType + " to " + toType)
                        .build().buildException();
            }
        } else if (sourceType == PTimestamp.INSTANCE
                || sourceType == PUnsignedTimestamp.INSTANCE) {
            if (toType != PLong.INSTANCE) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TYPE_MISMATCH)
                        .setMessage("Reinterpret cast cannot covert " + sourceType + " to " + toType)
                        .build().buildException();
            }
        }
        return new ReinterpretCastExpression(ImmutableList.of(expression), toType);
    }
    
    //Package protected for tests
    ReinterpretCastExpression(List<Expression> children, PDataType toType) {
        super(children);
        this.toType = toType;
        init();
    }
    
    public ReinterpretCastExpression clone(List<Expression> children) {
        return new ReinterpretCastExpression(children, this.getDataType());
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + toType.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        ReinterpretCastExpression other = (ReinterpretCastExpression)obj;
        return toType.equals(other.toType);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        int ordinal = WritableUtils.readVInt(input);
        toType = PDataType.values()[ordinal];
        init();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, toType.ordinal());
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        final Expression child = getChild();
        final PDataType childType = child.getDataType();
        if (child.evaluate(tuple, ptr)) {
            long dateTime = inputCodec.decodeLong(ptr, child.getSortOrder());
            if (toType == PLong.INSTANCE) {
                toType.getCodec().encodeLong(dateTime, ptr);
            } else { // PInteger
                if (childType == PDate.INSTANCE
                        || childType == PUnsignedDate.INSTANCE) {
                    int date = (int) dateTime / 86400000;
                    toType.getCodec().encodeInt(date, ptr);
                } else { // Time
                    int time = (int) dateTime % 86400000;
                    toType.getCodec().encodeInt(time, ptr);
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public PDataType getDataType() {
        return toType;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("TO_" + toType.toString() + "(");
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + ", ");
        }
        buf.append(children.get(children.size()-1) + ")");
        return buf.toString();
    }
    
    private void init() {
        PDataType childType = getChild().getDataType();
        inputCodec = DateUtil.getCodecFor(childType);
    }
}
