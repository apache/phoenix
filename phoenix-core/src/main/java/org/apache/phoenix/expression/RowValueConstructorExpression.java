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

/**
 * Implementation for row value constructor (a,b,c) expression.
 * 
 * 
 * @since 0.1
 */
package org.apache.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

public class RowValueConstructorExpression extends BaseCompoundExpression {
    
    private ImmutableBytesWritable ptrs[];
    private ImmutableBytesWritable literalExprPtr;
    private int counter;
    private int estimatedByteSize;

    public RowValueConstructorExpression() {
    }
    
    public RowValueConstructorExpression(List<Expression> children, boolean isConstant) {
        super(children);
        counter = 0;
        estimatedByteSize = 0;
        init(isConstant);
    }

    public RowValueConstructorExpression clone(List<Expression> children) {
        return new RowValueConstructorExpression(children, literalExprPtr != null);
    }
    
    public int getEstimatedSize() {
        return estimatedByteSize;
    }
    
    @Override
    public boolean isStateless() {
        return literalExprPtr != null;
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init(input.readBoolean());
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        output.writeBoolean(literalExprPtr != null);
    }
    
    private void init(boolean isConstant) {
        this.ptrs = new ImmutableBytesWritable[children.size()];
        if(isConstant) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            this.evaluate(null, ptr);
            literalExprPtr = ptr;
        }
    }
    
    @Override
    public PDataType getDataType() {
        return PVarbinary.INSTANCE;
    }
    
    @Override
    public void reset() {
        counter = 0;
        estimatedByteSize = 0;
        Arrays.fill(ptrs, null);
    }
    
    private static int getExpressionByteCount(Expression e) {
        PDataType childType = e.getDataType();
        if (childType != null && !childType.isFixedWidth()) {
            return 1;
        } else {
            // Write at least one null byte in the case of the child being null with a childType of null
            return childType == null || !childType.isFixedWidth() ? 1 : SchemaUtil.getFixedByteSize(e);
        }
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if(literalExprPtr != null) {
            // if determined during construction that the row value constructor is just comprised of literal expressions, 
            // let's just return the ptr we have already computed and be done with evaluation.
            ptr.set(literalExprPtr.get(), literalExprPtr.getOffset(), literalExprPtr.getLength());
            return true;
        }
        try {
            int j;
            int expressionCount = counter;
            for(j = counter; j < ptrs.length; j++) {
                final Expression expression = children.get(j);
                // TODO: handle overflow and underflow
                if (expression.evaluate(tuple, ptr)) {
                    if (ptr.getLength() == 0) {
                        estimatedByteSize += getExpressionByteCount(expression);
                    } else {
                        expressionCount = j+1;
                        ptrs[j] = new ImmutableBytesWritable();
                        ptrs[j].set(ptr.get(), ptr.getOffset(), ptr.getLength());
                        estimatedByteSize += ptr.getLength() + (expression.getDataType().isFixedWidth() ? 0 : 1); // 1 extra for the separator byte.
                    }
                    counter++;
                } else if (tuple == null || tuple.isImmutable()) {
                    estimatedByteSize += getExpressionByteCount(expression);
                    counter++;
                } else {
                    return false;
                }
            }
            
            if (j == ptrs.length) {
                if (expressionCount == 0) {
                    ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                    return true;
                }
                if (expressionCount == 1) {
                    ptr.set(ptrs[0].get(), ptrs[0].getOffset(), ptrs[0].getLength());
                    return true;
                }
                TrustedByteArrayOutputStream output = new TrustedByteArrayOutputStream(estimatedByteSize);
                try {
                    boolean previousCarryOver = false;
                    for (int i = 0; i< expressionCount; i++) {
                        Expression child = getChildren().get(i);
                        PDataType childType = child.getDataType();
                        ImmutableBytesWritable tempPtr = ptrs[i];
                        if (tempPtr == null) {
                            // Since we have a null and have no representation for null,
                            // we must decrement the value of the current. Otherwise,
                            // we'd have an ambiguity if this value happened to be the
                            // min possible value.
                            previousCarryOver = childType == null || childType.isFixedWidth();
                            int bytesToWrite = getExpressionByteCount(child);
                            for (int m = 0; m < bytesToWrite; m++) {
                                output.write(QueryConstants.SEPARATOR_BYTE);
                            }
                        } else {
                            output.write(tempPtr.get(), tempPtr.getOffset(), tempPtr.getLength());
                            if (!childType.isFixedWidth()) {
                                output.write(QueryConstants.SEPARATOR_BYTE);
                            }
                            if (previousCarryOver) {
                                previousCarryOver = !ByteUtil.previousKey(output.getBuffer(), output.size());
                            }
                        }
                    }
                    int outputSize = output.size();
                    byte[] outputBytes = output.getBuffer();
                    for (int k = expressionCount -1 ; 
                            k >=0 &&  getChildren().get(k).getDataType() != null && !getChildren().get(k).getDataType().isFixedWidth() && outputBytes[outputSize-1] == QueryConstants.SEPARATOR_BYTE ; k--) {
                        outputSize--;
                    }
                    ptr.set(outputBytes, 0, outputSize);
                    return true;
                } finally {
                    output.close();
                }
            }  
            return false;
        } catch (IOException e) {
            throw new RuntimeException(e); //Impossible.
        }
    }
    
    @Override
    public final String toString() {
        StringBuilder buf = new StringBuilder("(");
        for (int i = 0; i < children.size() - 1; i++) {
            buf.append(children.get(i) + ", ");
        }
        buf.append(children.get(children.size()-1) + ")");
        return buf.toString();
    }
    
    @Override
    public boolean requiresFinalEvaluation() {
        return true;
    }
}
