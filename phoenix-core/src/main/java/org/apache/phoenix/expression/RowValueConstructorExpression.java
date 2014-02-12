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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import org.apache.phoenix.expression.function.CeilDecimalExpression;
import org.apache.phoenix.expression.function.CeilTimestampExpression;
import org.apache.phoenix.expression.function.FloorDateExpression;
import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.expression.function.TimeUnit;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

public class RowValueConstructorExpression extends BaseCompoundExpression {
    
    private ImmutableBytesWritable ptrs[];
    private ImmutableBytesWritable literalExprPtr;
    private int counter;
    private int estimatedByteSize;
    
    public static interface ExpressionComparabilityWrapper {
        public Expression wrap(Expression lhs, Expression rhs) throws SQLException;
    }
    /*
     * Used to coerce the RHS to the expected type based on the LHS. In some circumstances,
     * we may need to round the value up or down. For example:
     * WHERE (a,b) < (2.4, 'foo')
     * We take the ceiling of 2.4 to make it 3 if a is an INTEGER to prevent needing to coerce
     * every time during evaluation.
     */
    private static ExpressionComparabilityWrapper[] WRAPPERS = new ExpressionComparabilityWrapper[CompareOp.values().length];
    static {
        WRAPPERS[CompareOp.LESS.ordinal()] = new ExpressionComparabilityWrapper() {

            @Override
            public Expression wrap(Expression lhs, Expression rhs) throws SQLException {
                Expression e = rhs;
                PDataType rhsType = rhs.getDataType();
                PDataType lhsType = lhs.getDataType();
                if (rhsType == PDataType.DECIMAL && lhsType != PDataType.DECIMAL) {
                    e = FloorDecimalExpression.create(rhs);
                } else if ((rhsType == PDataType.TIMESTAMP || rhsType == PDataType.UNSIGNED_TIMESTAMP)  && (lhsType != PDataType.TIMESTAMP && lhsType != PDataType.UNSIGNED_TIMESTAMP)) {
                    e = FloorDateExpression.create(rhs, TimeUnit.MILLISECOND);
                }
                e = new CoerceExpression(e, lhsType, lhs.getSortOrder(), lhs.getByteSize());
                return e;
            }
            
        };
        WRAPPERS[CompareOp.LESS_OR_EQUAL.ordinal()] = WRAPPERS[CompareOp.LESS.ordinal()];
        
        WRAPPERS[CompareOp.GREATER.ordinal()] = new ExpressionComparabilityWrapper() {

            @Override
            public Expression wrap(Expression lhs, Expression rhs) throws SQLException {
                Expression e = rhs;
                PDataType rhsType = rhs.getDataType();
                PDataType lhsType = lhs.getDataType();
                if (rhsType == PDataType.DECIMAL && lhsType != PDataType.DECIMAL) {
                    e = CeilDecimalExpression.create(rhs);
                } else if ((rhsType == PDataType.TIMESTAMP || rhsType == PDataType.UNSIGNED_TIMESTAMP)  && (lhsType != PDataType.TIMESTAMP && lhsType != PDataType.UNSIGNED_TIMESTAMP)) {
                    e = CeilTimestampExpression.create(rhs);
                }
                e = new CoerceExpression(e, lhsType, lhs.getSortOrder(), lhs.getByteSize());
                return e;
            }
            
        };
        WRAPPERS[CompareOp.GREATER_OR_EQUAL.ordinal()] = WRAPPERS[CompareOp.GREATER.ordinal()];
    }
    
    private static ExpressionComparabilityWrapper getWrapper(CompareOp op) {
        ExpressionComparabilityWrapper wrapper = WRAPPERS[op.ordinal()];
        if (wrapper == null) {
            throw new IllegalStateException("Unexpected compare op of " + op + " for row value constructor");
        }
        return wrapper;
    }
    
    /**
     * Recursively coerce the RHS to match the LHS type, throwing if the types are incompatible. The
     * recursion occurs when the RHS or LHS is a row value constructor.
     * TODO: this no longer needs to be recursive, as we flatten out rvc when we normalize the statement.
     * @param lhs left hand side expression
     * @param rhs right hand side expression
     * @param op operator being used to compare the expressions, which can affect rounding we may need to do.
     * @return the newly coerced expression
     * @throws SQLException
     */
    public static Expression coerce(Expression lhs, Expression rhs, CompareOp op) throws SQLException {
        return coerce(lhs, rhs, getWrapper(op));
    }
        
    public static Expression coerce(Expression lhs, Expression rhs, ExpressionComparabilityWrapper wrapper) throws SQLException {
        
        if (lhs instanceof RowValueConstructorExpression && rhs instanceof RowValueConstructorExpression) {
            int i = 0;
            List<Expression> coercedNodes = Lists.newArrayListWithExpectedSize(Math.max(lhs.getChildren().size(), rhs.getChildren().size()));
            for (; i < Math.min(lhs.getChildren().size(),rhs.getChildren().size()); i++) {
                coercedNodes.add(coerce(lhs.getChildren().get(i), rhs.getChildren().get(i), wrapper));
            }
            for (; i < lhs.getChildren().size(); i++) {
                coercedNodes.add(coerce(lhs.getChildren().get(i), null, wrapper));
            }
            for (; i < rhs.getChildren().size(); i++) {
                coercedNodes.add(coerce(null, rhs.getChildren().get(i), wrapper));
            }
            trimTrailingNulls(coercedNodes);
            return coercedNodes.equals(rhs.getChildren()) ? rhs : new RowValueConstructorExpression(coercedNodes, rhs.isStateless());
        } else if (lhs instanceof RowValueConstructorExpression) {
            List<Expression> coercedNodes = Lists.newArrayListWithExpectedSize(Math.max(rhs.getChildren().size(), lhs.getChildren().size()));
            coercedNodes.add(coerce(lhs.getChildren().get(0), rhs, wrapper));
            for (int i = 1; i < lhs.getChildren().size(); i++) {
                coercedNodes.add(coerce(lhs.getChildren().get(i), null, wrapper));
            }
            trimTrailingNulls(coercedNodes);
            return coercedNodes.equals(rhs.getChildren()) ? rhs : new RowValueConstructorExpression(coercedNodes, rhs.isStateless());
        } else if (rhs instanceof RowValueConstructorExpression) {
            List<Expression> coercedNodes = Lists.newArrayListWithExpectedSize(Math.max(rhs.getChildren().size(), lhs.getChildren().size()));
            coercedNodes.add(coerce(lhs, rhs.getChildren().get(0), wrapper));
            for (int i = 1; i < rhs.getChildren().size(); i++) {
                coercedNodes.add(coerce(null, rhs.getChildren().get(i), wrapper));
            }
            trimTrailingNulls(coercedNodes);
            return coercedNodes.equals(rhs.getChildren()) ? rhs : new RowValueConstructorExpression(coercedNodes, rhs.isStateless());
        } else if (lhs == null) { 
            return rhs;
        } else if (rhs == null) {
            return LiteralExpression.newConstant(null, lhs.getDataType(), lhs.isDeterministic());
        } else {
            if (rhs.getDataType() != null && lhs.getDataType() != null && !rhs.getDataType().isCastableTo(lhs.getDataType())) {
                throw TypeMismatchException.newException(lhs.getDataType(), rhs.getDataType());
            }
            return wrapper.wrap(lhs, rhs);
        }
    }
    
    private static void trimTrailingNulls(List<Expression> expressions) {
        for (int i = expressions.size() - 1; i >= 0; i--) {
            Expression e = expressions.get(i);
            if (e instanceof LiteralExpression && ((LiteralExpression)e).getValue() == null) {
                expressions.remove(i);
            } else {
                break;
            }
        }
    }


    public RowValueConstructorExpression() {
    }
    
    public RowValueConstructorExpression(List<Expression> children, boolean isConstant) {
        super(children);
        counter = 0;
        estimatedByteSize = 0;
        init(isConstant);
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
        return PDataType.VARBINARY;
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
            Integer byteSize = e.getByteSize();
            int bytesToWrite = byteSize == null ? 1 : Math.max(1, byteSize);
            return bytesToWrite;
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
}
