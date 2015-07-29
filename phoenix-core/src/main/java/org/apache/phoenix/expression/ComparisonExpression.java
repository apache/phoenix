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
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PUnsignedInt;
import org.apache.phoenix.schema.types.PUnsignedLong;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.StringUtil;

import com.google.common.collect.Lists;


/**
 * 
 * Implementation for <,<=,>,>=,=,!= comparison expressions
 * 
 * @since 0.1
 */
public class ComparisonExpression extends BaseCompoundExpression {
    private CompareOp op;
    
    private static void addEqualityExpression(Expression lhs, Expression rhs, List<Expression> andNodes, ImmutableBytesWritable ptr) throws SQLException {
        boolean isLHSNull = ExpressionUtil.isNull(lhs, ptr);
        boolean isRHSNull = ExpressionUtil.isNull(rhs, ptr);
        if (isLHSNull && isRHSNull) { // null == null will end up making the query degenerate
            andNodes.add(LiteralExpression.newConstant(false, PBoolean.INSTANCE));
        } else if (isLHSNull) { // AND rhs IS NULL
            andNodes.add(IsNullExpression.create(rhs, false, ptr));
        } else if (isRHSNull) { // AND lhs IS NULL
            andNodes.add(IsNullExpression.create(lhs, false, ptr));
        } else { // AND lhs = rhs
            andNodes.add(ComparisonExpression.create(CompareOp.EQUAL, Arrays.asList(lhs, rhs), ptr));
        }
    }
    
    /**
     * Rewrites expressions of the form (a, b, c) = (1, 2) as a = 1 and b = 2 and c is null
     * as this is equivalent and already optimized
     * @param lhs
     * @param rhs
     * @param andNodes
     * @throws SQLException 
     */
    private static void rewriteRVCAsEqualityExpression(Expression lhs, Expression rhs, List<Expression> andNodes, ImmutableBytesWritable ptr) throws SQLException {
        if (lhs instanceof RowValueConstructorExpression && rhs instanceof RowValueConstructorExpression) {
            int i = 0;
            for (; i < Math.min(lhs.getChildren().size(),rhs.getChildren().size()); i++) {
                addEqualityExpression(lhs.getChildren().get(i), rhs.getChildren().get(i), andNodes, ptr);
            }
            for (; i < lhs.getChildren().size(); i++) {
                addEqualityExpression(lhs.getChildren().get(i), LiteralExpression.newConstant(null, lhs.getChildren().get(i).getDataType()), andNodes, ptr);
            }
            for (; i < rhs.getChildren().size(); i++) {
                addEqualityExpression(LiteralExpression.newConstant(null, rhs.getChildren().get(i).getDataType()), rhs.getChildren().get(i), andNodes, ptr);
            }
        } else if (lhs instanceof RowValueConstructorExpression) {
            addEqualityExpression(lhs.getChildren().get(0), rhs, andNodes, ptr);
            for (int i = 1; i < lhs.getChildren().size(); i++) {
                addEqualityExpression(lhs.getChildren().get(i), LiteralExpression.newConstant(null, lhs.getChildren().get(i).getDataType()), andNodes, ptr);
            }
        } else if (rhs instanceof RowValueConstructorExpression) {
            addEqualityExpression(lhs, rhs.getChildren().get(0), andNodes, ptr);
            for (int i = 1; i < rhs.getChildren().size(); i++) {
                addEqualityExpression(LiteralExpression.newConstant(null, rhs.getChildren().get(i).getDataType()), rhs.getChildren().get(i), andNodes, ptr);
            }
        }
    }
    
    public static Expression create(CompareOp op, List<Expression> children, ImmutableBytesWritable ptr) throws SQLException {
        Expression lhsExpr = children.get(0);
        Expression rhsExpr = children.get(1);
        PDataType lhsExprDataType = lhsExpr.getDataType();
        PDataType rhsExprDataType = rhsExpr.getDataType();
        
        if ((lhsExpr instanceof RowValueConstructorExpression || rhsExpr instanceof RowValueConstructorExpression) && !(lhsExpr instanceof ArrayElemRefExpression) && !(rhsExpr instanceof ArrayElemRefExpression)) {
            if (op == CompareOp.EQUAL || op == CompareOp.NOT_EQUAL) {
                List<Expression> andNodes = Lists.<Expression>newArrayListWithExpectedSize(Math.max(lhsExpr.getChildren().size(), rhsExpr.getChildren().size()));
                rewriteRVCAsEqualityExpression(lhsExpr, rhsExpr, andNodes, ptr);
                Expression expr = AndExpression.create(andNodes);
                if (op == CompareOp.NOT_EQUAL) {
                    expr = NotExpression.create(expr, ptr);
                }
                return expr;
            }
            rhsExpr = RowValueConstructorExpression.coerce(lhsExpr, rhsExpr, op);
            // Always wrap both sides in row value constructor, so we don't have to consider comparing
            // a non rvc with a rvc.
            if ( ! ( lhsExpr instanceof RowValueConstructorExpression ) ) {
                lhsExpr = new RowValueConstructorExpression(Collections.singletonList(lhsExpr), lhsExpr.isStateless());
            }
            children = Arrays.asList(lhsExpr, rhsExpr);
        } else if(lhsExprDataType != null && rhsExprDataType != null && !lhsExprDataType.isComparableTo(rhsExprDataType)) {
            throw TypeMismatchException.newException(lhsExprDataType, rhsExprDataType,
                toString(op, children));
        }
        Determinism determinism =  lhsExpr.getDeterminism().combine(rhsExpr.getDeterminism());
        
        Object lhsValue = null;
        // Can't use lhsNode.isConstant(), because we have cases in which we don't know
        // in advance if a function evaluates to null (namely when bind variables are used)
        // TODO: use lhsExpr.isStateless instead
        if (lhsExpr instanceof LiteralExpression) {
            lhsValue = ((LiteralExpression)lhsExpr).getValue();
            if (lhsValue == null) {
                return LiteralExpression.newConstant(false, PBoolean.INSTANCE, lhsExpr.getDeterminism());
            }
        }
        Object rhsValue = null;
        // TODO: use lhsExpr.isStateless instead
        if (rhsExpr instanceof LiteralExpression) {
            rhsValue = ((LiteralExpression)rhsExpr).getValue();
            if (rhsValue == null) {
                return LiteralExpression.newConstant(false, PBoolean.INSTANCE, rhsExpr.getDeterminism());
            }
        }
        if (lhsValue != null && rhsValue != null) {
            return LiteralExpression.newConstant(ByteUtil.compare(op,lhsExprDataType.compareTo(lhsValue, rhsValue, rhsExprDataType)), determinism);
        }
        // Coerce constant to match type of lhs so that we don't need to
        // convert at filter time. Since we normalize the select statement
        // to put constants on the LHS, we don't need to check the RHS.
        if (rhsValue != null) {
            // Comparing an unsigned int/long against a negative int/long would be an example. We just need to take
            // into account the comparison operator.
            if (rhsExprDataType != lhsExprDataType 
                    || rhsExpr.getSortOrder() != lhsExpr.getSortOrder()
                    || (rhsExprDataType.isFixedWidth() && rhsExpr.getMaxLength() != null && lhsExprDataType.isFixedWidth() && lhsExpr.getMaxLength() != null && rhsExpr.getMaxLength() < lhsExpr.getMaxLength())) {
                // TODO: if lengths are unequal and fixed width?
                if (rhsExprDataType.isCoercibleTo(lhsExprDataType, rhsValue)) { // will convert 2.0 -> 2
                    children = Arrays.asList(children.get(0), LiteralExpression.newConstant(rhsValue, lhsExprDataType, 
                            lhsExpr.getMaxLength(), null, lhsExpr.getSortOrder(), determinism));
                } else if (op == CompareOp.EQUAL) {
                    return LiteralExpression.newConstant(false, PBoolean.INSTANCE, Determinism.ALWAYS);
                } else if (op == CompareOp.NOT_EQUAL) {
                    return LiteralExpression.newConstant(true, PBoolean.INSTANCE, Determinism.ALWAYS);
                } else { // TODO: generalize this with PDataType.getMinValue(), PDataTypeType.getMaxValue() methods
                  if (rhsExprDataType == PDecimal.INSTANCE) {
                        /*
                         * We're comparing an int/long to a constant decimal with a fraction part.
                         * We need the types to match in case this is used to form a key. To form the start/stop key,
                         * we need to adjust the decimal by truncating it or taking its ceiling, depending on the comparison
                         * operator, to get a whole number.
                         */
                    int increment = 0;
                    switch (op) {
                    case GREATER_OR_EQUAL:
                    case LESS: // get next whole number
                      increment = 1;
                    default: // Else, we truncate the value
                      BigDecimal bd = (BigDecimal)rhsValue;
                      rhsValue = bd.longValue() + increment;
                      children = Arrays.asList(lhsExpr, LiteralExpression.newConstant(rhsValue, lhsExprDataType, lhsExpr.getSortOrder(), rhsExpr.getDeterminism()));
                      break;
                    }
                  } else if (rhsExprDataType == PLong.INSTANCE) {
                        /*
                         * We are comparing an int, unsigned_int to a long, or an unsigned_long to a negative long.
                         * int has range of -2147483648 to 2147483647, and unsigned_int has a value range of 0 to 4294967295.
                         *
                         * If lhs is int or unsigned_int, since we already determined that we cannot coerce the rhs
                         * to become the lhs, we know the value on the rhs is greater than lhs if it's positive, or smaller than
                         * lhs if it's negative.
                         *
                         * If lhs is an unsigned_long, then we know the rhs is definitely a negative long. rhs in this case
                         * will always be bigger than rhs.
                         */
                    if (lhsExprDataType == PInteger.INSTANCE ||
                        lhsExprDataType == PUnsignedInt.INSTANCE) {
                      switch (op) {
                      case LESS:
                      case LESS_OR_EQUAL:
                        if ((Long)rhsValue > 0) {
                          return LiteralExpression.newConstant(true, PBoolean.INSTANCE, determinism);
                        } else {
                          return LiteralExpression.newConstant(false, PBoolean.INSTANCE, determinism);
                        }
                      case GREATER:
                      case GREATER_OR_EQUAL:
                        if ((Long)rhsValue > 0) {
                          return LiteralExpression.newConstant(false, PBoolean.INSTANCE, determinism);
                        } else {
                          return LiteralExpression.newConstant(true, PBoolean.INSTANCE, determinism);
                        }
                      default:
                        break;
                      }
                    } else if (lhsExprDataType == PUnsignedLong.INSTANCE) {
                      switch (op) {
                      case LESS:
                      case LESS_OR_EQUAL:
                        return LiteralExpression.newConstant(false, PBoolean.INSTANCE, determinism);
                      case GREATER:
                      case GREATER_OR_EQUAL:
                        return LiteralExpression.newConstant(true, PBoolean.INSTANCE, determinism);
                      default:
                        break;
                      }
                    }
                    children = Arrays.asList(lhsExpr, LiteralExpression.newConstant(rhsValue, rhsExprDataType, lhsExpr.getSortOrder(), determinism));
                  }
                }
            }

            // Determine if we know the expression must be TRUE or FALSE based on the max size of
            // a fixed length expression.
            if (children.get(1).getMaxLength() != null && lhsExpr.getMaxLength() != null && lhsExpr.getMaxLength() < children.get(1).getMaxLength()) {
                switch (op) {
                case EQUAL:
                    return LiteralExpression.newConstant(false, PBoolean.INSTANCE, determinism);
                case NOT_EQUAL:
                    return LiteralExpression.newConstant(true, PBoolean.INSTANCE, determinism);
                default:
                    break;
                }
            }
        }
        return new ComparisonExpression(children, op);
    }
    
    public ComparisonExpression() {
    }

    public ComparisonExpression(List<Expression> children, CompareOp op) {
        super(children);
        if (op == null) {
            throw new NullPointerException();
        }
        this.op = op;
    }

    public ComparisonExpression clone(List<Expression> children) {
        return new ComparisonExpression(children, this.getFilterOp());
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + op.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (getClass() != obj.getClass()) return false;
        ComparisonExpression other = (ComparisonExpression)obj;
        if (op != other.op) return false;
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PBoolean.INSTANCE;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!children.get(0).evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) { // null comparison evals to null
            return true;
        }
        byte[] lhsBytes = ptr.get();
        int lhsOffset = ptr.getOffset();
        int lhsLength = ptr.getLength();
        PDataType lhsDataType = children.get(0).getDataType();
        SortOrder lhsSortOrder = children.get(0).getSortOrder();
        
        if (!children.get(1).evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) { // null comparison evals to null
            return true;
        }
        
        byte[] rhsBytes = ptr.get();
        int rhsOffset = ptr.getOffset();
        int rhsLength = ptr.getLength();
        PDataType rhsDataType = children.get(1).getDataType();
        SortOrder rhsSortOrder = children.get(1).getSortOrder();   
        if (rhsDataType == PChar.INSTANCE) {
            rhsLength = StringUtil.getUnpaddedCharLength(rhsBytes, rhsOffset, rhsLength, rhsSortOrder);
        }
        if (lhsDataType == PChar.INSTANCE) {
            lhsLength = StringUtil.getUnpaddedCharLength(lhsBytes, lhsOffset, lhsLength, lhsSortOrder);
        }
        
        
        int comparisonResult = lhsDataType.compareTo(lhsBytes, lhsOffset, lhsLength, lhsSortOrder, 
                rhsBytes, rhsOffset, rhsLength, rhsSortOrder, rhsDataType);
        ptr.set(ByteUtil.compare(op, comparisonResult) ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
        return true;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        op = CompareOp.values()[WritableUtils.readVInt(input)];
        super.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, op.ordinal());
        super.write(output);
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

    public CompareOp getFilterOp() {
        return op;
    }
    
    public static String toString(CompareOp op, List<Expression> children) {
        return (children.get(0) + " " + QueryUtil.toSQL(op) + " " + children.get(1));
    }
    
    @Override
    public String toString() {
        return toString(getFilterOp(), children);
    }    
}
