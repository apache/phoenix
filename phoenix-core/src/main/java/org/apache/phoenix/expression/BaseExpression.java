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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.phoenix.expression.function.CeilDecimalExpression;
import org.apache.phoenix.expression.function.CeilTimestampExpression;
import org.apache.phoenix.expression.function.FloorDateExpression;
import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.expression.function.TimeUnit;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;

import com.google.common.collect.Lists;

/**
 * 
 * Base class for Expression hierarchy that provides common
 * default implementations for most methods
 *
 * 
 * @since 0.1
 */
public abstract class BaseExpression implements Expression {
    
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
                if (rhsType == PDecimal.INSTANCE && lhsType != PDecimal.INSTANCE) {
                    e = FloorDecimalExpression.create(rhs);
                } else if ((rhsType == PTimestamp.INSTANCE || rhsType == PUnsignedTimestamp.INSTANCE)  && (lhsType != PTimestamp.INSTANCE && lhsType != PUnsignedTimestamp.INSTANCE)) {
                    e = FloorDateExpression.create(rhs, TimeUnit.MILLISECOND);
                }
                e = CoerceExpression.create(e, lhsType, lhs.getSortOrder(), lhs.getMaxLength());
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
                if (rhsType == PDecimal.INSTANCE && lhsType != PDecimal.INSTANCE) {
                    e = CeilDecimalExpression.create(rhs);
                } else if ((rhsType == PTimestamp.INSTANCE || rhsType == PUnsignedTimestamp.INSTANCE)  && (lhsType != PTimestamp.INSTANCE && lhsType != PUnsignedTimestamp.INSTANCE)) {
                    e = CeilTimestampExpression.create(rhs);
                }
                e = CoerceExpression.create(e, lhsType, lhs.getSortOrder(), lhs.getMaxLength());
                return e;
            }
            
        };
        WRAPPERS[CompareOp.GREATER_OR_EQUAL.ordinal()] = WRAPPERS[CompareOp.GREATER.ordinal()];
        WRAPPERS[CompareOp.EQUAL.ordinal()] = new ExpressionComparabilityWrapper() {

            @Override
            public Expression wrap(Expression lhs, Expression rhs) throws SQLException {
                PDataType lhsType = lhs.getDataType();
                Expression e = CoerceExpression.create(rhs, lhsType, lhs.getSortOrder(), lhs.getMaxLength());
                return e;
            }
            
        };
    }
    
    private static ExpressionComparabilityWrapper getWrapper(CompareOp op) {
        ExpressionComparabilityWrapper wrapper = WRAPPERS[op.ordinal()];
        if (wrapper == null) {
            throw new IllegalStateException("Unexpected compare op of " + op + " for row value constructor");
        }
        return wrapper;
    }
    
    /**
     * Coerce the RHS to match the LHS type, throwing if the types are incompatible.
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
            return LiteralExpression.newConstant(null, lhs.getDataType(), lhs.getDeterminism());
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

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public Integer getMaxLength() {
        return null;
    }

    @Override
    public Integer getScale() {
        return null;
    }
    
    @Override
    public SortOrder getSortOrder() {
        return SortOrder.getDefault();
    }    

    @Override
    public void readFields(DataInput input) throws IOException {
    }

    @Override
    public void write(DataOutput output) throws IOException {
    }

    @Override
    public void reset() {
    }
    
    protected final <T> List<T> acceptChildren(ExpressionVisitor<T> visitor, Iterator<Expression> iterator) {
        if (iterator == null) {
            iterator = visitor.defaultIterator(this);
        }
        List<T> l = Collections.emptyList();
        while (iterator.hasNext()) {
            Expression child = iterator.next();
            T t = child.accept(visitor);
            if (t != null) {
                if (l.isEmpty()) {
                    l = new ArrayList<T>(getChildren().size());
                }
                l.add(t);
            }
        }
        return l;
    }
    
    @Override
    public Determinism getDeterminism() {
        return Determinism.ALWAYS;
    }
    
    @Override
    public boolean isStateless() {
        return false;
    }
    
    @Override
    public boolean requiresFinalEvaluation() {
        return false;
    }

}
