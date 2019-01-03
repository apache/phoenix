/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.util;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.visitor.StatelessTraverseAllExpressionVisitor;
import org.apache.phoenix.expression.visitor.StatelessTraverseNoExpressionVisitor;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;

public class ExpressionUtil {
	private ExpressionUtil() {
	}

	public static boolean isConstant(Expression expression) {
		return (expression.isStateless() && isContantForStatement(expression));
	}

    public static boolean isContantForStatement(Expression expression) {
        return  (expression.getDeterminism() == Determinism.ALWAYS
                || expression.getDeterminism() == Determinism.PER_STATEMENT);
    }

    public static LiteralExpression getConstantExpression(Expression expression, ImmutableBytesWritable ptr)
            throws SQLException {
        Object value = null;
        PDataType type = expression.getDataType();
        if (expression.evaluate(null, ptr) && ptr.getLength() != 0) {
            value = type.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(), type, expression.getSortOrder(), expression.getMaxLength(), expression.getScale());
        }
        return LiteralExpression.newConstant(value, type, expression.getDeterminism());
    }

    public static boolean isNull(Expression expression, ImmutableBytesWritable ptr) {
        return isConstant(expression) && (!expression.evaluate(null, ptr) || ptr.getLength() == 0);
    }

    public static LiteralExpression getNullExpression(Expression expression) throws SQLException {
        return LiteralExpression.newConstant(null, expression.getDataType(), expression.getDeterminism());
    }
    
    public static boolean evaluatesToTrue(Expression expression) {
        if (isConstant(expression)) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            expression.evaluate(null, ptr);
            return Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr));
        }
        return false;
    }

    public static boolean isPkPositionChanging(TableRef tableRef, List<Expression> projectedExpressions) throws SQLException {
        for (int i = 0; i < tableRef.getTable().getPKColumns().size(); i++) {
            PColumn column = tableRef.getTable().getPKColumns().get(i);
            Expression source = projectedExpressions.get(i);
            if (source == null || !source
                    .equals(new ColumnRef(tableRef, column.getPosition()).newColumnExpression())) { return true; }
        }
        return false;
    }

    /**
     * check the whereExpression to see if the columnExpression is constant.
     * eg. for "where a =3 and b > 9", a is constant,but b is not.
     * @param columnExpression
     * @param whereExpression
     * @return
     */
    public static boolean isColumnExpressionConstant(ColumnExpression columnExpression, Expression whereExpression) {
        if(whereExpression == null) {
            return false;
        }
        IsColumnConstantExpressionVisitor isColumnConstantExpressionVisitor =
                new IsColumnConstantExpressionVisitor(columnExpression);
        whereExpression.accept(isColumnConstantExpressionVisitor);
        return isColumnConstantExpressionVisitor.isConstant();
    }

    private static class IsColumnConstantExpressionVisitor extends StatelessTraverseNoExpressionVisitor<Void> {
        private final Expression columnExpression ;
        private Expression firstRhsConstantExpression = null;
        private int rhsConstantCount = 0;
        private boolean isNullExpressionVisited = false;

        public IsColumnConstantExpressionVisitor(Expression columnExpression) {
            this.columnExpression = columnExpression;
        }
        /**
         * only consider and,for "where a = 3 or b = 9", neither a or b is constant.
         */
        @Override
        public Iterator<Expression> visitEnter(AndExpression andExpression) {
            if(rhsConstantCount > 1) {
                return null;
            }
            return andExpression.getChildren().iterator();
        }
        /**
         * <pre>
         * We just consider {@link ComparisonExpression} because:
         * 1.for {@link InListExpression} as "a in ('2')", the {@link InListExpression} is rewritten to
         *  {@link ComparisonExpression} in {@link InListExpression#create}
         * 2.for {@link RowValueConstructorExpression} as "(a,b)=(1,2)",{@link RowValueConstructorExpression}
         *   is rewritten to {@link ComparisonExpression} in {@link ComparisonExpression#create}
         * 3.not consider {@link CoerceExpression}, because for "where cast(a as integer)=2", when a is double,
         *   a is not constant.
         * </pre>
         */
        @Override
        public Iterator<Expression> visitEnter(ComparisonExpression comparisonExpression) {
            if(rhsConstantCount > 1) {
                return null;
            }
            if(comparisonExpression.getFilterOp() != CompareOp.EQUAL) {
                return null;
            }
            Expression lhsExpresssion = comparisonExpression.getChildren().get(0);
            if(!this.columnExpression.equals(lhsExpresssion)) {
                return null;
            }
            Expression rhsExpression = comparisonExpression.getChildren().get(1);
            if(rhsExpression == null) {
                return null;
            }
            Boolean isConstant = rhsExpression.accept(new IsCompositeLiteralExpressionVisitor());
            if(isConstant != null && isConstant.booleanValue()) {
                checkConstantValue(rhsExpression);
            }
            return null;
        }

        public boolean isConstant() {
            return this.rhsConstantCount == 1;
        }

        @Override
        public Iterator<Expression> visitEnter(IsNullExpression isNullExpression) {
            if(rhsConstantCount > 1) {
                return null;
            }
            if(isNullExpression.isNegate()) {
                return null;
            }
            Expression lhsExpresssion = isNullExpression.getChildren().get(0);
            if(!this.columnExpression.equals(lhsExpresssion)) {
                return null;
            }
            this.checkConstantValue(null);
            return null;
        }

        private void checkConstantValue(Expression rhsExpression) {
            if(!this.isNullExpressionVisited && this.firstRhsConstantExpression == null) {
                this.firstRhsConstantExpression = rhsExpression;
                rhsConstantCount++;
                if(rhsExpression == null) {
                    this.isNullExpressionVisited = true;
                }
                return;
            }

            if(!isExpressionEquals(this.isNullExpressionVisited ? null : this.firstRhsConstantExpression, rhsExpression)) {
                rhsConstantCount++;
                return;
            }
        }

        private static boolean isExpressionEquals(Expression oldExpression,Expression newExpression) {
            if(oldExpression == null) {
                if(newExpression == null) {
                    return true;
                }
                return ExpressionUtil.isNull(newExpression, new ImmutableBytesWritable());
            }
            if(newExpression == null) {
                return ExpressionUtil.isNull(oldExpression, new ImmutableBytesWritable());
            }
            return oldExpression.equals(newExpression);
        }
    }

    private static class IsCompositeLiteralExpressionVisitor extends StatelessTraverseAllExpressionVisitor<Boolean> {
        @Override
        public Boolean defaultReturn(Expression expression, List<Boolean> childResultValues) {
            if (!ExpressionUtil.isContantForStatement(expression) ||
                    childResultValues.size() < expression.getChildren().size()) {
                return Boolean.FALSE;
            }
            for (Boolean childResultValue : childResultValues) {
                if (!childResultValue) {
                    return Boolean.FALSE;
                }
            }
            return Boolean.TRUE;
        }
        @Override
        public Boolean visit(LiteralExpression literalExpression) {
            return Boolean.TRUE;
        }
    }
}
