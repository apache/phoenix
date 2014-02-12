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
package org.apache.phoenix.compile;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.CaseExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.DateAddExpression;
import org.apache.phoenix.expression.DateSubtractExpression;
import org.apache.phoenix.expression.DecimalAddExpression;
import org.apache.phoenix.expression.DecimalDivideExpression;
import org.apache.phoenix.expression.DecimalMultiplyExpression;
import org.apache.phoenix.expression.DecimalSubtractExpression;
import org.apache.phoenix.expression.DoubleAddExpression;
import org.apache.phoenix.expression.DoubleDivideExpression;
import org.apache.phoenix.expression.DoubleMultiplyExpression;
import org.apache.phoenix.expression.DoubleSubtractExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.LikeExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.LongAddExpression;
import org.apache.phoenix.expression.LongDivideExpression;
import org.apache.phoenix.expression.LongMultiplyExpression;
import org.apache.phoenix.expression.LongSubtractExpression;
import org.apache.phoenix.expression.NotExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.StringConcatExpression;
import org.apache.phoenix.expression.TimestampAddExpression;
import org.apache.phoenix.expression.TimestampSubtractExpression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.parse.AddParseNode;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.ArithmeticParseNode;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.CaseParseNode;
import org.apache.phoenix.parse.CastParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.DivideParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import org.apache.phoenix.parse.InListParseNode;
import org.apache.phoenix.parse.IsNullParseNode;
import org.apache.phoenix.parse.LikeParseNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.MultiplyParseNode;
import org.apache.phoenix.parse.NotParseNode;
import org.apache.phoenix.parse.OrParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.RowValueConstructorParseNode;
import org.apache.phoenix.parse.StringConcatParseNode;
import org.apache.phoenix.parse.SubtractParseNode;
import org.apache.phoenix.parse.UnsupportedAllParseNodeVisitor;
import org.apache.phoenix.schema.ColumnModifier;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.DelegateDatum;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;


public class ExpressionCompiler extends UnsupportedAllParseNodeVisitor<Expression> {
    private boolean isAggregate;
    private ParseNode aggregateFunction;
    protected final StatementContext context;
    protected final GroupBy groupBy;
    private int nodeCount;

    ExpressionCompiler(StatementContext context) {
        this(context,GroupBy.EMPTY_GROUP_BY);
    }

    ExpressionCompiler(StatementContext context, GroupBy groupBy) {
        this.context = context;
        this.groupBy = groupBy;
    }

    public boolean isAggregate() {
        return isAggregate;
    }

    public boolean isTopLevel() {
        return nodeCount == 0;
    }

    public void reset() {
        this.isAggregate = false;
        this.nodeCount = 0;
    }

    @Override
    public boolean visitEnter(ComparisonParseNode node) {
        return true;
    }
    
    private void addBindParamMetaData(ParseNode parentNode, ParseNode lhsNode, ParseNode rhsNode, Expression lhsExpr, Expression rhsExpr) throws SQLException {
        if (lhsNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)lhsNode, rhsExpr);
        }
        if (rhsNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)rhsNode, lhsExpr);
        }
    }
    
    private static void checkComparability(ParseNode node, PDataType lhsDataType, PDataType rhsDataType) throws TypeMismatchException {
        if(lhsDataType != null && rhsDataType != null && !lhsDataType.isComparableTo(rhsDataType)) {
            throw new TypeMismatchException(lhsDataType, rhsDataType, node.toString());
        }
    }
    
    // TODO: this no longer needs to be recursive, as we flatten out rvc when we normalize the statement
    private void checkComparability(ParseNode parentNode, ParseNode lhsNode, ParseNode rhsNode, Expression lhsExpr, Expression rhsExpr) throws SQLException {
        if (lhsNode instanceof RowValueConstructorParseNode && rhsNode instanceof RowValueConstructorParseNode) {
            int i = 0;
            for (; i < Math.min(lhsExpr.getChildren().size(),rhsExpr.getChildren().size()); i++) {
                checkComparability(parentNode, lhsNode.getChildren().get(i), rhsNode.getChildren().get(i), lhsExpr.getChildren().get(i), rhsExpr.getChildren().get(i));
            }
            for (; i < lhsExpr.getChildren().size(); i++) {
                checkComparability(parentNode, lhsNode.getChildren().get(i), null, lhsExpr.getChildren().get(i), null);
            }
            for (; i < rhsExpr.getChildren().size(); i++) {
                checkComparability(parentNode, null, rhsNode.getChildren().get(i), null, rhsExpr.getChildren().get(i));
            }
        } else if (lhsExpr instanceof RowValueConstructorExpression) {
            checkComparability(parentNode, lhsNode.getChildren().get(0), rhsNode, lhsExpr.getChildren().get(0), rhsExpr);
            for (int i = 1; i < lhsExpr.getChildren().size(); i++) {
                checkComparability(parentNode, lhsNode.getChildren().get(i), null, lhsExpr.getChildren().get(i), null);
            }
        } else if (rhsExpr instanceof RowValueConstructorExpression) {
            checkComparability(parentNode, lhsNode, rhsNode.getChildren().get(0), lhsExpr, rhsExpr.getChildren().get(0));
            for (int i = 1; i < rhsExpr.getChildren().size(); i++) {
                checkComparability(parentNode, null, rhsNode.getChildren().get(i), null, rhsExpr.getChildren().get(i));
            }
        } else if (lhsNode == null && rhsNode == null) { // null == null will end up making the query degenerate
            
        } else if (lhsNode == null) { // AND rhs IS NULL
            addBindParamMetaData(parentNode, lhsNode, rhsNode, lhsExpr, rhsExpr);
        } else if (rhsNode == null) { // AND lhs IS NULL
            addBindParamMetaData(parentNode, lhsNode, rhsNode, lhsExpr, rhsExpr);
        } else { // AND lhs = rhs
            checkComparability(parentNode, lhsExpr.getDataType(), rhsExpr.getDataType());
            addBindParamMetaData(parentNode, lhsNode, rhsNode, lhsExpr, rhsExpr);
        }
    }

    @Override
    public Expression visitLeave(ComparisonParseNode node, List<Expression> children) throws SQLException {
        ParseNode lhsNode = node.getChildren().get(0);
        ParseNode rhsNode = node.getChildren().get(1);
        Expression lhsExpr = children.get(0);
        Expression rhsExpr = children.get(1);

        PDataType lhsExprDataType = lhsExpr.getDataType();
        PDataType rhsExprDataType = rhsExpr.getDataType();
        checkComparability(node, lhsNode, rhsNode, lhsExpr, rhsExpr);
        
        if (lhsExpr instanceof RowValueConstructorExpression || rhsExpr instanceof RowValueConstructorExpression) {
            rhsExpr = RowValueConstructorExpression.coerce(lhsExpr, rhsExpr, node.getFilterOp());
            // Always wrap both sides in row value constructor, so we don't have to consider comparing
            // a non rvc with a rvc.
            if ( ! ( lhsExpr instanceof RowValueConstructorExpression ) ) {
                lhsExpr = new RowValueConstructorExpression(Collections.singletonList(lhsExpr), lhsExpr.isConstant());
            }
            children = Arrays.asList(lhsExpr, rhsExpr);
        }
        
        Object lhsValue = null;
        // Can't use lhsNode.isConstant(), because we have cases in which we don't know
        // in advance if a function evaluates to null (namely when bind variables are used)
        if (lhsExpr instanceof LiteralExpression) {
            lhsValue = ((LiteralExpression)lhsExpr).getValue();
            if (lhsValue == null) {
                return LiteralExpression.FALSE_EXPRESSION;
            }
        }
        Object rhsValue = null;
        if (rhsExpr instanceof LiteralExpression) {
            rhsValue = ((LiteralExpression)rhsExpr).getValue();
            if (rhsValue == null) {
                return LiteralExpression.FALSE_EXPRESSION;
            }
        }
        if (lhsValue != null && rhsValue != null) {
            return LiteralExpression.newConstant(ByteUtil.compare(node.getFilterOp(),lhsExprDataType.compareTo(lhsValue, rhsValue, rhsExprDataType)));
        }
        // Coerce constant to match type of lhs so that we don't need to
        // convert at filter time. Since we normalize the select statement
        // to put constants on the LHS, we don't need to check the RHS.
        if (rhsValue != null) {
            // Comparing an unsigned int/long against a negative int/long would be an example. We just need to take
            // into account the comparison operator.
            if (rhsExprDataType != lhsExprDataType 
                    || rhsExpr.getColumnModifier() != lhsExpr.getColumnModifier()
                    || (rhsExpr.getMaxLength() != null && lhsExpr.getMaxLength() != null && rhsExpr.getMaxLength() < lhsExpr.getMaxLength())) {
                // TODO: if lengths are unequal and fixed width?
                if (rhsExprDataType.isCoercibleTo(lhsExprDataType, rhsValue)) { // will convert 2.0 -> 2
                    children = Arrays.asList(children.get(0), LiteralExpression.newConstant(rhsValue, lhsExprDataType, 
                            lhsExpr.getMaxLength(), null, lhsExpr.getColumnModifier()));
                } else if (node.getFilterOp() == CompareOp.EQUAL) {
                    return LiteralExpression.FALSE_EXPRESSION;
                } else if (node.getFilterOp() == CompareOp.NOT_EQUAL) {
                    return LiteralExpression.TRUE_EXPRESSION;
                } else { // TODO: generalize this with PDataType.getMinValue(), PDataTypeType.getMaxValue() methods
                    switch(rhsExprDataType) {
                    case DECIMAL:
                        /*
                         * We're comparing an int/long to a constant decimal with a fraction part.
                         * We need the types to match in case this is used to form a key. To form the start/stop key,
                         * we need to adjust the decimal by truncating it or taking its ceiling, depending on the comparison
                         * operator, to get a whole number.
                         */
                        int increment = 0;
                        switch (node.getFilterOp()) {
                        case GREATER_OR_EQUAL: 
                        case LESS: // get next whole number
                            increment = 1;
                        default: // Else, we truncate the value
                            BigDecimal bd = (BigDecimal)rhsValue;
                            rhsValue = bd.longValue() + increment;
                            children = Arrays.asList(children.get(0), LiteralExpression.newConstant(rhsValue, lhsExprDataType, lhsExpr.getColumnModifier()));
                            break;
                        }
                        break;
                    case LONG:
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
                        if (lhsExprDataType == PDataType.INTEGER || 
                        lhsExprDataType == PDataType.UNSIGNED_INT) {
                            switch (node.getFilterOp()) {
                            case LESS:
                            case LESS_OR_EQUAL:
                                if ((Long)rhsValue > 0) {
                                    return LiteralExpression.TRUE_EXPRESSION;
                                } else {
                                    return LiteralExpression.FALSE_EXPRESSION;
                                }
                            case GREATER:
                            case GREATER_OR_EQUAL:
                                if ((Long)rhsValue > 0) {
                                    return LiteralExpression.FALSE_EXPRESSION;
                                } else {
                                    return LiteralExpression.TRUE_EXPRESSION;
                                }
                            default:
                                break;
                            }
                        } else if (lhsExprDataType == PDataType.UNSIGNED_LONG) {
                            switch (node.getFilterOp()) {
                            case LESS:
                            case LESS_OR_EQUAL:
                                return LiteralExpression.FALSE_EXPRESSION;
                            case GREATER:
                            case GREATER_OR_EQUAL:
                                return LiteralExpression.TRUE_EXPRESSION;
                            default:
                                break;
                            }
                        }
                        children = Arrays.asList(children.get(0), LiteralExpression.newConstant(rhsValue, rhsExprDataType, lhsExpr.getColumnModifier()));
                        break;
                    }
                }
            }

            // Determine if we know the expression must be TRUE or FALSE based on the max size of
            // a fixed length expression.
            if (children.get(1).getMaxLength() != null && lhsExpr.getMaxLength() != null && lhsExpr.getMaxLength() < children.get(1).getMaxLength()) {
                switch (node.getFilterOp()) {
                case EQUAL:
                    return LiteralExpression.FALSE_EXPRESSION;
                case NOT_EQUAL:
                    return LiteralExpression.TRUE_EXPRESSION;
                default:
                    break;
                }
            }
        }
        return new ComparisonExpression(node.getFilterOp(), children);
    }

    @Override
    public boolean visitEnter(AndParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(AndParseNode node, List<Expression> children) throws SQLException {
        Iterator<Expression> iterator = children.iterator();
        while (iterator.hasNext()) {
            Expression child = iterator.next();
            if (child.getDataType() != PDataType.BOOLEAN) {
                throw new TypeMismatchException(PDataType.BOOLEAN, child.getDataType(), child.toString());
            }
            if (child == LiteralExpression.FALSE_EXPRESSION) {
                return child;
            }
            if (child == LiteralExpression.TRUE_EXPRESSION) {
                iterator.remove();
            }
        }
        if (children.size() == 0) {
            return LiteralExpression.TRUE_EXPRESSION;
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        return new AndExpression(children);
    }

    @Override
    public boolean visitEnter(OrParseNode node) throws SQLException {
        return true;
    }

    private Expression orExpression(List<Expression> children) throws SQLException {
        Iterator<Expression> iterator = children.iterator();
        while (iterator.hasNext()) {
            Expression child = iterator.next();
            if (child.getDataType() != PDataType.BOOLEAN) {
                throw new TypeMismatchException(PDataType.BOOLEAN, child.getDataType(), child.toString());
            }
            if (child == LiteralExpression.FALSE_EXPRESSION) {
                iterator.remove();
            }
            if (child == LiteralExpression.TRUE_EXPRESSION) {
                return child;
            }
        }
        if (children.size() == 0) {
            return LiteralExpression.TRUE_EXPRESSION;
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        return new OrExpression(children);
    }

    @Override
    public Expression visitLeave(OrParseNode node, List<Expression> children) throws SQLException {
        return orExpression(children);
    }

    @Override
    public boolean visitEnter(FunctionParseNode node) throws SQLException {
        // TODO: Oracle supports nested aggregate function while other DBs don't. Should we?
        if (node.isAggregate()) {
            if (aggregateFunction != null) {
                throw new SQLFeatureNotSupportedException("Nested aggregate functions are not supported");
            }
            this.aggregateFunction = node;
            this.isAggregate = true;

        }
        return true;
    }

    private Expression wrapGroupByExpression(Expression expression) {
        // If we're in an aggregate function, don't wrap a group by expression,
        // since in that case we're aggregating over the regular/ungrouped
        // column.
        if (aggregateFunction == null) {
            int index = groupBy.getExpressions().indexOf(expression);
            if (index >= 0) {
                isAggregate = true;
                RowKeyValueAccessor accessor = new RowKeyValueAccessor(groupBy.getKeyExpressions(), index);
                expression = new RowKeyColumnExpression(expression, accessor, groupBy.getKeyExpressions().get(index).getDataType());
            }
        }
        return expression;
    }

    /**
     * Add a Function expression to the expression manager.
     * Derived classes may use this as a hook to trap all function additions.
     * @return a Function expression
     * @throws SQLException if the arguments are invalid for the function.
     */
    protected Expression addFunction(FunctionExpression func) {
        return context.getExpressionManager().addIfAbsent(func);
    }

    @Override
    /**
     * @param node a function expression node
     * @param children the child expression arguments to the function expression node.
     */
    public Expression visitLeave(FunctionParseNode node, List<Expression> children) throws SQLException {
        children = node.validate(children, context);
        FunctionExpression func = node.create(children, context);
        if (node.isConstant()) {
            ImmutableBytesWritable ptr = context.getTempPtr();
            Object value = null;
            PDataType type = func.getDataType();
            if (func.evaluate(null, ptr)) {
                value = type.toObject(ptr);
            }
            return LiteralExpression.newConstant(value, type);
        }
        BuiltInFunctionInfo info = node.getInfo();
        for (int i = 0; i < info.getRequiredArgCount(); i++) { 
            // Optimization to catch cases where a required argument is null resulting in the function
            // returning null. We have to wait until after we create the function expression so that
            // we can get the proper type to use.
            if (node.evalToNullIfParamIsNull(context, i)) {
                Expression child = children.get(i);
                if (child instanceof LiteralExpression
                        && ((LiteralExpression)child).getValue() == null) {
                    return LiteralExpression.newConstant(null, func.getDataType());
                }
            }
        }
        Expression expression = addFunction(func);
        expression = wrapGroupByExpression(expression);
        if (aggregateFunction == node) {
            aggregateFunction = null; // Turn back off on the way out
        }
        return expression;
    }

    /**
     * Called by visitor to resolve a column expression node into a column reference.
     * Derived classes may use this as a hook to trap all column resolves.
     * @param node a column expression node
     * @return a resolved ColumnRef
     * @throws SQLException if the column expression node does not refer to a known/unambiguous column
     */
    protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
        return context.getResolver().resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
    }

    @Override
    public Expression visit(ColumnParseNode node) throws SQLException {
        ColumnRef ref = resolveColumn(node);
        if (!SchemaUtil.isPKColumn(ref.getColumn())) { // project only kv columns
            context.getScan().addColumn(ref.getColumn().getFamilyName().getBytes(), ref.getColumn().getName().getBytes());
        }
        Expression expression = ref.newColumnExpression();
        Expression wrappedExpression = wrapGroupByExpression(expression);
        // If we're in an aggregate expression
        // and we're not in the context of an aggregate function
        // and we didn't just wrap our column reference
        // then we're mixing aggregate and non aggregate expressions in the same expression.
        // This catches cases like this: SELECT sum(a_integer) + a_integer FROM atable GROUP BY a_string
        if (isAggregate && aggregateFunction == null && wrappedExpression == expression) {
            throwNonAggExpressionInAggException(expression.toString());
        }
        return wrappedExpression;
    }

    @Override
    public Expression visit(BindParseNode node) throws SQLException {
        Object value = context.getBindManager().getBindValue(node);
        return LiteralExpression.newConstant(value);
    }    

    @Override
    public Expression visit(LiteralParseNode node) throws SQLException {
        return LiteralExpression.newConstant(node.getValue(), node.getType());
    }

    @Override
    public List<Expression> newElementList(int size) {
        nodeCount += size;
        return new ArrayList<Expression>(size);
    }

    @Override
    public void addElement(List<Expression> l, Expression element) {
        nodeCount--;
        l.add(element);
    }

    @Override
    public boolean visitEnter(CaseParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(CaseParseNode node, List<Expression> l) throws SQLException {
        final CaseExpression caseExpression = new CaseExpression(l);
        for (int i = 0; i < node.getChildren().size(); i+=2) {
            ParseNode childNode = node.getChildren().get(i);
            if (childNode instanceof BindParseNode) {
                context.getBindManager().addParamMetaData((BindParseNode)childNode, new DelegateDatum(caseExpression));
            }
        }
        if (node.isConstant()) {
            ImmutableBytesWritable ptr = context.getTempPtr();
            int index = caseExpression.evaluateIndexOf(null, ptr);
            if (index < 0) {
                return LiteralExpression.NULL_EXPRESSION;
            }
            return caseExpression.getChildren().get(index);
        }
        return wrapGroupByExpression(caseExpression);
    }

    @Override
    public boolean visitEnter(LikeParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(LikeParseNode node, List<Expression> children) throws SQLException {
        ParseNode lhsNode = node.getChildren().get(0);
        ParseNode rhsNode = node.getChildren().get(1);
        Expression lhs = children.get(0);
        Expression rhs = children.get(1);
        if ( rhs.getDataType() != null && lhs.getDataType() != null && 
                !lhs.getDataType().isCoercibleTo(rhs.getDataType())  && 
                !rhs.getDataType().isCoercibleTo(lhs.getDataType())) {
            throw new TypeMismatchException(lhs.getDataType(), rhs.getDataType(), node.toString());
        }
        if (lhsNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)lhsNode, rhs);
        }
        if (rhsNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)rhsNode, lhs);
        }
        if (rhs instanceof LiteralExpression) {
            String pattern = (String)((LiteralExpression)rhs).getValue();
            if (pattern == null || pattern.length() == 0) {
                return LiteralExpression.NULL_EXPRESSION;
            }
            // TODO: for pattern of '%' optimize to strlength(lhs) > 0
            // We can't use lhs IS NOT NULL b/c if lhs is NULL we need
            // to return NULL.
            int index = LikeExpression.indexOfWildcard(pattern);
            // Can't possibly be as long as the constant, then FALSE
            Integer lhsByteSize = lhs.getByteSize();
            if (lhsByteSize != null && lhsByteSize < index) {
                return LiteralExpression.FALSE_EXPRESSION;
            }
            if (index == -1) {
                String rhsLiteral = LikeExpression.unescapeLike(pattern);
                if (lhsByteSize != null && lhsByteSize != rhsLiteral.length()) {
                    return LiteralExpression.FALSE_EXPRESSION;
                }
                CompareOp op = node.isNegate() ? CompareOp.NOT_EQUAL : CompareOp.EQUAL;
                if (pattern.equals(rhsLiteral)) {
                    return new ComparisonExpression(op, children);
                } else {
                    rhs = LiteralExpression.newConstant(rhsLiteral, PDataType.CHAR);
                    return new ComparisonExpression(op, Arrays.asList(lhs,rhs));
                }
            }
        }
        Expression expression = new LikeExpression(children);
        if (node.isConstant()) {
            ImmutableBytesWritable ptr = context.getTempPtr();
            if (!expression.evaluate(null, ptr)) {
                return LiteralExpression.NULL_EXPRESSION;
            } else {
                return LiteralExpression.newConstant(Boolean.TRUE.equals(PDataType.BOOLEAN.toObject(ptr)) ^ node.isNegate());
            }
        }
        if (node.isNegate()) {
            expression = new NotExpression(expression);
        }
        return expression;
    }


    @Override
    public boolean visitEnter(NotParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(NotParseNode node, List<Expression> children) throws SQLException {
        ParseNode childNode = node.getChildren().get(0);
        Expression child = children.get(0);
        if (!PDataType.BOOLEAN.isCoercibleTo(child.getDataType())) {
            throw new TypeMismatchException(PDataType.BOOLEAN, child.getDataType(), node.toString());
        }
        if (childNode instanceof BindParseNode) { // TODO: valid/possibe?
            context.getBindManager().addParamMetaData((BindParseNode)childNode, child);
        }
        Boolean value = null;
        // Can't use child.isConstant(), because we have cases in which we don't know
        // in advance if a function evaluates to null (namely when bind variables are used)
        // TODO: review: have ParseNode.getValue()?
        if (child instanceof LiteralExpression) {
            value = (Boolean)((LiteralExpression)child).getValue();
            if (value == null) {
                return LiteralExpression.NULL_EXPRESSION;
            }
        }
        if (value != null) {
            return LiteralExpression.newConstant(!value);
        }
        return new NotExpression(child);
    }

    @Override
    public boolean visitEnter(CastParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(CastParseNode node, List<Expression> children) throws SQLException {
        final ParseNode childNode = node.getChildren().get(0);
        final Expression child = children.get(0);
        final PDataType dataType = child.getDataType();
        final PDataType targetDataType = node.getDataType();

        if (childNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)childNode, child);
        }
        if (dataType!= null && targetDataType != null && !dataType.isCoercibleTo(targetDataType)) {
            // TODO: remove soon. Allow cast for indexes, as we know what we're doing :-)
            if (context.getResolver().getTables().get(0).getTable().getType() != PTableType.INDEX) {
                throw new TypeMismatchException(dataType, targetDataType, child.toString());
            }
        }
        return wrapGroupByExpression(CoerceExpression.create(child, targetDataType)); 
    }

    @Override
    public boolean visitEnter(InListParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(InListParseNode node, List<Expression> l) throws SQLException {
        List<Expression> inChildren = l;
        Expression firstChild = inChildren.get(0);
        ImmutableBytesWritable ptr = context.getTempPtr();
        PDataType firstChildType = firstChild.getDataType();
        ParseNode firstChildNode = node.getChildren().get(0);
        
        if (firstChildNode instanceof BindParseNode) {
            PDatum datum = firstChild;
            if (firstChildType == null) {
                datum = inferBindDatum(inChildren);
            }
            context.getBindManager().addParamMetaData((BindParseNode)firstChildNode, datum);
        }
        for (int i = 1; i < l.size(); i++) {
            ParseNode childNode = node.getChildren().get(i);
            if (childNode instanceof BindParseNode) {
                context.getBindManager().addParamMetaData((BindParseNode)childNode, firstChild);
            }
        }
        if (firstChildNode.isConstant() && firstChild.evaluate(null, ptr) && ptr.getLength() == 0) {
            return LiteralExpression.newConstant(null, PDataType.BOOLEAN);
        }
        
        Expression e = InListExpression.create(inChildren, ptr);
        if (node.isNegate()) {
            e = new NotExpression(e);
        }
        if (node.isConstant()) {
            if (!e.evaluate(null, ptr) || ptr.getLength() == 0) {
                return LiteralExpression.newConstant(null, e.getDataType());
            }
            Object value = e.getDataType().toObject(ptr);
            return LiteralExpression.newConstant(value, e.getDataType());
        }
        
        return e;
    }

    private static final PDatum DECIMAL_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return true;
        }
        @Override
        public PDataType getDataType() {
            return PDataType.DECIMAL;
        }
        @Override
        public Integer getByteSize() {
            return null;
        }
        @Override
        public Integer getMaxLength() {
            return null;
        }
        @Override
        public Integer getScale() {
            return PDataType.DEFAULT_SCALE;
        }
        @Override
        public ColumnModifier getColumnModifier() {
            return null;
        }        
    };

    private static PDatum inferBindDatum(List<Expression> children) {
        boolean isChildTypeUnknown = false;
        PDatum datum = children.get(1);
        for (int i = 2; i < children.size(); i++) {
            Expression child = children.get(i);
            PDataType childType = child.getDataType();
            if (childType == null) {
                isChildTypeUnknown = true;
            } else if (datum.getDataType() == null) {
                datum = child;
                isChildTypeUnknown = true;
            } else if (datum.getDataType() == childType || childType.isCoercibleTo(datum.getDataType())) {
                continue;
            } else if (datum.getDataType().isCoercibleTo(childType)) {
                datum = child;
            }
        }
        // If we found an "unknown" child type and the return type is a number
        // make the return type be the most general number type of DECIMAL.
        // TODO: same for TIMESTAMP for DATE/TIME?
        if (isChildTypeUnknown && datum.getDataType() != null && datum.getDataType().isCoercibleTo(PDataType.DECIMAL)) {
            return DECIMAL_DATUM;
        }
        return datum;
    }

    @Override
    public boolean visitEnter(IsNullParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(IsNullParseNode node, List<Expression> children) throws SQLException {
        ParseNode childNode = node.getChildren().get(0);
        Expression child = children.get(0);
        if (childNode instanceof BindParseNode) { // TODO: valid/possibe?
            context.getBindManager().addParamMetaData((BindParseNode)childNode, child);
        }
        Boolean value = null;
        // Can't use child.isConstant(), because we have cases in which we don't know
        // in advance if a function evaluates to null (namely when bind variables are used)
        // TODO: review: have ParseNode.getValue()?
        if (child instanceof LiteralExpression) {
            value = (Boolean)((LiteralExpression)child).getValue();
            return node.isNegate() ^ value == null ? LiteralExpression.FALSE_EXPRESSION : LiteralExpression.TRUE_EXPRESSION;
        }
        if (!child.isNullable()) { // If child expression is not nullable, we can rewrite this
            return node.isNegate() ? LiteralExpression.TRUE_EXPRESSION : LiteralExpression.FALSE_EXPRESSION;
        }
        return new IsNullExpression(child, node.isNegate());
    }

    private static interface ArithmeticExpressionFactory {
        Expression create(ArithmeticParseNode node, List<Expression> children) throws SQLException;
    }

    private static interface ArithmeticExpressionBinder {
        PDatum getBindMetaData(int i, List<Expression> children, Expression expression);
    }

    private Expression visitLeave(ArithmeticParseNode node, List<Expression> children, ArithmeticExpressionBinder binder, ArithmeticExpressionFactory factory)
            throws SQLException {

        boolean isLiteral = true;
        boolean isNull = false;
        for (Expression child : children) {
            boolean isChildLiteral = (child instanceof LiteralExpression);
            isNull |= isChildLiteral && ((LiteralExpression)child).getValue() == null;
            isLiteral &= isChildLiteral;
        }

        Expression expression = factory.create(node, children);

        for (int i = 0; i < node.getChildren().size(); i++) {
            ParseNode childNode = node.getChildren().get(i);
            if (childNode instanceof BindParseNode) {
                context.getBindManager().addParamMetaData((BindParseNode)childNode, binder == null ? expression : binder.getBindMetaData(i, children, expression));
            }
        }

        ImmutableBytesWritable ptr = context.getTempPtr();

        // If all children are literals, just evaluate now
        if (isLiteral) {
            // We can use null here because we don't need any row/tuple data when evaluating literals
            expression.evaluate(null,ptr);
            return LiteralExpression. newConstant(expression.getDataType().toObject(ptr), expression.getDataType());
        } else if (isNull) {
            return LiteralExpression. newConstant(null, expression.getDataType());
        }
        // Otherwise create and return the expression
        return wrapGroupByExpression(expression);
    }

    @Override
    public boolean visitEnter(SubtractParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(SubtractParseNode node,
            List<Expression> children) throws SQLException {
        return visitLeave(node, children, new ArithmeticExpressionBinder() {
            @Override
            public PDatum getBindMetaData(int i, List<Expression> children,
                    final Expression expression) {
                final PDataType type;
                // If we're binding the first parameter and the second parameter
                // is a date
                // we know that the first parameter must be a date type too.
                if (i == 0 && (type = children.get(1).getDataType()) != null
                        && type.isCoercibleTo(PDataType.DATE)) {
                    return new PDatum() {
                        @Override
                        public boolean isNullable() {
                            return expression.isNullable();
                        }
                        @Override
                        public PDataType getDataType() {
                            return type;
                        }
                        @Override
                        public Integer getByteSize() {
                            return type.getByteSize();
                        }
                        @Override
                        public Integer getMaxLength() {
                            return expression.getMaxLength();
                        }
                        @Override
                        public Integer getScale() {
                            return expression.getScale();
                        }
                        @Override
                        public ColumnModifier getColumnModifier() {
                            return expression.getColumnModifier();
                        }                        
                    };
                } else if (expression.getDataType() != null
                        && expression.getDataType().isCoercibleTo(
                                PDataType.DATE)) {
                    return new PDatum() { // Same as with addition
                        @Override
                        public boolean isNullable() {
                            return expression.isNullable();
                        }
                        @Override
                        public PDataType getDataType() {
                            return PDataType.DECIMAL;
                        }
                        @Override
                        public Integer getByteSize() {
                            return null;
                        }
                        @Override
                        public Integer getMaxLength() {
                            return expression.getMaxLength();
                        }
                        @Override
                        public Integer getScale() {
                            return expression.getScale();
                        }
                        @Override
                        public ColumnModifier getColumnModifier() {
                            return expression.getColumnModifier();
                        }
                    };
                }
                // Otherwise just go with what was calculated for the expression
                return expression;
            }
        }, new ArithmeticExpressionFactory() {
            @Override
            public Expression create(ArithmeticParseNode node,
                    List<Expression> children) throws SQLException {
                int i = 0;
                PDataType theType = null;
                PDataType type1 = children.get(0).getDataType();
                PDataType type2 = children.get(1).getDataType();
                // TODO: simplify this special case for DATE conversion
                /**
                 * For date1-date2, we want to coerce to a LONG because this
                 * cannot be compared against another date. It has essentially
                 * become a number. For date1-5, we want to preserve the DATE
                 * type because this can still be compared against another date
                 * and cannot be multiplied or divided. Any other time occurs is
                 * an error. For example, 5-date1 is an error. The nulls occur if
                 * we have bind variables.
                 */
                boolean isType1Date = 
                        type1 != null 
                        && type1 != PDataType.TIMESTAMP
                        && type1.isCoercibleTo(PDataType.DATE);
                boolean isType2Date = 
                        type2 != null
                        && type2 != PDataType.TIMESTAMP
                        && type2.isCoercibleTo(PDataType.DATE);
                if (isType1Date || isType2Date) {
                    if (isType1Date && isType2Date) {
                        i = 2;
                        theType = PDataType.LONG;
                    } else if (isType1Date && type2 != null
                            && type2.isCoercibleTo(PDataType.DECIMAL)) {
                        i = 2;
                        theType = PDataType.DATE;
                    } else if (type1 == null || type2 == null) {
                        /*
                         * FIXME: Could be either a Date or BigDecimal, but we
                         * don't know if we're comparing to a date or a number
                         * which would be disambiguate it.
                         */
                        i = 2;
                        theType = null;
                    }
                } else if(type1 == PDataType.TIMESTAMP || type2 == PDataType.TIMESTAMP) {
                    i = 2;
                    theType = PDataType.TIMESTAMP;
                }
                
                for (; i < children.size(); i++) {
                    // This logic finds the common type to which all child types are coercible
                    // without losing precision.
                    PDataType type = children.get(i).getDataType();
                    if (type == null) {
                        continue;
                    } else if (type.isCoercibleTo(PDataType.LONG)) {
                        if (theType == null) {
                            theType = PDataType.LONG;
                        }
                    } else if (type == PDataType.DECIMAL) {
                        // Coerce return type to DECIMAL from LONG or DOUBLE if DECIMAL child found,
                        // unless we're doing date arithmetic.
                        if (theType == null
                                || !theType.isCoercibleTo(PDataType.DATE)) {
                            theType = PDataType.DECIMAL;
                        }
                    } else if (type.isCoercibleTo(PDataType.DOUBLE)) {
                        // Coerce return type to DOUBLE from LONG if DOUBLE child found,
                        // unless we're doing date arithmetic or we've found another child of type DECIMAL
                        if (theType == null
                                || (theType != PDataType.DECIMAL && !theType.isCoercibleTo(PDataType.DATE) )) {
                            theType = PDataType.DOUBLE;
                        }
                    } else {
                        throw new TypeMismatchException(type, node.toString());
                    }
                }
                if (theType == PDataType.DECIMAL) {
                    return new DecimalSubtractExpression(children);
                } else if (theType == PDataType.LONG) {
                    return new LongSubtractExpression(children);
                } else if (theType == PDataType.DOUBLE) {
                    return new DoubleSubtractExpression(children);
                } else if (theType == null) {
                    return LiteralExpression.newConstant(null, theType);
                } else if (theType == PDataType.TIMESTAMP) {
                    return new TimestampSubtractExpression(children);
                } else if (theType.isCoercibleTo(PDataType.DATE)) {
                    return new DateSubtractExpression(children);
                } else {
                    throw new TypeMismatchException(theType, node.toString());
                }
            }
        });
    }

    @Override
    public boolean visitEnter(AddParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(AddParseNode node, List<Expression> children) throws SQLException {
        return visitLeave(node, children,
                new ArithmeticExpressionBinder() {
            @Override
            public PDatum getBindMetaData(int i, List<Expression> children, final Expression expression) {
                PDataType type = expression.getDataType();
                if (type != null && type.isCoercibleTo(PDataType.DATE)) {
                    return new PDatum() {
                        @Override
                        public boolean isNullable() {
                            return expression.isNullable();
                        }
                        @Override
                        public PDataType getDataType() {
                            return PDataType.DECIMAL;
                        }
                        @Override
                        public Integer getByteSize() {
                            return null;
                        }
                        @Override
                        public Integer getMaxLength() {
                            return expression.getMaxLength();
                        }
                        @Override
                        public Integer getScale() {
                            return expression.getScale();
                        }
                        @Override
                        public ColumnModifier getColumnModifier() {
                            return expression.getColumnModifier();
                        }
                    };
                }
                return expression;
            }
        },
        new ArithmeticExpressionFactory() {
            @Override
            public Expression create(ArithmeticParseNode node, List<Expression> children) throws SQLException {
                boolean foundDate = false;
                PDataType theType = null;
                for(int i = 0; i < children.size(); i++) {
                    PDataType type = children.get(i).getDataType();
                    if (type == null) {
                        continue; 
                    } else if (type.isCoercibleTo(PDataType.TIMESTAMP)) {
                        if (foundDate) {
                            throw new TypeMismatchException(type, node.toString());
                        }
                        if (theType == null || theType != PDataType.TIMESTAMP) {
                            theType = type;
                        }
                        foundDate = true;
                    }else if (type == PDataType.DECIMAL) {
                        if (theType == null || !theType.isCoercibleTo(PDataType.TIMESTAMP)) {
                            theType = PDataType.DECIMAL;
                        }
                    } else if (type.isCoercibleTo(PDataType.LONG)) {
                        if (theType == null) {
                            theType = PDataType.LONG;
                        }
                    } else if (type.isCoercibleTo(PDataType.DOUBLE)) {
                        if (theType == null) {
                            theType = PDataType.DOUBLE;
                        }
                    } else {
                        throw new TypeMismatchException(type, node.toString());
                    }
                }
                if (theType == PDataType.DECIMAL) {
                    return new DecimalAddExpression(children);
                } else if (theType == PDataType.LONG) {
                    return new LongAddExpression(children);
                } else if (theType == PDataType.DOUBLE) {
                    return new DoubleAddExpression(children);
                } else if (theType == null) {
                    return LiteralExpression.newConstant(null, theType);
                } else if (theType == PDataType.TIMESTAMP) {
                    return new TimestampAddExpression(children);
                } else if (theType.isCoercibleTo(PDataType.DATE)) {
                    return new DateAddExpression(children);
                } else {
                    throw new TypeMismatchException(theType, node.toString());
                }
            }
        });
    }

    @Override
    public boolean visitEnter(MultiplyParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(MultiplyParseNode node, List<Expression> children) throws SQLException {
        return visitLeave(node, children, null, new ArithmeticExpressionFactory() {
            @Override
            public Expression create(ArithmeticParseNode node, List<Expression> children) throws SQLException {
                PDataType theType = null;
                for(int i = 0; i < children.size(); i++) {
                    PDataType type = children.get(i).getDataType();
                    if (type == null) {
                        continue;
                    } else if (type == PDataType.DECIMAL) {
                        theType = PDataType.DECIMAL;
                    } else if (type.isCoercibleTo(PDataType.LONG)) {
                        if (theType == null) {
                            theType = PDataType.LONG;
                        }
                    } else if (type.isCoercibleTo(PDataType.DOUBLE)) {
                        if (theType == null) {
                            theType = PDataType.DOUBLE;
                        }
                    } else {
                        throw new TypeMismatchException(type, node.toString());
                    }
                }
                switch (theType) {
                case DECIMAL:
                    return new DecimalMultiplyExpression( children);
                case LONG:
                    return new LongMultiplyExpression( children);
                case DOUBLE:
                    return new DoubleMultiplyExpression( children);
                default:
                    return LiteralExpression.newConstant(null, theType);
                }
            }
        });
    }

    @Override
    public boolean visitEnter(DivideParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(DivideParseNode node, List<Expression> children) throws SQLException {
        for (int i = 1; i < children.size(); i++) { // Compile time check for divide by zero and null
            Expression child = children.get(i);
                if (child.getDataType() != null && child instanceof LiteralExpression) {
                    LiteralExpression literal = (LiteralExpression)child;
                    if (literal.getDataType() == PDataType.DECIMAL) {
                        if (PDataType.DECIMAL.compareTo(literal.getValue(), BigDecimal.ZERO) == 0) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.DIVIDE_BY_ZERO).build().buildException();
                        }
                    } else {
                        if (literal.getDataType().compareTo(literal.getValue(), 0L, PDataType.LONG) == 0) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.DIVIDE_BY_ZERO).build().buildException();
                        }
                    }
                }
        }
        return visitLeave(node, children, null, new ArithmeticExpressionFactory() {
            @Override
            public Expression create(ArithmeticParseNode node, List<Expression> children) throws SQLException {
                PDataType theType = null;
                for(int i = 0; i < children.size(); i++) {
                    PDataType type = children.get(i).getDataType();
                    if (type == null) {
                        continue;
                    } else if (type == PDataType.DECIMAL) {
                        theType = PDataType.DECIMAL;
                    } else if (type.isCoercibleTo(PDataType.LONG)) {
                        if (theType == null) {
                            theType = PDataType.LONG;
                        }
                    } else if (type.isCoercibleTo(PDataType.DOUBLE)) {
                        if (theType == null) {
                            theType = PDataType.DOUBLE;
                        }
                    } else {
                        throw new TypeMismatchException(type, node.toString());
                    }
                }
                switch (theType) {
                case DECIMAL:
                    return new DecimalDivideExpression( children);
                case LONG:
                    return new LongDivideExpression( children);
                case DOUBLE:
                    return new DoubleDivideExpression(children);
                default:
                    return LiteralExpression.newConstant(null, theType);
                }
            }
        });
    }

    public static void throwNonAggExpressionInAggException(String nonAggregateExpression) throws SQLException {
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.AGGREGATE_WITH_NOT_GROUP_BY_COLUMN)
        .setMessage(nonAggregateExpression).build().buildException();
    }

    @Override
    public Expression visitLeave(StringConcatParseNode node, List<Expression> children) throws SQLException {
        final StringConcatExpression expression=new StringConcatExpression(children);
        for (int i = 0; i < children.size(); i++) {
            ParseNode childNode=node.getChildren().get(i);
            if(childNode instanceof BindParseNode) {
                context.getBindManager().addParamMetaData((BindParseNode)childNode,expression);
            }
            PDataType type=children.get(i).getDataType();
            if(type==PDataType.VARBINARY){
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TYPE_NOT_SUPPORTED_FOR_OPERATOR)
                .setMessage("Concatenation does not support "+ type +" in expression" + node).build().buildException();
            }
        }
        boolean isLiteralExpression = true;
        for (Expression child:children) {
            isLiteralExpression&=(child instanceof LiteralExpression);
        }
        ImmutableBytesWritable ptr = context.getTempPtr();
        if (isLiteralExpression) {
            expression.evaluate(null,ptr);
            return LiteralExpression.newConstant(expression.getDataType().toObject(ptr));
        }
        return wrapGroupByExpression(expression);
    }

    @Override
    public boolean visitEnter(StringConcatParseNode node) throws SQLException {
        return true;
    }

    @Override
    public boolean visitEnter(RowValueConstructorParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(RowValueConstructorParseNode node, List<Expression> l) throws SQLException {
        // Don't trim trailing nulls here, as we'd potentially be dropping bind
        // variables that aren't bound yet.
        return new RowValueConstructorExpression(l, node.isConstant());
    }
}