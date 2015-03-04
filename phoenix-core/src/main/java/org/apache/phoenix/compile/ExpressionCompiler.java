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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ArrayConstructorExpression;
import org.apache.phoenix.expression.CaseExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.DateAddExpression;
import org.apache.phoenix.expression.DateSubtractExpression;
import org.apache.phoenix.expression.DecimalAddExpression;
import org.apache.phoenix.expression.DecimalDivideExpression;
import org.apache.phoenix.expression.DecimalMultiplyExpression;
import org.apache.phoenix.expression.DecimalSubtractExpression;
import org.apache.phoenix.expression.Determinism;
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
import org.apache.phoenix.expression.ModulusExpression;
import org.apache.phoenix.expression.NotExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.StringConcatExpression;
import org.apache.phoenix.expression.TimestampAddExpression;
import org.apache.phoenix.expression.TimestampSubtractExpression;
import org.apache.phoenix.expression.function.ArrayAllComparisonExpression;
import org.apache.phoenix.expression.function.ArrayAnyComparisonExpression;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.expression.function.RoundDecimalExpression;
import org.apache.phoenix.expression.function.RoundTimestampExpression;
import org.apache.phoenix.parse.AddParseNode;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.ArithmeticParseNode;
import org.apache.phoenix.parse.ArrayAllComparisonNode;
import org.apache.phoenix.parse.ArrayAnyComparisonNode;
import org.apache.phoenix.parse.ArrayConstructorNode;
import org.apache.phoenix.parse.ArrayElemRefNode;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.parse.CaseParseNode;
import org.apache.phoenix.parse.CastParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.DivideParseNode;
import org.apache.phoenix.parse.ExistsParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunctionInfo;
import org.apache.phoenix.parse.InListParseNode;
import org.apache.phoenix.parse.IsNullParseNode;
import org.apache.phoenix.parse.LikeParseNode;
import org.apache.phoenix.parse.LikeParseNode.LikeType;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ModulusParseNode;
import org.apache.phoenix.parse.MultiplyParseNode;
import org.apache.phoenix.parse.NotParseNode;
import org.apache.phoenix.parse.OrParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.RowValueConstructorParseNode;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.parse.StringConcatParseNode;
import org.apache.phoenix.parse.SubqueryParseNode;
import org.apache.phoenix.parse.SubtractParseNode;
import org.apache.phoenix.parse.UnsupportedAllParseNodeVisitor;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.DelegateDatum;
import org.apache.phoenix.schema.LocalIndexDataColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PUnsignedTimestamp;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.ExpressionUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;


public class ExpressionCompiler extends UnsupportedAllParseNodeVisitor<Expression> {
    private boolean isAggregate;
    protected ParseNode aggregateFunction;
    protected final StatementContext context;
    protected final GroupBy groupBy;
    private int nodeCount;
    private int totalNodeCount;
    private final boolean resolveViewConstants;

    public ExpressionCompiler(StatementContext context) {
        this(context,GroupBy.EMPTY_GROUP_BY, false);
    }

    ExpressionCompiler(StatementContext context, boolean resolveViewConstants) {
        this(context,GroupBy.EMPTY_GROUP_BY, resolveViewConstants);
    }

    ExpressionCompiler(StatementContext context, GroupBy groupBy) {
        this(context, groupBy, false);
    }

    ExpressionCompiler(StatementContext context, GroupBy groupBy, boolean resolveViewConstants) {
        this.context = context;
        this.groupBy = groupBy;
        this.resolveViewConstants = resolveViewConstants;
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
        this.totalNodeCount = 0;
    }

    @Override
    public boolean visitEnter(ComparisonParseNode node) {
        return true;
    }

    private void addBindParamMetaData(ParseNode lhsNode, ParseNode rhsNode, Expression lhsExpr, Expression rhsExpr) throws SQLException {
        if (lhsNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)lhsNode, rhsExpr);
        }
        if (rhsNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)rhsNode, lhsExpr);
        }
    }

    @Override
    public Expression visitLeave(ComparisonParseNode node, List<Expression> children) throws SQLException {
        ParseNode lhsNode = node.getChildren().get(0);
        ParseNode rhsNode = node.getChildren().get(1);
        Expression lhsExpr = children.get(0);
        Expression rhsExpr = children.get(1);
        CompareOp op = node.getFilterOp();

        if (lhsNode instanceof RowValueConstructorParseNode && rhsNode instanceof RowValueConstructorParseNode) {
            int i = 0;
            for (; i < Math.min(lhsExpr.getChildren().size(),rhsExpr.getChildren().size()); i++) {
                addBindParamMetaData(lhsNode.getChildren().get(i), rhsNode.getChildren().get(i), lhsExpr.getChildren().get(i), rhsExpr.getChildren().get(i));
            }
            for (; i < lhsExpr.getChildren().size(); i++) {
                addBindParamMetaData(lhsNode.getChildren().get(i), null, lhsExpr.getChildren().get(i), null);
            }
            for (; i < rhsExpr.getChildren().size(); i++) {
                addBindParamMetaData(null, rhsNode.getChildren().get(i), null, rhsExpr.getChildren().get(i));
            }
        } else if (lhsExpr instanceof RowValueConstructorExpression) {
            addBindParamMetaData(lhsNode.getChildren().get(0), rhsNode, lhsExpr.getChildren().get(0), rhsExpr);
            for (int i = 1; i < lhsExpr.getChildren().size(); i++) {
                addBindParamMetaData(lhsNode.getChildren().get(i), null, lhsExpr.getChildren().get(i), null);
            }
        } else if (rhsExpr instanceof RowValueConstructorExpression) {
            addBindParamMetaData(lhsNode, rhsNode.getChildren().get(0), lhsExpr, rhsExpr.getChildren().get(0));
            for (int i = 1; i < rhsExpr.getChildren().size(); i++) {
                addBindParamMetaData(null, rhsNode.getChildren().get(i), null, rhsExpr.getChildren().get(i));
            }
        } else {
            addBindParamMetaData(lhsNode, rhsNode, lhsExpr, rhsExpr);
        }
        return wrapGroupByExpression(ComparisonExpression.create(op, children, context.getTempPtr()));
    }

    @Override
    public boolean visitEnter(AndParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(AndParseNode node, List<Expression> children) throws SQLException {
        return wrapGroupByExpression(AndExpression.create(children));
    }

    @Override
    public boolean visitEnter(OrParseNode node) throws SQLException {
        return true;
    }

    private Expression orExpression(List<Expression> children) throws SQLException {
        Iterator<Expression> iterator = children.iterator();
        Determinism determinism = Determinism.ALWAYS;
        while (iterator.hasNext()) {
            Expression child = iterator.next();
            if (child.getDataType() != PBoolean.INSTANCE) {
                throw TypeMismatchException.newException(PBoolean.INSTANCE, child.getDataType(), child.toString());
            }
            if (LiteralExpression.isFalse(child)) {
                iterator.remove();
            }
            if (LiteralExpression.isTrue(child)) {
                return child;
            }
            determinism = determinism.combine(child.getDeterminism());
        }
        if (children.size() == 0) {
            return LiteralExpression.newConstant(true, determinism);
        }
        if (children.size() == 1) {
            return children.get(0);
        }
        return new OrExpression(children);
    }

    @Override
    public Expression visitLeave(OrParseNode node, List<Expression> children) throws SQLException {
        return wrapGroupByExpression(orExpression(children));
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
     * Add expression to the expression manager, returning the same one if
     * already used.
     */
    protected Expression addExpression(Expression expression) {
        return context.getExpressionManager().addIfAbsent(expression);
    }

    @Override
    /**
     * @param node a function expression node
     * @param children the child expression arguments to the function expression node.
     */
    public Expression visitLeave(FunctionParseNode node, List<Expression> children) throws SQLException {
        children = node.validate(children, context);
        Expression expression = node.create(children, context);
        ImmutableBytesWritable ptr = context.getTempPtr();
        if (ExpressionUtil.isConstant(expression)) {
            return ExpressionUtil.getConstantExpression(expression, ptr);
        }
        BuiltInFunctionInfo info = node.getInfo();
        for (int i = 0; i < info.getRequiredArgCount(); i++) { 
            // Optimization to catch cases where a required argument is null resulting in the function
            // returning null. We have to wait until after we create the function expression so that
            // we can get the proper type to use.
            if (node.evalToNullIfParamIsNull(context, i)) {
                Expression child = children.get(i);
                if (ExpressionUtil.isNull(child, ptr)) {
                    return ExpressionUtil.getNullExpression(expression);
                }
            }
        }
        expression = addExpression(expression);
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
        ColumnRef ref = null;
        try {
            ref = context.getResolver().resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
        } catch (ColumnNotFoundException e) {
            // Rather than not use a local index when a column not contained by it is referenced, we
            // join back to the data table in our coprocessor since this is a relatively cheap
            // operation given that we know the join is local.
            if (context.getCurrentTable().getTable().getIndexType() == IndexType.LOCAL) {
                try {
                    return new LocalIndexDataColumnRef(context, node.getName());
                } catch (ColumnFamilyNotFoundException c) {
                    throw e;
                }
            } else {
                throw e;
            }
        }
        PTable table = ref.getTable();
        int pkPosition = ref.getPKSlotPosition();
        // Disallow explicit reference to salting column, tenant ID column, and index ID column
        if (pkPosition >= 0) {
            boolean isSalted = table.getBucketNum() != null;
            boolean isMultiTenant = context.getConnection().getTenantId() != null && table.isMultiTenant();
            boolean isSharedViewIndex = table.getViewIndexId() != null;
            int minPosition = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isSharedViewIndex ? 1 : 0);
            if (pkPosition < minPosition) {
                throw new ColumnNotFoundException(table.getSchemaName().getString(), table.getTableName().getString(), null, ref.getColumn().getName().getString());
            }
        }
        return ref;
    }

    @Override
    public Expression visit(ColumnParseNode node) throws SQLException {
        ColumnRef ref = resolveColumn(node);
        TableRef tableRef = ref.getTableRef();
        ImmutableBytesWritable ptr = context.getTempPtr();
        PColumn column = ref.getColumn();
        // If we have an UPDATABLE view, then we compile those view constants (i.e. columns in equality constraints
        // in the view) to constants. This allows the optimize to optimize out reference to them in various scenarios.
        // If the column is matched in a WHERE clause against a constant not equal to it's constant, then the entire
        // query would become degenerate.
        if (!resolveViewConstants && IndexUtil.getViewConstantValue(column, ptr)) {
            return LiteralExpression.newConstant(column.getDataType().toObject(ptr), column.getDataType());
        }
        if (tableRef.equals(context.getCurrentTable()) && !SchemaUtil.isPKColumn(column)) { // project only kv columns
            context.getScan().addColumn(column.getFamilyName().getBytes(), column.getName().getBytes());
        }
        Expression expression = ref.newColumnExpression(node.isTableNameCaseSensitive(), node.isCaseSensitive());
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
        return LiteralExpression.newConstant(value, Determinism.ALWAYS);
    }

    @Override
    public Expression visit(LiteralParseNode node) throws SQLException {
        return LiteralExpression.newConstant(node.getValue(), node.getType(), Determinism.ALWAYS);
    }

    @Override
    public List<Expression> newElementList(int size) {
        nodeCount += size;
        return new ArrayList<Expression>(size);
    }

    @Override
    public void addElement(List<Expression> l, Expression element) {
        nodeCount--;
        totalNodeCount++;
        l.add(element);
    }

    @Override
    public boolean visitEnter(CaseParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(CaseParseNode node, List<Expression> l) throws SQLException {
        final Expression caseExpression = CaseExpression.create(l);
        for (int i = 0; i < node.getChildren().size(); i+=2) {
            ParseNode childNode = node.getChildren().get(i);
            if (childNode instanceof BindParseNode) {
                context.getBindManager().addParamMetaData((BindParseNode)childNode, new DelegateDatum(caseExpression));
            }
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
            throw TypeMismatchException.newException(lhs.getDataType(), rhs.getDataType(), node.toString());
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
                return LiteralExpression.newConstant(null, rhs.getDeterminism());
            }
            // TODO: for pattern of '%' optimize to strlength(lhs) > 0
            // We can't use lhs IS NOT NULL b/c if lhs is NULL we need
            // to return NULL.
            int index = LikeExpression.indexOfWildcard(pattern);
            // Can't possibly be as long as the constant, then FALSE
            Integer lhsMaxLength = lhs.getMaxLength();
            if (lhsMaxLength != null && lhsMaxLength < index) {
                return LiteralExpression.newConstant(false, rhs.getDeterminism());
            }
            if (index == -1) {
                String rhsLiteral = LikeExpression.unescapeLike(pattern);
                if (lhsMaxLength != null && lhsMaxLength != rhsLiteral.length()) {
                    return LiteralExpression.newConstant(false, rhs.getDeterminism());
                }
                if (node.getLikeType() == LikeType.CASE_SENSITIVE) {
                  CompareOp op = node.isNegate() ? CompareOp.NOT_EQUAL : CompareOp.EQUAL;
                  if (pattern.equals(rhsLiteral)) {
                      return new ComparisonExpression(children, op);
                  } else {
                      rhs = LiteralExpression.newConstant(rhsLiteral, PChar.INSTANCE, rhs.getDeterminism());
                      return new ComparisonExpression(Arrays.asList(lhs,rhs), op);
                  }
                }
            }
        }
        Expression expression = LikeExpression.create(children, node.getLikeType());
        if (ExpressionUtil.isConstant(expression)) {
            ImmutableBytesWritable ptr = context.getTempPtr();
            if (!expression.evaluate(null, ptr)) {
                return LiteralExpression.newConstant(null, expression.getDeterminism());
            } else {
                return LiteralExpression.newConstant(Boolean.TRUE.equals(PBoolean.INSTANCE.toObject(ptr)) ^ node.isNegate(), expression.getDeterminism());
            }
        }
        if (node.isNegate()) {
            expression = new NotExpression(expression);
        }
        return wrapGroupByExpression(expression);
    }


    @Override
    public boolean visitEnter(NotParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(NotParseNode node, List<Expression> children) throws SQLException {
        ParseNode childNode = node.getChildren().get(0);
        Expression child = children.get(0);
        if (!PBoolean.INSTANCE.isCoercibleTo(child.getDataType())) {
            throw TypeMismatchException.newException(PBoolean.INSTANCE, child.getDataType(), node.toString());
        }
        if (childNode instanceof BindParseNode) { // TODO: valid/possibe?
            context.getBindManager().addParamMetaData((BindParseNode)childNode, child);
        }
        return wrapGroupByExpression(NotExpression.create(child, context.getTempPtr()));
    }

    @Override
    public boolean visitEnter(CastParseNode node) throws SQLException {
        return true;
    }

    // TODO: don't repeat this ugly cast logic (maybe use isCastable in the last else block.
    private static Expression convertToRoundExpressionIfNeeded(PDataType fromDataType, PDataType targetDataType, List<Expression> expressions) throws SQLException {
        Expression firstChildExpr = expressions.get(0);
        if(fromDataType == targetDataType) {
            return firstChildExpr;
        } else if((fromDataType == PDecimal.INSTANCE || fromDataType == PTimestamp.INSTANCE || fromDataType == PUnsignedTimestamp.INSTANCE) && targetDataType.isCoercibleTo(
          PLong.INSTANCE)) {
            return RoundDecimalExpression.create(expressions);
        } else if((fromDataType == PDecimal.INSTANCE || fromDataType == PTimestamp.INSTANCE || fromDataType == PUnsignedTimestamp.INSTANCE) && targetDataType.isCoercibleTo(
          PDate.INSTANCE)) {
            return RoundTimestampExpression.create(expressions);
        } else if(fromDataType.isCastableTo(targetDataType)) {
            return firstChildExpr;
        } else {
            throw TypeMismatchException.newException(fromDataType, targetDataType, firstChildExpr.toString());
        }
    }

    @Override
    public Expression visitLeave(CastParseNode node, List<Expression> children) throws SQLException {
        ParseNode childNode = node.getChildren().get(0);
        PDataType targetDataType = node.getDataType();
        Expression childExpr = children.get(0);
        PDataType fromDataType = childExpr.getDataType();
        
        if (childNode instanceof BindParseNode) {
            context.getBindManager().addParamMetaData((BindParseNode)childNode, childExpr);
        }
        
        Expression expr = childExpr;
        if(fromDataType != null) {
            /*
             * IndexStatementRewriter creates a CAST parse node when rewriting the query to use
             * indexed columns. Without this check present we wrongly and unnecessarily
             * end up creating a RoundExpression. 
             */
            if (context.getCurrentTable().getTable().getType() != PTableType.INDEX) {
                expr =  convertToRoundExpressionIfNeeded(fromDataType, targetDataType, children);
            }
        }
        return wrapGroupByExpression(CoerceExpression.create(expr, targetDataType, SortOrder.getDefault(), expr.getMaxLength()));  
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
        return wrapGroupByExpression(InListExpression.create(inChildren, node.isNegate(), ptr));
    }

    private static final PDatum DECIMAL_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return true;
        }
        @Override
        public PDataType getDataType() {
            return PDecimal.INSTANCE;
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
        if (isChildTypeUnknown && datum.getDataType() != null && datum.getDataType().isCoercibleTo(
            PDecimal.INSTANCE)) {
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
        return wrapGroupByExpression(IsNullExpression.create(child, node.isNegate(), context.getTempPtr()));
    }

    private static interface ArithmeticExpressionFactory {
        Expression create(ArithmeticParseNode node, List<Expression> children) throws SQLException;
    }

    private static interface ArithmeticExpressionBinder {
        PDatum getBindMetaData(int i, List<Expression> children, Expression expression);
    }

    private Expression visitLeave(ArithmeticParseNode node, List<Expression> children, ArithmeticExpressionBinder binder, ArithmeticExpressionFactory factory)
            throws SQLException {

        boolean isNull = false;
        for (Expression child : children) {
            boolean isChildLiteral = (child instanceof LiteralExpression);
            isNull |= isChildLiteral && ((LiteralExpression)child).getValue() == null;
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
        if (ExpressionUtil.isConstant(expression)) {
            return ExpressionUtil.getConstantExpression(expression, ptr); 
        } 
        else if (isNull) {
            return LiteralExpression.newConstant(null, expression.getDataType(), expression.getDeterminism());
        }
        // Otherwise create and return the expression
        return wrapGroupByExpression(expression);
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
                if (type != null && type.isCoercibleTo(PDate.INSTANCE)) {
                    return new PDatum() {
                        @Override
                        public boolean isNullable() {
                            return expression.isNullable();
                        }
                        @Override
                        public PDataType getDataType() {
                            return PDecimal.INSTANCE;
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
                        public SortOrder getSortOrder() {
                            return expression.getSortOrder();
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
                Determinism determinism = Determinism.ALWAYS;
                PDataType theType = null;
                for(int i = 0; i < children.size(); i++) {
                    Expression e = children.get(i);
                    determinism = determinism.combine(e.getDeterminism());
                    PDataType type = e.getDataType();
                    if (type == null) {
                        continue; 
                    } else if (type.isCoercibleTo(PTimestamp.INSTANCE)) {
                        if (foundDate) {
                            throw TypeMismatchException.newException(type, node.toString());
                        }
                        if (theType == null || (theType != PTimestamp.INSTANCE && theType != PUnsignedTimestamp.INSTANCE)) {
                            theType = type;
                        }
                        foundDate = true;
                    }else if (type == PDecimal.INSTANCE) {
                        if (theType == null || !theType.isCoercibleTo(PTimestamp.INSTANCE)) {
                            theType = PDecimal.INSTANCE;
                        }
                    } else if (type.isCoercibleTo(PLong.INSTANCE)) {
                        if (theType == null) {
                            theType = PLong.INSTANCE;
                        }
                    } else if (type.isCoercibleTo(PDouble.INSTANCE)) {
                        if (theType == null) {
                            theType = PDouble.INSTANCE;
                        }
                    } else {
                        throw TypeMismatchException.newException(type, node.toString());
                    }
                }
                if (theType == PDecimal.INSTANCE) {
                    return new DecimalAddExpression(children);
                } else if (theType == PLong.INSTANCE) {
                    return new LongAddExpression(children);
                } else if (theType == PDouble.INSTANCE) {
                    return new DoubleAddExpression(children);
                } else if (theType == null) {
                	return LiteralExpression.newConstant(null, theType, determinism);
                } else if (theType == PTimestamp.INSTANCE || theType == PUnsignedTimestamp.INSTANCE) {
                    return new TimestampAddExpression(children);
                } else if (theType.isCoercibleTo(PDate.INSTANCE)) {
                    return new DateAddExpression(children);
                } else {
                    throw TypeMismatchException.newException(theType, node.toString());
                }
            }
        });
    }

    @Override
    public boolean visitEnter(SubtractParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(SubtractParseNode node, List<Expression> children) throws SQLException {
        return visitLeave(node, children, new ArithmeticExpressionBinder() {
            @Override
            public PDatum getBindMetaData(int i, List<Expression> children,
                    final Expression expression) {
                final PDataType type;
                // If we're binding the first parameter and the second parameter
                // is a date
                // we know that the first parameter must be a date type too.
                if (i == 0 && (type = children.get(1).getDataType()) != null
                        && type.isCoercibleTo(PDate.INSTANCE)) {
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
                        public Integer getMaxLength() {
                            return expression.getMaxLength();
                        }
                        @Override
                        public Integer getScale() {
                            return expression.getScale();
                        }
                        @Override
                        public SortOrder getSortOrder() {
                            return expression.getSortOrder();
                        }                        
                    };
                } else if (expression.getDataType() != null
                        && expression.getDataType().isCoercibleTo(
                    PDate.INSTANCE)) {
                    return new PDatum() { // Same as with addition
                        @Override
                        public boolean isNullable() {
                            return expression.isNullable();
                        }
                        @Override
                        public PDataType getDataType() {
                            return PDecimal.INSTANCE;
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
                        public SortOrder getSortOrder() {
                            return expression.getSortOrder();
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
                Expression e1 = children.get(0);
                Expression e2 = children.get(1);
                Determinism determinism = e1.getDeterminism().combine(e2.getDeterminism());
                PDataType type1 = e1.getDataType();
                PDataType type2 = e2.getDataType();
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
                        && type1 != PTimestamp.INSTANCE
                        && type1 != PUnsignedTimestamp.INSTANCE
                        && type1.isCoercibleTo(PDate.INSTANCE);
                boolean isType2Date = 
                        type2 != null
                        && type2 != PTimestamp.INSTANCE
                        && type2 != PUnsignedTimestamp.INSTANCE
                        && type2.isCoercibleTo(PDate.INSTANCE);
                if (isType1Date || isType2Date) {
                    if (isType1Date && isType2Date) {
                        i = 2;
                        theType = PDecimal.INSTANCE;
                    } else if (isType1Date && type2 != null
                            && type2.isCoercibleTo(PDecimal.INSTANCE)) {
                        i = 2;
                        theType = PDate.INSTANCE;
                    } else if (type1 == null || type2 == null) {
                        /*
                         * FIXME: Could be either a Date or BigDecimal, but we
                         * don't know if we're comparing to a date or a number
                         * which would be disambiguate it.
                         */
                        i = 2;
                        theType = null;
                    }
                } else if(type1 == PTimestamp.INSTANCE || type2 == PTimestamp.INSTANCE) {
                    i = 2;
                    theType = PTimestamp.INSTANCE;
                } else if(type1 == PUnsignedTimestamp.INSTANCE || type2 == PUnsignedTimestamp.INSTANCE) {
                    i = 2;
                    theType = PUnsignedTimestamp.INSTANCE;
                }
                
                for (; i < children.size(); i++) {
                    // This logic finds the common type to which all child types are coercible
                    // without losing precision.
                    Expression e = children.get(i);
                    determinism = determinism.combine(e.getDeterminism());
                    PDataType type = e.getDataType();
                    if (type == null) {
                        continue;
                    } else if (type.isCoercibleTo(PLong.INSTANCE)) {
                        if (theType == null) {
                            theType = PLong.INSTANCE;
                        }
                    } else if (type == PDecimal.INSTANCE) {
                        // Coerce return type to DECIMAL from LONG or DOUBLE if DECIMAL child found,
                        // unless we're doing date arithmetic.
                        if (theType == null
                                || !theType.isCoercibleTo(PDate.INSTANCE)) {
                            theType = PDecimal.INSTANCE;
                        }
                    } else if (type.isCoercibleTo(PDouble.INSTANCE)) {
                        // Coerce return type to DOUBLE from LONG if DOUBLE child found,
                        // unless we're doing date arithmetic or we've found another child of type DECIMAL
                        if (theType == null
                                || (theType != PDecimal.INSTANCE && !theType.isCoercibleTo(PDate.INSTANCE) )) {
                            theType = PDouble.INSTANCE;
                        }
                    } else {
                        throw TypeMismatchException.newException(type, node.toString());
                    }
                }
                if (theType == PDecimal.INSTANCE) {
                    return new DecimalSubtractExpression(children);
                } else if (theType == PLong.INSTANCE) {
                    return new LongSubtractExpression(children);
                } else if (theType == PDouble.INSTANCE) {
                    return new DoubleSubtractExpression(children);
                } else if (theType == null) {
                	return LiteralExpression.newConstant(null, theType, determinism);
                } else if (theType == PTimestamp.INSTANCE || theType == PUnsignedTimestamp.INSTANCE) {
                    return new TimestampSubtractExpression(children);
                } else if (theType.isCoercibleTo(PDate.INSTANCE)) {
                    return new DateSubtractExpression(children);
                } else {
                    throw TypeMismatchException.newException(theType, node.toString());
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
                Determinism determinism = Determinism.ALWAYS;
                for(int i = 0; i < children.size(); i++) {
                    Expression e = children.get(i);
                    determinism = determinism.combine(e.getDeterminism());
                    PDataType type = e.getDataType();
                    if (type == null) {
                        continue;
                    } else if (type == PDecimal.INSTANCE) {
                        theType = PDecimal.INSTANCE;
                    } else if (type.isCoercibleTo(PLong.INSTANCE)) {
                        if (theType == null) {
                            theType = PLong.INSTANCE;
                        }
                    } else if (type.isCoercibleTo(PDouble.INSTANCE)) {
                        if (theType == null) {
                            theType = PDouble.INSTANCE;
                        }
                    } else {
                        throw TypeMismatchException.newException(type, node.toString());
                    }
                }
                if (theType == PDecimal.INSTANCE) {
                  return new DecimalMultiplyExpression( children);
                } else if (theType == PLong.INSTANCE) {
                  return new LongMultiplyExpression( children);
                } else if (theType == PDouble.INSTANCE) {
                  return new DoubleMultiplyExpression( children);
                } else {
                  return LiteralExpression.newConstant(null, theType, determinism);
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
                    if (literal.getDataType() == PDecimal.INSTANCE) {
                        if (PDecimal.INSTANCE.compareTo(literal.getValue(), BigDecimal.ZERO) == 0) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.DIVIDE_BY_ZERO).build().buildException();
                        }
                    } else {
                        if (literal.getDataType().compareTo(literal.getValue(), 0L, PLong.INSTANCE) == 0) {
                            throw new SQLExceptionInfo.Builder(SQLExceptionCode.DIVIDE_BY_ZERO).build().buildException();
                        }
                    }
                }
        }
        return visitLeave(node, children, null, new ArithmeticExpressionFactory() {
            @Override
            public Expression create(ArithmeticParseNode node, List<Expression> children) throws SQLException {
                PDataType theType = null;
                Determinism determinism = Determinism.ALWAYS;
                for(int i = 0; i < children.size(); i++) {
                    Expression e = children.get(i);
                    determinism = determinism.combine(e.getDeterminism());
                    PDataType type = e.getDataType();
                    if (type == null) {
                        continue;
                    } else if (type == PDecimal.INSTANCE) {
                        theType = PDecimal.INSTANCE;
                    } else if (type.isCoercibleTo(PLong.INSTANCE)) {
                        if (theType == null) {
                            theType = PLong.INSTANCE;
                        }
                    } else if (type.isCoercibleTo(PDouble.INSTANCE)) {
                        if (theType == null) {
                            theType = PDouble.INSTANCE;
                        }
                    } else {
                        throw TypeMismatchException.newException(type, node.toString());
                    }
                }
                if (theType == PDecimal.INSTANCE) {
                  return new DecimalDivideExpression( children);
                } else if (theType == PLong.INSTANCE) {
                  return new LongDivideExpression( children);
                } else if (theType == PDouble.INSTANCE) {
                  return new DoubleDivideExpression(children);
                } else {
                  return LiteralExpression.newConstant(null, theType, determinism);
                }
            }
        });
    }
    
    @Override
    public boolean visitEnter(ModulusParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(ModulusParseNode node, List<Expression> children) throws SQLException {
        return visitLeave(node, children, null, new ArithmeticExpressionFactory() {
            @Override
            public Expression create(ArithmeticParseNode node, List<Expression> children) throws SQLException {
                // ensure integer types
                for(Expression child : children) {
                    PDataType type = child.getDataType();
                    if(type != null && !type.isCoercibleTo(PLong.INSTANCE)) {
                        throw TypeMismatchException.newException(type, node.toString());
                    }
                }
                
                return new ModulusExpression(children);
            }
        });
    }
    
    @Override
    public boolean visitEnter(ArrayAnyComparisonNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(ArrayAnyComparisonNode node, List<Expression> children) throws SQLException {
        return new ArrayAnyComparisonExpression(children);
    }

    @Override
    public boolean visitEnter(ArrayAllComparisonNode node) throws SQLException {
        return true;
    }
    
    @Override
    public boolean visitEnter(ArrayElemRefNode node) throws SQLException {
        return true;
    }
    
    @Override
    public Expression visitLeave(ArrayElemRefNode node, List<Expression> l) throws SQLException {
        return new ArrayElemRefExpression(l);
    }
    
    @Override
    public Expression visitLeave(ArrayAllComparisonNode node, List<Expression> children) throws SQLException {
        return new ArrayAllComparisonExpression(children);
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
            if(type == PVarbinary.INSTANCE){
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TYPE_NOT_SUPPORTED_FOR_OPERATOR)
                .setMessage("Concatenation does not support "+ type +" in expression" + node).build().buildException();
            }
        }
        ImmutableBytesWritable ptr = context.getTempPtr();
        if (ExpressionUtil.isConstant(expression)) {
            return ExpressionUtil.getConstantExpression(expression, ptr);
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
        return wrapGroupByExpression(new RowValueConstructorExpression(l, node.isStateless()));
    }

	@Override
	public Expression visit(SequenceValueParseNode node)
			throws SQLException {
	    // NEXT VALUE FOR is only supported in SELECT expressions and UPSERT VALUES
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_USE_OF_NEXT_VALUE_FOR)
        .setSchemaName(node.getTableName().getSchemaName())
        .setTableName(node.getTableName().getTableName()).build().buildException();
	}

    @Override
    public Expression visitLeave(ArrayConstructorNode node, List<Expression> children) throws SQLException {
        boolean isChildTypeUnknown = false;
        Expression arrayElemChild = null;
        PDataType arrayElemDataType = children.get(0).getDataType();
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            PDataType childType = child.getDataType();
            if (childType == null) {
                isChildTypeUnknown = true;
            } else if (arrayElemDataType == null) {
                arrayElemDataType = childType;
                isChildTypeUnknown = true;
                arrayElemChild = child;
            } else if (arrayElemDataType == childType || childType.isCoercibleTo(arrayElemDataType)) {
                continue;
            } else if (arrayElemDataType.isCoercibleTo(childType)) {
                arrayElemChild = child;
                arrayElemDataType = childType;
            } else {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TYPE_MISMATCH)
                        .setMessage(
                                "Case expressions must have common type: " + arrayElemDataType
                                        + " cannot be coerced to " + childType).build().buildException();
            }
        }
        // If we found an "unknown" child type and the return type is a number
        // make the return type be the most general number type of DECIMAL.
        if (isChildTypeUnknown && arrayElemDataType != null && arrayElemDataType.isCoercibleTo(
            PDecimal.INSTANCE)) {
            arrayElemDataType = PDecimal.INSTANCE;
        }
        final PDataType theArrayElemDataType = arrayElemDataType;
        for (int i = 0; i < node.getChildren().size(); i++) {
            ParseNode childNode = node.getChildren().get(i);
            if (childNode instanceof BindParseNode) {
                context.getBindManager().addParamMetaData((BindParseNode)childNode,
                        arrayElemDataType == arrayElemChild.getDataType() ? arrayElemChild :
                            new DelegateDatum(arrayElemChild) {
                    @Override
                    public PDataType getDataType() {
                        return theArrayElemDataType;
                    }
                });
            }
        }
        ImmutableBytesWritable ptr = context.getTempPtr();
        // the value object array type should match the java known type
        Object[] elements = (Object[]) java.lang.reflect.Array.newInstance(theArrayElemDataType.getJavaClass(), children.size());

        ArrayConstructorExpression arrayExpression = new ArrayConstructorExpression(children, arrayElemDataType);
        if (ExpressionUtil.isConstant(arrayExpression)) {
            for (int i = 0; i < children.size(); i++) {
                Expression child = children.get(i);
                child.evaluate(null, ptr);
                Object value = arrayElemDataType.toObject(ptr, child.getDataType(), child.getSortOrder());
                elements[i] = LiteralExpression.newConstant(value, child.getDataType(), child.getDeterminism()).getValue();
            }
            Object value = PArrayDataType.instantiatePhoenixArray(arrayElemDataType, elements);
            return LiteralExpression.newConstant(value,
                    PDataType.fromTypeId(arrayElemDataType.getSqlType() + PDataType.ARRAY_TYPE_BASE), Determinism.ALWAYS);
        }
        
        return wrapGroupByExpression(arrayExpression);
    }

    @Override
    public boolean visitEnter(ArrayConstructorNode node) throws SQLException {
        return true;
    }

    @Override
    public boolean visitEnter(ExistsParseNode node) throws SQLException {
        return true;
    }

    @Override
    public Expression visitLeave(ExistsParseNode node, List<Expression> l) throws SQLException {
        LiteralExpression child = (LiteralExpression) l.get(0);
        PhoenixArray array = (PhoenixArray) child.getValue();
        return LiteralExpression.newConstant(array.getDimensions() > 0 ^ node.isNegate(), PBoolean.INSTANCE);
    }

    @Override
    public Expression visit(SubqueryParseNode node) throws SQLException {
        Object result = context.getSubqueryResult(node.getSelectNode());
        return LiteralExpression.newConstant(result);
    }
    
    public int getTotalNodeCount() {
        return totalNodeCount;
    }
}
