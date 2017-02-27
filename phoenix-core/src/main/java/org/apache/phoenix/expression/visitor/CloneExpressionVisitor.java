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
package org.apache.phoenix.expression.visitor;

import java.util.List;

import org.apache.phoenix.compile.SequenceValueExpression;
import org.apache.phoenix.expression.AddExpression;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ArrayConstructorExpression;
import org.apache.phoenix.expression.CaseExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.CorrelateVariableFieldAccessExpression;
import org.apache.phoenix.expression.DivideExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LikeExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.ModulusExpression;
import org.apache.phoenix.expression.MultiplyExpression;
import org.apache.phoenix.expression.NotExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.SingleCellColumnExpression;
import org.apache.phoenix.expression.SingleCellConstructorExpression;
import org.apache.phoenix.expression.StringConcatExpression;
import org.apache.phoenix.expression.SubtractExpression;
import org.apache.phoenix.expression.function.ArrayAnyComparisonExpression;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.expression.function.UDFExpression;

public abstract class CloneExpressionVisitor extends TraverseAllExpressionVisitor<Expression> {

    public CloneExpressionVisitor() {
    }

    @Override
    public Expression defaultReturn(Expression node, List<Expression> l) {
        // Needed for Expressions derived from BaseTerminalExpression which don't
        // have accept methods. TODO: get rid of those
        return node;
    }

    @Override
    public Expression visit(CorrelateVariableFieldAccessExpression node) {
        return node;
    }

    @Override
    public Expression visit(LiteralExpression node) {
        return node;
    }

    @Override
    public Expression visit(RowKeyColumnExpression node) {
        return node;
    }

    @Override
    public Expression visit(KeyValueColumnExpression node) {
        return node;
    }
    
    @Override
    public Expression visit(SingleCellColumnExpression node) {
        return node;
    }

    @Override
    public Expression visit(ProjectedColumnExpression node) {
        return node.clone();
    }

    @Override
    public Expression visit(SequenceValueExpression node) {
        return node;
    }

    @Override
    public Expression visitLeave(AndExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? new AndExpression(l) : node;
    }

    @Override
    public Expression visitLeave(OrExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? new OrExpression(l) : node;
    }

    @Override
    public Expression visitLeave(ScalarFunction node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    public Expression visitLeave(UDFExpression node, List<Expression> l) {
        return new UDFExpression(l, node.getTenantId(), node.getFunctionClassName(),
                node.getJarPath(), node.getUdfFunction());
    }

    @Override
    public Expression visitLeave(ComparisonExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(LikeExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l): node;
    }

    @Override
    public Expression visitLeave(SingleAggregateFunction node, List<Expression> l) {
        // Do not clone aggregate functions, as they're executed on the server side,
        // so any state for evaluation will live there.
        return isCloneNode(node, l) ? node :  node;
    }

    @Override
    public Expression visitLeave(CaseExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? new CaseExpression(l) : node;
    }

    @Override
    public Expression visitLeave(NotExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? new NotExpression(l) : node;
    }

    @Override
    public Expression visitLeave(InListExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(IsNullExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(SubtractExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(MultiplyExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(AddExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(DivideExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(ModulusExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(CoerceExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(ArrayConstructorExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }
    
    @Override
    public Expression visitLeave(SingleCellConstructorExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(StringConcatExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? new StringConcatExpression(l) : node;
    }

    @Override
    public Expression visitLeave(RowValueConstructorExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? node.clone(l) : node;
    }

    @Override
    public Expression visitLeave(ArrayAnyComparisonExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? new ArrayAnyComparisonExpression(l) : node;
    }

    @Override
    public Expression visitLeave(ArrayElemRefExpression node, List<Expression> l) {
        return isCloneNode(node, l) ? new ArrayElemRefExpression(l) : node;
    }

    public abstract boolean isCloneNode(Expression node, List<Expression> children);
}
