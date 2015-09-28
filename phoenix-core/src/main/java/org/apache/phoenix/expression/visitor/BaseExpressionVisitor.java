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

import java.util.Iterator;
import java.util.List;

import org.apache.phoenix.expression.AddExpression;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.ArrayConstructorExpression;
import org.apache.phoenix.expression.CaseExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.DivideExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.InListExpression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.LikeExpression;
import org.apache.phoenix.expression.ModulusExpression;
import org.apache.phoenix.expression.MultiplyExpression;
import org.apache.phoenix.expression.NotExpression;
import org.apache.phoenix.expression.OrExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.StringConcatExpression;
import org.apache.phoenix.expression.SubtractExpression;
import org.apache.phoenix.expression.function.ArrayAnyComparisonExpression;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.function.SingleAggregateFunction;


public abstract class BaseExpressionVisitor<E> implements ExpressionVisitor<E> {

    @Override
    public E defaultReturn(Expression node, List<E> l) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(AndExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(OrExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(ScalarFunction node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(ComparisonExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(LikeExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(SingleAggregateFunction node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(CaseExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(NotExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(IsNullExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(InListExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(AddExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(SubtractExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(MultiplyExpression node) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(DivideExpression node) {
        return null;
    }
    
    @Override
    public Iterator<Expression> visitEnter(StringConcatExpression node) {
        return null;
    }
    
    @Override
    public Iterator<Expression> visitEnter(RowValueConstructorExpression node) {
        return null;
    }
    
    @Override
    public Iterator<Expression> visitEnter(CoerceExpression node) {
        return null;
    }
    
    @Override
    public Iterator<Expression> visitEnter(ArrayConstructorExpression node) {
        return null;
    }
    
    @Override
    public Iterator<Expression> visitEnter(ModulusExpression modulusExpression) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(ArrayAnyComparisonExpression arrayAnyComparisonExpression) {
        return null;
    }

    @Override
    public Iterator<Expression> visitEnter(ArrayElemRefExpression arrayElemRefExpression) {
        return null;
    }

}
