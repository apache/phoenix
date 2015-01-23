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
import org.apache.phoenix.expression.DivideExpression;
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
import org.apache.phoenix.expression.StringConcatExpression;
import org.apache.phoenix.expression.SubtractExpression;
import org.apache.phoenix.expression.function.ArrayAnyComparisonExpression;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.function.SingleAggregateFunction;

public class StatelessTraverseAllExpressionVisitor<E> extends TraverseAllExpressionVisitor<E> {

    @Override
    public E visitLeave(AndExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(OrExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(ScalarFunction node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(ComparisonExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(LikeExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(SingleAggregateFunction node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(CaseExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(NotExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(IsNullExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(InListExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visit(LiteralExpression node) {
        return null;
    }

    @Override
    public E visit(RowKeyColumnExpression node) {
        return null;
    }

    @Override
    public E visit(KeyValueColumnExpression node) {
        return null;
    }
    
    @Override
    public E visit(ProjectedColumnExpression node) {
        return null;
    }

    @Override
    public E visitLeave(AddExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(SubtractExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(MultiplyExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(DivideExpression node, List<E> l) {
        return null;
    }
    
    @Override
    public E visitLeave(StringConcatExpression node, List<E> l) {
        return null;
    }
    
    @Override
    public E visitLeave(RowValueConstructorExpression node, List<E> l) {
        return null;
    }
    
    @Override
    public E visitLeave(CoerceExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(ArrayConstructorExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(ModulusExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(ArrayAnyComparisonExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visitLeave(ArrayElemRefExpression node, List<E> l) {
        return null;
    }

    @Override
    public E visit(SequenceValueExpression node) {
        return null;
    }

}
