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
package org.apache.phoenix.filter;

import java.util.Iterator;

import org.apache.phoenix.expression.ArrayConstructorExpression;
import org.apache.phoenix.expression.CaseExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.expression.function.CoalesceFunction;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.visitor.TraverseAllExpressionVisitor;


/**
 * 
 * Implementation of ExpressionVisitor for the expression used by the
 * BooleanExpressionFilter that looks for expressions that need to be
 * evaluated upon completion. Examples include:
 * - CaseExpression with an else clause, since upon completion, the
 * else clause would apply if the when clauses could not be evaluated
 * due to the absense of a value.
 * - IsNullExpression that's not negated, since upon completion, we
 * know definitively that a column value was not found.
 * - row key columns are used, since we may never have encountered a
 * key value column of interest, but the expression may evaluate to true
 * just based on the row key columns.
 * 
 * TODO: this really should become a method on Expression
 * @since 0.1
 */
public class EvaluateOnCompletionVisitor extends TraverseAllExpressionVisitor<Void> {
    private boolean evaluateOnCompletion = false;
    
    public boolean evaluateOnCompletion() {
        return evaluateOnCompletion;
    }
    
    @Override
    public Iterator<Expression> visitEnter(IsNullExpression node) {
        evaluateOnCompletion |= !node.isNegate();
        return null;
    }
    @Override
    public Iterator<Expression> visitEnter(CaseExpression node) {
        evaluateOnCompletion |= node.hasElse();
        return null;
    }
    @Override
    public Void visit(RowKeyColumnExpression node) {
        evaluateOnCompletion = true;
        return null;
    }
    @Override
    public Iterator<Expression> visitEnter(RowValueConstructorExpression node) {
        evaluateOnCompletion = true;
        return null;
    }
    
    @Override
    public Iterator<Expression> visitEnter(ArrayConstructorExpression node) {
        evaluateOnCompletion = true;
        return null;
    }
    
    @Override
    public Iterator<Expression> visitEnter(ScalarFunction node) {
        evaluateOnCompletion |= node instanceof CoalesceFunction;
        return null;
    }
}
