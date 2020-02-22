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

import org.apache.phoenix.compile.WhereOptimizer;
import org.apache.phoenix.expression.Expression;

public class KeySlotsExpressionNode {
    private WhereOptimizer.KeyExpressionVisitor.KeySlots keySlots;
    private Expression expression;

    KeySlotsExpressionNode(WhereOptimizer.KeyExpressionVisitor.KeySlots keySlot, Expression expression) {
        this.keySlots = keySlot;
        this.expression = expression;
    }

    public Expression getExpression() {
        return this.expression;
    }

    public WhereOptimizer.KeyExpressionVisitor.KeySlots getKeySlots() {
        return this.keySlots;
    }
}