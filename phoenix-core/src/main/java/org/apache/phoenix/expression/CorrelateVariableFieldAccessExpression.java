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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.execute.RuntimeContext;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;

public class CorrelateVariableFieldAccessExpression extends VariableExpression {
    private final Expression fieldAccessExpression;
    
    public CorrelateVariableFieldAccessExpression(RuntimeContext context, String variableId, Expression fieldAccessExpression) {
        super(variableId, context);
        this.fieldAccessExpression = fieldAccessExpression;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Tuple variable = runtimeContext.getCorrelateVariable(name).getValue();
        if (variable == null)
            throw new RuntimeException("Variable '" + name + "' not set.");
        
        return fieldAccessExpression.evaluate(variable, ptr);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public PDataType getDataType() {
        return this.fieldAccessExpression.getDataType();
    }

}
