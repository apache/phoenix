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

import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Abstract expression implementation for compound AND and OR expressions
 *
 * 
 * @since 0.1
 */
public abstract class AndOrExpression extends BaseCompoundExpression {
    // Remember evaluation of child expression for partial evaluation
    private BitSet partialEvalState;
   
    public AndOrExpression() {
    }
    
    public AndOrExpression(List<Expression> children) {
        super(children);
    }
    
    @Override
    public PDataType getDataType() {
        return PBoolean.INSTANCE;
    }

    @Override
    public void reset() {
        if (partialEvalState == null) {
            partialEvalState = new BitSet(children.size());
        } else {
            partialEvalState.clear();
        }
        super.reset();
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        boolean isNull = false;
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            // If partial state is available, then use that to know we've already evaluated this
            // child expression and do not need to do so again.
            if (partialEvalState == null || !partialEvalState.get(i)) {
                // Call through to child evaluate method matching parent call to allow child to optimize
                // evaluate versus getValue code path.
                if (child.evaluate(tuple, ptr)) {
                    // Short circuit if we see our stop value
                    if (isStopValue((Boolean) PBoolean.INSTANCE.toObject(ptr, child.getDataType()))) {
                        return true;
                    } else if (partialEvalState != null) {
                        partialEvalState.set(i);
                    }
                } else {
                    isNull = true;
                }
            }
        }
        if (isNull) {
            return false;
        }
        return true;
    }

    protected abstract boolean isStopValue(Boolean value);
}
