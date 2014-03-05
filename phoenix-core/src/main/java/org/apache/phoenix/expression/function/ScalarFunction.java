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
package org.apache.phoenix.expression.function;

import java.util.List;

import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;


public abstract class ScalarFunction extends FunctionExpression {
    public static final int NO_TRAVERSAL = -1;
    
    public ScalarFunction() {
    }
    
    public ScalarFunction(List<Expression> children) {
        super(children);
    }
    
    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        List<T> l = acceptChildren(visitor, visitor.visitEnter(this));
        T t = visitor.visitLeave(this, l);
        if (t == null) {
            t = visitor.defaultReturn(this, l);
        }
        return t;
    }
    
    /**
     * Determines whether or not a function may be used to form
     * the start/stop key of a scan
     * @return the zero-based position of the argument to traverse
     *  into to look for a primary key column reference, or
     *  {@value #NO_TRAVERSAL} if the function cannot be used to
     *  form the scan key.
     */
    public int getKeyFormationTraversalIndex() {
        return NO_TRAVERSAL;
    }

    /**
     * Manufactures a KeyPart used to construct the KeyRange given
     * a constant and a comparison operator.
     * @param childPart the KeyPart formulated for the child expression
     *  at the {@link #getKeyFormationTraversalIndex()} position.
     * @return the KeyPart for constructing the KeyRange for this
     *  function.
     */
    public KeyPart newKeyPart(KeyPart childPart) {
        return null;
    }
}
