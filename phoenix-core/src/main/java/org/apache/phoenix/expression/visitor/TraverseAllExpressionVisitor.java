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
import java.util.NoSuchElementException;

import org.apache.phoenix.expression.Expression;




public abstract class TraverseAllExpressionVisitor<E> extends BaseExpressionVisitor<E> {

    @Override
    public Iterator<Expression> defaultIterator(Expression node) {
        final List<Expression> children = node.getChildren();
        return new Iterator<Expression>() {
            private int position;
            
            @Override
            public final boolean hasNext() {
                return position < children.size();
            }

            @Override
            public final Expression next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return children.get(position++);
            }

            @Override
            public final void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}
