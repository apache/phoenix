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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.schema.SortOrder;



/**
 * 
 * Base class for Expression hierarchy that provides common
 * default implementations for most methods
 *
 * 
 * @since 0.1
 */
public abstract class BaseExpression implements Expression {
    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public Integer getByteSize() {
        return getDataType().isFixedWidth() ? getDataType().getByteSize() : null;
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

    @Override
    public void readFields(DataInput input) throws IOException {
    }

    @Override
    public void write(DataOutput output) throws IOException {
    }

    @Override
    public void reset() {
    }
    
    protected final <T> List<T> acceptChildren(ExpressionVisitor<T> visitor, Iterator<Expression> iterator) {
        if (iterator == null) {
            iterator = visitor.defaultIterator(this);
        }
        List<T> l = Collections.emptyList();
        while (iterator.hasNext()) {
            Expression child = iterator.next();
            T t = child.accept(visitor);
            if (t != null) {
                if (l.isEmpty()) {
                    l = new ArrayList<T>(getChildren().size());
                }
                l.add(t);
            }
        }
        return l;
    }
    
    @Override
    public boolean isDeterministic() {
        return true;
    }
    
    @Override
    public boolean isStateless() {
        return false;
    }
}
