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
package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;



/**
 * 
 * Base class for parse node visitors.
 *
 * 
 * @since 0.1
 */
public abstract class BaseParseNodeVisitor<E> implements ParseNodeVisitor<E> {

    /**
     * Fall through visitEnter method. Anything coming through
     * here means that a more specific method wasn't found
     * and thus this CompoundNode is not yet supported.
     */
    @Override
    public boolean visitEnter(CompoundParseNode expressionNode) throws SQLException {
        throw new SQLFeatureNotSupportedException(expressionNode.toString());
    }

    @Override
    public E visitLeave(CompoundParseNode expressionNode, List<E> l) throws SQLException {
        throw new SQLFeatureNotSupportedException(expressionNode.toString());
    }

    /**
     * Fall through visit method. Anything coming through
     * here means that a more specific method wasn't found
     * and thus this Node is not yet supported.
     */
    @Override
    public E visit(ParseNode expressionNode) throws SQLException {
        throw new SQLFeatureNotSupportedException(expressionNode.toString());
    }
    
    @Override
    public List<E> newElementList(int size) {
        return null;
    }
    
    @Override
    public void addElement(List<E> l, E element) {
    }
}
