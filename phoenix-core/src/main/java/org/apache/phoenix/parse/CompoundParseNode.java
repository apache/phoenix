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
import java.util.Collections;
import java.util.List;



/**
 * 
 * Abstract node representing an expression node that has children
 *
 * 
 * @since 0.1
 */
public abstract class CompoundParseNode extends ParseNode {
    private final List<ParseNode> children;
    private final boolean isStateless;
    
    CompoundParseNode(List<ParseNode> children) {
        this.children = Collections.unmodifiableList(children);
        boolean isStateless = true;
        for (ParseNode child : children) {
            isStateless &= child.isStateless();
            if (!isStateless) {
                break;
            }
        }
        this.isStateless = isStateless;
    }
    
    @Override
    public boolean isStateless() {
        return isStateless;
    }
    
    @Override
    public final List<ParseNode> getChildren() {
        return children;
    }


    final <T> List<T> acceptChildren(ParseNodeVisitor<T> visitor) throws SQLException {
        List<T> l = visitor.newElementList(children.size());        
        for (int i = 0; i < children.size(); i++) {
            T e = children.get(i).accept(visitor);
            visitor.addElement(l, e);
        }
        return l;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + children.toString();
    }
}
