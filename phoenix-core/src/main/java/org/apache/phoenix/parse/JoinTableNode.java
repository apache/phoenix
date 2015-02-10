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

import org.apache.phoenix.compile.ColumnResolver;



/**
 * 
 * Node representing the join specified in the FROM clause of SQL
 *
 * 
 * @since 0.1
 */
public class JoinTableNode extends TableNode {
    public enum JoinType {
        Inner, 
        Left, 
        Right, 
        Full,
        // the following two types derive from sub-query rewriting
        Semi, 
        Anti,
    };
    
    private final JoinType type;
    private final TableNode lhs;
    private final TableNode rhs;
    private final ParseNode onNode;
    private final boolean singleValueOnly;
    
    JoinTableNode(JoinType type, TableNode lhs, TableNode rhs, ParseNode onNode, boolean singleValueOnly) {
        super(null);
        this.type = type;
        this.lhs = lhs;
        this.rhs = rhs;
        this.onNode = onNode;
        this.singleValueOnly = singleValueOnly;
    }
    
    public JoinType getType() {
        return type;
    }
    
    public TableNode getLHS() {
        return lhs;
    }
    
    public TableNode getRHS() {
        return rhs;
    }
    
    public ParseNode getOnNode() {
        return onNode;
    }
    
    public boolean isSingleValueOnly() {
        return singleValueOnly;
    }

    @Override
    public <T> T accept(TableNodeVisitor<T> visitor) throws SQLException {
        return visitor.visit(this);
    }

    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        buf.append(lhs);
        buf.append(' ');
        if (onNode == null) {
            buf.append(',');
            buf.append(rhs);
        } else {
            buf.append(type);
            buf.append(" JOIN ");
            buf.append(rhs);
            buf.append(" ON (");
            onNode.toSQL(resolver, buf);
            buf.append(')');
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((lhs == null) ? 0 : lhs.hashCode());
        result = prime * result + ((onNode == null) ? 0 : onNode.hashCode());
        result = prime * result + ((rhs == null) ? 0 : rhs.hashCode());
        result = prime * result + (singleValueOnly ? 1231 : 1237);
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        JoinTableNode other = (JoinTableNode)obj;
        if (lhs == null) {
            if (other.lhs != null) return false;
        } else if (!lhs.equals(other.lhs)) return false;
        if (onNode == null) {
            if (other.onNode != null) return false;
        } else if (!onNode.equals(other.onNode)) return false;
        if (rhs == null) {
            if (other.rhs != null) return false;
        } else if (!rhs.equals(other.rhs)) return false;
        if (singleValueOnly != other.singleValueOnly) return false;
        if (type != other.type) return false;
        return true;
    }
}

