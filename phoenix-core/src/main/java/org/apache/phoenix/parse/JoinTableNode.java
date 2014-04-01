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



/**
 * 
 * Node representing the join specified in the FROM clause of SQL
 *
 * 
 * @since 0.1
 */
public class JoinTableNode extends TableNode {
    public enum JoinType {Inner, Left, Right, Full};
    
    private final JoinType type;
    private final TableNode lhs;
    private final TableNode rhs;
    private final ParseNode onNode;
    
    JoinTableNode(JoinType type, TableNode lhs, TableNode rhs, ParseNode onNode) {
        super(null);
        this.type = type;
        this.lhs = lhs;
        this.rhs = rhs;
        this.onNode = onNode;
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

    @Override
    public <T> T accept(TableNodeVisitor<T> visitor) throws SQLException {
        return visitor.visit(this);
    }
}

