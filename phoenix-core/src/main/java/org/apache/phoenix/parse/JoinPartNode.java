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

import org.apache.phoenix.parse.JoinTableNode.JoinType;

/**
 * 
 * Node representing the partial join clause in the FROM clause of SQL
 *
 * 
 * @since 0.1
 */
class JoinPartNode {
    
    private final JoinType type;
    private final ParseNode onNode;
    private final TableNode table;
    
    JoinPartNode(JoinType type, ParseNode onNode, TableNode table) {
        this.type = type;
        this.onNode = onNode;
        this.table = table;
    }

    JoinType getType() {
        return type;
    }
    
    ParseNode getOnNode() {
        return onNode;
    }
    
    TableNode getTable() {
        return table;
    }
}

