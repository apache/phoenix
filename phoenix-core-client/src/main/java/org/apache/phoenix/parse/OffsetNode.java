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
import java.util.Objects;

public class OffsetNode {
    private final ParseNode node;

    OffsetNode(ParseNode node) throws SQLException {
        if(!(node instanceof BindParseNode || node instanceof LiteralParseNode || node instanceof ComparisonParseNode)) {
            throw new SQLException("Bad Expression Passed To Offset, node of type" + node.getClass().getName());
        }
        this.node = node;
    }
    
    public ParseNode getOffsetParseNode() {
        return node;
    }

    /**
     * As we usually consider RVC as having multiple binds treat bind as Integer offset.
     * @return true for Literal or Bind parse nodes.
     */
    public boolean isIntegerOffset() {
        return (node instanceof BindParseNode) || (node instanceof LiteralParseNode);
    }
    
    @Override
    public String toString() {
        return node.toString();
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetNode that = (OffsetNode) o;
        return Objects.equals(node, that.node);
    }

    @Override public int hashCode() {
        return Objects.hash(node);
    }
}
