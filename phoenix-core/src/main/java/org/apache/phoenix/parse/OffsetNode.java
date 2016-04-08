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


public class OffsetNode {
    private final BindParseNode bindNode;
    private final LiteralParseNode offsetNode;
    
    OffsetNode(BindParseNode bindNode) {
        this.bindNode = bindNode;
        offsetNode = null;
    }
    
    OffsetNode(LiteralParseNode limitNode) {
        this.offsetNode = limitNode;
        this.bindNode = null;
    }
    
    public ParseNode getOffsetParseNode() {
        return bindNode == null ? offsetNode : bindNode;
    }
    
    @Override
    public String toString() {
        return bindNode == null ? offsetNode.toString() : bindNode.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bindNode == null) ? 0 : bindNode.hashCode());
        result = prime * result + ((offsetNode == null) ? 0 : offsetNode.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        OffsetNode other = (OffsetNode)obj;
        if (bindNode == null) {
            if (other.bindNode != null) return false;
        } else if (!bindNode.equals(other.bindNode)) return false;
        if (offsetNode == null) {
            if (other.offsetNode != null) return false;
        } else if (!offsetNode.equals(other.offsetNode)) return false;
        return true;
    }
}
