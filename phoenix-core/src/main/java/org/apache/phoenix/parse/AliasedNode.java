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

import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.util.SchemaUtil;

/**
 *
 * Node representing an aliased parse node in a SQL select clause
 *
 * 
 * @since 0.1
 */
public class AliasedNode {
    private final String alias;
    private final ParseNode node;
    private final boolean isCaseSensitve;

    public AliasedNode(String alias, ParseNode node) {
        this.isCaseSensitve = alias != null && SchemaUtil.isCaseSensitive(alias);
        this.alias = alias == null ? null : SchemaUtil.normalizeIdentifier(alias);
        this.node = node;
    }

    public String getAlias() {
        return alias;
    }

    public ParseNode getNode() {
        return node;
    }

    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        node.toSQL(resolver, buf);
        if (alias != null) {
            buf.append(' ');
            if (isCaseSensitve) buf.append('"');
            buf.append(alias);
            if (isCaseSensitve) buf.append('"');
        }
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((alias == null) ? 0 : alias.hashCode());
        result = prime * result + ((node == null) ? 0 : node.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        AliasedNode other = (AliasedNode)obj;
        if (alias == null) {
            if (other.alias != null) return false;
        } else if (!alias.equals(other.alias)) return false;
        if (node == null) {
            if (other.node != null) return false;
        } else if (!node.equals(other.node)) return false;
        return true;
    }

    public boolean isCaseSensitve() {
        return isCaseSensitve;
    }
}
