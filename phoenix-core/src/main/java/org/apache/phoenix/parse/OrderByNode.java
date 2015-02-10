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


/**
 * 
 * Node representing an ORDER BY clause (including asc/desc and nulls first/last) in SQL
 *
 * 
 * @since 0.1
 */
public final class OrderByNode {
    private final ParseNode child;
    private final boolean nullsLast;
    private final boolean orderAscending;
    
    OrderByNode(ParseNode child, boolean nullsLast, boolean orderAscending) {
        this.child = child;
        this.nullsLast = nullsLast;
        this.orderAscending = orderAscending;
    }
    
    public boolean isNullsLast() {
        return nullsLast;
    }
    
    public boolean isAscending() {
        return orderAscending;
    }
    
    public ParseNode getNode() {
        return child;
    }
 
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((child == null) ? 0 : child.hashCode());
        result = prime * result + (nullsLast ? 1231 : 1237);
        result = prime * result + (orderAscending ? 1231 : 1237);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        OrderByNode other = (OrderByNode)obj;
        if (child == null) {
            if (other.child != null) return false;
        } else if (!child.equals(other.child)) return false;
        if (nullsLast != other.nullsLast) return false;
        if (orderAscending != other.orderAscending) return false;
        return true;
    }

    @Override
    public String toString() {
        return child.toString() + (orderAscending ? " asc" : " desc") + " nulls " + (nullsLast ? "last" : "first");
    }

    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        child.toSQL(resolver, buf);
        if (!orderAscending) buf.append(" DESC");
        if (nullsLast) buf.append(" NULLS LAST ");
    }
}
