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
import org.apache.phoenix.util.SchemaUtil;



/**
 * 
 * Node representing a subselect in the FROM clause of SQL
 *
 * 
 * @since 0.1
 */
public class DerivedTableNode extends TableNode {

    private final SelectStatement select;

    DerivedTableNode(String alias, SelectStatement select) {
        super(SchemaUtil.normalizeIdentifier(alias));
        this.select = select;
    }

    public SelectStatement getSelect() {
        return select;
    }

    @Override
    public <T> T accept(TableNodeVisitor<T> visitor) throws SQLException {
        return visitor.visit(this);
    }

    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        buf.append('(');
        select.toSQL(resolver, buf);
        buf.append(')');
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((select == null) ? 0 : select.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        DerivedTableNode other = (DerivedTableNode)obj;
        if (select == null) {
            if (other.select != null) return false;
        } else if (!select.equals(other.select)) return false;
        return true;
    }
}
