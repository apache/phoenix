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

import org.apache.phoenix.query.QueryConstants;

/**
 * Node representing a reference to a column in a SQL expression
 * 
 * 
 * @since 0.1
 */
public class ColumnParseNode extends NamedParseNode {
    // table name can also represent a column family 
    private final TableName tableName;
    private final String fullName;
    private final String alias;

    public ColumnParseNode(TableName tableName, String name, String alias) {
        // Upper case here so our Maps can depend on this (and we don't have to upper case and create a string on every
        // lookup
        super(name);
        this.alias = alias;
        this.tableName = tableName;
        fullName = tableName == null ? getName() : tableName.toString() + QueryConstants.NAME_SEPARATOR + getName();
    }

    public ColumnParseNode(TableName tableName, String name) {
        this(tableName, name, null);
    }
    
    @Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        return visitor.visit(this);
    }

    public String getTableName() {
        return tableName == null ? null : tableName.getTableName();
    }

    public String getSchemaName() {
        return tableName == null ? null : tableName.getSchemaName();
    }

    public String getFullName() {
        return fullName;
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public String toString() {
        return fullName;
    }

    @Override
    public int hashCode() {
        return fullName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnParseNode other = (ColumnParseNode)obj;
        return fullName.equals(other.fullName);
    }
    
    public boolean isTableNameCaseSensitive() {
        return tableName == null ? false : tableName.isTableNameCaseSensitive();
    }
}
