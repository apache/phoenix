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
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;

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

    @Override
    public void toSQL(ColumnResolver resolver, StringBuilder buf) {
        // If resolver is not null, then resolve to get fully qualified name
        String tableName = null;
        if (resolver == null) {
            if (this.tableName != null) {
                tableName = this.tableName.getTableName();
            }
        } else {
            try {
                ColumnRef ref = resolver.resolveColumn(this.getSchemaName(), this.getTableName(), this.getName());
                PColumn column = ref.getColumn();
                if (!SchemaUtil.isPKColumn(column)) {
                    PTable table = ref.getTable();
                    String defaultFamilyName = table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : table.getDefaultFamilyName().getString();
                    // Translate to the data table column name
                    String dataFamilyName = column.getFamilyName().getString() ;
                    tableName = defaultFamilyName.equals(dataFamilyName) ? null : dataFamilyName;
                }
                
            } catch (SQLException e) {
                throw new RuntimeException(e); // Already resolved, so not possible
            }
        }
        if (tableName != null) {
            if (isTableNameCaseSensitive()) {
                buf.append('"');
                buf.append(tableName);
                buf.append('"');
            } else {
                buf.append(tableName);
            }
            buf.append('.');
        }
        toSQL(buf);
    }
}
