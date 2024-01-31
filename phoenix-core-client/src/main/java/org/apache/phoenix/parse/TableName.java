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

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.SchemaUtil;

public class TableName {

    private final String tableName;
    private final String schemaName;
    private final boolean isTableNameCaseSensitive;
    private final boolean isSchemaNameCaseSensitive;
    
    public static TableName createNormalized(String schemaName, String tableName) {
        return new TableName(schemaName, tableName, true);
    }
    
    public static TableName create(String schemaName, String tableName) {
        return new TableName(schemaName, tableName, false);
    }
    
    private TableName(String schemaName, String tableName, boolean normalize) {
        this.schemaName = normalize ? SchemaUtil.normalizeIdentifier(schemaName) : schemaName;
        this.isSchemaNameCaseSensitive = normalize ? SchemaUtil.isCaseSensitive(schemaName) : false;
        this.tableName = normalize ? SchemaUtil.normalizeIdentifier(tableName) : tableName;
        this.isTableNameCaseSensitive = normalize ? SchemaUtil.isCaseSensitive(tableName) : false;
    }
    
    public boolean isTableNameCaseSensitive() {
        return isTableNameCaseSensitive;
    }

    public boolean isSchemaNameCaseSensitive() {
        return isSchemaNameCaseSensitive;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }
    
    @Override
    public String toString() {
        return (schemaName == null ? "" : ((isSchemaNameCaseSensitive ? "\"" : "") + schemaName
                + (isSchemaNameCaseSensitive ? "\"" : "") + QueryConstants.NAME_SEPARATOR))
                + ((isTableNameCaseSensitive ? "\"" : "") + tableName + (isTableNameCaseSensitive ? "\"" : ""));
    }
    
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((schemaName == null) ? 0 : schemaName.hashCode());
		result = prime * result
				+ ((tableName == null) ? 0 : tableName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TableName other = (TableName) obj;
		if (schemaName == null) {
			if (other.schemaName != null)
				return false;
		} else if (!schemaName.equals(other.schemaName))
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		return true;
	}
}
