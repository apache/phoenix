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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.TableRef;



/**
 * 
 * Interface used to resolve column references occurring
 * in the select statement.
 *
 * 
 * @since 0.1
 */
public interface ColumnResolver {
    
    /**
     * Returns the collection of resolved tables in the FROM clause.
     */
    public List<TableRef> getTables();
    
    /**
     * Resolves table using name or alias.
     * @param schemaName the schema name
     * @param tableName the table name or table alias
     * @return the resolved TableRef
     * @throws TableNotFoundException if the table could not be resolved
     * @throws AmbiguousTableException if the table name is ambiguous
     */
    public TableRef resolveTable(String schemaName, String tableName) throws SQLException;
    
    /**
     * Resolves column using name and alias.
     * @param schemaName TODO
     * @param tableName TODO
     * @param colName TODO
     * @return the resolved ColumnRef
     * @throws ColumnNotFoundException if the column could not be resolved
     * @throws AmbiguousColumnException if the column name is ambiguous
     */
    public ColumnRef resolveColumn(String schemaName, String tableName, String colName) throws SQLException;
}
