/*
 * Copyright 2010 The Apache Software Foundation
 *
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
    
    public static TableName createNormalized(String schemaName, String tableName) {
        schemaName = schemaName == null ? null : SchemaUtil.normalizeIdentifier(schemaName);
        tableName = SchemaUtil.normalizeIdentifier(tableName);
        return new TableName(schemaName, tableName);
    }
    
    public static TableName create(String schemaName, String tableName) {
        return new TableName(schemaName,tableName);
    }
    
    private TableName(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }
    
    @Override
    public String toString() {
        return (schemaName == null ? "" : schemaName + QueryConstants.NAME_SEPARATOR)  + tableName;
    }
}
