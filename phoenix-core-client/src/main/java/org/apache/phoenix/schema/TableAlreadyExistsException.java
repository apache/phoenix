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
package org.apache.phoenix.schema;

import java.sql.SQLException;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;


/**
 * 
 * Exception thrown when a table name could not be found in the schema
 *
 * 
 * @since 0.1
 */
public class TableAlreadyExistsException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.TABLE_ALREADY_EXIST;
    private final String schemaName;
    private final String tableName;
    private final PTable table;

    public TableAlreadyExistsException(String schemaName, String tableName) {
        this(schemaName, tableName, null, null);
    }

    public TableAlreadyExistsException(String schemaName, String tableName, String msg) {
        this(schemaName, tableName, msg, null);
    }

    public TableAlreadyExistsException(String schemaName, String tableName, PTable table) {
        this(schemaName, tableName, null, table);
    }

    public TableAlreadyExistsException(String schemaName, String tableName, String msg, PTable table) {
        super(new SQLExceptionInfo.Builder(code).setSchemaName(schemaName).setTableName(tableName).setMessage(msg).build().toString(),
                code.getSQLState(), code.getErrorCode());
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.table = table;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }
    
    public PTable getTable() {
        return table;
    }
}
