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
 * Exception thrown when a table name is used without being qualified with an alias
 * and more than one schema contains that table.
 *
 * 
 * @since 0.1
 */
public class AmbiguousTableException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.AMBIGUOUS_TABLE;

    public AmbiguousTableException() {
        super(new SQLExceptionInfo.Builder(code).build().toString(), code.getSQLState(), code.getErrorCode(), null);
    }

    public AmbiguousTableException(String tableName) {
        super(new SQLExceptionInfo.Builder(code).setTableName(tableName).toString(),
                code.getSQLState(), code.getErrorCode(), null);
    }

    public AmbiguousTableException(String tableName, Throwable cause) {
        super(new SQLExceptionInfo.Builder(code).setTableName(tableName).toString(),
                code.getSQLState(), code.getErrorCode(), cause);
    }
}
