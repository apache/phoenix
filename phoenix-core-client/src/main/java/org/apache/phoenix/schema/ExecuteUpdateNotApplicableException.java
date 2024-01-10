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
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;

public class ExecuteUpdateNotApplicableException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.EXECUTE_UPDATE_NOT_APPLICABLE;
    
    public ExecuteUpdateNotApplicableException(Operation op) {
        super(new SQLExceptionInfo.Builder(code).setMessage("Disallowed operation: " + op.name()).build().toString(), code.getSQLState(), code.getErrorCode());
    }

    public ExecuteUpdateNotApplicableException(String query) {
        super(new SQLExceptionInfo.Builder(code).setMessage("Query: " + query).build().toString(), code.getSQLState(), code.getErrorCode());
    }

    public ExecuteUpdateNotApplicableException(String command, String statement) {
        super(new SQLExceptionInfo.Builder(code).setMessage("Command: " + command + ". Statement: " + statement).build().toString(), code.getSQLState(), code.getErrorCode());
    }
}
