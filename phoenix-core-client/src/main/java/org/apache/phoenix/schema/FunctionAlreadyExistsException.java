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
import org.apache.phoenix.parse.PFunction;

public class FunctionAlreadyExistsException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.FUNCTION_ALREADY_EXIST;
    private final PFunction function;
    private final String functionName;

    public FunctionAlreadyExistsException(String functionName) {
        this(functionName, null, null);
    }

    public FunctionAlreadyExistsException(String functionName, String msg) {
        this(functionName, msg, null);
    }

    public FunctionAlreadyExistsException(String functionName, PFunction function) {
        this(functionName, null, function);
    }

    public FunctionAlreadyExistsException(String functionName, String msg, PFunction function) {
        super(new SQLExceptionInfo.Builder(code).setFunctionName(functionName).setMessage(msg).build().toString(),
                code.getSQLState(), code.getErrorCode());
        this.functionName = functionName;
        this.function = function;
    }

    public String getFunctionName() {
        return functionName;
    }
    
    public PFunction getFunction() {
        return function;
    }
}
