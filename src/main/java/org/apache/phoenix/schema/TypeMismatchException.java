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
package org.apache.phoenix.schema;

import java.sql.SQLException;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

/**
 * Exception thrown when we try to convert one type into a different incompatible type.
 * 
 * @author zhuang
 * @since 1.0
 */
public class TypeMismatchException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.TYPE_MISMATCH;

    public TypeMismatchException(PDataType type, String location) {
        super(new SQLExceptionInfo.Builder(code).setMessage(type + " for " + location).build().toString(), code.getSQLState(), code.getErrorCode());
    }

    public TypeMismatchException(PDataType lhs, PDataType rhs) {
        super(new SQLExceptionInfo.Builder(code).setMessage(lhs + " and " + rhs).build().toString(), code.getSQLState(), code.getErrorCode());
    }

    public TypeMismatchException(PDataType lhs, PDataType rhs, String location) {
        super(new SQLExceptionInfo.Builder(code).setMessage(lhs + " and " + rhs + " for " + location).build().toString(), code.getSQLState(), code.getErrorCode());
    }

    public TypeMismatchException(String lhs, String rhs, String location) {
        super(new SQLExceptionInfo.Builder(code).setMessage(lhs + " and " + rhs + " for " + location).build().toString(), code.getSQLState(), code.getErrorCode());
    }
}
