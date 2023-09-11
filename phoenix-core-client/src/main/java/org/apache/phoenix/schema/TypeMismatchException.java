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
import org.apache.phoenix.schema.types.PDataType;

/**
 * Exception thrown when we try to convert one type into a different incompatible type.
 * 
 * 
 * @since 1.0
 */
public class TypeMismatchException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.TYPE_MISMATCH;

    public TypeMismatchException(String msg) {
        super(new SQLExceptionInfo.Builder(code).setMessage(msg).build().toString(), code.getSQLState(), code.getErrorCode());
    }

    public static TypeMismatchException newException(PDataType lhs)  {
        return new TypeMismatchException(getMessage(lhs,null,null));
    }
    
    public static TypeMismatchException newException(PDataType lhs, String location)  {
        return new TypeMismatchException(getMessage(lhs,null,location));
    }
    
    public static TypeMismatchException newException(PDataType lhs, PDataType rhs)  {
        return new TypeMismatchException(getMessage(lhs,rhs,null));
    }
    
    public static TypeMismatchException newException(PDataType lhs, PDataType rhs, String location)  {
        return new TypeMismatchException(getMessage(lhs,rhs,location));
    }
    
    public static String getMessage(PDataType lhs, PDataType rhs, String location) {
        return lhs + (rhs == null ? "" : " and " + rhs) + (location == null ? "" : " for " + location);
    }
}
