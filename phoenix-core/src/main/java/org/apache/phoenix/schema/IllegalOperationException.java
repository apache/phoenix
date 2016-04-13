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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

/**
 * 
 * Exception thrown when any illegal operation is performed.
 *
 * 
 * @since 180
 */
public class IllegalOperationException extends RuntimeException {
	private static final long serialVersionUID = 1L;

    public IllegalOperationException() {
        this((String)null);
    }
    
    public IllegalOperationException(String message) {
        super(new SQLExceptionInfo.Builder(
                SQLExceptionCode.ILLEGAL_OPERATION).setMessage(message).build().buildException());
    }
    
    public IllegalOperationException(Throwable cause) {
        super(cause); // Already wrapped - don't rewrap
    }
}
