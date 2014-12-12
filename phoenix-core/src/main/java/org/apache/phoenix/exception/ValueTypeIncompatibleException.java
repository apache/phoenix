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
package org.apache.phoenix.exception;

import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDataType;


public class ValueTypeIncompatibleException extends IllegalDataException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY;

    public ValueTypeIncompatibleException(PDataType type, Integer precision, Integer scale) {
        super(new SQLExceptionInfo.Builder(code).setMessage(getTypeDisplayString(type, precision, scale))
                .build().buildException());
    }

    private static String getTypeDisplayString(PDataType type, Integer precision, Integer scale) {
        return type.toString() + "(" + precision + (scale == null ? "" : ("," + scale + ")"));
    }
}
