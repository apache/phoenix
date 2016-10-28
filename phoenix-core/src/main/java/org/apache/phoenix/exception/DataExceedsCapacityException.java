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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;


public class DataExceedsCapacityException extends IllegalDataException {
    private static final long serialVersionUID = 1L;

    public DataExceedsCapacityException(String message) {
        super(new SQLExceptionInfo.Builder(
                SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY).setMessage(message).build().buildException());
    }
    
    public DataExceedsCapacityException(PDataType type, Integer precision, Integer scale, String columnName, ImmutableBytesWritable value) {
        super(new SQLExceptionInfo.Builder(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY)
            .setMessage((columnName == null ? "" : columnName + " ") + getTypeDisplayString(type, precision, scale, value))
            .build().buildException());
    }
    public DataExceedsCapacityException(PDataType type, Integer precision, Integer scale) {
        this(type, precision, scale, null, null);
    }

    private static String getTypeDisplayString(PDataType type, Integer precision, Integer scale, ImmutableBytesWritable value) {
        return type.toString() + "(" + precision + (scale == null ? "" : ("," + scale)) + ")" + (value == null || value.getLength() == 0 ? "" : (" value="+SchemaUtil.toString(type, value)));
    }
}
