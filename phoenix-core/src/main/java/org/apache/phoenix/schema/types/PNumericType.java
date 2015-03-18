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
package org.apache.phoenix.schema.types;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;

/**
 * Base class for numeric PDataType, including PInteger, PFloat etc.
 * @since 4.3.0
 */
public abstract class PNumericType<T> extends PDataType<T> {

    protected PNumericType(String sqlTypeName, int sqlType, Class clazz,
            org.apache.phoenix.schema.types.PDataType.PDataCodec codec, int ordinal) {
        super(sqlTypeName, sqlType, clazz, codec, ordinal);
    }

    public final int signum(byte[] bytes, int offset, int length, SortOrder sortOrder) {
        return signum(bytes, offset, length, sortOrder, null, null);
    }

    public final int signum(ImmutableBytesWritable ptr, SortOrder sortOrder) {
        return signum(ptr.get(), ptr.getOffset(), ptr.getLength(), sortOrder);
    }

    abstract public int signum(byte[] bytes, int offset, int length, SortOrder sortOrder,
            Integer maxLength, Integer scale);
}
