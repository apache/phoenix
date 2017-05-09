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

import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;

public abstract class PRealNumber<T> extends PNumericType<T> {

    protected PRealNumber(String sqlTypeName, int sqlType, Class clazz,
            org.apache.phoenix.schema.types.PDataType.PDataCodec codec, int ordinal) {
        super(sqlTypeName, sqlType, clazz, codec, ordinal);
    }

    @Override
    public int signum(byte[] bytes, int offset, int length, SortOrder sortOrder, Integer maxLength,
            Integer scale) {
        double d = getCodec().decodeDouble(bytes, offset, sortOrder);
        if (Double.isNaN(d)) {
            throw new IllegalDataException();
        }
        return (d > 0) ? 1 : ((d < 0) ? -1 : 0);
    }
}
