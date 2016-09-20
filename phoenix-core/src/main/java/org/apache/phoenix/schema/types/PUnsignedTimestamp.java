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

public class PUnsignedTimestamp extends PTimestamp {

    public static final PUnsignedTimestamp INSTANCE = new PUnsignedTimestamp();

    private PUnsignedTimestamp() {
        super("UNSIGNED_TIMESTAMP", 20, 12);
    }

    @Override
    public boolean isBytesComparableWith(PDataType otherType) {
        return equalsAny(this, otherType, PVarbinary.INSTANCE, PBinary.INSTANCE, PUnsignedTime.INSTANCE, PUnsignedDate.INSTANCE, PUnsignedLong.INSTANCE);
    }

    @Override
    public Object toObject(Object object, PDataType actualType) {
        java.sql.Timestamp ts = (java.sql.Timestamp) super.toObject(object, actualType);
        throwIfNonNegativeDate(ts);
        return ts;
    }

    @Override
    public boolean isCastableTo(PDataType targetType) {
        return PUnsignedDate.INSTANCE.isCastableTo(targetType);
    }

    @Override
    public boolean isCoercibleTo(PDataType targetType) {
        return targetType.equals(this) || PUnsignedDate.INSTANCE.isCoercibleTo(targetType);
    }

    @Override
    public boolean isCoercibleTo(PDataType targetType, Object value) {
        return super.isCoercibleTo(targetType, value) || PTimestamp.INSTANCE
                .isCoercibleTo(targetType, value);
    }

    @Override
    public int getResultSetSqlType() {
        return PTimestamp.INSTANCE.getResultSetSqlType();
    }

    @Override
    public int getNanos(ImmutableBytesWritable ptr, SortOrder sortOrder) {
        int nanos = PUnsignedInt.INSTANCE.getCodec()
                .decodeInt(ptr.get(), ptr.getOffset() + PLong.INSTANCE.getByteSize(), sortOrder);
        return nanos;
    }

    @Override
    public Object getSampleValue(Integer maxLength, Integer arrayLength) {
        return new java.sql.Timestamp(
                (Long) PUnsignedLong.INSTANCE.getSampleValue(maxLength, arrayLength));
    }
}
