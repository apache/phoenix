/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.sql.SQLException;
import java.util.UUID;

import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.UUIDUtil;

/**
 * Class that represents the UUID type, but since it is not fixedWidth (although internally
 * it will always have 36 bytes since it does a UUID.toString() ), it can be part of an index even
 * if it is not a primary key. This type should not be used directly in the DDL and let Phoenix do
 * the translation when necessary from {@link PUUID}.
 */
public class PUUIDIndexable extends PDataType<UUID> {

    private PUUIDIndexable() {
        super("UUID_INDEXABLE", SQLTYPE_UUID_INDEXABLE /* no constant available in Types */,
                UUID.class, null, ORDINAL_UUID_INDEXABLE);
    }

    @Override
    public boolean isComparableTo(PDataType targetType) {
        if (targetType.equals(PUUID.INSTANCE) || targetType.equals(PUUIDIndexable.INSTANCE)) {
            return true;
        }
        return false;
    }

    public static final PUUIDIndexable INSTANCE = new PUUIDIndexable();

    @Override
    public Integer getByteSize() {
        return null;
    }

    @Override
    public int estimateByteSize(Object o) {
        return o == null ? 0 : UUIDUtil.UUID_BYTES_LENGTH;
    }

    @Override
    public boolean isFixedWidth() {
        return false;
    }

    @Override
    public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
        if (lhs == rhs) {
            return 0;
        }
        if (lhs == null) {
            return -1;
        }
        if (rhs == null) {
            return 1;
        }
        return ((UUID) lhs).compareTo((UUID) rhs);
    }

    @Override
    public int toBytes(Object object, byte[] bytes, int offset) {
        if (object == null) {
            return 0;
        }
        byte[] b = toBytes(object);
        System.arraycopy(b, 0, bytes, offset, b.length);
        return b.length;
    }

    @Override
    public byte[] toBytes(Object object) {
        if (object == null) {
            return null;
        }
        return UUIDUtil.getIndexablesBytesFromUUID((UUID) object);
    }

    @Override
    public UUID toObject(String value) {
        if (value == null || value.length() == 0) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw newIllegalDataException(e);
        }
    }

    @Override
    public Object toObject(Object object, PDataType actualType) {
        if (equalsAny(actualType, PUUID.INSTANCE, PUUIDIndexable.INSTANCE)) {
            return object;
        }

        return throwConstraintViolationException(actualType, this);
    }

    @Override
    public UUID toObject(byte[] bytes, int offset, int length, PDataType actualType,
            SortOrder sortOrder, Integer maxLength, Integer scale) {
        if (length == 0) {
            return null;
        }

        if (equalsAny(actualType, PUUIDIndexable.INSTANCE)) {
            return UUIDUtil.getUUIDFromIndexablesBytes(bytes, offset, length, sortOrder);
        } else if (equalsAny(actualType, PUUID.INSTANCE)) {
            return PUUID.INSTANCE.toObject(bytes, offset, length, actualType, sortOrder, maxLength,
                scale);
        }

        throwConstraintViolationException(actualType, this);
        return null;
    }
    
    @Override
    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
            SortOrder sortOrder, Integer maxLength, Integer scale, Class jdbcType)
            throws SQLException {
        return toObject(bytes, offset, length, actualType, sortOrder, maxLength, scale);
    }    

    @Override
    public UUID getSampleValue(Integer maxLength, Integer arrayLength) {
        return UUID.randomUUID();
    }



}
