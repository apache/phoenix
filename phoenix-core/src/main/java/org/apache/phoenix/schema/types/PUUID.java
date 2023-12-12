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
 * Class representing the UUID type. It is fixedWidth to 16 bytes. Therefore it can only be used as
 * part of a primary key or as a non-primary field; but never as an index of a non-primary field. In
 * case you want to make an index with a UUID that is not part of the primary key, the UUID type is
 * translated to {@link PUUIDIndexable}.
 * It is possible to build an UUID ARRAY ({@link PUUIDArray})
 */
public class PUUID extends PDataType<UUID> {

    public boolean isCoercibleTo(PDataType targetType) {
        return this.equals(targetType) || targetType.equals(PUUIDIndexable.INSTANCE);
    }

    public boolean isCoercibleTo(PDataType targetType, Object value) {
        return isCoercibleTo(targetType);
    }

    private PUUID() {
        super("UUID", SQLTYPE_UUID /* no constant available in Types */, UUID.class, null,
                ORDINAL_UUID);
    }

    public static final PUUID INSTANCE = new PUUID();

    @Override
    public Integer getByteSize() {
        return UUIDUtil.UUID_BYTES_LENGTH;
    }

    @Override
    public int estimateByteSize(Object o) {
        return o == null ? 0 : UUIDUtil.UUID_BYTES_LENGTH;
    }

    @Override
    public boolean isFixedWidth() {
        return true;
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
            throw newIllegalDataException(this + " may not be null");
        }

        return UUIDUtil.getBytesFromUUID((UUID) object);

    }

    @Override
    public boolean isComparableTo(PDataType targetType) {
        return PUUIDIndexable.INSTANCE.isComparableTo(targetType);
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

        if (equalsAny(actualType, PUUID.INSTANCE)) {
            if (length != UUIDUtil.UUID_BYTES_LENGTH) {
                throwConstraintViolationException(actualType, this);
                return null;
            }
            return UUIDUtil.getUUIDFromBytes(bytes, offset, sortOrder);
        } else if (equalsAny(actualType, PUUIDIndexable.INSTANCE)) {
            if (length != UUIDUtil.UUID_BYTES_LENGTH
                    && length != UUIDUtil.UUID_BYTES_LENGTH_CODED) {
                throwConstraintViolationException(actualType, this);
                return null;
            }
            return PUUIDIndexable.INSTANCE.toObject(bytes, offset, length, actualType, sortOrder,
                maxLength, scale);
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
