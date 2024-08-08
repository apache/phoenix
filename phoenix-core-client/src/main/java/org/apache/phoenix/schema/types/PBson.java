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

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;

/**
 * <p>
 * A Phoenix data type to represent Bson. The Bson can represent Scalar types as well as
 * Complex nested types in Binary Encoded JSON.
 * </p>
 */
public class PBson extends PVarbinary {

    public static final PBson INSTANCE = new PBson();

    private PBson() {
        super("BSON", PDataType.BSON_TYPE, byte[].class, null, 49);
    }

    @Override
    public boolean canBePrimaryKey() {
        return false;
    }

    @Override
    public boolean isComparisonSupported() {
        return false;
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
            return ByteUtil.EMPTY_BYTE_ARRAY;
        }
        if (!(object instanceof BsonDocument)) {
            throw new IllegalArgumentException("The object should be of type BsonDocument");
        }
        if (object instanceof RawBsonDocument) {
            return Bytes.toBytes(((RawBsonDocument) object).getByteBuffer().asNIO());
        } else {
            RawBsonDocument rawBsonDocument =
                new RawBsonDocument((BsonDocument) object, new BsonDocumentCodec());
            return Bytes.toBytes((rawBsonDocument).getByteBuffer().asNIO());
        }
    }

    @Override
    public Object toObject(byte[] bytes, int offset, int length,
            @SuppressWarnings("rawtypes") PDataType actualType, SortOrder sortOrder,
            Integer maxLength, Integer scale) {
        if (length == 0) {
            return null;
        }
        return new RawBsonDocument(bytes, offset, length);
    }

    @Override
    public Object toObject(Object object, @SuppressWarnings("rawtypes") PDataType actualType) {
        if (object == null) {
            return null;
        }
        if (equalsAny(actualType, PVarchar.INSTANCE)) {
            return toObject((String) object);
        }
        return object;
    }

    @Override
    public Object toObject(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        return RawBsonDocument.parse(value);
    }

    @Override
    public boolean isCoercibleTo(@SuppressWarnings("rawtypes") PDataType targetType) {
        return equalsAny(targetType, this, PBinary.INSTANCE, PVarbinary.INSTANCE);
    }

    @Override
    public int estimateByteSize(Object o) {
        byte[] value = toBytes(o);
        return value == null ? 1 : value.length;
    }

    @Override
    public Integer getByteSize() {
        return null;
    }

    @Override
    public boolean isBytesComparableWith(@SuppressWarnings("rawtypes") PDataType otherType) {
        return otherType == PVarbinary.INSTANCE;
    }

    @Override
    public Object getSampleValue(Integer maxLength, Integer arrayLength) {
        String mapStr = "{\"map\":{\"attr_0\":{\"s\":\"val_0\"}}}";
        return this.toObject(mapStr);
    }

}
