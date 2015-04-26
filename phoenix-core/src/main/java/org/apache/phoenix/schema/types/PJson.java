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

import java.sql.SQLException;
import java.sql.Types;
import java.text.Format;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A Phoenix data type to represent JSON. The json data type stores an exact copy of the input text,
 * which processing functions must reparse on each execution. Because the json type stores an exact
 * copy of the input text, it will preserve semantically-insignificant white space between tokens,
 * as well as the order of keys within JSON objects. Also, if a JSON object within the value
 * contains the same key more than once, all the key/value pairs are kept. It stores the data as
 * string in single column of HBase and it has same data size limit as Phoenix's Varchar.
 * <p>
 * JSON data types are for storing JSON (JavaScript Object Notation) data, as specified in RFC 7159.
 * Such data can also be stored as text, but the JSON data types have the advantage of enforcing
 * that each stored value is valid according to the JSON rules.
 */
public class PJson extends PDataType<String> {

    public static final PJson INSTANCE = new PJson();

    PJson() {
        super("JSON", Types.OTHER, PhoenixJson.class, null, 48);
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
        PhoenixJson phoenixJson = (PhoenixJson) object;
        return phoenixJson.toBytes();
    }

    @Override
    public Object toObject(byte[] bytes, int offset, int length,
            @SuppressWarnings("rawtypes") PDataType actualType, SortOrder sortOrder,
            Integer maxLength, Integer scale) {

        Object object =
                PVarchar.INSTANCE.toObject(bytes, offset, length, actualType, sortOrder, maxLength,
                    scale);
        /*
         * avoiding the type casting of object to String by calling toString() since String's
         * toString() returns itself.
         */
        return object == null ? object : getPhoenixJson(object.toString());

    }

    @Override
    public Object toObject(Object object, @SuppressWarnings("rawtypes") PDataType actualType) {
        if (object == null) {
            return null;
        }
        if (equalsAny(actualType, PJson.INSTANCE)) {
            return object;
        }
        if (equalsAny(actualType, PVarchar.INSTANCE)) {
            return getPhoenixJson(object.toString());
        }
        return throwConstraintViolationException(actualType, this);
    }

    @Override
    public boolean isCoercibleTo(@SuppressWarnings("rawtypes") PDataType targetType) {
        return equalsAny(targetType, this, PVarchar.INSTANCE);

    }

    @Override
    public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value,
            @SuppressWarnings("rawtypes") PDataType srcType, Integer maxLength, Integer scale,
            Integer desiredMaxLength, Integer desiredScale) {
        return PVarchar.INSTANCE.isSizeCompatible(ptr, value, srcType, maxLength, scale,
            desiredMaxLength, desiredScale);
    }

    @Override
    public boolean isFixedWidth() {
        return false;
    }

    @Override
    public int estimateByteSize(Object o) {
        PhoenixJson phoenixJson = (PhoenixJson) o;
        return phoenixJson.estimateByteSize();
    }

    @Override
    public Integer getByteSize() {
        return null;
    }

    @Override
    public int compareTo(Object lhs, Object rhs, @SuppressWarnings("rawtypes") PDataType rhsType) {
        if (PJson.INSTANCE != rhsType) {
            throwConstraintViolationException(rhsType, this);
        }
        PhoenixJson phoenixJsonLHS = (PhoenixJson) lhs;
        PhoenixJson phoenixJsonRHS = (PhoenixJson) rhs;
        return phoenixJsonLHS.compareTo(phoenixJsonRHS);
    }

    @Override
    public Object toObject(String value) {
        return getPhoenixJson(value);
    }

    @Override
    public boolean isBytesComparableWith(@SuppressWarnings("rawtypes") PDataType otherType) {
        return otherType == PJson.INSTANCE || otherType == PVarchar.INSTANCE;
    }

    @Override
    public String toStringLiteral(Object o, Format formatter) {
        if (o == null) {
            return StringUtil.EMPTY_STRING;
        }
        PhoenixJson phoenixJson = (PhoenixJson) o;
        return PVarchar.INSTANCE.toStringLiteral(phoenixJson.toString(), formatter);
    }

    @Override
    public Object getSampleValue(Integer maxLength, Integer arrayLength) {
        Preconditions.checkArgument(maxLength == null || maxLength >= 0);

        char[] key = new char[4];
        char[] value = new char[4];
        int length = maxLength != null ? maxLength : 1;
        if (length > (key.length + value.length)) {
            key = new char[length + 2];
            value = new char[length - key.length];
        }
        int j = 1;
        key[0] = '"';
        key[j++] = 'k';
        for (int i = 2; i < key.length - 1; i++) {
            key[j++] = (char) ('0' + RANDOM.get().nextInt(Byte.MAX_VALUE) % 10);
        }
        key[j] = '"';

        int k = 1;
        value[0] = '"';
        value[k++] = 'v';
        for (int i = 2; i < value.length - 1; i++) {
            value[k++] = (char) ('0' + RANDOM.get().nextInt(Byte.MAX_VALUE) % 10);
        }
        value[k] = '"';
        StringBuilder sbr = new StringBuilder();
        sbr.append("{").append(key).append(":").append(value).append("}");

        return getPhoenixJson(sbr.toString());
    }

    private Object getPhoenixJson(String jsonData) {
        try {
            return PhoenixJson.getInstance(jsonData);
        } catch (SQLException sqe) {
            throw new IllegalDataException(sqe);
        }
    }
}
