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
package org.apache.phoenix.util.json;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.hadoop.hbase.util.Bytes;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.ByteBufferBsonInput;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;

public class BsonDataFormat implements JsonDataFormat {
    @Override
    public byte[] toBytes(Object object) {
        return Bytes.toBytes(((RawBsonDocument) object).getByteBuffer().asNIO());
    }

    @Override
    public Object toObject(String value) {
        return RawBsonDocument.parse(value);
    }

    @Override
    public Object toObject(byte[] bytes, int offset, int length) {
        return new RawBsonDocument(bytes, offset, length);
    }

    @Override
    public int estimateByteSize(Object o) {
        RawBsonDocument rawBSON = (RawBsonDocument) o;
        return rawBSON.size();
    }

    @Override
    public int getValueType(Object obj, String jsonPathExprStr) {
        BsonValue value = getBsonValue(jsonPathExprStr, (RawBsonDocument) obj);
        return getSqlType(value);
    }

    @Override
    public Object getValue(Object obj, String jsonPathExprStr) {
        BsonValue value = getBsonValue(jsonPathExprStr, (RawBsonDocument) obj);
        return getValue(value);
    }

    private Object getValue(BsonValue value) {
        if (value != null) {
            switch (value.getBsonType()) {
            case INT32:
                return value.asInt32().getValue();
            case INT64:
                return value.asInt64().getValue();
            case STRING:
            case SYMBOL:
                return value.asString().getValue();
            case DECIMAL128:
                return value.asDecimal128().doubleValue();
            case DOUBLE:
                return value.asDouble().getValue();
            case BOOLEAN:
                return value.asBoolean().getValue();
            case BINARY:
                return value.asBinary().getData();
            case DATE_TIME:
                return value.asDateTime().getValue();
            case DOCUMENT:
                return value.asDocument().toJson();
            case ARRAY:
                return readArray(value).toString();
            default:
                return null;
            }
        }
        return null;
    }

    @Override
    public ByteBuffer updateValue(Object top, String jsonPathExprStr, String newVal) {
        Configuration conf = Configuration.builder().jsonProvider(new BsonJsonProvider()).build();
        BsonValue newValue = JsonPath.using(conf).parse(newVal).json();
        BsonDocument root = fromRaw((RawBsonDocument) top);
        JsonPath.using(conf).parse(root).set(jsonPathExprStr, newValue);
        RawBsonDocument
                updated =
                new RawBsonDocumentCodec().decode(new BsonDocumentReader(root),
                        DecoderContext.builder().build());
        return updated.getByteBuffer().asNIO();
    }

    // Ref: https://github.com/json-path/JsonPath/pull/828
    @Override
    public boolean isPathValid(Object top, String path) {
        try{
            Configuration conf = Configuration.builder().jsonProvider(new BsonJsonProvider()).build();
            BsonDocument root = fromRaw((RawBsonDocument) top);
            JsonPath.using(conf).parse(root).read(path);
            return true;
        }
        catch (PathNotFoundException e){
            return false;
        }
    }

    private BsonValue getBsonValue(String jsonPathExprStr, RawBsonDocument top) {
        Configuration conf = getConfiguration();
        BsonValue value = JsonPath.using(conf).parse(top).read(jsonPathExprStr, BsonValue.class);
        return value;
    }

    private List<Object> readArray(BsonValue value) {
        return value.asArray().stream().map(e -> {
            // The reason for handling string in a special way is because:
            // Given a string array in JSON - ["hello","world"]
            // A string array when converted to a string returns
            // as [hello, world] - the quotes stripped
            // This change allows to retain those quotes.
            if (e.isString() || e.isSymbol()) {
                return "\"" + getValue(e) + "\"";
            } else {
                return String.valueOf(getValue(e));
            }
        }).collect(Collectors.toList());
    }

    private Configuration getConfiguration() {
        Configuration conf = Configuration.builder().jsonProvider(new BsonJsonProvider()).build();
        // This options will make us work in lax mode.
        conf = conf.addOptions(Option.SUPPRESS_EXCEPTIONS);
        return conf;
    }

    // Transform to an in memory BsonDocument instance
    private BsonDocument fromRaw(RawBsonDocument rawDocument) {
        // Transform to an in memory BsonDocument instance
        BsonBinaryReader
                bsonReader =
                new BsonBinaryReader(new ByteBufferBsonInput(rawDocument.getByteBuffer()));
        try {
            return new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
        } finally {
            bsonReader.close();
        }
    }

    private int getSqlType(BsonValue value) {
        if (value == null) {
            return Types.NULL;
        }
        switch (value.getBsonType()) {
        case INT32:
            return Types.INTEGER;
        case INT64:
            return Types.BIGINT;
        case DECIMAL128:
        case DOUBLE:
            return Types.DOUBLE;
        case STRING:
        case SYMBOL:
            return Types.VARCHAR;
        case BOOLEAN:
            return Types.BOOLEAN;
        case BINARY:
            return Types.BINARY;
        case DATE_TIME:
            return Types.DATE;
        case ARRAY:
            return Types.ARRAY;
        case DOCUMENT:
            return Types.NVARCHAR;
        default:
            return Types.OTHER;
        }
    }
}
