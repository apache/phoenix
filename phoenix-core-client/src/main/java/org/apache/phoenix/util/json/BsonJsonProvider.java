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

import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.spi.json.AbstractJsonProvider;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.json.JsonReader;
import org.bson.types.ObjectId;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class BsonJsonProvider extends AbstractJsonProvider {

    @Override
    public Object parse(final String json) throws InvalidJsonException {
        JsonReader jsonReader = new JsonReader(json);
        BsonType bsonType = jsonReader.readBsonType();
        switch (bsonType) {
        case ARRAY:
            return BsonArray.parse(json);
        case DOCUMENT:
            return BsonDocument.parse(json);
        case STRING:
            return new BsonString(jsonReader.readString());
        case INT32:
            return new BsonInt32(jsonReader.readInt32());
        default:
            throw new InvalidJsonException(String.format("Unsupported bson type %s", bsonType));
        }
    }

    @Override
    public Object parse(InputStream jsonStream, String charset) throws InvalidJsonException {
        return null;
    }

    @Override
    public String toJson(Object obj) {
        return null;
    }

    @Override
    public Object createArray() {
        return new BsonArray();
    }

    @Override
    public boolean isArray(final Object obj) {

        return (obj instanceof BsonArray || obj instanceof List);
    }

    @Override
    public Object getArrayIndex(final Object obj, final int idx) {

        return toBsonArray(obj).get(idx);
    }

    @Override
    public void setArrayIndex(final Object array, final int index, final Object newValue) {
        if (!isArray(array)) {
            throw new UnsupportedOperationException();
        } else {
            BsonArray arr = toBsonArray(array);
            if (index == arr.size()) {
                arr.add(toBsonValue(newValue));
            } else {
                arr.set(index, toBsonValue(newValue));
            }
        }
    }

    @Override
    public Object createMap() {
        return new BsonDocument();
    }

    @Override
    public boolean isMap(final Object obj) {
        return (obj instanceof BsonDocument);
    }

    @Override
    public Object getMapValue(final Object obj, final String key) {
        BsonDocument bsonDocument = toBsonDocument(obj);
        Object o = bsonDocument.get(key);
        if (!bsonDocument.containsKey(key)) {
            return UNDEFINED;
        } else {
            return unwrap(o);
        }
    }

    @Override
    public Iterable<?> toIterable(final Object obj) {
        BsonArray arr = toBsonArray(obj);
        List<Object> values = new ArrayList<Object>(arr.size());
        for (Object o : arr) {
            values.add(toJavaType(toBsonValue(o)));
        }
        return values;
    }

    @Override
    public void setProperty(final Object obj, final Object key, final Object value) {
        if (isMap(obj)) {
            toBsonDocument(obj).put(key.toString(), toBsonValue(value));
        } else {
            BsonArray array = toBsonArray(obj);
            int index;
            if (key != null) {
                index = key instanceof Integer ? (Integer) key : Integer.parseInt(key.toString());
            } else {
                index = array.size();
            }

            if (index == array.size()) {
                array.add(toBsonValue(value));
            } else {
                array.set(index, toBsonValue(value));
            }
        }
    }

    private static BsonArray toBsonArray(final Object o) {
        return (BsonArray) o;
    }

    private static BsonDocument toBsonDocument(final Object o) {
        return (BsonDocument) o;
    }

    /**
     * Refer to this link for background on the implementation :
     * https://github.com/spring-projects/spring-data-mongodb/blob/main/spring-data-mongodb/src/main/java/org/springframework/data/mongodb/util/BsonUtils.java#L66
     * @param source
     * @return
     */
    private static BsonValue toBsonValue(Object source) {

        if (source instanceof BsonValue) {
            return (BsonValue) source;
        }

        if (source instanceof String) {
            return new BsonString((String) source);
        }

        if (source instanceof ObjectId) {
            return new BsonObjectId((ObjectId) source);
        }

        if (source instanceof Double) {
            return new BsonDouble((Double) source);
        }

        if (source instanceof Integer) {
            return new BsonInt32((Integer) source);
        }

        if (source instanceof Long) {
            return new BsonInt64((Long) source);
        }

        if (source instanceof byte[]) {
            return new BsonBinary((byte[]) source);
        }

        if (source instanceof Boolean) {
            return new BsonBoolean((Boolean) source);
        }

        if (source instanceof Float) {
            return new BsonDouble((Float) source);
        }

        throw new IllegalArgumentException(String.format("Unable to convert %s (%s) to BsonValue.", source,
            source != null ? source.getClass().getName() : "null"));
    }

    /**
     * Extract the corresponding plain value from {@link BsonValue}. Eg. plain {@link String} from
     * {@link org.bson.BsonString}.
     *
     * @param value must not be {@literal null}.
     * @return
     * @since 2.1
     */
    public static Object toJavaType(BsonValue value) {

        switch (value.getBsonType()) {
            case INT32:
                return value.asInt32().getValue();
            case INT64:
                return value.asInt64().getValue();
            case STRING:
                return value.asString().getValue();
            case DECIMAL128:
                return value.asDecimal128().doubleValue();
            case DOUBLE:
                return value.asDouble().getValue();
            case BOOLEAN:
                return value.asBoolean().getValue();
            case OBJECT_ID:
                return value.asObjectId().getValue();
            case BINARY:
                return value.asBinary().getData();
            case DATE_TIME:
                return new Date(value.asDateTime().getValue());
            case SYMBOL:
                return value.asSymbol().getSymbol();
            case ARRAY:
                return value.asArray().toArray();
            case DOCUMENT:
                return Document.parse(value.asDocument().toJson());
            default:
                return value;
        }
    }

}
