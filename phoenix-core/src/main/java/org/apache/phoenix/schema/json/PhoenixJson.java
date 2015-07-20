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

package org.apache.phoenix.schema.json;

import java.io.IOException;
import java.sql.SQLException;
import java.text.Format;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.schema.EqualityNotSupportedException;
import org.apache.phoenix.schema.types.*;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ValueNode;

import com.google.common.base.Preconditions;

/**
 * The {@link PhoenixJson} wraps json and uses Jackson library to parse and traverse the json. It
 * should be used to represent the JSON data type and also should be used to parse Json data and
 * read the value from it. It always conside the last value if same key exist more than once.
 */
public class PhoenixJson implements Comparable<PhoenixJson> {
    private final JsonNode rootNode;
    /*
     * input data has been stored as it is, since some data is lost when json parser runs, for
     * example if a JSON object within the value contains the same key more than once then only last
     * one is stored rest all of them are ignored, which will defy the contract of PJsonDataType of
     * keeping user data as it is.
     */
    private final String jsonAsString;

    /**
     * Static Factory method to get an {@link PhoenixJson} object. It also validates the json and
     * throws {@link SQLException} if it is invalid with line number and character.
     * @param jsonData Json data as {@link String}.
     * @return {@link PhoenixJson}.
     * @throws SQLException
     */
    public static PhoenixJson getInstance(String jsonData) throws SQLException {
        if (jsonData == null) {
           return null;
        }
        try {
            JsonFactory jsonFactory = new JsonFactory();
            JsonParser jsonParser = jsonFactory.createJsonParser(jsonData);
            JsonNode jsonNode = getRootJsonNode(jsonParser);
            return new PhoenixJson(jsonNode, jsonData);
        } catch (IOException x) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_JSON_DATA).setRootCause(x)
                    .setMessage(x.getMessage()).build().buildException();
        }

    }

    /**
     * Returns the root of the resulting {@link JsonNode} tree.
     */
    private static JsonNode getRootJsonNode(JsonParser jsonParser) throws IOException,
            JsonProcessingException {
        jsonParser.configure(Feature.ALLOW_COMMENTS, true);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readTree(jsonParser);
        } finally {
            jsonParser.close();
        }
    }

    /* Default for unit testing */PhoenixJson(final JsonNode node, final String jsonData) {
        Preconditions.checkNotNull(node, "root node cannot be null for json");
        this.rootNode = node;
        this.jsonAsString = jsonData;
    }

    /**
     * Get {@link PhoenixJson} for a given json paths. For example :
     * <p>
     * <code>
     * {"f2":{"f3":1},"f4":{"f5":99,"f6":{"f7":"2"}}}'
     * </code>
     * <p>
     * for this source json, if we want to know the json at path {'f4','f6'} it will return
     * {@link PhoenixJson} object for json {"f7":"2"}. It always returns the last key if same key
     * exist more than once.
     * <p>
     * If the given path is unreachable then it throws {@link SQLException}.
     * @param paths {@link String []} of path in the same order as they appear in json.
     * @return {@link PhoenixJson} for the json against @paths.
     * @throws SQLException
     */
    public PhoenixJson getPhoenixJson(String[] paths) throws SQLException {
        try {
            PhoenixJson phoenixJson = getPhoenixJsonInternal(paths);
            if (phoenixJson == null) {
                throw new SQLException("path: " + Arrays.asList(paths) + " not found.");
            }
            return phoenixJson;
        } catch (NumberFormatException nfe) {
            throw new SQLException("path: " + Arrays.asList(paths) + " not found.", nfe);
        }
    }

    /**
     * Get {@link PhoenixJson} for a given json paths. For example :
     * <p>
     * <code>
     * {"f2":{"f3":1},"f4":{"f5":99,"f6":{"f7":"2"}}}'
     * </code>
     * <p>
     * for this source json, if we want to know the json at path {'f4','f6'} it will return
     * {@link PhoenixJson} object for json {"f7":"2"}. It always returns the last key if same key
     * exist more than once.
     * <p>
     * If the given path is unreachable then it return null.
     * @param paths {@link String []} of path in the same order as they appear in json.
     * @return {@link PhoenixJson} for the json against @paths.
     */
    public PhoenixJson getPhoenixJsonOrNull(String[] paths) {
        try {
            return getPhoenixJsonInternal(paths);
        } catch (NumberFormatException nfe) {
            // ignore
        }
        return null;
    }

    /**
     * Serialize the current {@link PhoenixJson} to String. Its required for
     * json_extract_path_text(). If we just return node.toString() it will wrap String value in
     * double quote which is not the expectation, hence avoiding calling toString() on
     * {@link JsonNode} until PhoenixJson represent a Json Array or container for Json object. If
     * PhoenixJson just represent a {@link ValueNode} then it should return value returned from
     * objects toString().
     */
    public String serializeToString() {
        if (this.rootNode == null || this.rootNode.isNull()) {
            return null;
        } else if (this.rootNode.isValueNode()) {

            if (this.rootNode.isNumber()) {
                return this.rootNode.getNumberValue().toString();
            } else if (this.rootNode.isBoolean()) {
                return String.valueOf(this.rootNode.getBooleanValue());
            } else if (this.rootNode.isTextual()) {
                return this.rootNode.getTextValue();
            } else {
                return this.jsonAsString;
            }
        } else if (this.rootNode.isArray()) {
            return this.jsonAsString;
        } else if (this.rootNode.isContainerNode()) {
            return this.jsonAsString;
        }

        return null;

    }

    @Override
    public String toString() {
        return this.jsonAsString;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.jsonAsString.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        throw new EqualityNotSupportedException(PJson.INSTANCE);
    }

    /**
     * @return length of the string represented by the current {@link PhoenixJson}.
     */
    public int estimateByteSize() {
        String jsonStr = toString();
        return jsonStr == null ? 1 : jsonStr.length();
    }

    public byte[] toBytes() {
        return Bytes.toBytes(this.jsonAsString);
    }

    @Override
    public int compareTo(PhoenixJson o) {
        throw new EqualityNotSupportedException(PJson.INSTANCE);
    }

    private PhoenixJson getPhoenixJsonInternal(String[] paths) {
        JsonNode node = this.rootNode;
        for (String path : paths) {
            JsonNode nodeTemp = null;
            if (node.isArray()) {
                int index = Integer.parseInt(path);
                nodeTemp = node.path(index);
            } else {
                nodeTemp = node.path(path);
            }
            if (nodeTemp == null || nodeTemp.isMissingNode()) {
                return null;
            }
            node = nodeTemp;
        }
        return new PhoenixJson(node, node.toString());
    }

    /**
     * If the current {@link PhoenixJson} is a JsonArray,then it returns the length of the JsonArray.
     * <p>For example:[1,2,3] ,it will return 3
     * <p>Its required for json_array_length().
     * @throws SQLException
     */
    public int getJsonArrayLength() throws SQLException {
       if(this.rootNode.isArray()){
           return this.rootNode.size();
       }else{
           throw new SQLException("The JsonNode should be an Array");
       }
    }

    /**
     * If the current {@link PhoenixJson} is a JsonArray,then it returns the set of array elements.
     * <p>For example:[1,false,[2,"string"]]
     * it will return (new Object[]{"1","false","[2,\"string\"]"})
     * <p>Its required for json_array_elements().
     * @return {@link String []} as the set of JSON elements
     * @throws SQLException
     */
    public Object[] getJsonArrayElements() throws SQLException {
        if(this.rootNode.isArray()) {
            List<String> elementlist = new ArrayList();
            Iterator<JsonNode> elements = this.rootNode.getElements();
            while (elements.hasNext()) {
                JsonNode e = elements.next();
                elementlist.add(e.toString());
            }
            return elementlist.toArray();
        }else{
            throw new SQLException("The JsonNode should be an Array");
        }
    }
    /**
     * It returns the set of JSON keys for the current {@link PhoenixJson}.Only the outermost keys will be generated
     * <p>For example:{"f1":"abc","f2":{"f3":"a", "f4":"b"}}
     * it will return (new Object[]{"f1","f2"})
     * <p>Its required for json_object_keys().
     * @return {@link String []} as the set of JSON keys
     */
    public Object[] getJsonObjectKeys() {
        List<String> elementlist = new ArrayList();
        Iterator<String> fieldnames = this.rootNode.getFieldNames();
        while(fieldnames.hasNext()){
            elementlist.add(fieldnames.next());
        }
        return elementlist.toArray();
    }

    /**
     * It returns the SET of JSON key/value pairs for the current {@link PhoenixJson}.Only the outermost key/value will be generated
     * Probably it seems we should use a special SET class which can hold different types of elements.
     * but we use the {@link org.apache.phoenix.schema.types.PVarcharArray} as an alternative of SET TYPE
     * when implementing the build-in function ,so we use {@link String} directly to store the pair
     * and in this case we use "," to separate key and value
     * <p>For example:{"f1":"abc","f2":"edf"}
     * it will return (new Object[]{"f1,abc","f2,edf"})
     *
     * <p>Its required for json_each().
     * @return {@link String []} as the SET of JSON key/value pairs
     */
    public Object[] getJsonFields() {
        List<String> elementlist = new ArrayList();
        Iterator<Map.Entry<String, JsonNode>> fields = this.rootNode.getFields();
        while(fields.hasNext()){
            Map.Entry<String, JsonNode> entry = fields.next();
            StringBuilder fieldBuilder = new StringBuilder();
            fieldBuilder.append(entry.getKey());
            fieldBuilder.append(",");
            fieldBuilder.append(entry.getValue().toString());
            elementlist.add(fieldBuilder.toString());
        }
        return elementlist.toArray();
    }
    /**
     * Expands the object in current {@link PhoenixJson} to a record whose columns match the record type defined by base.
     * Conversion will be best effort; columns in base with no corresponding key will be left null.
     * If a column is specified more than once, the last value is used.
     * Also use "," to separate each columns
     * <p>For example:types :{"a","b"} json: {"a":"1","b":"2"}
     * it will return new String("1,2")
     *
     * <p>Its required for json_populate_record().
     * @param types {@link String} the record type
     * @return {@link String} as the result record
     */
    public String jsonPopulateRecord(String [] types) {
        StringBuilder recordsBuilder = new StringBuilder();
        for(int i =0 ;i < types.length; i++){
            List<JsonNode> nodelist =this.rootNode.findValues(types[i]);
            if(nodelist.size()!=0){
                recordsBuilder.append(nodelist.get(0).toString());
            }else{
                recordsBuilder.append("null");
            }
            if(i != types.length-1){
                recordsBuilder.append(",");
            }
        }
        return recordsBuilder.toString();
    }
    /**
     * Expands the outermost set of objects in current {@link PhoenixJson} to a SET of records whose columns match the record type defined by base.
     * Conversion will be best effort; columns in base with no corresponding key will be left null.
     * If a column is specified more than once, the last value is used.
     * Also use "," to separate each columns
     * <p>For example:types :{"a","b"} json: {[{"a":"1","b":"2"},{"a":"3","b":"4"}]}
     * it will return (new Object[]{"1,2","3,4"})
     *
     * <p>Its required for json_populate_recordset().
     * @param types {@link String} the record type
     * @return {@link String []} as the SET of records
     */
    public Object[] jsonPopulateRecordSet(String[] types) {
        List<String> recordsList = new ArrayList();
        Iterator<JsonNode> elements = this.rootNode.getElements();
        while(elements.hasNext()){
            JsonNode e = elements.next();
            StringBuilder recordsBuilder = new StringBuilder();
            for(int i =0 ;i < types.length; i++){
                List<JsonNode> nodelist =e.findValues(types[i]);
                if(nodelist.size()!=0){
                    recordsBuilder.append(nodelist.get(0).toString());
                }else{
                    recordsBuilder.append("null");
                }
                if(i != types.length-1){
                    recordsBuilder.append(",");
                }
            }
            recordsList.add(recordsBuilder.toString());
        }
        return recordsList.toArray();
    }


    /**
     * Returns the value as JSON.
     * <p>If the data type is not built in, and there is a cast from the type to json,
     * the cast function will be used to perform the conversion.
     * Otherwise, for any value other than a number, a Boolean, or a null value,
     * the text representation will be used, escaped and quoted so that it is legal JSON.
     * If the formatter is given ,perform the conversion based on it.
     *
     * <p>Its required for to_json(),array_to_json().
     * @param targetType {@link PDataType} type of the value
     * @param obj {@link Object} the value object
     * @param formatter {@link Format}  format of the value
     * @return {@link String} as JSON
     */
    public static String dataToJsonValue(PDataType targetType, Object obj,Format formatter) {
        StringBuilder  valueBuilder = new StringBuilder();
        if (obj != null) {
            if (PDataType.equalsAny(targetType, PUnsignedDouble.INSTANCE, PUnsignedFloat.INSTANCE,
                    PDouble.INSTANCE)) {
                valueBuilder.append(PDouble.INSTANCE.toStringLiteral(obj, formatter));

            } else if (PDataType.equalsAny(targetType, PInteger.INSTANCE, PUnsignedSmallint.INSTANCE,
                    PUnsignedLong.INSTANCE, PUnsignedInt.INSTANCE,PUnsignedTinyint.INSTANCE)) {
                valueBuilder.append(PLong.INSTANCE.toStringLiteral(obj, formatter));
            }else if (PDataType.equalsAny(targetType, PBoolean.INSTANCE)) {
                valueBuilder.append(PBoolean.INSTANCE.toStringLiteral(obj, formatter));
            } else if (PDataType.equalsAny(targetType, PVarchar.INSTANCE,PChar.INSTANCE)) {
                valueBuilder.append("\"");
                String tmp = PVarchar.INSTANCE.toStringLiteral(obj,formatter);
                valueBuilder.append(tmp.substring(1,tmp.length()-1));
                valueBuilder.append("\"");
            }else if (PDataType.equalsAny(targetType,PDate.INSTANCE,PTime.INSTANCE,PTimestamp.INSTANCE)){
                valueBuilder.append("\"");
                String tmp = PVarchar.INSTANCE.toStringLiteral(obj,formatter);
                valueBuilder.append(tmp.substring(1,tmp.length()-1));
                valueBuilder.append("\"");
            } else{
                valueBuilder.append("\"");
                String tmp = PVarchar.INSTANCE.toStringLiteral(obj,formatter);
                valueBuilder.append(tmp.substring(1,tmp.length()-1));
                valueBuilder.append("\"");
            }
        }else{
            valueBuilder.append("null");
        }
        return valueBuilder.toString() ;
    }

    public static String dataToJsonValue(PDataType targetType, Object obj){
        return dataToJsonValue(targetType,obj,null);
    }



}
