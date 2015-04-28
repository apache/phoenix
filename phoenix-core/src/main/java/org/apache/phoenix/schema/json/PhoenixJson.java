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
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
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
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        PhoenixJson other = (PhoenixJson) obj;
        return this.jsonAsString.equals(other.jsonAsString);
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
        if (o == null) {
            return 1;
        }
        return this.jsonAsString.compareTo(o.jsonAsString);
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
}
