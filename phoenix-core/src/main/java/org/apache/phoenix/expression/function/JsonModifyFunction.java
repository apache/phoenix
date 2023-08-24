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
package org.apache.phoenix.expression.function;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.JsonValueParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
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
import java.util.List;

/**
 * Built-in function for JSON_MODIFY
 * JSON_MODIFY(<column_with_json/json_string>, <path> [returning <type>], newValue)
 * Updates the value of a property in a JSON string and returns the updated JSON string.
 *
 */
@FunctionParseNode.BuiltInFunction(name = JsonModifyFunction.NAME, nodeClass = JsonValueParseNode.class,
        args = {
                @FunctionParseNode.Argument(allowedTypes = { PJson.class, PVarchar.class }),
                @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }) ,
                @FunctionParseNode.Argument(allowedTypes = { PVarchar.class })})
public class JsonModifyFunction extends ScalarFunction {

    public static final String NAME = "JSON_MODIFY";

    // This is called from ExpressionType newInstance
    public JsonModifyFunction() {

    }

    public JsonModifyFunction(List<Expression> children) {
        super(children);
        Preconditions.checkNotNull(getJSONPathExpr());
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getColValExpr().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr == null || ptr.getLength() == 0) {
            return true;
        }

        // Column name or JSON string
        RawBsonDocument top = (RawBsonDocument) PJson.INSTANCE.toObject(ptr, getColValExpr().getSortOrder());

        if (!getJSONPathExpr().evaluate(tuple, ptr)) {
            return false;
        }

        if (ptr.getLength() == 0) {
            return true;
        }

        String jsonPathExprStr = (String) PVarchar.INSTANCE.toObject(ptr,
            getJSONPathExpr().getSortOrder());
        if (jsonPathExprStr == null) {
            return true;
        }

        if (!getNewValueExpr().evaluate(tuple, ptr)) {
            return false;
        }

        String newVal = (String)PVarchar.INSTANCE.toObject(ptr,
            getNewValueExpr().getSortOrder());

        Configuration conf = Configuration
            .builder()
            .jsonProvider(new BsonJsonProvider())
            .build();
        BsonValue newValue = JsonPath.using(conf).parse(newVal).json();
        BsonDocument root = fromRaw(top);
        JsonPath.using(conf).parse(root).set(jsonPathExprStr, newValue);
        RawBsonDocument updated =
            new RawBsonDocumentCodec().decode(new BsonDocumentReader(root), DecoderContext.builder().build());
        ByteBuffer buffer = updated.getByteBuffer().asNIO();
        ptr.set(buffer.array(), buffer.arrayOffset(), buffer.limit());
        return true;
    }

    private Expression getNewValueExpr() {
        return getChildren().get(2);
    }

    private Expression getColValExpr() {
        return getChildren().get(0);
    }

    private Expression getJSONPathExpr() {
        return getChildren().get(1);
    }

    private BsonDocument fromRaw(RawBsonDocument rawDocument) {
        // Transform to an in memory BsonDocument instance
        BsonBinaryReader bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(rawDocument.getByteBuffer()));
        try {
            return new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
        } finally {
            bsonReader.close();
        }
    }

    @Override
    public PDataType getDataType() {
        return PJson.INSTANCE;
    }
}
