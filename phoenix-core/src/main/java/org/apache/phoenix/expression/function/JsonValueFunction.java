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

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.JsonValueParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import java.util.List;

/**
 * Built-in function for JSON_VALUE JSON_VALUE(<column_with_json/json_string>, <path> [returning
 * <type>]) Extracts a scalar JSON value—everything except object and array—and returns it as a
 * native type. The optional returning clause performs a typecast. Without a returning clause,
 * JSON_VALUE returns a string.
 */
@FunctionParseNode.BuiltInFunction(name = JsonValueFunction.NAME,
        nodeClass = JsonValueParseNode.class,
        args = { @FunctionParseNode.Argument(allowedTypes = { PJson.class, PVarbinary.class }),
                @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }) })
public class JsonValueFunction extends ScalarFunction {

    public static final String NAME = "JSON_VALUE";
    private PDataType dataType = PVarchar.INSTANCE;

    // This is called from ExpressionType newInstance
    public JsonValueFunction() {

    }

    public JsonValueFunction(List<Expression> children) {
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
        RawBsonDocument
                top =
                (RawBsonDocument) PJson.INSTANCE.toObject(ptr, getColValExpr().getSortOrder());

        if (!getJSONPathExpr().evaluate(tuple, ptr)) {
            return false;
        }

        if (ptr.getLength() == 0) {
            return true;
        }

        String
                jsonPathExprStr =
                (String) PVarchar.INSTANCE.toObject(ptr, getJSONPathExpr().getSortOrder());
        if (jsonPathExprStr == null) {
            return true;
        }

        Configuration conf = Configuration.builder().jsonProvider(new BsonJsonProvider()).build();
        BsonValue value = JsonPath.using(conf).parse(top).read(jsonPathExprStr, BsonValue.class);
        if (value != null) {
            switch (value.getBsonType()) {
            case INT32:
                this.dataType = PInteger.INSTANCE;
                ptr.set(PInteger.INSTANCE.toBytes(value.asInt32().getValue()));
                break;
            case INT64:
                this.dataType = PLong.INSTANCE;
                ptr.set(PLong.INSTANCE.toBytes(value.asInt64().getValue()));
                break;
            case STRING:
            case SYMBOL:
                this.dataType = PVarchar.INSTANCE;
                ptr.set(PVarchar.INSTANCE.toBytes(value.asString().getValue()));
                break;
            case DECIMAL128:
                this.dataType = PDouble.INSTANCE;
                ptr.set(PDouble.INSTANCE.toBytes(value.asDecimal128().doubleValue()));
                break;
            case DOUBLE:
                this.dataType = PDouble.INSTANCE;
                ptr.set(PDouble.INSTANCE.toBytes(value.asDouble().getValue()));
                break;
            case BOOLEAN:
                ptr.set(PVarchar.INSTANCE.toBytes(String.valueOf(value.asBoolean().getValue())));
                break;
            case BINARY:
                this.dataType = PBinary.INSTANCE;
                ptr.set(PBinary.INSTANCE.toBytes(value.asBinary().getData()));
                break;
            case DATE_TIME:
                this.dataType = PDate.INSTANCE;
                ptr.set(PDate.INSTANCE.toBytes(value.asDateTime().getValue()));
                break;
            default:
                return false;
            }
        }

        return true;
    }

    private Expression getColValExpr() {
        return getChildren().get(0);
    }

    private Expression getJSONPathExpr() {
        return getChildren().get(1);
    }

    @Override
    public PDataType getDataType() {
        return this.dataType;
    }
}
