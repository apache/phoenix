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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.JsonValueParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.json.JsonDataFormat;
import org.apache.phoenix.util.json.JsonDataFormatFactory;

import java.sql.Types;
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
    private final JsonDataFormat
            jsonDataFormat =
            JsonDataFormatFactory.getJsonDataFormat(JsonDataFormatFactory.DataFormat.BSON);

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
        Object top = PJson.INSTANCE.toObject(ptr, getColValExpr().getSortOrder());

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

        Object value = jsonDataFormat.getValue(top, jsonPathExprStr);
        int valueType = jsonDataFormat.getValueType(top, jsonPathExprStr);
        if (value != null) {
            switch (valueType) {
            case Types.INTEGER:
            case Types.BOOLEAN:
            case Types.DOUBLE:
            case Types.VARCHAR:
            case Types.BIGINT:
            case Types.BINARY:
            case Types.DATE:
                ptr.set(PVarchar.INSTANCE.toBytes(String.valueOf(value)));
                break;
            default:
                return false;
            }
        } else {
            return false;
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
        return PVarchar.INSTANCE;
    }
}
