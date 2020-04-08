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
import com.jayway.jsonpath.ParseContext;
import net.minidev.json.JSONArray;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static com.jayway.jsonpath.Option.ALWAYS_RETURN_LIST;

/**
 * A UDF that extracts a field from the row using the provided JSON path
 * Expected usage:
 *      JSON_PARSE(<column_name/JSON String>, <JSON Path expression>)
 * potentially within a SELECT/WHERE clause
 */
@BuiltInFunction(name=JSONParserFunction.NAME, args={
        @Argument(allowedTypes={PVarchar.class}),
        @Argument(allowedTypes={PVarchar.class})})
public class JSONParserFunction extends ScalarFunction {

    public static final String NAME = "JSON_PARSER";
    private ParseContext parseContext;

    public JSONParserFunction(List<Expression> children) {
        super(children);
        init();
    }

    private void init() {
        Configuration jsonPathConf = Configuration.builder()
                .options(new HashSet<>(Collections.singleton(ALWAYS_RETURN_LIST)))
                .build();
        this.parseContext = JsonPath.using(jsonPathConf);
    }

    @Override
    public String getName() {
        return JSONParserFunction.NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getColValExpr().evaluate(tuple,  ptr)) {
            return false;
        }
        if (ptr == null || ptr.getLength() == 0) {
            return true;
        }
        String colValue = (String)PVarchar.INSTANCE.toObject(ptr,
                getColValExpr().getSortOrder());
        if (colValue == null) {
            return true;
        }

        if (!getJsonPathExpr().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return true;
        }
        String jsonPathExprStr = (String)PVarchar.INSTANCE.toObject(ptr,
                getJsonPathExpr().getSortOrder());
        if (jsonPathExprStr == null) {
            return true;
        }

        JSONArray fields = this.parseContext.parse(colValue).read(jsonPathExprStr);
        String jsonFieldsString = fields.toJSONString();
        ptr.set(PVarchar.INSTANCE.toBytes(jsonFieldsString));
        return true;
    }

    private Expression getColValExpr() {
        return getChildren().get(0);
    }

    private Expression getJsonPathExpr() {
        return getChildren().get(1);
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }
}
