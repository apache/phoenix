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

import java.util.List;

import org.apache.phoenix.expression.util.bson.DocumentComparisonExpressionUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.util.bson.SQLComparisonExpressionUtils;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.BsonConditionExpressionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

@FunctionParseNode.BuiltInFunction(name = BsonConditionExpressionFunction.NAME,
    nodeClass = BsonConditionExpressionParseNode.class,
    args =
        {
            @FunctionParseNode.Argument(allowedTypes = {PBson.class, PVarbinary.class}),
            @FunctionParseNode.Argument(allowedTypes = {PBson.class, PVarbinary.class},
                    isConstant = true)
        })
public class BsonConditionExpressionFunction extends ScalarFunction {

    public static final String NAME = "BSON_CONDITION_EXPRESSION";

    public BsonConditionExpressionFunction() {
    }

    public BsonConditionExpressionFunction(List<Expression> children) {
        super(children);
        Preconditions.checkNotNull(getConditionExpression());
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
            return false;
        }

        RawBsonDocument rawBsonDocument =
            (RawBsonDocument) PBson.INSTANCE.toObject(ptr, getColValExpr().getSortOrder());

        if (!getConditionExpression().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return false;
        }

        final RawBsonDocument conditionExpressionBsonDoc;
        if (getConditionExpression().getDataType() == PVarchar.INSTANCE) {
            String conditionExpression =
                    (String) PVarchar.INSTANCE.toObject(ptr,
                            getConditionExpression().getSortOrder());
            if (conditionExpression == null || conditionExpression.isEmpty()) {
                return true;
            }
            conditionExpressionBsonDoc = RawBsonDocument.parse(conditionExpression);
        } else {
            conditionExpressionBsonDoc =
                    (RawBsonDocument) PBson.INSTANCE.toObject(ptr,
                            getConditionExpression().getSortOrder());
            if (conditionExpressionBsonDoc == null || conditionExpressionBsonDoc.isEmpty()) {
                return true;
            }
        }

        BsonValue conditionExp = conditionExpressionBsonDoc.get("$EXPR");
        BsonValue exprValues = conditionExpressionBsonDoc.get("$VAL");
        if (conditionExp != null && exprValues != null) {
            if (conditionExp.isString() && exprValues.isDocument()) {
                SQLComparisonExpressionUtils sqlComparisonExpressionUtils =
                        new SQLComparisonExpressionUtils(rawBsonDocument,
                                (BsonDocument) exprValues);
                boolean result = sqlComparisonExpressionUtils.isConditionExpressionMatching(
                        ((BsonString) conditionExp).getValue());
                ptr.set(PBoolean.INSTANCE.toBytes(result));
                return true;
            }
            throw new IllegalArgumentException(
                    "Condition Expression should contain valid expression and values");
        } else {
            boolean result = DocumentComparisonExpressionUtils.isConditionExpressionMatching(
                    rawBsonDocument, conditionExpressionBsonDoc);
            ptr.set(PBoolean.INSTANCE.toBytes(result));
            return true;
        }
    }

    private Expression getColValExpr() {
        return getChildren().get(0);
    }

    private Expression getConditionExpression() {
        return getChildren().get(1);
    }

    @Override
    public PDataType getDataType() {
        return PBoolean.INSTANCE;
    }
}
