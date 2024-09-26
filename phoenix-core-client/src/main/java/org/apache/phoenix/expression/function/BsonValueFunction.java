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

import java.util.Date;
import java.util.List;

import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.util.bson.CommonComparisonExpressionUtils;
import org.apache.phoenix.parse.BsonValueParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
//import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.ByteUtil;

/**
 * BSON_VALUE function to retrieve the value of any field in BSON. This can be used for any
 * top-level or nested Bson fields.
 * 1. The first argument represents BSON Object on which the function performs scan.
 * 2. The second argument represents the field key. The field key can represent any top level or
 * nested fields within the document. The caller should use "." notation for accessing nested
 * document elements and "[n]" notation for accessing nested array elements.
 * Top level fields do not require any additional character.
 * 3. The third argument represents the data type that the client expects the value of the
 * field to be converted to while returning the value.
 */
@FunctionParseNode.BuiltInFunction(
    name = BsonValueFunction.NAME,
    nodeClass = BsonValueParseNode.class,
    args = {
        @FunctionParseNode.Argument(allowedTypes = {PJson.class, PBson.class, PVarbinary.class}),
        @FunctionParseNode.Argument(allowedTypes = {PVarchar.class}, isConstant = true),
        @FunctionParseNode.Argument(allowedTypes = {PVarchar.class}, isConstant = true),
    }
)
public class BsonValueFunction extends ScalarFunction {

    public static final String NAME = "BSON_VALUE";

    public BsonValueFunction() {
        // no-op
    }

    public BsonValueFunction(List<Expression> children) {
        super(children);
        Preconditions.checkNotNull(getChildren().get(1));
        Preconditions.checkNotNull(getChildren().get(2));
    }

    private PDataType<?> getPDataType() {
        String dataType = (String) ((LiteralExpression) getChildren().get(2)).getValue();
        return PDataType.fromSqlTypeName(dataType);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getChildren().get(0).evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr == null || ptr.getLength() == 0) {
            return false;
        }

        Object object = PBson.INSTANCE.toObject(ptr, getChildren().get(0).getSortOrder());
        RawBsonDocument rawBsonDocument = (RawBsonDocument) object;

        if (!getChildren().get(1).evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return false;
        }

        String documentFieldKey =
                (String) PVarchar.INSTANCE.toObject(ptr, getChildren().get(1).getSortOrder());
        if (documentFieldKey == null) {
            return false;
        }

        PDataType<?> bsonValueDataType = getPDataType();
        BsonValue bsonValue =
            CommonComparisonExpressionUtils.getFieldFromDocument(documentFieldKey, rawBsonDocument);
        if (bsonValue == null) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }
        if (bsonValueDataType == PVarchar.INSTANCE) {
            if (bsonValue instanceof BsonString) {
                ptr.set(PVarchar.INSTANCE.toBytes(((BsonString) bsonValue).getValue()));
            } else if (bsonValue instanceof BsonNumber) {
                ptr.set(PVarchar.INSTANCE.toBytes(
                    String.valueOf(((BsonNumber) bsonValue).doubleValue())));
            } else if (bsonValue instanceof BsonBoolean) {
                ptr.set(PVarchar.INSTANCE.toBytes(
                    String.valueOf(((BsonBoolean) bsonValue).getValue())));
            } else if (bsonValue instanceof BsonBinary) {
                ptr.set(PVarchar.INSTANCE.toBytes(((BsonBinary) bsonValue).getData().toString()));
            } else if (bsonValue instanceof BsonDateTime) {
                ptr.set(PVarchar.INSTANCE.toBytes(
                    new Date(((BsonDateTime) bsonValue).getValue()).toString()));
            }
        } else if (bsonValueDataType == PInteger.INSTANCE && bsonValue instanceof BsonNumber) {
            ptr.set(PInteger.INSTANCE.toBytes(((BsonNumber) bsonValue).intValue()));
        } else if (bsonValueDataType == PLong.INSTANCE && bsonValue instanceof BsonNumber) {
            ptr.set(PLong.INSTANCE.toBytes(((BsonNumber) bsonValue).longValue()));
        } else if (bsonValueDataType == PDouble.INSTANCE && bsonValue instanceof BsonNumber) {
            ptr.set(PDouble.INSTANCE.toBytes(((BsonNumber) bsonValue).doubleValue()));
        } else if (bsonValueDataType == PDecimal.INSTANCE && bsonValue instanceof BsonNumber) {
            ptr.set(PDecimal.INSTANCE.toBytes(((BsonNumber) bsonValue).decimal128Value()));
        } else if (bsonValueDataType == PBoolean.INSTANCE && bsonValue instanceof BsonBoolean) {
            ptr.set(PBoolean.INSTANCE.toBytes(((BsonBoolean) bsonValue).getValue()));
        } else if (bsonValueDataType == PVarbinary.INSTANCE && bsonValue instanceof BsonBinary) {
            ptr.set(PVarbinary.INSTANCE.toBytes(((BsonBinary) bsonValue).getData()));
//        TODO : uncomment after PHOENIX-7357
//        } else if (bsonValueDataType == PVarbinaryEncoded.INSTANCE
//            && bsonValue instanceof BsonBinary) {
//            ptr.set(PVarbinaryEncoded.INSTANCE.toBytes(((BsonBinary) bsonValue).getData()));
        } else if (bsonValueDataType == PDate.INSTANCE && bsonValue instanceof BsonDateTime) {
            ptr.set(PDate.INSTANCE.toBytes(new Date(((BsonDateTime) bsonValue).getValue())));
        } else {
            throw new IllegalArgumentException(
                "The function data type does not match with actual data type");
        }
        return true;
    }

    @Override
    public PDataType<?> getDataType() {
        return getPDataType();
    }
}
