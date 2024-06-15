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

import java.nio.ByteBuffer;
import java.util.List;

import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.io.ByteBufferBsonInput;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.util.bson.UpdateExpressionUtils;
import org.apache.phoenix.parse.BsonUpdateExpressionParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

@FunctionParseNode.BuiltInFunction(name = BsonUpdateExpressionFunction.NAME,
    nodeClass = BsonUpdateExpressionParseNode.class,
    args =
        {
            @FunctionParseNode.Argument(allowedTypes = {PBson.class, PVarbinary.class}),
            @FunctionParseNode.Argument(allowedTypes = {PBson.class, PVarbinary.class},
                    isConstant = true)
        })
public class BsonUpdateExpressionFunction extends ScalarFunction {

    public static final String NAME = "BSON_UPDATE_EXPRESSION";

    public BsonUpdateExpressionFunction() {
    }

    public BsonUpdateExpressionFunction(List<Expression> children) {
        super(children);
        Preconditions.checkNotNull(getUpdateExpression());
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
        BsonDocument bsonDocument;
        try (BsonBinaryReader bsonReader = new BsonBinaryReader(
            new ByteBufferBsonInput(rawBsonDocument.getByteBuffer()))) {
            bsonDocument =
                new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
        }

        if (!getUpdateExpression().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr.getLength() == 0) {
            return false;
        }

        final RawBsonDocument updateExpressionBsonDoc;
        if (getUpdateExpression().getDataType() == PVarchar.INSTANCE) {
            String updateExpression =
                    (String) PVarchar.INSTANCE.toObject(ptr,
                            getUpdateExpression().getSortOrder());
            if (updateExpression == null || updateExpression.isEmpty()) {
                return true;
            }
            updateExpressionBsonDoc = RawBsonDocument.parse(updateExpression);
        } else {
            updateExpressionBsonDoc =
                    (RawBsonDocument) PBson.INSTANCE.toObject(ptr,
                            getUpdateExpression().getSortOrder());
            if (updateExpressionBsonDoc == null || updateExpressionBsonDoc.isEmpty()) {
                return true;
            }
        }

        UpdateExpressionUtils.updateExpression(updateExpressionBsonDoc,
            bsonDocument);

        RawBsonDocument updatedDocument =
            new RawBsonDocument(bsonDocument, new BsonDocumentCodec());
        ByteBuffer buffer = updatedDocument.getByteBuffer().asNIO();

        ptr.set(buffer.array(), buffer.arrayOffset(), buffer.limit());
        return true;
    }

    private Expression getColValExpr() {
        return getChildren().get(0);
    }

    private Expression getUpdateExpression() {
        return getChildren().get(1);
    }

    @Override
    public PDataType getDataType() {
        return PBson.INSTANCE;
    }
}
