/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.function;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.util.bson.CommonComparisonExpressionUtils;
import org.apache.phoenix.parse.BsonVectorValueParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.ByteUtil;
import org.bson.BinaryVector;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

/**
 * Extracts a float vector from a field of a BSON document.
 * <p>
 * Arguments:
 * <ol>
 * <li>The BSON document expression (PBson, PJson, or PVarbinary)</li>
 * <li>The literal path of the target field</li>
 * <li>The literal vector dimension</li>
 * </ol>
 * </p>
 * The field must hold a BSON Binary value of subtype 9 (VECTOR) with FLOAT32 elements. The
 * little-endian FLOAT32 payload is the same as the {@link PVectorFloat} encoding, so the function
 * returns the payload bytes with no element conversion. A missing document, a missing field, or a
 * BSON null field gives SQL NULL. An incorrect structure, a NaN or infinite element, or an
 * incorrect dimension causes an {@link IllegalDataException}.
 */
@FunctionParseNode.BuiltInFunction(name = BsonVectorValueFunction.NAME,
    nodeClass = BsonVectorValueParseNode.class,
    args = {
      @FunctionParseNode.Argument(allowedTypes = { PJson.class, PBson.class, PVarbinary.class }),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
      @FunctionParseNode.Argument(allowedTypes = { PInteger.class }, isConstant = true) })
public class BsonVectorValueFunction extends ScalarFunction {

  public static final String NAME = "BSON_VECTOR_VALUE";

  /**
   * The size in bytes of the BSON binary vector header: one data type byte and one padding byte.
   */
  public static final int VECTOR_HEADER_SIZE = 2;

  private static final byte FLOAT32_DTYPE = BinaryVector.DataType.FLOAT32.getValue();

  // The constant path and dimension arguments, resolved at construction and after deserialization
  private String path;
  private int dimension;

  public BsonVectorValueFunction() {
    // For deserialization. readFields resolves the arguments.
  }

  public BsonVectorValueFunction(List<Expression> children) {
    super(children);
    Preconditions.checkNotNull(children, "children cannot be null");
    Preconditions.checkArgument(children.size() >= 3,
      "BSON_VECTOR_VALUE requires 3 arguments: document, path, and dimension");
    Preconditions.checkNotNull(children.get(0), "document expression cannot be null");
    Preconditions.checkNotNull(children.get(1), "path expression cannot be null");
    Preconditions.checkNotNull(children.get(2), "dimension expression cannot be null");
    init();
  }

  private void init() {
    this.path = evaluateConstant(children.get(1), PVarchar.INSTANCE, String.class, "path");
    Number dim = evaluateConstant(children.get(2), PInteger.INSTANCE, Number.class, "dimension");
    this.dimension = dim.intValue();
    if (dimension <= 0) {
      throw new IllegalArgumentException(
        "Dimension must be a positive integer, but got: " + dimension);
    }
    if (dimension > PVectorFloat.MAX_VECTOR_DIMENSION) {
      throw new IllegalArgumentException("Dimension exceeds maximum allowed vector dimension: "
        + dimension + " > " + PVectorFloat.MAX_VECTOR_DIMENSION);
    }
  }

  private static <T> T evaluateConstant(Expression expr, PDataType<?> type, Class<T> clazz,
    String argName) {
    Object val = null;
    if (expr instanceof LiteralExpression) {
      val = ((LiteralExpression) expr).getValue();
    } else {
      ImmutableBytesWritable ptr = new ImmutableBytesWritable();
      if (expr.evaluate(null, ptr) && ptr.getLength() > 0) {
        val = type.toObject(ptr, expr.getDataType(), expr.getSortOrder());
      }
    }
    if (!clazz.isInstance(val)) {
      throw new IllegalArgumentException(
        "BSON_VECTOR_VALUE " + argName + " argument must be a non-null constant");
    }
    return clazz.cast(val);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    init();
  }

  public int getDimension() {
    return dimension;
  }

  public String getPath() {
    return path;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PDataType<?> getDataType() {
    return PVectorFloat.INSTANCE;
  }

  @Override
  public Integer getMaxLength() {
    return dimension;
  }

  @Override
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    if (!getChildren().get(0).evaluate(tuple, ptr)) {
      return false;
    }
    if (ptr.getLength() == 0) {
      return true;
    }
    Object object = PBson.INSTANCE.toObject(ptr, getChildren().get(0).getSortOrder());
    if (!(object instanceof BsonDocument)) {
      throw malformed("expected a BSON document, but found "
        + (object == null ? "null" : object.getClass().getSimpleName()));
    }
    BsonValue bsonValue =
      CommonComparisonExpressionUtils.getFieldFromDocument(path, (BsonDocument) object);
    if (bsonValue == null || bsonValue.isNull()) {
      ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
      return true;
    }
    if (!(bsonValue instanceof BsonBinary)) {
      throw malformed("expected a BSON Binary value, but found " + bsonValue.getBsonType());
    }
    BsonBinary binary = (BsonBinary) bsonValue;
    if (binary.getType() != BsonBinarySubType.VECTOR.getValue()) {
      throw malformed("expected BSON Binary subtype " + BsonBinarySubType.VECTOR.getValue()
        + " (VECTOR), but found subtype " + binary.getType());
    }
    byte[] data = binary.getData();
    if (data.length < VECTOR_HEADER_SIZE) {
      throw malformed("truncated vector of " + data.length + " bytes");
    }
    if (data[0] != FLOAT32_DTYPE) {
      throw malformed("expected FLOAT32 (0x" + Integer.toHexString(FLOAT32_DTYPE & 0xFF)
        + ") elements, but found 0x" + Integer.toHexString(data[0] & 0xFF));
    }
    if (data[1] != 0) {
      throw malformed("expected zero padding for FLOAT32, but found " + (data[1] & 0xFF));
    }
    // A NaN or infinite element makes the vector malformed. No distance can rank such a vector, and
    // it can corrupt centroid training.
    for (int i = 0; i < (data.length - VECTOR_HEADER_SIZE) / Bytes.SIZEOF_FLOAT; i++) {
      float element = Float.intBitsToFloat(
        Integer.reverseBytes(Bytes.toInt(data, VECTOR_HEADER_SIZE + i * Bytes.SIZEOF_FLOAT)));
      if (!Float.isFinite(element)) {
        throw malformed("element " + i + " is " + element);
      }
    }
    int payloadLength = data.length - VECTOR_HEADER_SIZE;
    if (payloadLength != dimension * Bytes.SIZEOF_FLOAT) {
      throw new IllegalDataException(
        new SQLExceptionInfo.Builder(SQLExceptionCode.VECTOR_DIMENSION_MISMATCH)
          .setMessage("BSON vector dimension mismatch at path '" + path + "': expected " + dimension
            + ", found " + payloadLength / (double) Bytes.SIZEOF_FLOAT)
          .build().buildException());
    }
    // The little-endian FLOAT32 payload is the same as the PVectorFloat encoding
    ptr.set(data, VECTOR_HEADER_SIZE, payloadLength);
    return true;
  }

  private IllegalDataException malformed(String detail) {
    return new IllegalDataException("Malformed BSON vector at path '" + path + "': " + detail);
  }
}
