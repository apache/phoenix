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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.util.bson.CommonComparisonExpressionUtils;
import org.apache.phoenix.parse.BsonVectorValueParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
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
 * Scalar function that extracts a typed float vector from a BSON document field.
 * <p>
 * Arguments: 1. BSON column expression (PBson, PJson, or PVarbinary) 2. String literal path
 * expression (e.g., 'search.embedding') 3. Integer literal declared dimension
 * </p>
 * The value at the target path must be a BSON Binary of subtype 9 (VECTOR) laid out per the BSON
 * Binary Vector specification: a two byte header holding the element data type ({@code 0x27} for
 * FLOAT32) and a padding byte (always {@code 0} for FLOAT32), followed by {@code dimension}
 * little-endian IEEE 754 single precision values. The payload is transcoded into the big-endian
 * packed layout of {@link PVectorFloat}.
 * <p>
 * A missing path or BSON null evaluates to SQL NULL. Any structural mismatch (non-binary value,
 * invalid subtype, non-FLOAT32 element type, bad padding, or dimension mismatch) results in an
 * evaluation exception to prevent processing malformed vector data.
 */
@FunctionParseNode.BuiltInFunction(name = BsonVectorValueFunction.NAME,
    nodeClass = BsonVectorValueParseNode.class,
    args = {
      @FunctionParseNode.Argument(allowedTypes = { PJson.class, PBson.class, PVarbinary.class }),
      @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }, isConstant = true),
      @FunctionParseNode.Argument(allowedTypes = { PInteger.class }, isConstant = true) })
public class BsonVectorValueFunction extends ScalarFunction {

  public static final String NAME = "BSON_VECTOR_VALUE";

  /** Size of the BSON Binary Vector header: element data type byte plus padding byte. */
  public static final int VECTOR_HEADER_SIZE = 2;

  private static final byte FLOAT32_DTYPE = BinaryVector.DataType.FLOAT32.getValue();

  // Cached constant arguments resolved during initialization.
  private String path;
  private int dimension;

  public BsonVectorValueFunction() {
    // no-op
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
    if (ptr == null || ptr.getLength() == 0) {
      return false;
    }

    Object object = PBson.INSTANCE.toObject(ptr, getChildren().get(0).getSortOrder());
    if (object == null) {
      return false;
    }
    if (!(object instanceof BsonDocument)) {
      throw new IllegalArgumentException("Expected BsonDocument, but found " + object.getClass());
    }

    BsonValue bsonValue =
      CommonComparisonExpressionUtils.getFieldFromDocument(path, (BsonDocument) object);
    if (bsonValue == null || bsonValue.isNull()) {
      ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
      return false;
    }

    if (!(bsonValue instanceof BsonBinary)) {
      throw new IllegalArgumentException(
        "Expected BSON Binary value at path '" + path + "', but found " + bsonValue.getBsonType());
    }

    BsonBinary binary = (BsonBinary) bsonValue;
    if (binary.getType() != BsonBinarySubType.VECTOR.getValue()) {
      throw new IllegalArgumentException(
        "Expected BSON Binary subtype " + BsonBinarySubType.VECTOR.getValue()
          + " (VECTOR), but found subtype " + binary.getType() + " at path '" + path + "'");
    }

    byte[] data = binary.getData();
    if (data == null || data.length < VECTOR_HEADER_SIZE) {
      throw new IllegalArgumentException(
        "BSON vector at path '" + path + "' is truncated: expected at least " + VECTOR_HEADER_SIZE
          + " header bytes, but got " + (data == null ? 0 : data.length));
    }
    if (data[0] != FLOAT32_DTYPE) {
      throw new IllegalArgumentException("Expected FLOAT32 (0x"
        + Integer.toHexString(FLOAT32_DTYPE & 0xFF) + ") BSON vector element type at path '" + path
        + "', but found 0x" + Integer.toHexString(data[0] & 0xFF));
    }
    if (data[1] != 0) {
      throw new IllegalArgumentException("Expected zero padding for FLOAT32 BSON vector at path '"
        + path + "', but found " + (data[1] & 0xFF));
    }

    int payloadLength = data.length - VECTOR_HEADER_SIZE;
    int expectedByteLength = dimension * Bytes.SIZEOF_FLOAT;
    if (payloadLength != expectedByteLength) {
      throw new IllegalArgumentException("Vector dimension mismatch at path '" + path
        + "': expected dimension " + dimension + " (" + expectedByteLength + " bytes), but got "
        + (payloadLength / Bytes.SIZEOF_FLOAT) + " (" + payloadLength + " bytes)");
    }

    // Transcode little-endian float32 elements from the BSON binary payload into big-endian
    // representation required by PVectorFloat.
    byte[] out = new byte[payloadLength];
    for (int i = 0; i < payloadLength; i += Bytes.SIZEOF_FLOAT) {
      int src = VECTOR_HEADER_SIZE + i;
      out[i] = data[src + 3];
      out[i + 1] = data[src + 2];
      out[i + 2] = data[src + 1];
      out[i + 3] = data[src];
    }
    ptr.set(out);
    return true;
  }
}
