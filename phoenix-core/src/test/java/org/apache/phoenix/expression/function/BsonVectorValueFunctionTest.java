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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.parse.BsonVectorValueParseNode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.bson.BinaryVector;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.Test;

public class BsonVectorValueFunctionTest {

  private static final float DELTA = 1e-6f;

  /** Returns a BSON binary value of subtype 9 (VECTOR) with FLOAT32 elements. */
  private static BsonBinary vector(float... values) {
    return new BsonBinary(BinaryVector.floatVector(values));
  }

  /** Returns a subtype 9 binary value with the specified header bytes, to test validation. */
  private static BsonBinary rawVector(byte dtype, byte padding, float... values) {
    ByteBuffer buf = ByteBuffer.allocate(2 + values.length * 4).order(ByteOrder.LITTLE_ENDIAN);
    buf.put(dtype).put(padding);
    for (float v : values) {
      buf.putFloat(v);
    }
    return new BsonBinary(BsonBinarySubType.VECTOR, buf.array());
  }

  private static BsonVectorValueFunction func(BsonDocument doc, String path, int dim)
    throws SQLException {
    return new BsonVectorValueFunction(
      Arrays.asList(LiteralExpression.newConstant(doc, PBson.INSTANCE),
        LiteralExpression.newConstant(path, PVarchar.INSTANCE),
        LiteralExpression.newConstant(dim, PInteger.INSTANCE)));
  }

  private static float[] eval(BsonVectorValueFunction f) {
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    assertTrue("Function evaluation should succeed", f.evaluate(null, ptr));
    assertEquals(f.getDimension() * 4, ptr.getLength());
    float[] actual = (float[]) PVectorFloat.INSTANCE.toObject(ptr);
    assertNotNull(actual);
    return actual;
  }

  private static void assertEvalFails(BsonVectorValueFunction f, String expectedFragment) {
    try {
      f.evaluate(null, new ImmutableBytesWritable());
      fail("Expected evaluation error containing '" + expectedFragment + "'");
    } catch (IllegalDataException e) {
      assertTrue("Unexpected message: " + e.getMessage(),
        e.getMessage().contains(expectedFragment));
    }
  }

  @Test
  public void testExtractsSpecEncodedVector() throws Exception {
    // Make sure that extraction gives the original IEEE 754 float values.
    float[] expected = new float[] { 1.5f, -2.5f, 3.0e-3f };
    BsonDocument doc = new BsonDocument("search", new BsonDocument("embedding", vector(expected)));
    BsonVectorValueFunction f = func(doc, "search.embedding", 3);

    assertEquals(PVectorFloat.INSTANCE, f.getDataType());
    assertEquals(Integer.valueOf(3), f.getMaxLength());
    assertEquals(Determinism.ALWAYS, f.getDeterminism());
    assertArrayEquals(expected, eval(f), DELTA);
  }

  @Test
  public void testOutputIsBsonPayloadUnchanged() throws Exception {
    // The BSON FLOAT32 payload is the PVectorFloat encoding, so the output must be the same bytes.
    float[] values = new float[] { 1.5f, -2.5f, 3.0e-3f, Float.MIN_VALUE };
    BsonBinary binary = vector(values);
    BsonVectorValueFunction f = func(new BsonDocument("v", binary), "v", values.length);
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    assertTrue(f.evaluate(null, ptr));
    byte[] data = binary.getData();
    assertArrayEquals(
      Arrays.copyOfRange(data, BsonVectorValueFunction.VECTOR_HEADER_SIZE, data.length),
      ptr.copyBytes());
    assertArrayEquals(PVectorFloat.INSTANCE.toBytes(values), ptr.copyBytes());
  }

  @Test
  public void testExtractsFromRawDocumentAsStoredInCell() throws Exception {
    // The function must also read a RawBsonDocument, the form of a document in a stored cell
    float[] expected = new float[] { 0.25f, 0.5f, 0.75f, 1.0f };
    BsonDocument doc = new BsonDocument("vec", vector(expected));
    byte[] bytes = PBson.INSTANCE.toBytes(doc);
    RawBsonDocument raw = new RawBsonDocument(bytes);
    assertArrayEquals(expected, eval(func(raw, "vec", 4)), DELTA);
  }

  @Test
  public void testExtractsFromArrayElementPath() throws Exception {
    float[] first = new float[] { 1f, 2f };
    float[] second = new float[] { 3f, 4f };
    BsonDocument doc = new BsonDocument("chunks", new BsonArray(Arrays
      .asList(new BsonDocument("emb", vector(first)), new BsonDocument("emb", vector(second)))));
    assertArrayEquals(first, eval(func(doc, "chunks[0].emb", 2)), DELTA);
    assertArrayEquals(second, eval(func(doc, "chunks[1].emb", 2)), DELTA);
  }

  @Test
  public void testMissingPathReturnsNull() throws Exception {
    BsonDocument doc =
      new BsonDocument("search", new BsonDocument("embedding", vector(1.0f, 2.0f)));
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    assertTrue("Missing path should evaluate to SQL NULL",
      func(doc, "nonexistent.path", 2).evaluate(null, ptr));
    assertEquals(0, ptr.getLength());
    // A missing leaf under an existing parent document also gives SQL NULL
    assertTrue(func(doc, "search.other", 2).evaluate(null, ptr));
    assertEquals(0, ptr.getLength());
  }

  @Test
  public void testBsonNullAtPathReturnsNull() throws Exception {
    BsonDocument doc = new BsonDocument("search", new BsonDocument("embedding", BsonNull.VALUE));
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    assertTrue(func(doc, "search.embedding", 3).evaluate(null, ptr));
    assertEquals(0, ptr.getLength());
  }

  @Test
  public void testNullDocumentReturnsNull() throws Exception {
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    assertTrue(func(null, "search.embedding", 3).evaluate(null, ptr));
    assertEquals(0, ptr.getLength());
  }

  @Test
  public void testDimensionMismatchError() throws Exception {
    BsonDocument doc =
      new BsonDocument("search", new BsonDocument("embedding", vector(1.5f, 2.5f, 3.5f)));
    assertEvalFails(func(doc, "search.embedding", 5), "dimension mismatch");
    assertEvalFails(func(doc, "search.embedding", 2), "dimension mismatch");
  }

  @Test
  public void testWrongBinarySubtypeError() throws Exception {
    // A binary value of subtype 0, not the vector subtype 9, is malformed
    byte[] payload = vector(1.5f, 2.5f, 3.5f).getData();
    BsonDocument doc = new BsonDocument("search",
      new BsonDocument("embedding", new BsonBinary(BsonBinarySubType.BINARY, payload)));
    assertEvalFails(func(doc, "search.embedding", 3), "subtype");
  }

  @Test
  public void testNonFloat32ElementTypeError() throws Exception {
    // INT8 and PACKED_BIT vectors are malformed, because only FLOAT32 elements are supported
    BsonBinary int8 = new BsonBinary(BinaryVector.int8Vector(new byte[] { 1, 2, 3, 4 }));
    BsonDocument doc = new BsonDocument("v", int8);
    assertEvalFails(func(doc, "v", 1), "FLOAT32");
    BsonBinary packed =
      new BsonBinary(BinaryVector.packedBitVector(new byte[] { (byte) 0xF0 }, (byte) 4));
    assertEvalFails(func(new BsonDocument("v", packed), "v", 1), "FLOAT32");
  }

  @Test
  public void testBadPaddingError() throws Exception {
    BsonDocument doc = new BsonDocument("v", rawVector((byte) 0x27, (byte) 3, 1f, 2f));
    assertEvalFails(func(doc, "v", 2), "padding");
  }

  @Test
  public void testNonFiniteElementError() throws Exception {
    // A NaN or infinite element is malformed at any position in the payload
    assertEvalFails(func(new BsonDocument("v", vector(1f, Float.NaN, 3f)), "v", 3),
      "element 1 is NaN");
    assertEvalFails(func(new BsonDocument("v", vector(Float.NEGATIVE_INFINITY, 2f)), "v", 2),
      "element 0 is -Infinity");
    assertEvalFails(func(new BsonDocument("v", vector(1f, 2f, Float.POSITIVE_INFINITY)), "v", 3),
      "element 2 is Infinity");
    // Extreme finite values are valid
    float[] extremes = { Float.MAX_VALUE, -Float.MAX_VALUE, Float.MIN_VALUE };
    assertArrayEquals(extremes, eval(func(new BsonDocument("v", vector(extremes)), "v", 3)), 0f);
  }

  @Test
  public void testHeaderlessPayloadError() throws Exception {
    // A payload without the vector header is malformed
    byte[] headerless = PVectorFloat.INSTANCE.toBytes(new float[] { 1f, 2f, 3f });
    BsonDocument doc = new BsonDocument("v", new BsonBinary(BsonBinarySubType.VECTOR, headerless));
    // Float bytes without the header fail the check of the element type byte
    assertEvalFails(func(doc, "v", 3), "FLOAT32");
    BsonDocument empty =
      new BsonDocument("v", new BsonBinary(BsonBinarySubType.VECTOR, new byte[1]));
    assertEvalFails(func(empty, "v", 3), "truncated");
  }

  @Test
  public void testNonBinaryValueAtPathError() throws Exception {
    BsonDocument doc =
      new BsonDocument("search", new BsonDocument("embedding", new BsonString("not_a_vector")));
    assertEvalFails(func(doc, "search.embedding", 3), "BSON Binary value");
  }

  @Test
  public void testConstructorValidation() throws Exception {
    Expression docExpr = LiteralExpression.newConstant(new BsonDocument(), PBson.INSTANCE);
    Expression pathExpr = LiteralExpression.newConstant("path", PVarchar.INSTANCE);
    for (int badDim : new int[] { -1, 0 }) {
      try {
        new BsonVectorValueFunction(Arrays.asList(docExpr, pathExpr,
          LiteralExpression.newConstant(badDim, PInteger.INSTANCE)));
        fail("Expected exception for dimension " + badDim);
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage().contains("positive integer"));
      }
    }
    try {
      new BsonVectorValueFunction(Arrays.asList(docExpr, pathExpr,
        LiteralExpression.newConstant(PVectorFloat.MAX_VECTOR_DIMENSION + 1, PInteger.INSTANCE)));
      fail("Expected exception for dimension exceeding maximum");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("exceeds maximum"));
    }
    try {
      new BsonVectorValueFunction(Arrays.asList(docExpr, pathExpr));
      fail("Expected exception for fewer than 3 arguments");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("requires 3 arguments"));
    }
    try {
      new BsonVectorValueFunction(
        Arrays.asList(docExpr, LiteralExpression.newConstant(null, PVarchar.INSTANCE),
          LiteralExpression.newConstant(3, PInteger.INSTANCE)));
      fail("Expected exception for null path");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("path"));
    }
  }

  @Test
  public void testIntegrationWithDistanceFunction() throws Exception {
    BsonDocument doc =
      new BsonDocument("search", new BsonDocument("embedding", vector(3.0f, 0.0f)));
    BsonVectorValueFunction f = func(doc, "search.embedding", 2);
    // A distance function can use the extracted vector as an operand
    Expression queryVec =
      LiteralExpression.newConstant(new float[] { 0.0f, 4.0f }, PVectorFloat.INSTANCE);
    L2DistanceFunction distFunc = new L2DistanceFunction(Arrays.asList(f, queryVec));
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    assertTrue(distFunc.evaluate(null, ptr));
    assertEquals(5.0, ((Number) PDouble.INSTANCE.toObject(ptr)).doubleValue(), DELTA);
  }

  @Test
  public void testExpressionTypeSerializationRoundTrip() throws Exception {
    float[] expected = new float[] { 1.5f, 2.5f, 3.5f };
    BsonDocument doc = new BsonDocument("search", new BsonDocument("embedding", vector(expected)));
    BsonVectorValueFunction f = func(doc, "search.embedding", 3);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    WritableUtils.writeVInt(out, ExpressionType.valueOf(f).ordinal());
    f.write(out);
    out.close();

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    Expression deserialized = ExpressionType.values()[WritableUtils.readVInt(in)].newInstance();
    deserialized.readFields(in);

    assertTrue(deserialized instanceof BsonVectorValueFunction);
    BsonVectorValueFunction df = (BsonVectorValueFunction) deserialized;
    // The deserialized function must have the same path, dimension, and result as the original
    assertEquals(3, df.getDimension());
    assertEquals("search.embedding", df.getPath());
    assertEquals(Integer.valueOf(3), df.getMaxLength());
    assertEquals(f, df);
    assertArrayEquals(expected, eval(df), DELTA);
  }

  @Test
  public void testParseNodeCreation() throws Exception {
    BsonVectorValueParseNode parseNode =
      new BsonVectorValueParseNode("BSON_VECTOR_VALUE", Collections.emptyList(), null);

    Expression docBson = LiteralExpression.newConstant(new BsonDocument(), PBson.INSTANCE);
    Expression docJson = LiteralExpression.newConstant("{}", PJson.INSTANCE);
    Expression path = LiteralExpression.newConstant("embedding", PVarchar.INSTANCE);
    Expression dim = LiteralExpression.newConstant(128, PInteger.INSTANCE);

    assertTrue(
      parseNode.create(Arrays.asList(docBson, path, dim), null) instanceof BsonVectorValueFunction);
    assertTrue(
      parseNode.create(Arrays.asList(docJson, path, dim), null) instanceof BsonVectorValueFunction);

    Expression unsupportedDoc = LiteralExpression.newConstant(123, PInteger.INSTANCE);
    try {
      parseNode.create(Arrays.asList(unsupportedDoc, path, dim), null);
      fail("Expected SQLException for unsupported data type");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("unsupported for BSON_VECTOR_VALUE"));
    }
  }
}
