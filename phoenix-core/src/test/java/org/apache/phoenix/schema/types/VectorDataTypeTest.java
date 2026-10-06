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
package org.apache.phoenix.schema.types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;

/** Tests for vector data types, type factory resolution, column metadata, and serialization. */
public class VectorDataTypeTest {

  @Test
  public void testFactoryTypeForSqlType() {
    PDataTypeFactory factory = PDataTypeFactory.getInstance();
    assertSame(PVectorFloat.INSTANCE, PDataType.fromTypeId(PDataType.VECTOR_FLOAT_TYPE));
    assertSame(PVectorDouble.INSTANCE, PDataType.fromTypeId(PDataType.VECTOR_DOUBLE_TYPE));
    assertEquals(4001, PVectorFloat.INSTANCE.getSqlType());
    assertEquals(4002, PVectorDouble.INSTANCE.getSqlType());
  }

  @Test
  public void testFactoryTypeEnumeration() {
    PDataTypeFactory factory = PDataTypeFactory.getInstance();
    assertTrue(factory.getTypes().contains(PVectorFloat.INSTANCE));
    assertTrue(factory.getTypes().contains(PVectorDouble.INSTANCE));
  }

  /**
   * SQL type ids and serialization ordinals identify a type across versions, so the vector types
   * must hold fixed values that collide with no other registered type.
   */
  @Test
  public void testTypeIdentifiersAreUniqueAndStable() {
    assertEquals(51, PVectorFloat.INSTANCE.ordinal());
    assertEquals(52, PVectorDouble.INSTANCE.ordinal());
    java.util.Set<Integer> sqlTypes = new java.util.HashSet<>();
    java.util.Set<Integer> ordinals = new java.util.HashSet<>();
    for (PDataType<?> type : PDataTypeFactory.getInstance().getTypes()) {
      assertTrue("duplicate sql type " + type.getSqlType() + " for " + type,
        sqlTypes.add(type.getSqlType()));
      assertTrue("duplicate ordinal " + type.ordinal() + " for " + type,
        ordinals.add(type.ordinal()));
    }
  }

  @Test
  public void testSqlTypeNameResolution() {
    PDataTypeFactory factory = PDataTypeFactory.getInstance();
    assertSame(PVectorFloat.INSTANCE, PDataType.fromSqlTypeName("VECTOR(FLOAT)"));
    assertSame(PVectorFloat.INSTANCE, PDataType.fromSqlTypeName("vector(float)"));
    assertSame(PVectorDouble.INSTANCE, PDataType.fromSqlTypeName("VECTOR(DOUBLE)"));
    assertSame(PVectorFloat.INSTANCE, factory.typeForVector("float"));
    assertSame(PVectorDouble.INSTANCE, factory.typeForVector("DOUBLE"));
    assertNull(factory.typeForVector("INTEGER"));
  }

  @Test
  public void testVectorFloatProperties() {
    PVectorFloat type = PVectorFloat.INSTANCE;
    assertTrue(type.isVectorType());
    assertTrue(type.isFixedWidth());
    assertNull(type.getByteSize());
    assertFalse(type.isComparisonSupported());

    assertEquals(Integer.valueOf(12), type.estimateByteSizeFromLength(3));
    assertTrue(type.isCoercibleTo(PFloatArray.INSTANCE));
    assertFalse(type.isCoercibleTo(PVarchar.INSTANCE));
    assertTrue(type.isCastableTo(PFloatArray.INSTANCE));
    assertTrue(type.isCastableTo(PVarchar.INSTANCE));
  }

  @Test
  public void testVectorFloatSerializationRoundTrip() {
    PVectorFloat type = PVectorFloat.INSTANCE;
    float[] original = new float[] { 1.0f, -2.5f, 3.14159f };
    byte[] bytes = type.toBytes(original);
    assertEquals(original.length * Bytes.SIZEOF_FLOAT, bytes.length);

    float[] deserialized = (float[]) type.toObject(bytes);
    assertArrayEquals(original, deserialized, 0.00001f);

    byte[] bytesDesc = type.toBytes(original, SortOrder.DESC);
    float[] deserializedDesc = (float[]) type.toObject(bytesDesc, 0, bytesDesc.length, type,
      SortOrder.DESC, original.length, null);
    assertArrayEquals(original, deserializedDesc, 0.00001f);
  }

  @Test
  public void testVectorFloatCompareTo() {
    PVectorFloat type = PVectorFloat.INSTANCE;
    float[] a = new float[] { 1.0f, 2.0f, 3.0f };
    float[] b = new float[] { 1.0f, 2.0f, 3.0f };
    float[] c = new float[] { 1.0f, 2.0f, 4.0f };
    float[] d = new float[] { 1.0f, 2.0f };

    assertEquals(0, type.compareTo(a, b, type));
    assertTrue(type.compareTo(a, c, type) < 0);
    assertTrue(type.compareTo(c, a, type) > 0);
    assertTrue(type.compareTo(a, d, type) > 0);
    assertTrue(type.compareTo(d, a, type) < 0);
  }

  @Test
  public void testVectorFloatCoerceBytes() {
    PVectorFloat type = PVectorFloat.INSTANCE;
    float[] original = new float[] { 1.0f, 2.0f, 3.0f };
    byte[] ascBytes = type.toBytes(original, SortOrder.ASC);
    ImmutableBytesWritable ptr = new ImmutableBytesWritable(ascBytes);

    type.coerceBytes(ptr, null, type, 3, null, SortOrder.ASC, 3, null, SortOrder.DESC);
    float[] deserialized = (float[]) type.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(),
      type, SortOrder.DESC, 3, null);
    assertArrayEquals(original, deserialized, 0.00001f);

    try {
      type.coerceBytes(ptr, null, type, 3, null, SortOrder.DESC, 4, null, SortOrder.ASC);
      fail("Should have thrown ConstraintViolationException on dimension mismatch");
    } catch (ConstraintViolationException expected) {
    }
  }

  @Test
  public void testVectorFloatIsSizeCompatible() {
    PVectorFloat type = PVectorFloat.INSTANCE;
    float[] vector = new float[] { 1.0f, 2.0f, 3.0f };
    byte[] bytes = type.toBytes(vector);
    ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);

    assertTrue(type.isSizeCompatible(ptr, vector, type, SortOrder.ASC, 3, null, 3, null));
    try {
      type.isSizeCompatible(ptr, vector, type, SortOrder.ASC, 3, null, 4, null);
      fail("Should have thrown ConstraintViolationException on dimension mismatch");
    } catch (ConstraintViolationException expected) {
    }
  }

  @Test
  public void testVectorFloatStringConversion() {
    PVectorFloat type = PVectorFloat.INSTANCE;
    float[] vector = new float[] { 1.0f, 2.5f, -3.0f };
    assertEquals("[1.0, 2.5, -3.0]", type.toStringLiteral(vector, null));

    float[] parsed1 = (float[]) type.toObject("[1.0, 2.5, -3.0]");
    assertArrayEquals(vector, parsed1, 0.00001f);

    float[] parsed2 = (float[]) type.toObject("1.0, 2.5, -3.0");
    assertArrayEquals(vector, parsed2, 0.00001f);

    float[] parsedEmpty = (float[]) type.toObject("[]");
    assertEquals(0, parsedEmpty.length);
  }

  @Test
  public void testVectorDoubleProperties() {
    PVectorDouble type = PVectorDouble.INSTANCE;
    assertTrue(type.isVectorType());
    assertTrue(type.isFixedWidth());
    assertNull(type.getByteSize());
    assertEquals(Integer.valueOf(24), type.estimateByteSizeFromLength(3));
    assertFalse(type.isComparisonSupported());

    assertEquals(Integer.valueOf(24), type.estimateByteSizeFromLength(3));
    assertTrue(type.isCoercibleTo(PDoubleArray.INSTANCE));
    assertFalse(type.isCoercibleTo(PVarchar.INSTANCE));
    assertTrue(type.isCastableTo(PDoubleArray.INSTANCE));
    assertTrue(type.isCastableTo(PVarchar.INSTANCE));
  }

  @Test
  public void testCanBePrimaryKeyReturnsFalse() {
    assertFalse(PVectorFloat.INSTANCE.canBePrimaryKey());
    assertFalse(PVectorDouble.INSTANCE.canBePrimaryKey());
  }

  @Test
  public void testVectorDoubleSerializationRoundTrip() {
    PVectorDouble type = PVectorDouble.INSTANCE;
    double[] original = new double[] { 1.0, -2.5, 3.141592653589793 };
    byte[] bytes = type.toBytes(original);
    assertEquals(original.length * Bytes.SIZEOF_DOUBLE, bytes.length);

    double[] deserialized = (double[]) type.toObject(bytes);
    assertArrayEquals(original, deserialized, 0.0000001);

    byte[] bytesDesc = type.toBytes(original, SortOrder.DESC);
    double[] deserializedDesc = (double[]) type.toObject(bytesDesc, 0, bytesDesc.length, type,
      SortOrder.DESC, original.length, null);
    assertArrayEquals(original, deserializedDesc, 0.0000001);
  }

  @Test
  public void testVectorDoubleCompareTo() {
    PVectorDouble type = PVectorDouble.INSTANCE;
    double[] a = new double[] { 1.0, 2.0, 3.0 };
    double[] b = new double[] { 1.0, 2.0, 3.0 };
    double[] c = new double[] { 1.0, 2.0, 4.0 };
    double[] d = new double[] { 1.0, 2.0 };

    assertEquals(0, type.compareTo(a, b, type));
    assertTrue(type.compareTo(a, c, type) < 0);
    assertTrue(type.compareTo(c, a, type) > 0);
    assertTrue(type.compareTo(a, d, type) > 0);
    assertTrue(type.compareTo(d, a, type) < 0);
  }

  @Test
  public void testVectorDoubleCoerceBytes() {
    PVectorDouble type = PVectorDouble.INSTANCE;
    double[] original = new double[] { 1.0, 2.0, 3.0 };
    byte[] ascBytes = type.toBytes(original, SortOrder.ASC);
    ImmutableBytesWritable ptr = new ImmutableBytesWritable(ascBytes);

    type.coerceBytes(ptr, null, type, 3, null, SortOrder.ASC, 3, null, SortOrder.DESC);
    double[] deserialized = (double[]) type.toObject(ptr.get(), ptr.getOffset(), ptr.getLength(),
      type, SortOrder.DESC, 3, null);
    assertArrayEquals(original, deserialized, 0.0000001);

    try {
      type.coerceBytes(ptr, null, type, 3, null, SortOrder.DESC, 4, null, SortOrder.ASC);
      fail("Should have thrown ConstraintViolationException on dimension mismatch");
    } catch (ConstraintViolationException expected) {
    }
  }

  @Test
  public void testVectorDoubleIsSizeCompatible() {
    PVectorDouble type = PVectorDouble.INSTANCE;
    double[] vector = new double[] { 1.0, 2.0, 3.0 };
    byte[] bytes = type.toBytes(vector);
    ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);

    assertTrue(type.isSizeCompatible(ptr, vector, type, SortOrder.ASC, 3, null, 3, null));
    try {
      type.isSizeCompatible(ptr, vector, type, SortOrder.ASC, 3, null, 4, null);
      fail("Should have thrown ConstraintViolationException on dimension mismatch");
    } catch (ConstraintViolationException expected) {
    }
  }

  @Test
  public void testVectorDoubleStringConversion() {
    PVectorDouble type = PVectorDouble.INSTANCE;
    double[] vector = new double[] { 1.0, 2.5, -3.0 };
    assertEquals("[1.0, 2.5, -3.0]", type.toStringLiteral(vector, null));

    double[] parsed1 = (double[]) type.toObject("[1.0, 2.5, -3.0]");
    assertArrayEquals(vector, parsed1, 0.0000001);

    double[] parsed2 = (double[]) type.toObject("1.0, 2.5, -3.0");
    assertArrayEquals(vector, parsed2, 0.0000001);

    double[] parsedEmpty = (double[]) type.toObject("[]");
    assertEquals(0, parsedEmpty.length);
  }

  @Test
  public void testVectorFloatProtobufRoundTrip() {
    PColumn column =
      new PColumnImpl(PNameFactory.newName("V"), PNameFactory.newName("0"), PVectorFloat.INSTANCE,
        128, null, true, 1, SortOrder.ASC, null, null, false, null, false, false, null, 0L);

    assertEquals(PNameFactory.newName("V"), column.getName());
    assertEquals(PNameFactory.newName("0"), column.getFamilyName());
    assertEquals(PVectorFloat.INSTANCE, column.getDataType());
    assertEquals(Integer.valueOf(128), column.getMaxLength());
    assertEquals(1, column.getPosition());
    assertTrue(column.isNullable());

    PTableProtos.PColumn proto = PColumnImpl.toProto(column);
    assertNotNull(proto);
    assertEquals("VECTOR(FLOAT)", proto.getDataType());
    assertEquals(128, proto.getMaxLength());

    PColumn deserialized = PColumnImpl.createFromProto(proto);
    assertNotNull(deserialized);
    assertEquals(column.getName(), deserialized.getName());
    assertEquals(column.getFamilyName(), deserialized.getFamilyName());
    assertEquals(PVectorFloat.INSTANCE, deserialized.getDataType());
    assertEquals(Integer.valueOf(128), deserialized.getMaxLength());
    assertEquals(column.getPosition(), deserialized.getPosition());
    assertEquals(column.isNullable(), deserialized.isNullable());
  }

  @Test
  public void testVectorDoubleProtobufRoundTrip() {
    PColumn column =
      new PColumnImpl(PNameFactory.newName("VD"), PNameFactory.newName("0"), PVectorDouble.INSTANCE,
        256, null, false, 2, SortOrder.ASC, null, null, false, null, false, false, null, 0L);

    assertEquals(PNameFactory.newName("VD"), column.getName());
    assertEquals(PNameFactory.newName("0"), column.getFamilyName());
    assertEquals(PVectorDouble.INSTANCE, column.getDataType());
    assertEquals(Integer.valueOf(256), column.getMaxLength());
    assertEquals(2, column.getPosition());
    assertFalse(column.isNullable());

    PTableProtos.PColumn proto = PColumnImpl.toProto(column);
    assertNotNull(proto);
    assertEquals("VECTOR(DOUBLE)", proto.getDataType());
    assertEquals(256, proto.getMaxLength());

    PColumn deserialized = PColumnImpl.createFromProto(proto);
    assertNotNull(deserialized);
    assertEquals(column.getName(), deserialized.getName());
    assertEquals(column.getFamilyName(), deserialized.getFamilyName());
    assertEquals(PVectorDouble.INSTANCE, deserialized.getDataType());
    assertEquals(Integer.valueOf(256), deserialized.getMaxLength());
    assertEquals(column.getPosition(), deserialized.getPosition());
    assertEquals(column.isNullable(), deserialized.isNullable());
  }

  @Test
  public void testFloatNativeEncodingRoundTrip() {
    float[] original = new float[] { 1.0f, -2.5f, Float.NaN, Float.POSITIVE_INFINITY, 0.0f };
    byte[] encoded = PVectorFloat.INSTANCE.toBytes(original);
    assertEquals(original.length * Bytes.SIZEOF_FLOAT, encoded.length);

    for (int i = 0; i < original.length; i++) {
      int rawBits =
        ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN).getInt(i * Bytes.SIZEOF_FLOAT);
      assertEquals("Element " + i + " bit pattern mismatch", Float.floatToRawIntBits(original[i]),
        rawBits);
    }

    float[] decoded = (float[]) PVectorFloat.INSTANCE.toObject(encoded, 0, encoded.length,
      PVectorFloat.INSTANCE, SortOrder.ASC, null, null);
    assertArrayEquals(original, decoded, 0.0f);
  }

  @Test
  public void testDoubleNativeEncodingRoundTrip() {
    double[] original = new double[] { 1.0, -2.5, Double.NaN, Double.NEGATIVE_INFINITY, 0.0 };
    byte[] encoded = PVectorDouble.INSTANCE.toBytes(original);
    assertEquals(original.length * Bytes.SIZEOF_DOUBLE, encoded.length);

    for (int i = 0; i < original.length; i++) {
      long rawBits =
        ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN).getLong(i * Bytes.SIZEOF_DOUBLE);
      assertEquals("Element " + i + " bit pattern mismatch",
        Double.doubleToRawLongBits(original[i]), rawBits);
    }

    double[] decoded = (double[]) PVectorDouble.INSTANCE.toObject(encoded, 0, encoded.length,
      PVectorDouble.INSTANCE, SortOrder.ASC, null, null);
    assertArrayEquals(original, decoded, 0.0);
  }

  @Test
  public void testFloatReadElementASC() {
    float[] vector = new float[] { 3.14f, -1.0f, 0.0f, Float.MAX_VALUE };
    byte[] bytes = PVectorFloat.INSTANCE.toBytes(vector);

    for (int i = 0; i < vector.length; i++) {
      float read = PVectorFloat.readElement(bytes, 0, i);
      assertEquals("readElement[" + i + "]", vector[i], read, 0.0f);
    }
  }

  @Test
  public void testFloatReadElementDESC() {
    float[] vector = new float[] { 1.0f, 2.0f, 3.0f };
    byte[] bytes = PVectorFloat.INSTANCE.toBytes(vector, SortOrder.DESC);

    for (int i = 0; i < vector.length; i++) {
      float read = PVectorFloat.readElement(bytes, 0, i, SortOrder.DESC);
      assertEquals("readElement DESC[" + i + "]", vector[i], read, 1e-5f);
    }
  }

  @Test
  public void testDoubleReadElementASC() {
    double[] vector = new double[] { Math.PI, -Math.E, 0.0, Double.MAX_VALUE };
    byte[] bytes = PVectorDouble.INSTANCE.toBytes(vector);

    for (int i = 0; i < vector.length; i++) {
      double read = PVectorDouble.readElement(bytes, 0, i);
      assertEquals("readElement[" + i + "]", vector[i], read, 0.0);
    }
  }

  @Test
  public void testFloatWriteElementsAndReadElements() {
    float[] vector = new float[] { 10.0f, 20.0f, 30.0f, 40.0f };
    byte[] buf = new byte[vector.length * Bytes.SIZEOF_FLOAT + 4]; // Offset padding
    int offset = 4;
    PVectorFloat.writeElements(vector, buf, offset);

    float[] decoded = PVectorFloat.readElements(buf, offset, vector.length * Bytes.SIZEOF_FLOAT);
    assertArrayEquals(vector, decoded, 0.0f);
  }

  @Test
  public void testDoubleWriteElementsAndReadElements() {
    double[] vector = new double[] { 100.0, 200.0, 300.0 };
    byte[] buf = new byte[vector.length * Bytes.SIZEOF_DOUBLE + 8];
    int offset = 8;
    PVectorDouble.writeElements(vector, buf, offset);

    double[] decoded = PVectorDouble.readElements(buf, offset, vector.length * Bytes.SIZEOF_DOUBLE);
    assertArrayEquals(vector, decoded, 0.0);
  }

  @Test
  public void testFloatReadElementsDescRoundTrip() {
    float[] original = new float[] { 1.0f, 2.0f, 3.0f };
    byte[] descBytes = PVectorFloat.INSTANCE.toBytes(original, SortOrder.DESC);

    float[] decoded = PVectorFloat.readElements(descBytes, 0, descBytes.length, SortOrder.DESC);
    assertArrayEquals(original, decoded, 1e-5f);
  }

  @Test
  public void testDoubleReadElementsDescRoundTrip() {
    double[] original = new double[] { 1.0, 2.0, 3.0 };
    byte[] descBytes = PVectorDouble.INSTANCE.toBytes(original, SortOrder.DESC);

    double[] decoded = PVectorDouble.readElements(descBytes, 0, descBytes.length, SortOrder.DESC);
    assertArrayEquals(original, decoded, 1e-7);
  }

  @Test
  public void testZeroCopyL2DistancePattern() {
    // Direct buffer Euclidean distance calculation
    float[] a = { 3.0f, 0.0f };
    float[] b = { 0.0f, 4.0f };

    byte[] bytesA = PVectorFloat.INSTANCE.toBytes(a);
    byte[] bytesB = PVectorFloat.INSTANCE.toBytes(b);
    int dim = bytesA.length / Bytes.SIZEOF_FLOAT;

    double sumSq = 0.0;
    for (int i = 0; i < dim; i++) {
      float ai = PVectorFloat.readElement(bytesA, 0, i);
      float bi = PVectorFloat.readElement(bytesB, 0, i);
      double d = ai - bi;
      sumSq += d * d;
    }
    assertEquals(5.0, Math.sqrt(sumSq), 1e-6);
  }

  @Test
  public void testZeroCopyL2DistancePatternDouble() {
    // Direct buffer Euclidean distance calculation for double vectors
    double[] a = { 3.0, 0.0 };
    double[] b = { 0.0, 4.0 };

    byte[] bytesA = PVectorDouble.INSTANCE.toBytes(a);
    byte[] bytesB = PVectorDouble.INSTANCE.toBytes(b);
    int dim = bytesA.length / Bytes.SIZEOF_DOUBLE;

    double sumSq = 0.0;
    for (int i = 0; i < dim; i++) {
      double ai = PVectorDouble.readElement(bytesA, 0, i);
      double bi = PVectorDouble.readElement(bytesB, 0, i);
      double d = ai - bi;
      sumSq += d * d;
    }
    assertEquals(5.0, Math.sqrt(sumSq), 1e-9);
  }

  @Test
  public void testWriteElementsBufferTooSmall() {
    float[] vector = new float[] { 1.0f, 2.0f, 3.0f };
    byte[] tooSmall = new byte[8];
    try {
      PVectorFloat.writeElements(vector, tooSmall, 0);
      fail("Should throw on buffer too small");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testDoubleWriteElementsBufferTooSmall() {
    double[] vector = new double[] { 1.0, 2.0 };
    byte[] tooSmall = new byte[8];
    try {
      PVectorDouble.writeElements(vector, tooSmall, 0);
      fail("Should throw on buffer too small");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testNumericArraysOfAnyElementTypeConvert() throws Exception {
    // Numeric array coercion across element types and widths
    PhoenixArray ints =
      PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Integer[] { 1, 0, -2 });
    assertArrayEquals(new float[] { 1f, 0f, -2f },
      (float[]) PVectorFloat.INSTANCE.toObject(ints, PIntegerArray.INSTANCE), 0f);
    assertArrayEquals(new double[] { 1, 0, -2 },
      (double[]) PVectorDouble.INSTANCE.toObject(ints, PIntegerArray.INSTANCE), 0);
    assertArrayEquals(new float[] { 1.5f, 2.5f },
      (float[]) PVectorFloat.INSTANCE.toObject(new double[] { 1.5, 2.5 }, PDoubleArray.INSTANCE),
      0f);
    assertArrayEquals(new double[] { 1.5, 2.5 },
      (double[]) PVectorDouble.INSTANCE.toObject(new float[] { 1.5f, 2.5f }, PFloatArray.INSTANCE),
      0);
    assertArrayEquals(new float[] { 1f, 2f },
      (float[]) PVectorFloat.INSTANCE.toObject(new Long[] { 1L, 2L }, PLongArray.INSTANCE), 0f);
    assertEquals(Integer.valueOf(3), PVectorFloat.INSTANCE.getMaxLength(ints));
    assertTrue(PIntegerArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
    assertTrue(PDecimalArray.INSTANCE.isCoercibleTo(PVectorDouble.INSTANCE));
    assertFalse(PVarcharArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testNullElementsRejected() {
    try {
      PVectorFloat.INSTANCE.toObject(new Float[] { 1f, null, 3f }, PFloatArray.INSTANCE);
      fail("A NULL element must not be stored as zero");
    } catch (ConstraintViolationException expected) {
    }
    try {
      PVectorDouble.INSTANCE.toObject(new Object[] { 1.0, null }, PDoubleArray.INSTANCE);
      fail("A NULL element must not be stored as zero");
    } catch (ConstraintViolationException expected) {
    }
  }
}
