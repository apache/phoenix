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

import java.util.Collections;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.DelegateTable;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;

/** Tests for vector data types, type factory resolution, column metadata, and serialization. */
public class VectorDataTypeTest {

  @Test
  public void testFactoryTypeForSqlType() {
    PDataTypeFactory factory = PDataTypeFactory.getInstance();
    assertSame(PVectorFloat.INSTANCE, factory.typeForSqlType(PDataType.VECTOR_FLOAT_TYPE));
    assertSame(PVectorDouble.INSTANCE, factory.typeForSqlType(PDataType.VECTOR_DOUBLE_TYPE));
    assertEquals(4001, PVectorFloat.INSTANCE.getSqlType());
    assertEquals(4002, PVectorDouble.INSTANCE.getSqlType());
  }

  @Test
  public void testFactoryTypeEnumeration() {
    PDataTypeFactory factory = PDataTypeFactory.getInstance();
    assertTrue(factory.getTypes().contains(PVectorFloat.INSTANCE));
    assertTrue(factory.getTypes().contains(PVectorDouble.INSTANCE));
  }

  @Test
  public void testFactoryTypeForSqlTypeName() {
    PDataTypeFactory factory = PDataTypeFactory.getInstance();
    assertSame(PVectorFloat.INSTANCE, factory.typeForSqlTypeName("VECTOR(FLOAT)"));
    assertSame(PVectorFloat.INSTANCE, factory.typeForSqlTypeName("vector(float)"));
    assertSame(PVectorDouble.INSTANCE, factory.typeForSqlTypeName("VECTOR(DOUBLE)"));
    assertSame(PVectorDouble.INSTANCE, factory.typeForSqlTypeName("vector(double)"));
    assertSame(PVectorFloat.INSTANCE, factory.typeForSqlTypeName("VECTOR"));
    assertSame(PVectorFloat.INSTANCE, factory.typeForSqlTypeName("vector"));
  }

  @Test
  public void testVectorFloatProperties() {
    PVectorFloat type = PVectorFloat.INSTANCE;
    assertTrue(type.isVectorType());
    assertTrue(type.isFixedWidth());
    assertNull(type.getByteSize());
    assertEquals(12, type.getByteSize(3));
    assertEquals(512, type.getByteSize(128));

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
  public void testVectorFloatToBytesWithDimension() {
    PVectorFloat type = PVectorFloat.INSTANCE;
    float[] original = new float[] { 1.0f, 2.0f, 3.0f };
    byte[] bytes = type.toBytes(original, SortOrder.ASC, 3);
    assertEquals(12, bytes.length);

    try {
      type.toBytes(original, SortOrder.ASC, 4);
      fail("Should have thrown ConstraintViolationException on dimension mismatch");
    } catch (ConstraintViolationException expected) {
    }
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
    assertEquals(24, type.getByteSize(3));
    assertEquals(1024, type.getByteSize(128));

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
  public void testVectorDoubleToBytesWithDimension() {
    PVectorDouble type = PVectorDouble.INSTANCE;
    double[] original = new double[] { 1.0, 2.0, 3.0 };
    byte[] bytes = type.toBytes(original, SortOrder.ASC, 3);
    assertEquals(24, bytes.length);

    try {
      type.toBytes(original, SortOrder.ASC, 5);
      fail("Should have thrown ConstraintViolationException on dimension mismatch");
    } catch (ConstraintViolationException expected) {
    }
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
    PColumn column = new PColumnImpl.Builder().setName(PNameFactory.newName("V"))
      .setFamilyName(PNameFactory.newName("0")).setDataType(PVectorFloat.INSTANCE).setMaxLength(128)
      .setPosition(1).setNullable(true).build();

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
    PColumn column = new PColumnImpl.Builder().setName(PNameFactory.newName("VD"))
      .setFamilyName(PNameFactory.newName("0")).setDataType(PVectorDouble.INSTANCE)
      .setMaxLength(256).setPosition(2).setNullable(false).build();

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
  public void testBuilderCopyConstructor() {
    PColumn original = new PColumnImpl.Builder().setName(PNameFactory.newName("V"))
      .setFamilyName(PNameFactory.newName("F")).setDataType(PVectorFloat.INSTANCE).setMaxLength(512)
      .setPosition(3).setNullable(true).setSortOrder(SortOrder.DESC).build();

    PColumn copy = new PColumnImpl.Builder(original).setMaxLength(1024).build();

    assertEquals(original.getName(), copy.getName());
    assertEquals(original.getFamilyName(), copy.getFamilyName());
    assertEquals(original.getDataType(), copy.getDataType());
    assertEquals(Integer.valueOf(1024), copy.getMaxLength());
    assertEquals(original.getPosition(), copy.getPosition());
    assertEquals(original.isNullable(), copy.isNullable());
    assertEquals(SortOrder.DESC, copy.getSortOrder());
  }

  @Test
  public void testFloatNativeEncodingRoundTrip() {
    float[] original = new float[] { 1.0f, -2.5f, Float.NaN, Float.POSITIVE_INFINITY, 0.0f };
    byte[] encoded = PVectorFloat.INSTANCE.toBytes(original);
    assertEquals(original.length * Bytes.SIZEOF_FLOAT, encoded.length);

    for (int i = 0; i < original.length; i++) {
      int rawBits = Bytes.toInt(encoded, i * Bytes.SIZEOF_FLOAT);
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
      long rawBits = Bytes.toLong(encoded, i * Bytes.SIZEOF_DOUBLE);
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
    byte[] buf = new byte[vector.length * Bytes.SIZEOF_FLOAT + 4]; // extra prefix bytes
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
  public void testFloatDecodeCentroid() {
    float[] centroid = new float[] { 0.5f, 1.5f, -0.5f };
    byte[] bytes = PVectorFloat.INSTANCE.toBytes(centroid);

    float[] decoded = PVectorFloat.decodeCentroid(bytes, 0, bytes.length, SortOrder.ASC);
    assertArrayEquals(centroid, decoded, 1e-5f);

    ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
    float[] decodedPtr = PVectorFloat.decodeCentroid(ptr, SortOrder.ASC);
    assertArrayEquals(centroid, decodedPtr, 1e-5f);

    assertNull(PVectorFloat.decodeCentroid((ImmutableBytesWritable) null, SortOrder.ASC));
    assertNull(PVectorFloat.decodeCentroid(new ImmutableBytesWritable(new byte[0]), SortOrder.ASC));
  }

  @Test
  public void testDoubleDecodeCentroid() {
    double[] centroid = new double[] { 0.1, 0.2, 0.3 };
    byte[] bytes = PVectorDouble.INSTANCE.toBytes(centroid);

    double[] decoded = PVectorDouble.decodeCentroid(bytes, 0, bytes.length, SortOrder.ASC);
    assertArrayEquals(centroid, decoded, 1e-7);

    ImmutableBytesWritable ptr = new ImmutableBytesWritable(bytes);
    double[] decodedPtr = PVectorDouble.decodeCentroid(ptr, SortOrder.ASC);
    assertArrayEquals(centroid, decodedPtr, 1e-7);
  }

  @Test
  public void testZeroCopyL2DistancePattern() {
    // Verify Euclidean distance computation directly on serialized float vector buffers.
    float[] a = { 3.0f, 0.0f };
    float[] b = { 0.0f, 4.0f };

    byte[] bytesA = PVectorFloat.INSTANCE.toBytes(a);
    byte[] bytesB = PVectorFloat.INSTANCE.toBytes(b);
    int dim = PVectorFloat.dimension(bytesA.length);

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
    // Verify Euclidean distance computation directly on serialized double vector buffers.
    double[] a = { 3.0, 0.0 };
    double[] b = { 0.0, 4.0 };

    byte[] bytesA = PVectorDouble.INSTANCE.toBytes(a);
    byte[] bytesB = PVectorDouble.INSTANCE.toBytes(b);
    int dim = PVectorDouble.dimension(bytesA.length);

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
  public void testPTableIsVectorIndexPredicate() throws Exception {
    PTable vectorTable = new PTableImpl.Builder().vectorIndexAlgorithm("IVF").build();
    assertTrue("Expected isVectorIndex to be true", vectorTable.isVectorIndex());
    assertEquals("IVF", vectorTable.getVectorIndexAlgorithm());

    PTable nonVectorTable = new PTableImpl.Builder().build();
    assertFalse("Expected isVectorIndex to be false", nonVectorTable.isVectorIndex());
    assertNull(nonVectorTable.getVectorIndexAlgorithm());
  }

  @Test
  public void testPTableVectorMetadataAccessors() throws Exception {
    PTable table = new PTableImpl.Builder().vectorIndexAlgorithm("IVF")
      .vectorDistanceMetric("COSINE").vectorDimension(128).vectorIvfLists(64)
      .vectorIvfSampleSize(2048).vectorCentroidGeneration(1001L).build();

    assertTrue(table.isVectorIndex());
    assertEquals("IVF", table.getVectorIndexAlgorithm());
    assertEquals("COSINE", table.getVectorDistanceMetric());
    assertEquals(Integer.valueOf(128), table.getVectorDimension());
    assertEquals(Integer.valueOf(64), table.getVectorIvfLists());
    assertEquals(Integer.valueOf(2048), table.getVectorIvfSampleSize());
    assertEquals(Long.valueOf(1001L), table.getVectorCentroidGeneration());
  }

  @Test
  public void testPTableSerializationRoundTrip() throws Exception {
    PTable original = new PTableImpl.Builder().setType(PTableType.INDEX)
      .setName(PNameFactory.newName("IDX_VEC")).setTableName(PNameFactory.newName("IDX_VEC"))
      .setParentTableName(PNameFactory.newName("DATA_TBL")).setAllColumns(Collections.emptyList())
      .setPkColumns(Collections.emptyList()).setIndexes(Collections.emptyList())
      .setPhysicalNames(Collections.emptyList()).vectorIndexAlgorithm("IVF")
      .vectorDistanceMetric("L2").vectorDimension(256).vectorIvfLists(32).vectorIvfSampleSize(1024)
      .vectorCentroidGeneration(42L).build();

    PTableProtos.PTable proto = PTableImpl.toProto(original);
    assertNotNull(proto);
    assertTrue(proto.hasVectorIndexAlgorithm());
    assertEquals("IVF", proto.getVectorIndexAlgorithm());
    assertTrue(proto.hasVectorDistanceMetric());
    assertEquals("L2", proto.getVectorDistanceMetric());
    assertTrue(proto.hasVectorDimension());
    assertEquals(256, proto.getVectorDimension());
    assertTrue(proto.hasVectorIvfLists());
    assertEquals(32, proto.getVectorIvfLists());
    assertTrue(proto.hasVectorIvfSampleSize());
    assertEquals(1024, proto.getVectorIvfSampleSize());
    assertTrue(proto.hasVectorCentroidGeneration());
    assertEquals(42L, proto.getVectorCentroidGeneration());

    PTable deserialized = PTableImpl.fromProto(proto);
    assertNotNull(deserialized);
    assertTrue(deserialized.isVectorIndex());
    assertEquals("IVF", deserialized.getVectorIndexAlgorithm());
    assertEquals("L2", deserialized.getVectorDistanceMetric());
    assertEquals(Integer.valueOf(256), deserialized.getVectorDimension());
    assertEquals(Integer.valueOf(32), deserialized.getVectorIvfLists());
    assertEquals(Integer.valueOf(1024), deserialized.getVectorIvfSampleSize());
    assertEquals(Long.valueOf(42L), deserialized.getVectorCentroidGeneration());
  }

  @Test
  public void testPTableBuilderFromExisting() throws Exception {
    PTable original = new PTableImpl.Builder().setType(PTableType.INDEX)
      .setName(PNameFactory.newName("IDX_VEC")).setTableName(PNameFactory.newName("IDX_VEC"))
      .setParentTableName(PNameFactory.newName("DATA_TBL"))
      .setPhysicalNames(Collections.emptyList()).vectorIndexAlgorithm("DISKANN")
      .vectorDistanceMetric("INNER_PRODUCT").vectorDimension(512).vectorIvfLists(128)
      .vectorIvfSampleSize(4096).vectorCentroidGeneration(777L).build();

    PTable cloned = PTableImpl.builderFromExisting(original).build();
    assertTrue(cloned.isVectorIndex());
    assertEquals("DISKANN", cloned.getVectorIndexAlgorithm());
    assertEquals("INNER_PRODUCT", cloned.getVectorDistanceMetric());
    assertEquals(Integer.valueOf(512), cloned.getVectorDimension());
    assertEquals(Integer.valueOf(128), cloned.getVectorIvfLists());
    assertEquals(Integer.valueOf(4096), cloned.getVectorIvfSampleSize());
    assertEquals(Long.valueOf(777L), cloned.getVectorCentroidGeneration());
  }

  @Test
  public void testDelegateTableVectorMetadata() throws Exception {
    PTable inner = new PTableImpl.Builder().vectorIndexAlgorithm("IVF")
      .vectorDistanceMetric("COSINE").vectorDimension(64).vectorIvfLists(16)
      .vectorIvfSampleSize(512).vectorCentroidGeneration(99L).build();

    DelegateTable delegate = new DelegateTable(inner);
    assertTrue(delegate.isVectorIndex());
    assertEquals("IVF", delegate.getVectorIndexAlgorithm());
    assertEquals("COSINE", delegate.getVectorDistanceMetric());
    assertEquals(Integer.valueOf(64), delegate.getVectorDimension());
    assertEquals(Integer.valueOf(16), delegate.getVectorIvfLists());
    assertEquals(Integer.valueOf(512), delegate.getVectorIvfSampleSize());
    assertEquals(Long.valueOf(99L), delegate.getVectorCentroidGeneration());
  }

  @Test
  public void testPTableSerializationRoundTripWithoutVectorFields() throws Exception {
    PTable original = new PTableImpl.Builder().setType(PTableType.TABLE)
      .setName(PNameFactory.newName("NON_VECTOR")).setTableName(PNameFactory.newName("NON_VECTOR"))
      .setAllColumns(Collections.emptyList()).setPkColumns(Collections.emptyList())
      .setIndexes(Collections.emptyList()).setPhysicalNames(Collections.emptyList()).build();

    assertFalse("Non-vector table must not be a vector index", original.isVectorIndex());

    PTableProtos.PTable proto = PTableImpl.toProto(original);
    assertNotNull(proto);
    assertFalse("Proto should not have vectorIndexAlgorithm", proto.hasVectorIndexAlgorithm());
    assertFalse("Proto should not have vectorDistanceMetric", proto.hasVectorDistanceMetric());
    assertFalse("Proto should not have vectorDimension", proto.hasVectorDimension());
    assertFalse("Proto should not have vectorIvfLists", proto.hasVectorIvfLists());
    assertFalse("Proto should not have vectorIvfSampleSize", proto.hasVectorIvfSampleSize());
    assertFalse("Proto should not have vectorCentroidGeneration",
      proto.hasVectorCentroidGeneration());

    PTable deserialized = PTableImpl.fromProto(proto);
    assertNotNull(deserialized);
    assertFalse("Deserialized non-vector table must not be a vector index",
      deserialized.isVectorIndex());
    assertNull("vectorIndexAlgorithm must be null", deserialized.getVectorIndexAlgorithm());
    assertNull("vectorDistanceMetric must be null", deserialized.getVectorDistanceMetric());
    assertNull("vectorDimension must be null", deserialized.getVectorDimension());
    assertNull("vectorIvfLists must be null", deserialized.getVectorIvfLists());
    assertNull("vectorIvfSampleSize must be null", deserialized.getVectorIvfSampleSize());
    assertNull("vectorCentroidGeneration must be null", deserialized.getVectorCentroidGeneration());
  }
}
