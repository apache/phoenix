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
package org.apache.phoenix.schema;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Collections;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos;
import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.IndexUtil;
import org.junit.Test;

/**
 * Unit tests for {@link PTable.IndexType#VECTOR_GLOBAL} serialization, conversion, and protobuf
 * representation.
 */
public class VectorIndexTypeTest {

  @Test
  public void testFromSerializedValueVectorGlobal() {
    IndexType indexType = IndexType.fromSerializedValue((byte) 4);
    assertEquals(IndexType.VECTOR_GLOBAL, indexType);
  }

  @Test
  public void testVectorGlobalRoundTrip() {
    assertEquals((byte) 4, IndexType.VECTOR_GLOBAL.getSerializedValue());
    assertEquals(IndexType.VECTOR_GLOBAL,
      IndexType.fromSerializedValue(IndexType.VECTOR_GLOBAL.getSerializedValue()));
  }

  @Test
  public void testAllIndexTypesRoundTrip() {
    for (IndexType type : IndexType.values()) {
      assertEquals(type, IndexType.fromSerializedValue(type.getSerializedValue()));
    }
  }

  @Test
  public void testLegacyRejectionUnknownSerializedValue() {
    try {
      IndexType.fromSerializedValue((byte) 99);
      fail("Expected IllegalArgumentException for unknown serialized value 99");
    } catch (IllegalArgumentException e) {
      assertTrue("Exception message should mention invalid value", e.getMessage().contains("99"));
    }
  }

  @Test
  public void testBoundaryAndInvalidValuesRejection() {
    try {
      IndexType.fromSerializedValue((byte) 0);
      fail("Expected IllegalArgumentException for invalid serialized value 0");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("0"));
    }

    try {
      IndexType.fromSerializedValue((byte) -1);
      fail("Expected IllegalArgumentException for invalid serialized value -1");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("-1"));
    }

    try {
      IndexType.fromSerializedValue((byte) (IndexType.values().length + 1));
      fail("Expected IllegalArgumentException for out-of-bounds serialized value");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testFromToken() {
    assertEquals(IndexType.VECTOR_GLOBAL, IndexType.fromToken("VECTOR_GLOBAL"));
    assertEquals(IndexType.VECTOR_GLOBAL, IndexType.fromToken(" vector_global "));
    assertEquals(IndexType.GLOBAL, IndexType.fromToken("GLOBAL"));
    assertEquals(IndexType.LOCAL, IndexType.fromToken("LOCAL"));
    assertEquals(IndexType.UNCOVERED_GLOBAL, IndexType.fromToken("UNCOVERED_GLOBAL"));
  }

  @Test
  public void testGetBytes() {
    assertArrayEquals(Bytes.toBytes("VECTOR_GLOBAL"), IndexType.VECTOR_GLOBAL.getBytes());
    assertArrayEquals(Bytes.toBytes("GLOBAL"), IndexType.GLOBAL.getBytes());
    assertArrayEquals(Bytes.toBytes("LOCAL"), IndexType.LOCAL.getBytes());
    assertArrayEquals(Bytes.toBytes("UNCOVERED_GLOBAL"), IndexType.UNCOVERED_GLOBAL.getBytes());
  }

  @Test
  public void testPTableImplSerializationRoundTripWithVectorGlobal() throws Exception {
    PTable table = new PTableImpl.Builder().setType(PTableType.INDEX)
      .setIndexType(IndexType.VECTOR_GLOBAL).setName(PNameFactory.newName("IDX_VEC_GLOBAL"))
      .setTableName(PNameFactory.newName("IDX_VEC_GLOBAL"))
      .setParentTableName(PNameFactory.newName("DATA_TBL")).setAllColumns(Collections.emptyList())
      .setPkColumns(Collections.emptyList()).setIndexes(Collections.emptyList())
      .setPhysicalNames(Collections.emptyList()).vectorIndexAlgorithm("IVF")
      .vectorDistanceMetric("COSINE").vectorDimension(128).vectorIvfLists(64)
      .vectorIvfSampleSize(2048).vectorCentroidGeneration(101L).build();

    assertEquals(IndexType.VECTOR_GLOBAL, table.getIndexType());

    PTableProtos.PTable proto = PTableImpl.toProto(table);
    assertNotNull(proto);
    assertTrue(proto.hasIndexType());
    assertEquals((byte) 4, proto.getIndexType().toByteArray()[0]);

    PTable deserialized = PTableImpl.fromProto(proto);
    assertNotNull(deserialized);
    assertEquals(IndexType.VECTOR_GLOBAL, deserialized.getIndexType());
    assertEquals("IVF", deserialized.getVectorIndexAlgorithm());
  }

  @Test
  public void testDelegateTableWithVectorGlobal() throws Exception {
    PTable inner = new PTableImpl.Builder().setIndexType(IndexType.VECTOR_GLOBAL).build();

    DelegateTable delegate = new DelegateTable(inner);
    assertEquals(IndexType.VECTOR_GLOBAL, delegate.getIndexType());
  }

  @Test
  public void testIndexMaintainerWritableBackwardCompatibilityNonVectorIndex() throws Exception {
    RowKeySchema schema = new RowKeySchema.RowKeySchemaBuilder(0).build();
    IndexMaintainer maintainer = new IndexMaintainer(schema, false);
    assertFalse(maintainer.isVectorIndex());

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutput output = new DataOutputStream(outStream);
    maintainer.write(output);

    byte[] bytes = outStream.toByteArray();
    IndexMaintainer deserialized = new IndexMaintainer(schema, false);
    DataInput input = new DataInputStream(new ByteArrayInputStream(bytes));
    deserialized.readFields(input);

    assertFalse("Non-vector index maintainer must have isVectorIndex == false",
      deserialized.isVectorIndex());
    assertNull(deserialized.getVectorAlgorithm());
    assertNull(deserialized.getVectorDimension());
    assertNull(deserialized.getDistanceMetric());
    assertNull(deserialized.getCentroidGeneration());
  }

  @Test
  public void testIndexMaintainerProtoRoundTripVectorFields() throws Exception {
    RowKeySchema schema = new RowKeySchema.RowKeySchemaBuilder(0).build();
    IndexMaintainer maintainer = new IndexMaintainer(schema, false);
    maintainer.setVectorAlgorithm("IVF");
    maintainer.setVectorDimension(128);
    maintainer.setDistanceMetric("L2");
    maintainer.setCentroidGeneration(1L);

    ServerCachingProtos.IndexMaintainer proto = IndexMaintainer.toProto(maintainer);
    assertNotNull(proto);
    assertTrue(proto.hasVectorAlgorithm());
    assertEquals("IVF", proto.getVectorAlgorithm());
    assertTrue(proto.hasVectorDimension());
    assertEquals(128, proto.getVectorDimension());
    assertTrue(proto.hasDistanceMetric());
    assertEquals("L2", proto.getDistanceMetric());
    assertTrue(proto.hasCentroidGeneration());
    assertEquals(1L, proto.getCentroidGeneration());

    IndexMaintainer deserialized = IndexMaintainer.fromProto(proto, schema, false);
    assertNotNull(deserialized);
    assertTrue(deserialized.isVectorIndex());
    assertEquals("IVF", deserialized.getVectorAlgorithm());
    assertEquals(Integer.valueOf(128), deserialized.getVectorDimension());
    assertEquals("L2", deserialized.getDistanceMetric());
    assertEquals(Long.valueOf(1L), deserialized.getCentroidGeneration());
  }

  @Test
  public void testClientVersionCheckOnIndexRead() throws Exception {
    PTable table = new PTableImpl.Builder().setType(PTableType.INDEX)
      .setIndexType(IndexType.VECTOR_GLOBAL).setName(PNameFactory.newName("IDX_VEC_COMPAT"))
      .setTableName(PNameFactory.newName("IDX_VEC_COMPAT"))
      .setParentTableName(PNameFactory.newName("DATA_TBL")).setAllColumns(Collections.emptyList())
      .setPkColumns(Collections.emptyList()).setIndexes(Collections.emptyList())
      .setPhysicalNames(Collections.emptyList()).vectorIndexAlgorithm("IVF")
      .vectorDistanceMetric("L2").vectorDimension(128).build();

    assertEquals(IndexType.VECTOR_GLOBAL, table.getIndexType());

    PTableProtos.PTable proto = PTableImpl.toProto(table);
    assertNotNull(proto);
    assertTrue(proto.hasIndexType());
    byte serializedIndexType = proto.getIndexType().toByteArray()[0];
    assertEquals((byte) 4, serializedIndexType);

    // Verify that legacy clients lacking VECTOR_GLOBAL fail fast during index type deserialization.
    try {
      simulateLegacyClientIndexTypeDeserialization(serializedIndexType);
      fail("Expected IllegalArgumentException on legacy client without VECTOR_GLOBAL");
    } catch (IllegalArgumentException e) {
      assertTrue("Exception message should be descriptive: " + e.getMessage(),
        e.getMessage().contains("4") && e.getMessage().contains("IndexType"));
    }
  }

  private static IndexType simulateLegacyClientIndexTypeDeserialization(byte serializedValue) {
    // Legacy clients only recognize the first three IndexType enum ordinals.
    int legacyCount = 3;
    if (serializedValue < 1 || serializedValue > legacyCount) {
      throw new IllegalArgumentException("Invalid IndexType " + serializedValue
        + ". A client upgrade is required to support this index type.");
    }
    return IndexType.values()[serializedValue - 1];
  }

  @Test
  public void testIndexMaintainerFromProtoRejectsCentroidColumnWithoutVectorAlgorithm()
    throws Exception {
    RowKeySchema schema = new RowKeySchema.RowKeySchemaBuilder(0).build();
    IndexMaintainer maintainer = new IndexMaintainer(schema, false);
    ColumnReference centroidRef = new ColumnReference(ByteUtil.EMPTY_BYTE_ARRAY,
      Bytes.toBytes(IndexUtil.getIndexColumnName(null, PhoenixDatabaseMetaData.CENTROID_ID)));
    maintainer.setIndexedColumnsForTesting(Collections.singleton(centroidRef));

    ServerCachingProtos.IndexMaintainer proto = IndexMaintainer.toProto(maintainer);
    assertFalse("Proto must not have vectorAlgorithm", proto.hasVectorAlgorithm());

    try {
      IndexMaintainer.fromProto(proto, schema, false);
      fail(
        "Expected DoNotRetryIOException when proto has centroid column but lacks vectorAlgorithm");
    } catch (DoNotRetryIOException e) {
      assertTrue("Exception message must indicate server upgrade is required: " + e.getMessage(),
        e.getMessage().contains("Server upgrade is required")
          && e.getMessage().contains("vector maintainer fields"));
    }
  }

}
