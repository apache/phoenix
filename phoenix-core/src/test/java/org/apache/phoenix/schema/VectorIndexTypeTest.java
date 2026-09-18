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

import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.tool.SchemaExtractionProcessor;
import org.junit.Test;

/**
 * Tests {@link PTable.IndexType#VECTOR_GLOBAL} serialization, token conversion, and protobuf
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
      .setPhysicalNames(Collections.emptyList()).setVectorIndexAlgorithm("IVF")
      .setVectorDistanceMetric("COSINE").setVectorDimension(128).setVectorIvfLists(64)
      .setVectorIvfSampleSize(2048).setVectorCentroidGeneration(101L).build();

    assertEquals(IndexType.VECTOR_GLOBAL, table.getIndexType());

    PTableProtos.PTable proto = PTableImpl.toProto(table);
    assertNotNull(proto);
    assertTrue(proto.hasIndexType());
    assertEquals((byte) 4, proto.getIndexType().toByteArray()[0]);

    PTable deserialized = PTableImpl.createFromProto(proto);
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
  public void testPTableIsVectorIndexPredicate() throws Exception {
    PTable vectorTable = new PTableImpl.Builder().setIndexType(IndexType.VECTOR_GLOBAL)
      .setVectorIndexAlgorithm("IVF").build();
    assertTrue("Expected isVectorIndex to be true", vectorTable.isVectorIndex());
    assertEquals("IVF", vectorTable.getVectorIndexAlgorithm());

    PTable nonVectorTable = new PTableImpl.Builder().build();
    assertFalse("Expected isVectorIndex to be false", nonVectorTable.isVectorIndex());
    assertNull(nonVectorTable.getVectorIndexAlgorithm());
  }

  @Test
  public void testPTableVectorMetadataAccessors() throws Exception {
    PTable table =
      new PTableImpl.Builder().setIndexType(IndexType.VECTOR_GLOBAL).setVectorIndexAlgorithm("IVF")
        .setVectorDistanceMetric("COSINE").setVectorDimension(128).setVectorIvfLists(64)
        .setVectorIvfSampleSize(2048).setVectorCentroidGeneration(1001L).build();

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
      .setIndexType(IndexType.VECTOR_GLOBAL).setParentTableName(PNameFactory.newName("DATA_TBL"))
      .setAllColumns(Collections.emptyList()).setPkColumns(Collections.emptyList())
      .setIndexes(Collections.emptyList()).setPhysicalNames(Collections.emptyList())
      .setVectorIndexAlgorithm("IVF").setVectorDistanceMetric("L2").setVectorDimension(256)
      .setVectorIvfLists(32).setVectorIvfSampleSize(1024).setVectorCentroidGeneration(42L).build();

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

    PTable deserialized = PTableImpl.createFromProto(proto);
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
      .setIndexType(IndexType.VECTOR_GLOBAL).setParentTableName(PNameFactory.newName("DATA_TBL"))
      .setPhysicalNames(Collections.emptyList()).setVectorIndexAlgorithm("IVF")
      .setVectorDistanceMetric("INNER_PRODUCT").setVectorDimension(512).setVectorIvfLists(128)
      .setVectorIvfSampleSize(4096).setVectorCentroidGeneration(777L).build();

    PTable cloned = PTableImpl.builderFromExisting(original).build();
    assertTrue(cloned.isVectorIndex());
    assertEquals("IVF", cloned.getVectorIndexAlgorithm());
    assertEquals("INNER_PRODUCT", cloned.getVectorDistanceMetric());
    assertEquals(Integer.valueOf(512), cloned.getVectorDimension());
    assertEquals(Integer.valueOf(128), cloned.getVectorIvfLists());
    assertEquals(Integer.valueOf(4096), cloned.getVectorIvfSampleSize());
    assertEquals(Long.valueOf(777L), cloned.getVectorCentroidGeneration());
  }

  @Test
  public void testDelegateTableVectorMetadata() throws Exception {
    PTable inner = new PTableImpl.Builder().setIndexType(IndexType.VECTOR_GLOBAL)
      .setVectorIndexAlgorithm("IVF").setVectorDistanceMetric("COSINE").setVectorDimension(64)
      .setVectorIvfLists(16).setVectorIvfSampleSize(512).setVectorCentroidGeneration(99L).build();

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

    PTable deserialized = PTableImpl.createFromProto(proto);
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

  @Test
  public void testSchemaExtractionRejectsVectorIndex() throws Exception {
    PTable index = new PTableImpl.Builder().setType(PTableType.INDEX)
      .setIndexType(IndexType.VECTOR_GLOBAL).setName(PNameFactory.newName("IDX_VEC"))
      .setTableName(PNameFactory.newName("IDX_VEC")).setSchemaName(PNameFactory.newName(""))
      .setParentTableName(PNameFactory.newName("DATA_TBL")).build();
    try {
      new SchemaExtractionProcessor(null, new Configuration(), index, false).process();
      fail("Schema extraction cannot reproduce CREATE VECTOR INDEX and must say so");
    } catch (SQLFeatureNotSupportedException expected) {
    }
  }
}
