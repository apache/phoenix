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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.schema.PTable.IndexType;
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
}
