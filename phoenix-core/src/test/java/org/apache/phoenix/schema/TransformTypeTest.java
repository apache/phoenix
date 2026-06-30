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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import org.apache.phoenix.schema.PTable.TransformType;
import org.junit.Test;

/**
 * Unit tests for {@link PTable.TransformType#fromSerializedValue(int)}. Verifies that known
 * serialized values still resolve to their constants and that unrecognised values return the
 * {@link PTable.TransformType#UNKNOWN} sentinel rather than throwing — this is the forward-
 * compatibility guarantee required when an older binary deserializes a SYSTEM.TRANSFORM row written
 * by a newer binary that introduced a new transform type.
 */
public class TransformTypeTest {

  @Test
  public void fromSerializedValueResolvesMetadataTransform() {
    assertSame(TransformType.METADATA_TRANSFORM, TransformType.fromSerializedValue(1));
  }

  @Test
  public void fromSerializedValueResolvesMetadataTransformPartial() {
    assertSame(TransformType.METADATA_TRANSFORM_PARTIAL, TransformType.fromSerializedValue(2));
  }

  @Test
  public void fromSerializedValueReturnsUnknownForFutureValue() {
    // Simulates a SYSTEM.TRANSFORM row written by a newer binary that introduced a third type.
    TransformType resolved = TransformType.fromSerializedValue(3);
    assertNotNull(resolved);
    assertSame(TransformType.UNKNOWN, resolved);
  }

  @Test
  public void fromSerializedValueReturnsUnknownForZero() {
    // Zero is not a valid serialized value for any known transform type.
    assertSame(TransformType.UNKNOWN, TransformType.fromSerializedValue(0));
  }

  @Test
  public void fromSerializedValueReturnsUnknownForNegativeNonSentinel() {
    assertSame(TransformType.UNKNOWN, TransformType.fromSerializedValue(-42));
  }

  @Test
  public void unknownSentinelIsSelfConsistent() {
    // The UNKNOWN constant's own serialized value (-1) also resolves to UNKNOWN: the lookup
    // table only contains known constants, and the sentinel is the fallback for everything else.
    assertSame(TransformType.UNKNOWN,
      TransformType.fromSerializedValue(TransformType.UNKNOWN.getSerializedValue()));
  }

  @Test
  public void unknownHasNonCollidingSerializedValue() {
    int unknownValue = TransformType.UNKNOWN.getSerializedValue();
    for (TransformType type : TransformType.values()) {
      if (type != TransformType.UNKNOWN) {
        assertEquals(false, type.getSerializedValue() == unknownValue);
      }
    }
  }

  @Test
  public void getDefaultIsNotUnknown() {
    assertSame(TransformType.METADATA_TRANSFORM, TransformType.getDefault());
  }
}
