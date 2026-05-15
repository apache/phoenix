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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

public class PColumnImplVirtualFlagTest {

  private PColumnImpl makeCol(boolean isVirtual) {
    return new PColumnImpl(
        PNameFactory.newName("EXTRA"),
        PNameFactory.newName("0"),
        PVarchar.INSTANCE,
        null, null, true, 1,
        SortOrder.getDefault(), null, null,
        false, null, false, false,
        new byte[]{0, 0, 0, 0}, 0L, false, isVirtual);
  }

  @Test
  public void defaultIsFalse() {
    PColumnImpl col = makeCol(false);
    assertFalse(col.isVirtual());
  }

  @Test
  public void roundTripsTrueThroughProto() throws Exception {
    PColumnImpl orig = makeCol(true);
    PTableProtos.PColumn proto = PColumnImpl.toProto(orig);
    assertTrue(proto.getIsVirtual());
    PColumn copy = PColumnImpl.createFromProto(proto);
    assertTrue(copy.isVirtual());
  }

  @Test
  public void roundTripsFalseThroughProto() throws Exception {
    PColumnImpl orig = makeCol(false);
    PTableProtos.PColumn proto = PColumnImpl.toProto(orig);
    assertFalse(proto.getIsVirtual());
    PColumn copy = PColumnImpl.createFromProto(proto);
    assertFalse(copy.isVirtual());
  }

  @Test
  public void copyConstructorPreservesVirtual() {
    PColumnImpl orig = makeCol(true);
    PColumnImpl copy = new PColumnImpl(orig, orig.getPosition());
    assertTrue(copy.isVirtual());
  }
}
