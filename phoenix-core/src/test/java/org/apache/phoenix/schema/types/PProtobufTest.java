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

import java.sql.SQLException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;

public class PProtobufTest {
  @Test
  public void instanceIsRegisteredAndDiscoverable() {
    assertNotNull("PProtobuf.INSTANCE must be initialized", PProtobuf.INSTANCE);
    assertSame("fromSqlTypeName(\"PROTOBUF\") must return the singleton", PProtobuf.INSTANCE,
      PDataType.fromSqlTypeName("PROTOBUF"));
    assertSame("fromTypeId(PROTOBUF_TYPE) must return the singleton", PProtobuf.INSTANCE,
      PDataType.fromTypeId(PDataType.PROTOBUF_TYPE));
    assertTrue("PDataTypeFactory must contain PProtobuf.INSTANCE",
      PDataTypeFactory.getInstance().getTypes().contains(PProtobuf.INSTANCE));
  }

  @Test
  public void slotIdMatchesSpec() {
    assertEquals(9500, PDataType.PROTOBUF_TYPE);
    assertEquals(9500, PProtobuf.INSTANCE.getSqlType());
    assertEquals("PROTOBUF", PProtobuf.INSTANCE.getSqlTypeName());
  }

  @Test
  public void cannotBePrimaryKeyOrCompared() {
    assertFalse("PROTOBUF must not be allowed as a primary key column",
      PProtobuf.INSTANCE.canBePrimaryKey());
    assertFalse("PROTOBUF must not support =,!=,<,>,<=,>= comparisons",
      PProtobuf.INSTANCE.isComparisonSupported());
  }

  @Test
  public void isVariableWidth() {
    assertFalse("PROTOBUF storage size is bytes.length, not fixed",
      PProtobuf.INSTANCE.isFixedWidth());
    assertNull("PROTOBUF has no fixed byte size", PProtobuf.INSTANCE.getByteSize());
  }

  @Test
  public void coercionRulesMatchSpec() {
    assertTrue(PProtobuf.INSTANCE.isCoercibleTo(PProtobuf.INSTANCE));
    assertTrue(PProtobuf.INSTANCE.isCoercibleTo(PBinary.INSTANCE));
    assertTrue(PProtobuf.INSTANCE.isCoercibleTo(PVarbinary.INSTANCE));
    assertFalse(PProtobuf.INSTANCE.isCoercibleTo(PVarbinaryEncoded.INSTANCE));
    assertFalse(PProtobuf.INSTANCE.isCoercibleTo(PVarchar.INSTANCE));
    assertFalse(PProtobuf.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
  }

  @Test
  public void bytesComparableOnlyWithBinaryFamily() {
    assertTrue(PProtobuf.INSTANCE.isBytesComparableWith(PProtobuf.INSTANCE));
    assertTrue(PProtobuf.INSTANCE.isBytesComparableWith(PVarbinary.INSTANCE));
    assertFalse(PProtobuf.INSTANCE.isBytesComparableWith(PVarchar.INSTANCE));
    assertFalse(PProtobuf.INSTANCE.isBytesComparableWith(PInteger.INSTANCE));
  }

  @Test
  public void bytePassthroughInheritedFromVarbinary() {
    byte[] payload =
      new byte[] { 0x08, 0x2a, 0x12, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, (byte) 0xff };
    byte[] encoded = PProtobuf.INSTANCE.toBytes(payload);
    assertArrayEquals("toBytes must pass payload through unchanged", payload, encoded);

    Object decoded = PProtobuf.INSTANCE.toObject(encoded, 0, encoded.length, PProtobuf.INSTANCE,
      SortOrder.ASC, null, null);
    assertArrayEquals("toObject must round-trip back to the same bytes", payload, (byte[]) decoded);
  }

  @Test
  public void nullCellEncodesToEmpty() {
    byte[] result = PProtobuf.INSTANCE.toBytes(null);
    assertNotNull(result);
    assertEquals(0, result.length);
  }

  @Test
  public void toBytesThreeArgWritesToDestNotInput() {
    byte[] payload = new byte[] { 0x08, 0x2a, 0x00, (byte) 0xff };
    byte[] dest = new byte[16];
    int written = PProtobuf.INSTANCE.toBytes(payload, dest, 0);
    assertEquals(payload.length, written);

    assertArrayEquals("toBytes(Object,byte[],int) must not modify the input payload",
      new byte[] { 0x08, 0x2a, 0x00, (byte) 0xff }, payload);

    assertEquals((byte) 0x08, dest[0]);
    assertEquals((byte) 0x2a, dest[1]);
    assertEquals((byte) 0x00, dest[2]);
    assertEquals((byte) 0xff, dest[3]);
    for (int i = 4; i < dest.length; i++) {
      assertEquals("dest tail must remain zero past the written prefix", (byte) 0x00, dest[i]);
    }
  }

  @Test
  public void toBytesThreeArgWithOffset() {
    byte[] payload = new byte[] { 0x11, 0x22, 0x33 };
    byte[] dest = new byte[8];
    int written = PProtobuf.INSTANCE.toBytes(payload, dest, 2);
    assertEquals(payload.length, written);
    assertEquals((byte) 0x00, dest[0]);
    assertEquals((byte) 0x00, dest[1]);
    assertEquals((byte) 0x11, dest[2]);
    assertEquals((byte) 0x22, dest[3]);
    assertEquals((byte) 0x33, dest[4]);
    assertEquals((byte) 0x00, dest[5]);
  }

  @Test
  public void toObjectFromStringIsRejected() {
    try {
      PProtobuf.INSTANCE.toObject("any-string-input");
      fail("Expected IllegalDataException for PROTOBUF.toObject(String)");
    } catch (IllegalDataException e) {
      Throwable cause = e.getCause();
      assertNotNull("IllegalDataException must wrap the SQLException cause", cause);
      assertTrue("cause should be a SQLException", cause instanceof SQLException);
      assertEquals(SQLExceptionCode.PROTOBUF_FROM_STRING_NOT_ALLOWED.getErrorCode(),
        ((SQLException) cause).getErrorCode());
    }
  }

  @Test
  public void toObjectFromNullOrEmptyStringReturnsNull() {
    assertNull(PProtobuf.INSTANCE.toObject((String) null));
    assertNull(PProtobuf.INSTANCE.toObject(""));
  }
}
