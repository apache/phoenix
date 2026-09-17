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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Tests coercion and casting rules between numeric array types and vector types. */
public class PArrayDataTypeVectorCoercionTest {

  @Test
  public void testFloatArrayCoercibleToVectorFloat() {
    assertTrue("FLOAT ARRAY should be coercible to VECTOR(FLOAT)",
      PFloatArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testFloatArrayCoercibleToVectorDouble() {
    assertTrue("FLOAT ARRAY should be coercible to VECTOR(DOUBLE)",
      PFloatArray.INSTANCE.isCoercibleTo(PVectorDouble.INSTANCE));
  }

  @Test
  public void testDoubleArrayCoercibleToVectorFloat() {
    assertTrue("DOUBLE ARRAY should be coercible to VECTOR(FLOAT)",
      PDoubleArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testDoubleArrayCoercibleToVectorDouble() {
    assertTrue("DOUBLE ARRAY should be coercible to VECTOR(DOUBLE)",
      PDoubleArray.INSTANCE.isCoercibleTo(PVectorDouble.INSTANCE));
  }

  @Test
  public void testIntegerArrayCoercibleToVector() {
    assertTrue(
      "INTEGER ARRAY should be coercible to VECTOR(FLOAT) " + "(integer is coercible to double)",
      PIntegerArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testDecimalArrayCoercibleToVector() {
    assertTrue("DECIMAL ARRAY should be coercible to VECTOR(FLOAT)",
      PDecimalArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testLongArrayCoercibleToVector() {
    assertTrue("BIGINT ARRAY should be coercible to VECTOR(FLOAT)",
      PLongArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testSmallintArrayCoercibleToVector() {
    assertTrue("SMALLINT ARRAY should be coercible to VECTOR(FLOAT)",
      PSmallintArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testTinyintArrayCoercibleToVector() {
    assertTrue("TINYINT ARRAY should be coercible to VECTOR(FLOAT)",
      PTinyintArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  /** Non-numeric array types cannot be coerced to vector types. */
  @Test
  public void testVarcharArrayNotCoercibleToVector() {
    assertFalse("VARCHAR ARRAY must not be coercible to VECTOR(FLOAT)",
      PVarcharArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testBooleanArrayNotCoercibleToVector() {
    assertFalse("BOOLEAN ARRAY must not be coercible to VECTOR(FLOAT)",
      PBooleanArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testBinaryArrayNotCoercibleToVector() {
    assertFalse("VARBINARY ARRAY must not be coercible to VECTOR(FLOAT)",
      PVarbinaryArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testDateArrayNotCoercibleToVector() {
    assertFalse("DATE ARRAY must not be coercible to VECTOR(FLOAT)",
      PDateArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testFloatArrayCastableToVectorFloat() {
    assertTrue("FLOAT ARRAY should be castable to VECTOR(FLOAT)",
      PFloatArray.INSTANCE.isCastableTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testDoubleArrayCastableToVectorDouble() {
    assertTrue("DOUBLE ARRAY should be castable to VECTOR(DOUBLE)",
      PDoubleArray.INSTANCE.isCastableTo(PVectorDouble.INSTANCE));
  }

  @Test
  public void testVarcharArrayNotCastableToVector() {
    assertFalse("VARCHAR ARRAY must not be castable to VECTOR(FLOAT)",
      PVarcharArray.INSTANCE.isCastableTo(PVectorFloat.INSTANCE));
  }

  @Test
  public void testFloatArrayTwoArgCoercibleToVector() {
    assertTrue("PFloatArray.isCoercibleTo(PVectorFloat, PFloatArray) should return true",
      PFloatArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE, PFloatArray.INSTANCE));
  }

  @Test
  public void testDoubleArrayTwoArgCoercibleToVector() {
    assertTrue("PDoubleArray.isCoercibleTo(PVectorDouble, PDoubleArray) should return true",
      PDoubleArray.INSTANCE.isCoercibleTo(PVectorDouble.INSTANCE, PDoubleArray.INSTANCE));
  }

  @Test
  public void testDecimalArrayTwoArgCoercibleToVector() {
    assertTrue("PDecimalArray.isCoercibleTo(PVectorFloat, PDecimalArray) should return true",
      PDecimalArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE, PDecimalArray.INSTANCE));
  }

  @Test
  public void testVarcharArrayTwoArgNotCoercibleToVector() {
    assertFalse("PVarcharArray.isCoercibleTo(PVectorFloat, PVarcharArray) must return false",
      PVarcharArray.INSTANCE.isCoercibleTo(PVectorFloat.INSTANCE, PVarcharArray.INSTANCE));
  }

  @Test
  public void testFloatArrayStillCoercibleToFloatArray() {
    assertTrue("FLOAT ARRAY should still be coercible to FLOAT ARRAY",
      PFloatArray.INSTANCE.isCoercibleTo(PFloatArray.INSTANCE));
  }

  @Test
  public void testFloatArrayStillCoercibleToDoubleArray() {
    assertTrue("FLOAT ARRAY should still be coercible to DOUBLE ARRAY",
      PFloatArray.INSTANCE.isCoercibleTo(PDoubleArray.INSTANCE));
  }

  @Test
  public void testFloatArrayNotCoercibleToVarcharArray() {
    assertFalse("FLOAT ARRAY must not be coercible to VARCHAR ARRAY",
      PFloatArray.INSTANCE.isCoercibleTo(PVarcharArray.INSTANCE));
  }
}
