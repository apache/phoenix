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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;

/** Tests serialization and deserialization round-tripping for vector distance expressions. */
public class DistanceFunctionSerializationTest {

  private static final byte[] CF = Bytes.toBytes("0");
  private static final byte[] CQ_V = Bytes.toBytes("V");

  @Test
  public void testL2DistanceFunctionRoundTrip() throws Exception {
    verifyRoundTrip(createDistanceFunction(L2DistanceFunction.class), "L2_DISTANCE");
  }

  @Test
  public void testL2DistanceSquaredFunctionRoundTrip() throws Exception {
    verifyRoundTrip(createDistanceFunction(L2DistanceSquaredFunction.class), "L2_DISTANCE_SQUARED");
  }

  @Test
  public void testCosineDistanceFunctionRoundTrip() throws Exception {
    verifyRoundTrip(createDistanceFunction(CosineDistanceFunction.class), "COSINE_DISTANCE");
  }

  @Test
  public void testInnerProductDistanceFunctionRoundTrip() throws Exception {
    verifyRoundTrip(createDistanceFunction(InnerProductDistanceFunction.class), "INNER_PRODUCT");
  }

  @Test
  public void testExpressionTypeRegistration() throws Exception {
    DistanceFunction[] funcs = { createDistanceFunction(L2DistanceFunction.class),
      createDistanceFunction(L2DistanceSquaredFunction.class),
      createDistanceFunction(CosineDistanceFunction.class),
      createDistanceFunction(InnerProductDistanceFunction.class), };

    Class<?>[] expectedClasses = { L2DistanceFunction.class, L2DistanceSquaredFunction.class,
      CosineDistanceFunction.class, InnerProductDistanceFunction.class, };

    for (int i = 0; i < funcs.length; i++) {
      ExpressionType type = ExpressionType.valueOf(funcs[i]);
      assertNotNull(funcs[i].getName() + " must have ExpressionType registration", type);
      assertEquals(funcs[i].getName() + " ExpressionType class mismatch", expectedClasses[i],
        type.getExpressionClass());
    }
  }

  @Test
  public void testDistanceUpperBoundPreserved() throws Exception {
    DistanceFunction original = createDistanceFunction(L2DistanceFunction.class);
    original.setDistanceUpperBound(42.5);

    DistanceFunction deserialized = serializeAndDeserialize(original);
    assertEquals("distanceUpperBound must survive round-trip", 42.5,
      deserialized.getDistanceUpperBound(), 0.0);
  }

  private DistanceFunction createDistanceFunction(Class<? extends DistanceFunction> clazz)
    throws Exception {
    int dim = 3;
    PColumn col =
      new PColumnImpl(PNameFactory.newName("V"), PNameFactory.newName("0"), PVectorFloat.INSTANCE,
        dim, null, true, 1, SortOrder.ASC, 0, null, false, null, false, false, null, 0);
    KeyValueColumnExpression colExpr = new KeyValueColumnExpression(col);
    float[] queryVec = { 0.5f, 0.5f, 0.5f };
    LiteralExpression queryLit =
      LiteralExpression.newConstant(queryVec, PVectorFloat.INSTANCE, dim, null);
    return clazz.getConstructor(List.class).newInstance(Arrays.asList(colExpr, queryLit));
  }

  private void verifyRoundTrip(DistanceFunction original, String name) throws Exception {
    float[] testVec = { 1.0f, 0.0f, 0.0f };
    byte[] rowKey = Bytes.toBytes(1);
    byte[] vBytes = PVectorFloat.INSTANCE.toBytes(testVec);
    Cell cell = new KeyValue(rowKey, CF, CQ_V, vBytes);
    Tuple tuple = new MultiKeyValueTuple(Arrays.asList(cell));

    ImmutableBytesWritable ptr1 = new ImmutableBytesWritable();
    assertTrue(name + ": original evaluate must succeed", original.evaluate(tuple, ptr1));
    double originalDist = (Double) PDouble.INSTANCE.toObject(ptr1);

    DistanceFunction deserialized = serializeAndDeserialize(original);

    assertEquals(name + ": deserialized class mismatch", original.getClass(),
      deserialized.getClass());

    ImmutableBytesWritable ptr2 = new ImmutableBytesWritable();
    assertTrue(name + ": deserialized evaluate must succeed", deserialized.evaluate(tuple, ptr2));
    double deserializedDist = (Double) PDouble.INSTANCE.toObject(ptr2);

    assertEquals(name + ": distances must match after round-trip", originalDist, deserializedDist,
      0.0);
  }

  private DistanceFunction serializeAndDeserialize(DistanceFunction original) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    int ordinal = ExpressionType.valueOf(original).ordinal();
    WritableUtils.writeVInt(out, ordinal);
    original.write(out);
    out.flush();

    byte[] bytes = baos.toByteArray();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    int readOrdinal = WritableUtils.readVInt(in);
    Expression expr = ExpressionType.values()[readOrdinal].newInstance();
    expr.readFields(in);

    assertTrue("Deserialized expression must be a DistanceFunction",
      expr instanceof DistanceFunction);
    return (DistanceFunction) expr;
  }
}
