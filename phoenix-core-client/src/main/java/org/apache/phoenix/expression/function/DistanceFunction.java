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

import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;

/**
 * Base class for the vector distance functions. The functions read the packed byte encodings of the
 * operands directly and do not decode the elements into heap arrays. Each evaluation writes the
 * result into a new buffer, because downstream sort steps keep a reference to each result. A null
 * operand gives a null result. Operands with different dimensions cause an
 * {@link IllegalDataException}.
 */
public abstract class DistanceFunction extends ScalarFunction {

  /**
   * Upper bound for a bounded top-N scan. If the distance of a candidate is more than this bound,
   * the result is {@link Double#MAX_VALUE}, and the L2 kernels can stop early. Expression
   * serialization does not include this value, but {@link #clone(List)} copies it.
   */
  private double distanceUpperBound = Double.MAX_VALUE;

  public DistanceFunction() {
  }

  public DistanceFunction(List<Expression> children) {
    super(children);
  }

  /**
   * Makes sure that both arguments are vectors and, if both dimensions are known at compile time,
   * that the dimensions are equal. An argument of unknown type passes. If a dimension is not known,
   * {@link #evaluate} checks the dimensions for each row.
   * @throws SQLException if an argument is not a vector or the two dimensions are different
   */
  public static void validateChildren(List<Expression> children) throws SQLException {
    for (int i = 0; i < 2; i++) {
      PDataType type = children.get(i).getDataType();
      if (type != null && !type.isVectorType()) {
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.TYPE_MISMATCH)
          .setMessage("Distance function argument " + (i + 1) + " must be a vector, but was "
            + type.getSqlTypeName())
          .build().buildException();
      }
    }
    Integer dim1 = children.get(0).getMaxLength();
    Integer dim2 = children.get(1).getMaxLength();
    if (dim1 != null && dim2 != null && !dim1.equals(dim2)) {
      throw dimensionMismatch(dim1, dim2);
    }
  }

  private static SQLException dimensionMismatch(int dim1, int dim2) {
    return new SQLExceptionInfo.Builder(SQLExceptionCode.VECTOR_DIMENSION_MISMATCH)
      .setMessage(dim1 + " != " + dim2).build().buildException();
  }

  public double getDistanceUpperBound() {
    return distanceUpperBound;
  }

  public void setDistanceUpperBound(double distanceUpperBound) {
    this.distanceUpperBound = distanceUpperBound;
  }

  @Override
  public PDataType getDataType() {
    return PDouble.INSTANCE;
  }

  @Override
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    Expression child1 = children.get(0);
    if (!child1.evaluate(tuple, ptr)) {
      return false;
    }
    if (ptr.getLength() == 0) {
      return true;
    }
    byte[] buf1 = ptr.get();
    int off1 = ptr.getOffset();
    boolean isDouble1 = isDouble(child1);
    int dim1 = ptr.getLength() / (isDouble1 ? Bytes.SIZEOF_DOUBLE : Bytes.SIZEOF_FLOAT);

    // Keep the first operand buffer, because the second child evaluation resets ptr
    Expression child2 = children.get(1);
    if (!child2.evaluate(tuple, ptr)) {
      return false;
    }
    if (ptr.getLength() == 0) {
      return true;
    }
    byte[] buf2 = ptr.get();
    int off2 = ptr.getOffset();
    boolean isDouble2 = isDouble(child2);
    int dim2 = ptr.getLength() / (isDouble2 ? Bytes.SIZEOF_DOUBLE : Bytes.SIZEOF_FLOAT);
    if (dim1 != dim2) {
      throw new IllegalDataException(dimensionMismatch(dim1, dim2));
    }

    double distance;
    if (
      !isDouble1 && !isDouble2 && child1.getSortOrder() == SortOrder.ASC
        && child2.getSortOrder() == SortOrder.ASC
    ) {
      distance = computeFloat(buf1, off1, buf2, off2, dim1, distanceUpperBound);
    } else {
      distance = computeGeneric(buf1, off1, isDouble1, child1.getSortOrder(), buf2, off2, isDouble2,
        child2.getSortOrder(), dim1, distanceUpperBound);
    }
    byte[] out = new byte[Bytes.SIZEOF_DOUBLE];
    PDouble.INSTANCE.getCodec().encodeDouble(distance, out, 0);
    ptr.set(out);
    return true;
  }

  private static boolean isDouble(Expression child) {
    return child.getDataType() == PVectorDouble.INSTANCE;
  }

  /**
   * Computes the distance between two float vectors in ASC sort order with the
   * {@link VectorDistanceUtil} kernels, which can use SIMD. Returns {@link Double#MAX_VALUE} if the
   * distance is more than {@code bound}.
   */
  protected abstract double computeFloat(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound);

  /**
   * Computes the distance for all other combinations of element precision and sort order. Returns
   * {@link Double#MAX_VALUE} if the distance is more than {@code bound}.
   */
  protected abstract double computeGeneric(byte[] a, int aOff, boolean aIsDouble, SortOrder aOrder,
    byte[] b, int bOff, boolean bIsDouble, SortOrder bOrder, int dim, double bound);

  /** Decodes the element at index {@code i} for the given precision and sort order. */
  protected static double element(byte[] buf, int off, boolean isDouble, SortOrder order, int i) {
    return isDouble
      ? PVectorDouble.readElement(buf, off, i, order)
      : PVectorFloat.readElement(buf, off, i, order);
  }

  @Override
  public DistanceFunction clone(List<Expression> children) {
    try {
      DistanceFunction clone =
        (DistanceFunction) getClass().getConstructor(List.class).newInstance(children);
      clone.setDistanceUpperBound(this.distanceUpperBound);
      return clone;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
