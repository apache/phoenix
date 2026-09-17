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
 * Base class for vector distance functions. Evaluates distances directly over packed byte encodings
 * without deserializing vector elements to the heap. Each evaluated result allocates a new encoded
 * buffer to maintain pointer validity across downstream sort pipelines.
 */
public abstract class DistanceFunction extends ScalarFunction {

  /**
   * Transient upper bound for early termination during bounded top-N scans. Candidates exceeding
   * this bound return {@link Double#MAX_VALUE} without completing distance evaluation. Not
   * serialized across RPC boundaries.
   */
  private double distanceUpperBound = Double.MAX_VALUE;

  public DistanceFunction() {
  }

  public DistanceFunction(List<Expression> children) {
    super(children);
  }

  /**
   * Validates vector type compatibility and dimension matching when compile time dimensions are
   * available. Dynamic dimensions are deferred to runtime evaluation.
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

    // Retain first operand buffer reference prior to evaluating second child
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
   * Evaluates distance between ASC-ordered float vectors via {@link VectorDistanceUtil} kernels.
   */
  protected abstract double computeFloat(byte[] a, int aOff, byte[] b, int bOff, int dim,
    double bound);

  /** Evaluates distance across mixed element precision and sort orders. */
  protected abstract double computeGeneric(byte[] a, int aOff, boolean aIsDouble, SortOrder aOrder,
    byte[] b, int bOff, boolean bIsDouble, SortOrder bOrder, int dim, double bound);

  /** Decodes a vector element at the specified index for given precision and sort order. */
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
