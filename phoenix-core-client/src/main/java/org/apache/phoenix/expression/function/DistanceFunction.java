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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVectorDouble;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

/**
 * Base class for vector distance scalar functions. Operates directly on packed vector byte buffers,
 * decoding no vector elements onto the Java heap; only the eight byte encoded result is allocated
 * per evaluation.
 */
public abstract class DistanceFunction extends ScalarFunction {

  private double distanceUpperBound = Double.MAX_VALUE;

  // Scanner-thread scratch pointer reused across evaluate calls to avoid heap allocation.
  private final ImmutableBytesWritable scratch = new ImmutableBytesWritable();

  public DistanceFunction() {
  }

  public DistanceFunction(List<Expression> children) {
    this(children, Double.MAX_VALUE);
  }

  public DistanceFunction(List<Expression> children, double distanceUpperBound) {
    super(children);
    validateChildren(children);
    this.distanceUpperBound = distanceUpperBound;
  }

  public static void validateChildren(List<Expression> children) {
    Preconditions.checkNotNull(children, "Children cannot be null");
    Preconditions.checkArgument(children.size() >= 2,
      "Distance functions require at least two arguments");
    Expression firstChild = children.get(0);
    Expression secondChild = children.get(1);
    if (firstChild.getDataType() != null && !firstChild.getDataType().isVectorType()) {
      throw new IllegalArgumentException(
        "First argument must be a vector type, but was " + firstChild.getDataType());
    }
    if (secondChild.getDataType() != null && !secondChild.getDataType().isVectorType()) {
      throw new IllegalArgumentException(
        "Second argument must be a vector type, but was " + secondChild.getDataType());
    }
    Integer dim1 = firstChild.getMaxLength();
    Integer dim2 = secondChild.getMaxLength();
    if (dim1 != null && dim2 != null && !dim1.equals(dim2)) {
      throw new IllegalArgumentException("Vector dimension mismatch: " + dim1 + " != " + dim2);
    }
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

  /**
   * Evaluates distance against a tuple using an upper bound for early termination pruning.
   */
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr, double distanceUpperBound) {
    double prevBound = this.distanceUpperBound;
    try {
      this.distanceUpperBound = distanceUpperBound;
      return evaluate(tuple, ptr);
    } finally {
      this.distanceUpperBound = prevBound;
    }
  }

  @Override
  public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
    Expression child1 = children.get(0);
    if (!child1.evaluate(tuple, ptr) || ptr.getLength() == 0) {
      return false;
    }
    byte[] buf1 = ptr.get();
    int off1 = ptr.getOffset();
    int len1 = ptr.getLength();
    SortOrder order1 = child1.getSortOrder();
    boolean isDouble1 = child1.getDataType() == PVectorDouble.INSTANCE;
    int dim1 = isDouble1 ? (len1 / Bytes.SIZEOF_DOUBLE) : (len1 / Bytes.SIZEOF_FLOAT);

    Expression child2 = children.get(1);
    if (!child2.evaluate(tuple, scratch) || scratch.getLength() == 0) {
      return false;
    }
    byte[] buf2 = scratch.get();
    int off2 = scratch.getOffset();
    int len2 = scratch.getLength();
    SortOrder order2 = child2.getSortOrder();
    boolean isDouble2 = child2.getDataType() == PVectorDouble.INSTANCE;
    int dim2 = isDouble2 ? (len2 / Bytes.SIZEOF_DOUBLE) : (len2 / Bytes.SIZEOF_FLOAT);

    if (dim1 != dim2) {
      throw new IllegalArgumentException("Vector dimension mismatch: " + dim1 + " != " + dim2);
    }

    double distance = computeDistance(buf1, off1, order1, isDouble1, buf2, off2, order2, isDouble2,
      dim1, distanceUpperBound);

    // Allocate a distinct buffer per call because downstream iterators retain sort keys across
    // rows.
    byte[] outBuf = new byte[Bytes.SIZEOF_DOUBLE];
    PDouble.INSTANCE.getCodec().encodeDouble(distance, outBuf, 0);
    ptr.set(outBuf);
    return true;
  }

  @Override
  public DistanceFunction clone(List<Expression> children) {
    try {
      DistanceFunction clone =
        (DistanceFunction) getClass().getConstructor(List.class).newInstance(children);
      clone.setDistanceUpperBound(this.distanceUpperBound);
      return clone;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    super.readFields(input);
    this.distanceUpperBound = input.readDouble();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    super.write(output);
    output.writeDouble(distanceUpperBound);
  }

  protected abstract double computeDistance(byte[] buf1, int off1, SortOrder order1,
    boolean isDouble1, byte[] buf2, int off2, SortOrder order2, boolean isDouble2, int dim,
    double bound);
}
