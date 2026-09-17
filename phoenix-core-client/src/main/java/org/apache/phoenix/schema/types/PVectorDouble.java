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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;

/**
 * Data type representing fixed-dimension double-precision floating point vectors,
 * {@code VECTOR(DOUBLE, N)}. Stored as packed big-endian IEEE 754 values.
 */
public class PVectorDouble extends PVectorDataType<double[]> {

  public static final PVectorDouble INSTANCE = new PVectorDouble();

  private PVectorDouble() {
    super("VECTOR(DOUBLE)", PDataType.VECTOR_DOUBLE_TYPE, double[].class, 52);
  }

  /** Reads the element at the given index from packed vector bytes in ASC sort order. */
  public static double readElement(byte[] buf, int offset, int index) {
    return Bytes.toDouble(buf, offset + index * Bytes.SIZEOF_DOUBLE);
  }

  /** Reads the element at the given index from packed vector bytes in the given sort order. */
  public static double readElement(byte[] buf, int offset, int index, SortOrder sortOrder) {
    long b = Bytes.toLong(buf, offset + index * Bytes.SIZEOF_DOUBLE);
    if (sortOrder == SortOrder.DESC) {
      b ^= 0xFFFFFFFFFFFFFFFFL;
    }
    return Double.longBitsToDouble(b);
  }

  /** Reads the element at the given index from a packed vector pointer in ASC sort order. */
  public static double readElement(ImmutableBytesWritable ptr, int index) {
    return readElement(ptr.get(), ptr.getOffset(), index);
  }

  /** Decodes all elements from a packed vector byte range in ASC sort order. */
  public static double[] readElements(byte[] buf, int offset, int length) {
    return readElements(buf, offset, length, SortOrder.ASC);
  }

  /** Decodes all elements from a packed vector byte range in the given sort order. */
  public static double[] readElements(byte[] buf, int offset, int length, SortOrder sortOrder) {
    double[] out = new double[length / Bytes.SIZEOF_DOUBLE];
    for (int i = 0; i < out.length; i++) {
      out[i] = readElement(buf, offset, i, sortOrder);
    }
    return out;
  }

  /**
   * Writes a vector into a byte buffer as packed contiguous big-endian IEEE 754 values.
   * @throws IllegalArgumentException if the buffer is too small to contain the vector
   */
  public static void writeElements(double[] vector, byte[] buf, int offset) {
    if (buf.length < offset + (long) vector.length * Bytes.SIZEOF_DOUBLE) {
      throw new IllegalArgumentException("Buffer too small: need "
        + (offset + vector.length * Bytes.SIZEOF_DOUBLE) + " bytes, have " + buf.length);
    }
    for (int i = 0; i < vector.length; i++) {
      Bytes.putDouble(buf, offset + i * Bytes.SIZEOF_DOUBLE, vector[i]);
    }
  }

  @Override
  public int getElementByteSize() {
    return Bytes.SIZEOF_DOUBLE;
  }

  @Override
  public PDataType getElementType() {
    return PDouble.INSTANCE;
  }

  @Override
  protected int length(double[] vector) {
    return vector.length;
  }

  @Override
  protected double[] allocate(int dimension) {
    return new double[dimension];
  }

  @Override
  protected void set(double[] vector, int index, Number value) {
    vector[index] = value.doubleValue();
  }

  @Override
  protected Number get(double[] vector, int index) {
    return vector[index];
  }

  @Override
  protected Number parseElement(String s) {
    return Double.valueOf(s);
  }

  @Override
  protected void pack(double[] vector, byte[] buf, int offset) {
    writeElements(vector, buf, offset);
  }

  @Override
  protected double[] unpack(byte[] buf, int offset, int length, SortOrder sortOrder) {
    return readElements(buf, offset, length, sortOrder);
  }

  @Override
  protected int indexOfNonFinite(double[] vector) {
    for (int i = 0; i < vector.length; i++) {
      if (!Double.isFinite(vector[i])) {
        return i;
      }
    }
    return -1;
  }
}
