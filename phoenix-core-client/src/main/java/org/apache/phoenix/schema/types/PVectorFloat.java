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
 * Data type for fixed-dimension vectors of single-precision floating point values,
 * {@code VECTOR(FLOAT, N)}.
 * <p>
 * The serialized form is a packed sequence of little-endian IEEE 754 values. Because x86_64 and
 * arm64 use this byte order, SIMD kernels can load elements directly from cell bytes. BSON FLOAT32
 * vector payloads use the same encoding, so they need no conversion. The encoding does not keep the
 * sort order of values, so a byte comparison of two encoded vectors does not give a numeric order.
 */
public class PVectorFloat extends PVectorDataType<float[]> {

  public static final PVectorFloat INSTANCE = new PVectorFloat();

  private PVectorFloat() {
    super("VECTOR(FLOAT)", PDataType.VECTOR_FLOAT_TYPE, float[].class, 51);
  }

  /** Reads the element at the given index from packed vector bytes in ASC sort order. */
  public static float readElement(byte[] buf, int offset, int index) {
    return Float.intBitsToFloat(readBits(buf, offset, index));
  }

  /** Reads the element at the given index from packed vector bytes in the given sort order. */
  public static float readElement(byte[] buf, int offset, int index, SortOrder sortOrder) {
    int b = readBits(buf, offset, index);
    if (sortOrder == SortOrder.DESC) {
      b ^= 0xFFFFFFFF;
    }
    return Float.intBitsToFloat(b);
  }

  private static int readBits(byte[] buf, int offset, int index) {
    return Integer.reverseBytes(Bytes.toInt(buf, offset + index * Bytes.SIZEOF_FLOAT));
  }

  /** Reads the element at the given index from a packed vector pointer in ASC sort order. */
  public static float readElement(ImmutableBytesWritable ptr, int index) {
    return readElement(ptr.get(), ptr.getOffset(), index);
  }

  /** Decodes all elements from a packed vector byte range in ASC sort order. */
  public static float[] readElements(byte[] buf, int offset, int length) {
    return readElements(buf, offset, length, SortOrder.ASC);
  }

  /** Decodes all elements from a packed vector byte range in the given sort order. */
  public static float[] readElements(byte[] buf, int offset, int length, SortOrder sortOrder) {
    float[] out = new float[length / Bytes.SIZEOF_FLOAT];
    for (int i = 0; i < out.length; i++) {
      out[i] = readElement(buf, offset, i, sortOrder);
    }
    return out;
  }

  /**
   * Writes a vector into a byte buffer as a packed sequence of little-endian IEEE 754 values.
   * @throws IllegalArgumentException if the buffer cannot hold the vector at the given offset
   */
  public static void writeElements(float[] vector, byte[] buf, int offset) {
    if (buf.length < offset + (long) vector.length * Bytes.SIZEOF_FLOAT) {
      throw new IllegalArgumentException("Buffer too small: need "
        + (offset + vector.length * Bytes.SIZEOF_FLOAT) + " bytes, have " + buf.length);
    }
    for (int i = 0; i < vector.length; i++) {
      Bytes.putInt(buf, offset + i * Bytes.SIZEOF_FLOAT,
        Integer.reverseBytes(Float.floatToRawIntBits(vector[i])));
    }
  }

  @Override
  public int getElementByteSize() {
    return Bytes.SIZEOF_FLOAT;
  }

  @Override
  public PDataType getElementType() {
    return PFloat.INSTANCE;
  }

  @Override
  protected int length(float[] vector) {
    return vector.length;
  }

  @Override
  protected float[] allocate(int dimension) {
    return new float[dimension];
  }

  @Override
  protected void set(float[] vector, int index, Number value) {
    vector[index] = value.floatValue();
  }

  @Override
  protected Number get(float[] vector, int index) {
    return vector[index];
  }

  @Override
  protected Number parseElement(String s) {
    return Float.valueOf(s);
  }

  @Override
  protected void pack(float[] vector, byte[] buf, int offset) {
    writeElements(vector, buf, offset);
  }

  @Override
  protected float[] unpack(byte[] buf, int offset, int length, SortOrder sortOrder) {
    return readElements(buf, offset, length, sortOrder);
  }

  @Override
  protected int indexOfNonFinite(float[] vector) {
    for (int i = 0; i < vector.length; i++) {
      if (!Float.isFinite(vector[i])) {
        return i;
      }
    }
    return -1;
  }
}
