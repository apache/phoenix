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
package org.apache.phoenix.mapreduce.vector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.phoenix.schema.types.PVectorFloat;

/**
 * {@link WritableComparable} wrapper for a dense float vector. Encodes elements using contiguous
 * IEEE 754 single-precision float values in big-endian byte order via {@link PVectorFloat}.
 */
public class VectorWritable implements WritableComparable<VectorWritable> {

  private int dimension;
  private byte[] bytes;
  private float[] vector;

  public VectorWritable() {
    this.dimension = 0;
    this.bytes = new byte[0];
    this.vector = new float[0];
  }

  public VectorWritable(float[] vector) {
    if (vector == null) {
      throw new IllegalArgumentException("vector must not be null");
    }
    this.dimension = vector.length;
    this.vector = Arrays.copyOf(vector, vector.length);
    this.bytes = new byte[this.dimension * Bytes.SIZEOF_FLOAT];
    PVectorFloat.writeElements(this.vector, this.bytes, 0);
  }

  public VectorWritable(byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes must not be null");
    }
    if (bytes.length % Bytes.SIZEOF_FLOAT != 0) {
      throw new IllegalArgumentException("bytes length must be a multiple of 4: " + bytes.length);
    }
    this.dimension = bytes.length / Bytes.SIZEOF_FLOAT;
    this.bytes = Arrays.copyOf(bytes, bytes.length);
    this.vector = PVectorFloat.readElements(this.bytes, 0, this.bytes.length);
  }

  public VectorWritable(int dimension, byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes must not be null");
    }
    if (bytes.length != dimension * Bytes.SIZEOF_FLOAT) {
      throw new IllegalArgumentException(
        "Byte length " + bytes.length + " does not match dimension " + dimension);
    }
    this.dimension = dimension;
    this.bytes = Arrays.copyOf(bytes, bytes.length);
    this.vector = PVectorFloat.readElements(this.bytes, 0, this.bytes.length);
  }

  public VectorWritable(VectorWritable other) {
    if (other == null) {
      throw new IllegalArgumentException("other must not be null");
    }
    this.dimension = other.dimension;
    this.bytes = Arrays.copyOf(other.bytes, other.bytes.length);
    this.vector = Arrays.copyOf(other.vector, other.vector.length);
  }

  public int getDimension() {
    return dimension;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public float[] getVector() {
    return vector;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(dimension);
    out.write(bytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.dimension = in.readInt();
    int byteLen = this.dimension * Bytes.SIZEOF_FLOAT;
    this.bytes = new byte[byteLen];
    in.readFully(this.bytes);
    this.vector = PVectorFloat.readElements(this.bytes, 0, byteLen);
  }

  @Override
  public int compareTo(VectorWritable o) {
    if (this == o) {
      return 0;
    }
    if (o == null) {
      return 1;
    }
    int cmp = Integer.compare(this.dimension, o.dimension);
    if (cmp != 0) {
      return cmp;
    }
    return Bytes.compareTo(this.bytes, o.bytes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VectorWritable)) {
      return false;
    }
    VectorWritable that = (VectorWritable) o;
    return dimension == that.dimension && Arrays.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    return 31 * dimension + Arrays.hashCode(bytes);
  }

  @Override
  public String toString() {
    return "VectorWritable{" + "dimension=" + dimension + ", vector=" + Arrays.toString(vector)
      + '}';
  }
}
