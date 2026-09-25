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
import java.util.Objects;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.index.vector.KMeansTrainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Writable} aggregation container representing (centroid_id, partial_sum_vector, count)
 * triples for MapReduce Lloyd's iterations. Uses double[] for component partial sums to prevent
 * float rounding at scale. Centroid ID -1 is reserved for worst-fit vector candidates during empty
 * cluster re-seeding.
 */
public class CentroidWritable implements Writable {

  private static final Logger LOGGER = LoggerFactory.getLogger(CentroidWritable.class);

  private int centroidId;
  private double[] partialSum;
  private long count;
  private double distance;

  public CentroidWritable() {
    this.centroidId = 0;
    this.partialSum = new double[0];
    this.count = 0;
    this.distance = 0.0;
  }

  public CentroidWritable(int centroidId, int dimension) {
    this.centroidId = centroidId;
    this.partialSum = new double[dimension];
    this.count = 0;
    this.distance = 0.0;
  }

  public CentroidWritable(int centroidId, double[] partialSum, long count) {
    this(centroidId, partialSum, count, 0.0);
  }

  public CentroidWritable(int centroidId, double[] partialSum, long count, double distance) {
    this.centroidId = centroidId;
    this.partialSum =
      (partialSum != null) ? Arrays.copyOf(partialSum, partialSum.length) : new double[0];
    this.count = count;
    this.distance = distance;
  }

  public CentroidWritable(CentroidWritable other) {
    this.centroidId = other.centroidId;
    this.partialSum = (other.partialSum != null)
      ? Arrays.copyOf(other.partialSum, other.partialSum.length)
      : new double[0];
    this.count = other.count;
    this.distance = other.distance;
  }

  public int getCentroidId() {
    return centroidId;
  }

  public void setCentroidId(int centroidId) {
    this.centroidId = centroidId;
  }

  public double[] getPartialSum() {
    return partialSum;
  }

  public void setPartialSum(double[] partialSum) {
    this.partialSum = partialSum;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public double getDistance() {
    return distance;
  }

  public void setDistance(double distance) {
    this.distance = distance;
  }

  public int getDimension() {
    return partialSum != null ? partialSum.length : 0;
  }

  /**
   * Adds a single vector to this accumulator.
   * @param vector float vector
   */
  public void addVector(float[] vector) {
    addVector(vector, 0.0);
  }

  /**
   * Adds a single vector and its distance to this accumulator.
   * @param vector float vector
   * @param dist   distance to centroid
   */
  public void addVector(float[] vector, double dist) {
    if (vector == null) {
      return;
    }
    if (partialSum == null || partialSum.length != vector.length) {
      partialSum = new double[vector.length];
    }
    for (int i = 0; i < vector.length; i++) {
      partialSum[i] += vector[i];
    }
    this.count++;
    this.distance += dist;
  }

  /**
   * Merges another CentroidWritable into this one. For special centroid ID -1 (worst-fit
   * candidates), keeps the candidate with the larger distance.
   * @param other other CentroidWritable
   */
  public void add(CentroidWritable other) {
    if (other == null) {
      return;
    }
    if (this.centroidId == -1 || other.centroidId == -1) {
      if (other.distance > this.distance || this.count == 0) {
        this.centroidId = -1;
        this.count = other.count;
        this.distance = other.distance;
        if (other.partialSum != null) {
          this.partialSum = Arrays.copyOf(other.partialSum, other.partialSum.length);
        }
      }
      return;
    }

    if (this.partialSum == null || this.partialSum.length == 0) {
      if (other.partialSum != null) {
        this.partialSum = Arrays.copyOf(other.partialSum, other.partialSum.length);
      }
    } else if (other.partialSum != null) {
      if (this.partialSum.length != other.partialSum.length) {
        LOGGER.warn("CentroidWritable dimension mismatch during merge: {} vs {}",
          this.partialSum.length, other.partialSum.length);
      }
      int len = Math.min(this.partialSum.length, other.partialSum.length);
      for (int i = 0; i < len; i++) {
        this.partialSum[i] += other.partialSum[i];
      }
    }
    this.count += other.count;
    this.distance += other.distance;
  }

  /**
   * Computes the arithmetic mean centroid from partialSum and count. Applies L2 normalization if
   * metric is COSINE.
   * @param metric distance metric ("L2", "COSINE", "INNER_PRODUCT")
   * @return float[] centroid
   */
  public float[] computeCentroid(String metric) {
    if (count <= 0 || partialSum == null || partialSum.length == 0) {
      return null;
    }
    int dim = partialSum.length;
    float[] centroid = new float[dim];
    for (int i = 0; i < dim; i++) {
      centroid[i] = (float) (partialSum[i] / count);
    }
    if ("COSINE".equalsIgnoreCase(metric)) {
      centroid = KMeansTrainer.l2Normalize(centroid);
    }
    return centroid;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(centroidId);
    out.writeLong(count);
    out.writeDouble(distance);
    int dim = (partialSum != null) ? partialSum.length : 0;
    out.writeInt(dim);
    for (int i = 0; i < dim; i++) {
      out.writeDouble(partialSum[i]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.centroidId = in.readInt();
    this.count = in.readLong();
    this.distance = in.readDouble();
    int dim = in.readInt();
    this.partialSum = new double[dim];
    for (int i = 0; i < dim; i++) {
      this.partialSum[i] = in.readDouble();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CentroidWritable)) {
      return false;
    }
    CentroidWritable that = (CentroidWritable) o;
    return centroidId == that.centroidId && count == that.count
      && Double.compare(that.distance, distance) == 0 && Arrays.equals(partialSum, that.partialSum);
  }

  @Override
  public int hashCode() {
    return Objects.hash(centroidId, Arrays.hashCode(partialSum), count, distance);
  }

  @Override
  public String toString() {
    return "CentroidWritable{" + "centroidId=" + centroidId + ", count=" + count + ", distance="
      + distance + ", dim=" + (partialSum != null ? partialSum.length : 0) + '}';
  }
}
