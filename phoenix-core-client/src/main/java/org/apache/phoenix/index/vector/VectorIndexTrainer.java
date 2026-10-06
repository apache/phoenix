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
package org.apache.phoenix.index.vector;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates IVF index centroid training, persistence, and catalog registration across index
 * creation and rebuild operations.
 * <p>
 * Samples training vectors from the base table expression using server side Bernoulli filtering
 * combined with client side reservoir sampling, then executes in-memory k-means clustering.
 */
public final class VectorIndexTrainer {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexTrainer.class);
  /** Oversampling margin to ensure sufficient sample volume for the reservoir. */
  private static final double OVERSAMPLE = 1.2;
  /** Initial creation trigger reason for newly trained vector indexes. */
  public static final String CREATE_INDEX_REASON = "CREATE_INDEX";

  private VectorIndexTrainer() {
  }

  /**
   * Identifies the primary vector column definition within the index schema.
   */
  public static PColumn getIndexedVectorColumn(PTable index) {
    for (PColumn column : index.getColumns()) {
      if (
        column.getFamilyName() != null && column.getDataType() != null
          && column.getDataType().isVectorType()
      ) {
        return column;
      }
    }
    throw new IllegalStateException("Vector index " + index.getName() + " has no vector column");
  }

  /** Samples base table vectors and trains centroids according to the index configuration. */
  public static KMeansResult train(PhoenixConnection conn, PTable dataTable, PTable index)
    throws SQLException {
    return train(conn, dataTable, index, index.getVectorIvfLists());
  }

  /**
   * Samples base table vectors and trains the requested number of centroids using the index
   * configuration.
   * @return trained k-means model, or null if sample volume is insufficient for the requested
   *         clusters
   */
  public static KMeansResult train(PhoenixConnection conn, PTable dataTable, PTable index,
    int lists) throws SQLException {
    int sampleSize = index.getVectorIvfSampleSize();
    DistanceMetric metric = DistanceMetric.fromString(index.getVectorDistanceMetric());
    PColumn vectorColumn = getIndexedVectorColumn(index);
    String expr = vectorColumn.getExpressionStr();
    String from = SchemaUtil.getEscapedFullTableName(dataTable.getName().getString());
    String notNull = " WHERE " + expr + " IS NOT NULL";

    long count;
    try (Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + from + notNull)) {
      count = rs.next() ? rs.getLong(1) : 0;
    }
    if (count < lists) {
      LOGGER.info("Deferring centroid training for vector index {}: {} non-null vectors, fewer than"
        + " lists={}", index.getName(), count, lists);
      return null;
    }
    double p = Math.min(1.0, OVERSAMPLE * sampleSize / count);
    String sql = "SELECT " + expr + " FROM " + from + notNull;
    List<float[]> sample = sample(conn, p < 1.0 ? sql + " AND RAND() < " + p : sql, sampleSize);
    if (sample.size() < lists) {
      // Fallback to full scan when Bernoulli sampling yields fewer rows than requested lists
      sample = sample(conn, sql, sampleSize);
    }
    KMeansConfig config = KMeansConfig.newBuilder().distanceMetric(metric).build();
    LOGGER.info("Training vector index {} with {} sampled vectors, lists={}, metric={}",
      index.getName(), sample.size(), lists, metric);
    return KMeansTrainer.train(sample, lists, config);
  }

  /**
   * Trains a new centroid generation, persists centroid vectors, updates catalog metadata, and
   * installs the model into the local centroid cache.
   */
  public static Long trainAndRecord(PhoenixConnection conn, PTable dataTable, PTable index)
    throws SQLException {
    KMeansResult result = train(conn, dataTable, index);
    if (result == null) {
      return null;
    }
    String indexName = index.getName().getString();
    long generation = CentroidManager.nextGeneration(index.getVectorCentroidGeneration());
    CentroidManager.persistCentroids(conn, indexName, generation, result.getCentroids());
    CentroidManager.persistGenerationSummary(conn, indexName, generation,
      new GenerationSummary(GenerationSummary.ACTIVE, CREATE_INDEX_REASON, result.getRequestedK(),
        result.getSkewMetrics(), null, null));
    CentroidManager.setGenerationAndLists(conn, index, generation, result.getEffectiveK());
    VectorCentroidCache.getInstance(conn.getQueryServices().getConfiguration()).put(indexName,
      generation, new CachedCentroids(result.getCentroids(), result.getDistanceMetric()));
    return generation;
  }

  private static List<float[]> sample(PhoenixConnection conn, String sql, int sampleSize)
    throws SQLException {
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      return KMeansTrainer.reservoirSample(new VectorIterator(rs), sampleSize, new Random());
    } catch (VectorIterator.ReadException e) {
      throw e.getCause();
    }
  }

  /** Extracts float vector arrays from a result set stream. */
  private static final class VectorIterator implements Iterator<float[]> {
    private final ResultSet rs;
    private Boolean hasNext;

    VectorIterator(ResultSet rs) {
      this.rs = rs;
    }

    @Override
    public boolean hasNext() {
      if (hasNext == null) {
        try {
          hasNext = rs.next();
        } catch (SQLException e) {
          throw new ReadException(e);
        }
      }
      return hasNext;
    }

    @Override
    public float[] next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      hasNext = null;
      try {
        return toFloats(rs.getObject(1));
      } catch (SQLException e) {
        throw new ReadException(e);
      }
    }

    private static float[] toFloats(Object value) {
      if (value instanceof float[]) {
        return (float[]) value;
      }
      if (value instanceof double[]) {
        double[] d = (double[]) value;
        float[] f = new float[d.length];
        for (int i = 0; i < d.length; i++) {
          f[i] = (float) d[i];
        }
        return f;
      }
      return null;
    }

    private static final class ReadException extends RuntimeException {
      ReadException(SQLException cause) {
        super(cause);
      }

      @Override
      public synchronized SQLException getCause() {
        return (SQLException) super.getCause();
      }
    }
  }
}
