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
 * Trains the IVF centroids of an index, stores them, and records the generation in
 * {@code SYSTEM.CATALOG}, for index creation and rebuild.
 * <p>
 * The training sample comes from a Bernoulli filter that the server evaluates on each data row.
 * Reservoir sampling on the client then limits the sample size. Training runs k-means in memory.
 */
public final class VectorIndexTrainer {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexTrainer.class);
  /**
   * Margin of the Bernoulli rate, so that the filter usually returns enough rows for the sample.
   */
  private static final double OVERSAMPLE = 1.2;
  /** Trigger reason that the first generation of a new vector index records. */
  public static final String CREATE_INDEX_REASON = "CREATE_INDEX";

  private VectorIndexTrainer() {
  }

  /**
   * Returns the vector column of the index.
   * @throws IllegalStateException if the index has no vector column
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

  /**
   * Samples the data table and trains the number of centroids that the index definition requests.
   * Returns null if the data table has too few vectors.
   */
  public static KMeansResult train(PhoenixConnection conn, PTable dataTable, PTable index)
    throws SQLException {
    return train(conn, dataTable, index, index.getVectorIvfLists());
  }

  /**
   * Samples vectors from the base table and trains the requested number of centroids. The sample
   * size and the distance metric come from the index.
   * @return the trained k-means model, or null if there are fewer non-null vectors or sampled
   *         vectors than the requested number of lists
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
      // The Bernoulli filter can return fewer rows than lists. Then sample from a full scan.
      sample = sample(conn, sql, sampleSize);
      if (sample.size() < lists) {
        LOGGER.info("Deferring centroid training for vector index {}: {} trainable vectors, fewer"
          + " than lists={}", index.getName(), sample.size(), lists);
        return null;
      }
    }
    KMeansConfig config = KMeansConfig.newBuilder().distanceMetric(metric).build();
    LOGGER.info("Training vector index {} with {} sampled vectors, lists={}, metric={}",
      index.getName(), sample.size(), lists, metric);
    return KMeansTrainer.train(sample, lists, config);
  }

  /**
   * Trains a new centroid generation, stores its centroids and summary, and records it as the
   * active generation in {@code SYSTEM.CATALOG}. It also puts the model in the centroid cache of
   * this process. Returns the new generation ID, or null if the data table has too few vectors.
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

  /**
   * Trains the first centroid generation of an index that has no generation. The training is the
   * same as {@link #trainAndRecord}.
   * <p>
   * The caller must hold the rebuild claim of the index, from {@link CentroidManager#claim}. The
   * caller must take the claim before this call and keep it until the index build after this call
   * is complete, as ALTER INDEX ... REBUILD does. Without the claim, two trainers can mix their
   * centroid rows, which both number from 0, or record different generations. Also, two builds can
   * race with a rebuild of the index. If the index has a generation when this call reads it, this
   * call does not train.
   * @param conn connection of the caller, which this call uses to read the index
   * @return {@link VectorIndexRebuilder.Outcome#REBUILT} if this call recorded the first
   *         generation, {@link VectorIndexRebuilder.Outcome#UP_TO_DATE} if the index had a
   *         generation before the claim, or {@link VectorIndexRebuilder.Outcome#UNTRAINED} if the
   *         data table has too few vectors to train
   */
  public static VectorIndexRebuilder.Outcome trainFirstGeneration(PhoenixConnection conn,
    PTable dataTable, PTable index) throws SQLException {
    PTable current = conn.getTableNoCache(index.getName().getString());
    if (current.getVectorCentroidGeneration() != null) {
      return VectorIndexRebuilder.Outcome.UP_TO_DATE;
    }
    try (PhoenixConnection internal = CentroidManager.newInternalConnection(conn)) {
      return trainAndRecord(internal, dataTable, current) != null
        ? VectorIndexRebuilder.Outcome.REBUILT
        : VectorIndexRebuilder.Outcome.UNTRAINED;
    }
  }

  private static List<float[]> sample(PhoenixConnection conn, String sql, int sampleSize)
    throws SQLException {
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      return KMeansTrainer.reservoirSample(new VectorIterator(rs), sampleSize, new Random());
    } catch (VectorIterator.ReadException e) {
      throw e.getCause();
    }
  }

  /**
   * Iterates the vectors of a result set as float arrays. It returns null for a null or non-finite
   * vector, and it wraps an SQLException in a ReadException.
   */
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
        float[] vector = toFloats(rs.getObject(1));
        for (int i = 0; vector != null && i < vector.length; i++) {
          if (!Float.isFinite(vector[i])) {
            // K-means cannot place a non-finite vector, for example a DOUBLE element beyond
            // float range that narrows to infinity. The sample skips it, as it skips a null one.
            return null;
          }
        }
        return vector;
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
