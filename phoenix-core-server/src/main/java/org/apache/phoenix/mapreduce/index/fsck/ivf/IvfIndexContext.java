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
package org.apache.phoenix.mapreduce.index.fsck.ivf;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.VectorIndexTrainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckContext;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;

/**
 * Context container for IVF vector index tooling.
 * <p>
 * Manages index metadata, active and building centroid generations, cached centroid models, and
 * tenant agnostic internal connections for accessing vector system catalog tables.
 */
public class IvfIndexContext implements AutoCloseable {
  private final IndexFsckContext fsckContext;
  private final PhoenixConnection connection;
  private final PhoenixConnection internalConnection;
  private final DistanceMetric metric;
  private final Map<Long, CachedCentroids> models = new HashMap<>();

  public IvfIndexContext(IndexFsckContext fsckContext) throws SQLException {
    this.fsckContext = fsckContext;
    this.connection = fsckContext.getConnection().unwrap(PhoenixConnection.class);
    this.internalConnection = CentroidManager.newInternalConnection(connection);
    this.metric = DistanceMetric.fromString(getIndexTable().getVectorDistanceMetric());
  }

  public IndexFsckContext getFsckContext() {
    return fsckContext;
  }

  /** Returns the client connection, configured with the target tenant context if applicable. */
  public PhoenixConnection getConnection() {
    return connection;
  }

  /** Returns a global non-tenant connection for querying vector system tables. */
  public PhoenixConnection getInternalConnection() {
    return internalConnection;
  }

  public PTable getDataTable() {
    return fsckContext.getDataTable();
  }

  public PTable getIndexTable() {
    return fsckContext.getIndexTable();
  }

  /** Returns the full physical name of the vector index. */
  public String getIndexName() {
    return getIndexTable().getName().getString();
  }

  public DistanceMetric getMetric() {
    return metric;
  }

  public Long getActiveGeneration() {
    return getIndexTable().getVectorCentroidGeneration();
  }

  /**
   * Returns the building centroid generation during an active reindex/migration, or null if none.
   */
  public Long getBuildingGeneration() {
    return getIndexTable().isVectorRebuildInProgress()
      ? getIndexTable().getVectorBuildingGeneration()
      : null;
  }

  /** Returns the active and (if currently rebuilding) building centroid generations. */
  public List<Long> getLiveGenerations() {
    List<Long> generations = new ArrayList<>(2);
    if (getActiveGeneration() != null) {
      generations.add(getActiveGeneration());
    }
    if (getBuildingGeneration() != null) {
      generations.add(getBuildingGeneration());
    }
    return generations;
  }

  /** Retrieves the cached centroid model for the specified generation. */
  public CachedCentroids getModel(long generation) throws SQLException {
    CachedCentroids model = models.get(generation);
    if (model == null) {
      model = VectorCentroidCache.getInstance(connection.getQueryServices().getConfiguration())
        .get(internalConnection, getIndexName(), generation, metric);
      models.put(generation, model);
    }
    return model;
  }

  /**
   * Resolves the centroid generation containing the specified centroid ID, or null if unassigned.
   */
  public Long generationOf(int centroidId) throws SQLException {
    for (long generation : getLiveGenerations()) {
      CachedCentroids model = getModel(generation);
      if (centroidId >= model.getFirstId() && centroidId < model.getFirstId() + model.size()) {
        return generation;
      }
    }
    return null;
  }

  /** Returns the SQL expression for the indexed vector column on the data table. */
  public String getVectorExpression() {
    return VectorIndexTrainer.getIndexedVectorColumn(getIndexTable()).getExpressionStr();
  }

  /**
   * Returns data table primary key columns excluding salt and tenant ID columns.
   */
  public List<PColumn> getDataKeyColumns() {
    PTable data = getDataTable();
    List<PColumn> columns = new ArrayList<>(data.getPKColumns());
    if (data.getBucketNum() != null) {
      columns.remove(0);
    }
    if (data.isMultiTenant() && connection.getTenantId() != null) {
      columns.remove(0);
    }
    return columns;
  }

  /**
   * Builds an equality predicate string across data table primary key columns using bind
   * parameters.
   */
  public String getDataKeyPredicate() {
    StringBuilder sb = new StringBuilder();
    for (PColumn column : getDataKeyColumns()) {
      sb.append(sb.length() == 0 ? "" : " AND ")
        .append(SchemaUtil.getEscapedFullColumnName(column.getName().getString())).append(" = ?");
    }
    return sb.toString();
  }

  @Override
  public void close() throws SQLException {
    internalConnection.close();
  }
}
