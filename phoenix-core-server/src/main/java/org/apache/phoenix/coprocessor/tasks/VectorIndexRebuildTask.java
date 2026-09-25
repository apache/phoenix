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
package org.apache.phoenix.coprocessor.tasks;

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Method;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.cache.VectorCentroidCache;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessorclient.tasks.IndexRebuildTaskConstants;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.GenerationSummary;
import org.apache.phoenix.index.vector.KMeansConfig;
import org.apache.phoenix.index.vector.KMeansResult;
import org.apache.phoenix.index.vector.KMeansTrainer;
import org.apache.phoenix.index.vector.ScorecardRow;
import org.apache.phoenix.index.vector.VectorIndexScorecard;
import org.apache.phoenix.index.vector.VectorIndexScorecard.DriftEvaluationResult;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background task that evaluates vector index drift scorecards and drives asynchronous index
 * rebuilds. Follows the 7-step rebuild lifecycle: 1. Reconcile and Evaluate Drift 2. Train New
 * Centroids 3. Persist New Centroids (marked BUILDING 'B') 4. Populate New Index 5. Catch-up Pass
 * 6. Switch Active Generation in SYSTEM.CATALOG 7. Seed Scorecard and Retire Prior Generation
 */
public class VectorIndexRebuildTask extends BaseTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexRebuildTask.class);

  /**
   * Test hook to pause or inspect state between centroid persistence (Step 3) and generation switch
   * (Step 6).
   */
  public interface RebuildHook {
    void beforeGenerationSwitch(String indexName, long buildingGeneration) throws Exception;
  }

  private static volatile RebuildHook testHook = null;

  public static void setTestHook(RebuildHook hook) {
    testHook = hook;
  }

  public static void clearTestHook() {
    testHook = null;
  }

  @Override
  public TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord) {
    Configuration conf = env != null ? env.getConfiguration() : HBaseConfiguration.create();
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(conf).unwrap(PhoenixConnection.class)) {
      return rebuild(conn, conf, taskRecord);
    } catch (Throwable t) {
      LOGGER.error("VectorIndexRebuildTask failed for taskRecord: " + taskRecord, t);
      return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
        t.toString());
    }
  }

  @Override
  public TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord)
    throws Exception {
    return null;
  }

  public static TaskRegionObserver.TaskResult rebuild(PhoenixConnection conn, Configuration conf,
    Task.TaskRecord taskRecord) throws Exception {
    String indexName = null;
    boolean isManual = false;
    String manualTriggerReason = null;

    if (taskRecord.getData() != null && !taskRecord.getData().isEmpty()) {
      try {
        JsonNode jsonNode =
          JacksonUtil.getObjectReader(JsonNode.class).readValue(taskRecord.getData());
        if (jsonNode.has(IndexRebuildTaskConstants.INDEX_NAME)) {
          indexName =
            jsonNode.get(IndexRebuildTaskConstants.INDEX_NAME).asText().replaceAll("\"", "");
        }
        if (jsonNode.has("manual") && jsonNode.get("manual").asBoolean()) {
          isManual = true;
        }
        if (jsonNode.has("force") && jsonNode.get("force").asBoolean()) {
          isManual = true;
        }
        if (jsonNode.has("triggerReason")) {
          manualTriggerReason = jsonNode.get("triggerReason").asText();
        }
      } catch (Exception ignored) {
      }
    }

    if (indexName == null) {
      indexName = SchemaUtil.getTableName(taskRecord.getSchemaName(), taskRecord.getTableName());
    }

    return rebuild(conn, conf, indexName, isManual, manualTriggerReason);
  }

  public static TaskRegionObserver.TaskResult rebuild(PhoenixConnection conn, Configuration conf,
    String indexName, boolean isManual, String manualTriggerReason) throws Exception {
    return doRebuild(conn, conf, indexName, isManual, manualTriggerReason);
  }

  private static TaskRegionObserver.TaskResult doRebuild(PhoenixConnection conn, Configuration conf,
    String indexName, boolean isManual, String manualTriggerReason) throws Exception {

    long currentGen;
    try {
      currentGen = CentroidManager.getGeneration(conn, indexName);
    } catch (TableNotFoundException e) {
      LOGGER.info("Vector index {} no longer exists; retiring rebuild task", indexName);
      return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS,
        "Index not found");
    }

    // Step 1: Reconcile and Evaluate Drift
    List<ScorecardRow> preResetRows = VectorIndexScorecard.reconcile(conn, indexName, currentGen);
    DriftEvaluationResult evalResult = VectorIndexScorecard.evaluateRows(preResetRows, conf);
    CentroidManager.persistGenerationSummary(conn, indexName, currentGen, null, null,
      evalResult.getTriggerReason(), null, System.currentTimeMillis());

    if (!isManual) {
      if (!evalResult.shouldRebuild()) {
        LOGGER.info("Vector index {} generation {} does not exceed drift thresholds", indexName,
          currentGen);
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED,
          "Drift thresholds not exceeded");
      }

      boolean autoEnabled = conf.getBoolean(QueryServices.VECTOR_INDEX_REBUILD_AUTO_ENABLED_ATTRIB,
        QueryServicesOptions.DEFAULT_VECTOR_INDEX_REBUILD_AUTO_ENABLED);
      if (!autoEnabled) {
        LOGGER.info("Automatic vector index rebuild is disabled for {}", indexName);
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED,
          "Automatic rebuild is disabled");
      }

      GenerationSummary summary =
        CentroidManager.loadGenerationSummary(conn, indexName, currentGen);
      Long lastRebuildTime = (summary != null) ? summary.getLastRebuildTime() : null;
      long minIntervalMs = conf.getLong(QueryServices.VECTOR_INDEX_REBUILD_MIN_INTERVAL_MS_ATTRIB,
        QueryServicesOptions.DEFAULT_VECTOR_INDEX_REBUILD_MIN_INTERVAL_MS);
      long now = System.currentTimeMillis();
      if (lastRebuildTime != null && (now - lastRebuildTime < minIntervalMs)) {
        LOGGER.info(
          "Rebuild storm guard: minimum interval of {} ms not elapsed for {} (elapsed: {} ms)",
          minIntervalMs, indexName, (now - lastRebuildTime));
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED,
          "Minimum rebuild interval has not elapsed");
      }
    }

    String triggerReason = isManual
      ? (manualTriggerReason != null ? manualTriggerReason : "MANUAL")
      : evalResult.getTriggerReason();

    // Step 2: Train New Centroids
    PTable pIndexTable;
    try {
      conn.removeTable(conn.getTenantId(), indexName, null, HConstants.LATEST_TIMESTAMP);
      pIndexTable = conn.getTableNoCache(indexName);
    } catch (TableNotFoundException e) {
      LOGGER.info("Vector index {} no longer exists; retiring rebuild task", indexName);
      return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS,
        "Index not found");
    }

    String fullDataTableName =
      pIndexTable.getParentName() != null ? pIndexTable.getParentName().getString() : null;
    if (fullDataTableName == null) {
      String tenantId = conn.getTenantId() != null ? conn.getTenantId().getString() : null;
      fullDataTableName = CentroidManager.getParentDataTableName(conn, tenantId,
        pIndexTable.getSchemaName() != null ? pIndexTable.getSchemaName().getString() : null,
        pIndexTable.getTableName().getString());
    }
    PTable pDataTable = conn.getTableNoCache(fullDataTableName);

    IndexMaintainer maintainer = pIndexTable.getIndexMaintainer(pDataTable, conn);
    // For functional vector indexes, resolve the indexed expression directly from the index column
    // definition rather than inspecting data table columns.
    String vectorColName =
      maintainer != null ? maintainer.getIndexedVectorColumnName(pDataTable) : null;
    String vectorColSqlExpr = null;
    if (vectorColName != null) {
      vectorColSqlExpr = '"' + vectorColName + '"';
    } else {
      PColumn indexVectorCol = IndexUtil.findVectorColumn(pIndexTable);
      String expressionStr = indexVectorCol == null ? null : indexVectorCol.getExpressionStr();
      if (expressionStr != null && !expressionStr.trim().isEmpty()) {
        vectorColSqlExpr = expressionStr;
      }
    }
    if (vectorColSqlExpr == null) {
      return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
        "Vector column or expression not found for index " + indexName);
    }

    int k = pIndexTable.getVectorIvfLists() != null && pIndexTable.getVectorIvfLists() > 0
      ? pIndexTable.getVectorIvfLists()
      : 4;
    String distanceMetric =
      pIndexTable.getVectorDistanceMetric() != null ? pIndexTable.getVectorDistanceMetric() : "L2";
    int sampleSize =
      pIndexTable.getVectorIvfSampleSize() != null && pIndexTable.getVectorIvfSampleSize() > 0
        ? pIndexTable.getVectorIvfSampleSize()
        : Math.max(k * 100, 1000);

    boolean localKMeans = conf.getBoolean(QueryServices.VECTOR_KMEANS_LOCAL_ATTRIB,
      QueryServicesOptions.DEFAULT_VECTOR_KMEANS_LOCAL);
    KMeansResult kmeansResult = null;
    KMeansConfig kMeansConfig = KMeansConfig.builder().distanceMetric(distanceMetric)
      .sampleSize(sampleSize).maxIterations(20).build();

    // Distributed KMeans training requires a physical data column; functional vector expressions
    // fall back to in-memory sampling.
    if (!localKMeans && vectorColName != null) {
      try {
        Class<?> toolClass = Class.forName("org.apache.phoenix.mapreduce.vector.KMeansTool");
        Method trainMethod = toolClass.getMethod("trainDistributed", Configuration.class,
          String.class, String.class, int.class, int.class, KMeansConfig.class);
        int dimension =
          pIndexTable.getVectorDimension() != null ? pIndexTable.getVectorDimension() : 0;
        kmeansResult = (KMeansResult) trainMethod.invoke(null, conf, fullDataTableName,
          vectorColName, dimension, k, kMeansConfig);
      } catch (Throwable t) {
        LOGGER.warn(
          "Distributed KMeansTool training failed or unavailable, falling back to client-side trainer: {}",
          t.getMessage());
      }
    }

    if (kmeansResult == null) {
      String baseTableSqlName = SchemaUtil.getEscapedFullTableName(fullDataTableName);
      List<float[]> samples =
        KMeansTrainer.sampleVectors(conn, baseTableSqlName, vectorColSqlExpr, sampleSize);
      if (samples == null || samples.isEmpty()) {
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
          "No sample vectors found in base table " + fullDataTableName);
      }
      int actualK = Math.min(k, samples.size());
      kmeansResult = KMeansTrainer.train(samples, actualK, kMeansConfig);
    }

    // Step 3: Persist New Centroids
    long nextGen = currentGen + 1L;
    CentroidManager.persistCentroids(conn, indexName, nextGen, kmeansResult, triggerReason);
    CentroidManager.persistGenerationSummary(conn, indexName, nextGen, null,
      PhoenixDatabaseMetaData.REBUILD_STATE_BUILDING, null, null, null);

    // Pre-load generation N and N+1 centroids into cache for population
    VectorCentroidCache.getInstance(conf).loadCentroids(indexName, currentGen, conn);
    VectorCentroidCache.getInstance(conf).putCentroidsFromFloatList(indexName, nextGen,
      kmeansResult.getCentroids());

    // Execute test hook if registered
    if (testHook != null) {
      testHook.beforeGenerationSwitch(indexName, nextGen);
    }

    // Steps 4+5: Migrate index entries to the new generation, updating row keys and removing
    // obsolete assignments.
    long populationStartTs = System.currentTimeMillis();
    int[] mainResult =
      scanDataTable(conn, conf, pDataTable, pIndexTable, currentGen, nextGen, null, null);
    int[] catchUpResult = scanDataTable(conn, conf, pDataTable, pIndexTable, currentGen, nextGen,
      populationStartTs, System.currentTimeMillis());
    int totalMigrated = mainResult[0] + catchUpResult[0];
    int totalFailures = mainResult[1] + catchUpResult[1];

    if (totalFailures > 0) {
      LOGGER.error("Migration had {} failures out of {} rows for index {}; aborting rebuild",
        totalFailures, totalMigrated, indexName);
      return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
        "Migration failed with " + totalFailures + " errors");
    }

    // Step 6: Switch Active Generation
    CentroidManager.setGenerationAndLists(conn, indexName, nextGen, kmeansResult.getEffectiveK());
    VectorCentroidCache.getInstance(conf).invalidate(indexName);
    VectorCentroidCache.getInstance(conf).putCentroidsFromFloatList(indexName, nextGen,
      kmeansResult.getCentroids());

    // Step 7: Finalize scorecard and retire previous generation
    VectorIndexScorecard.reconcile(conn, indexName, nextGen);
    long completionTime = System.currentTimeMillis();
    CentroidManager.persistGenerationSummary(conn, indexName, nextGen, null,
      PhoenixDatabaseMetaData.REBUILD_STATE_ACTIVE, null, completionTime, completionTime);
    CentroidManager.deleteGeneration(conn, indexName, currentGen);

    LOGGER.info("Successfully rebuilt vector index {} from generation {} to generation {}",
      indexName, currentGen, nextGen);
    return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS,
      "Rebuilt vector index " + indexName + " to generation " + nextGen);
  }

  /** Scans base table records to update index rows for the new centroid generation. */
  private static int[] scanDataTable(PhoenixConnection conn, Configuration conf, PTable dataTable,
    PTable indexTable, long currentGen, long nextGen, Long minTimestamp, Long maxTimestamp)
    throws Exception {
    String indexName = indexTable.getName().getString();
    String centroidColName =
      IndexUtil.getIndexColumnName(null, PhoenixDatabaseMetaData.CENTROID_ID);
    String metric =
      indexTable.getVectorDistanceMetric() != null ? indexTable.getVectorDistanceMetric() : "L2";

    List<String> indexColumnNames = new ArrayList<>();
    List<String> dataColumnExprs = new ArrayList<>();
    int vectorIndexInSelected = -1;
    PColumn vectorIndexColumn = null;

    boolean isSalted = indexTable.getBucketNum() != null;
    boolean isMultiTenant = conn.getTenantId() != null && indexTable.isMultiTenant();
    boolean isViewIndex = indexTable.getViewIndexId() != null;
    int posOffset = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isViewIndex ? 1 : 0);

    List<PColumn> indexPKColumns = indexTable.getPKColumns();
    List<String> pkColNames = new ArrayList<>();
    pkColNames.add(centroidColName);
    for (int i = posOffset; i < indexPKColumns.size(); i++) {
      PColumn col = indexPKColumns.get(i);
      String indexColName = col.getName().getString();
      if (centroidColName.equals(indexColName)) {
        continue;
      }
      pkColNames.add(indexColName);
      String dataColExpr = IndexUtil.getCaseSensitiveDataColumnFullName(indexColName);
      indexColumnNames.add(indexColName);
      dataColumnExprs.add(dataColExpr);
      if (col.getDataType() != null && col.getDataType().isVectorType()) {
        vectorIndexInSelected = dataColumnExprs.size() - 1;
        vectorIndexColumn = col;
      }
    }

    for (PColumnFamily family : indexTable.getColumnFamilies()) {
      for (PColumn col : family.getColumns()) {
        if (col.getViewConstant() == null) {
          String indexColName = col.getName().getString();
          String dataColExpr = IndexUtil.getCaseSensitiveDataColumnFullName(indexColName);
          indexColumnNames.add(indexColName);
          dataColumnExprs.add(dataColExpr);
          if (col.getDataType() != null && col.getDataType().isVectorType()) {
            vectorIndexInSelected = dataColumnExprs.size() - 1;
            vectorIndexColumn = col;
          }
        }
      }
    }

    IndexMaintainer maintainer = indexTable.getIndexMaintainer(dataTable, conn);
    String indexedVectorColName =
      maintainer != null ? maintainer.getIndexedVectorColumnName(dataTable) : null;
    if (vectorIndexInSelected < 0 && indexedVectorColName != null) {
      dataColumnExprs.add('"' + indexedVectorColName + '"');
      vectorIndexInSelected = dataColumnExprs.size() - 1;
      try {
        vectorIndexColumn = dataTable.getColumnForColumnName(indexedVectorColName);
      } catch (Exception ignored) {
      }
    }

    String escapedDataTable = SchemaUtil.getEscapedFullTableName(dataTable.getName().getString());
    StringBuilder selectSql = new StringBuilder("SELECT /*+ NO_INDEX */ ");
    for (int i = 0; i < dataColumnExprs.size(); i++) {
      if (i > 0) {
        selectSql.append(", ");
      }
      selectSql.append(dataColumnExprs.get(i));
    }
    selectSql.append(" FROM ").append(escapedDataTable);

    StringBuilder upsertSql = new StringBuilder("UPSERT /*+ NO_INDEX */ INTO ");
    upsertSql.append(SchemaUtil.getEscapedFullTableName(indexName)).append(" (");
    upsertSql.append('"').append(centroidColName).append('"');
    for (String colName : indexColumnNames) {
      upsertSql.append(", \"").append(colName).append('"');
    }
    upsertSql.append(") VALUES (?");
    for (int i = 0; i < indexColumnNames.size(); i++) {
      upsertSql.append(", ?");
    }
    upsertSql.append(")");

    StringBuilder deleteSql = new StringBuilder("DELETE FROM ");
    deleteSql.append(SchemaUtil.getEscapedFullTableName(indexName)).append(" WHERE ");
    for (int i = 0; i < pkColNames.size(); i++) {
      if (i > 0) {
        deleteSql.append(" AND ");
      }
      deleteSql.append('"').append(pkColNames.get(i)).append("\" = ?");
    }

    VectorCentroidCache centroidCache = VectorCentroidCache.getInstance(conf);
    centroidCache.loadCentroids(indexName, nextGen, conn);
    centroidCache.loadCentroids(indexName, currentGen, conn);

    String upsertStr = upsertSql.toString();
    String deleteStr = deleteSql.toString();
    int[] result;
    if (minTimestamp != null) {
      try (PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class)) {
        QueryPlan plan = stmt.compileQuery(selectSql.toString());
        plan.getContext().getScan().setTimeRange(minTimestamp,
          maxTimestamp != null ? maxTimestamp : HConstants.LATEST_TIMESTAMP);
        try (
          PhoenixResultSet rs =
            new PhoenixResultSet(plan.iterator(), plan.getProjector(), plan.getContext());
          PreparedStatement upsertPs = conn.prepareStatement(upsertStr);
          PreparedStatement deletePs = conn.prepareStatement(deleteStr)) {
          result = applyRows(rs, upsertPs, deletePs, centroidCache, indexName, currentGen, nextGen,
            metric, vectorIndexInSelected, vectorIndexColumn, indexColumnNames.size(),
            pkColNames.size(), conn);
        }
      }
    } else {
      try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(selectSql.toString());
        PreparedStatement upsertPs = conn.prepareStatement(upsertStr);
        PreparedStatement deletePs = conn.prepareStatement(deleteStr)) {
        result = applyRows(rs, upsertPs, deletePs, centroidCache, indexName, currentGen, nextGen,
          metric, vectorIndexInSelected, vectorIndexColumn, indexColumnNames.size(),
          pkColNames.size(), conn);
      }
    }
    return result;
  }

  private static int[] applyRows(ResultSet rs, PreparedStatement upsertPs,
    PreparedStatement deletePs, VectorCentroidCache centroidCache, String indexName,
    long currentGen, long nextGen, String metric, int vectorIndexInSelected,
    PColumn vectorIndexColumn, int nonCentroidColCount, int pkColCount, PhoenixConnection conn)
    throws SQLException {
    int rowsProcessed = 0;
    int failures = 0;
    int batchCount = 0;
    while (rs.next()) {
      float[] queryFloats = readVector(rs, vectorIndexInSelected, vectorIndexColumn);
      if (queryFloats == null || queryFloats.length == 0) {
        continue;
      }

      try {
        int newCentroidId =
          centroidCache.findNearestCentroid(indexName, nextGen, queryFloats, metric);

        // Upsert the new-generation index row
        upsertPs.setInt(1, newCentroidId);
        for (int k = 0; k < nonCentroidColCount; k++) {
          Object obj = rs.getObject(k + 1);
          int psIdx = k + 2;
          if (obj == null) {
            upsertPs.setNull(psIdx, Types.NULL);
          } else if (obj instanceof Array) {
            upsertPs.setArray(psIdx, (Array) obj);
          } else {
            upsertPs.setObject(psIdx, obj);
          }
        }
        upsertPs.executeUpdate();

        // Delete the old-generation index row if the centroid assignment changed
        int priorCentroidId =
          centroidCache.findNearestCentroid(indexName, currentGen, queryFloats, metric);
        if (priorCentroidId != newCentroidId) {
          deletePs.setInt(1, priorCentroidId);
          for (int k = 0; k < pkColCount - 1; k++) {
            Object obj = rs.getObject(k + 1);
            if (obj == null) {
              deletePs.setNull(k + 2, Types.NULL);
            } else {
              deletePs.setObject(k + 2, obj);
            }
          }
          deletePs.executeUpdate();
        }

        rowsProcessed++;
      } catch (Exception e) {
        failures++;
        LOGGER.warn("Failed to migrate row for index {}: {}", indexName, e.getMessage());
      }

      if (++batchCount % 1000 == 0) {
        conn.commit();
      }
    }
    conn.commit();
    return new int[] { rowsProcessed, failures };
  }

  /** Decodes the selected vector column into a {@code float[]}, or null when it is absent. */
  private static float[] readVector(ResultSet rs, int vectorIndexInSelected,
    PColumn vectorIndexColumn) throws SQLException {
    if (vectorIndexInSelected < 0) {
      return null;
    }
    Object vecObj = rs.getObject(vectorIndexInSelected + 1);
    if (vecObj == null) {
      return null;
    }
    if (vecObj instanceof float[]) {
      return (float[]) vecObj;
    }
    if (vecObj instanceof Float[]) {
      return unbox((Float[]) vecObj);
    }
    if (vecObj instanceof Array) {
      Object arr = ((Array) vecObj).getArray();
      if (arr instanceof float[]) {
        return (float[]) arr;
      }
      if (arr instanceof Float[]) {
        return unbox((Float[]) arr);
      }
      return null;
    }
    if (vecObj instanceof byte[]) {
      byte[] bytes = (byte[]) vecObj;
      if (vectorIndexColumn != null && vectorIndexColumn.getDataType() instanceof PVectorDouble) {
        double[] doubles = PVectorDouble.readElements(bytes, 0, bytes.length);
        float[] floats = new float[doubles.length];
        for (int d = 0; d < doubles.length; d++) {
          floats[d] = (float) doubles[d];
        }
        return floats;
      }
      return PVectorFloat.readElements(bytes, 0, bytes.length);
    }
    return null;
  }

  private static float[] unbox(Float[] boxed) {
    float[] floats = new float[boxed.length];
    for (int d = 0; d < boxed.length; d++) {
      floats[d] = boxed[d] != null ? boxed[d] : 0.0f;
    }
    return floats;
  }
}
