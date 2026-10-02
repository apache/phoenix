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

import static org.apache.phoenix.query.QueryServices.VECTOR_INDEX_REBUILD_AUTO_ENABLED_ATTRIB;
import static org.apache.phoenix.query.QueryServices.VECTOR_INDEX_SCORECARD_RECONCILE_INTERVAL_MS_ATTRIB;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_VECTOR_INDEX_REBUILD_AUTO_ENABLED;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_VECTOR_INDEX_SCORECARD_RECONCILE_INTERVAL_MS;

import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.GenerationSummary;
import org.apache.phoenix.index.vector.VectorIndexScorecard;
import org.apache.phoenix.index.vector.VectorIndexScorecard.DriftEvaluationResult;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically reconciles a vector index's drift scorecard against physical index table counts.
 * <p>
 * Recomputes cluster sizes at configured intervals to correct for untracked mutations and deletes.
 * When automatic rebuilds are enabled, evaluates drift thresholds following reconciliation and
 * enqueues rebuild tasks as needed.
 */
public class VectorScorecardReconcileTask extends BaseTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorScorecardReconcileTask.class);

  @Override
  public TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord) {
    String indexName =
      SchemaUtil.getTableName(taskRecord.getSchemaName(), taskRecord.getTableName());
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {

      // Check if reconciliation is due before checking table existence to avoid metadata RPCs.
      long generation = CentroidManager.getGeneration(conn, indexName);
      long intervalMs =
        env.getConfiguration().getLong(VECTOR_INDEX_SCORECARD_RECONCILE_INTERVAL_MS_ATTRIB,
          DEFAULT_VECTOR_INDEX_SCORECARD_RECONCILE_INTERVAL_MS);
      GenerationSummary summary =
        CentroidManager.loadGenerationSummary(conn, indexName, generation);
      if (!isDue(summary, intervalMs, System.currentTimeMillis())) {
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED,
          "Reconcile interval has not elapsed");
      }

      // If the index has been dropped, retire the task.
      try {
        conn.getTableNoCache(indexName);
      } catch (TableNotFoundException e) {
        LOGGER.info("Vector index {} no longer exists; retiring its scorecard reconcile task.",
          indexName);
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS, "");
      }

      java.util.List<org.apache.phoenix.index.vector.ScorecardRow> preResetRows =
        VectorIndexScorecard.reconcile(conn, indexName, generation);
      LOGGER.info("Reconciled scorecard for vector index {} generation {}", indexName, generation);

      // If automatic rebuilds are enabled, evaluate drift on reconciled counts and enqueue rebuild
      // task.
      if (
        env.getConfiguration().getBoolean(VECTOR_INDEX_REBUILD_AUTO_ENABLED_ATTRIB,
          DEFAULT_VECTOR_INDEX_REBUILD_AUTO_ENABLED)
      ) {
        DriftEvaluationResult drift =
          VectorIndexScorecard.evaluateRows(preResetRows, env.getConfiguration());
        CentroidManager.persistGenerationSummary(conn, indexName, generation, null, null,
          drift.getTriggerReason(), null, System.currentTimeMillis());
        if (drift.shouldRebuild()) {
          LOGGER.info("Vector index {} generation {} exceeded drift thresholds ({}); "
            + "enqueueing a rebuild.", indexName, generation, drift.getTriggerReason());
          CentroidManager.scheduleRebuildTask(conn, indexName, false);
        }
      }
    } catch (Throwable t) {
      // Return SKIPPED so transient failures can be retried on subsequent sweeps.
      LOGGER.warn("Scorecard reconciliation failed for vector index {}; will retry on the next "
        + "task sweep.", indexName, t);
      return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED,
        t.toString());
    }
    return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED,
      "Reconciled; awaiting next interval");
  }

  /**
   * Returns true if at least {@code intervalMs} has elapsed since the last reconciliation, or if
   * the generation has not yet been reconciled.
   */
  static boolean isDue(GenerationSummary summary, long intervalMs, long now) {
    if (summary == null || summary.getLastScorecardUpdate() == null) {
      return true;
    }
    return now - summary.getLastScorecardUpdate() >= intervalMs;
  }

  @Override
  public TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord) {
    return null;
  }
}
