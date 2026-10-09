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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TASK_TABLE_TTL;

import java.util.concurrent.Future;
import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResult;
import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResultCode;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.index.vector.VectorIndexRebuilder;
import org.apache.phoenix.index.vector.VectorIndexRebuilder.ReconcileOutcome;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.ServerTask;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recurring server side task that reconciles the scorecard of a vector index. When the reconcile
 * interval is complete, the reconcile updates the scorecard and assesses drift. If it finds drift,
 * it can enqueue a background rebuild. The reconcile runs on {@link VectorIndexTaskRunner}. Each
 * sweep gets the result of the last reconcile and starts the next one.
 */
public class VectorScorecardReconcileTask extends BaseTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorScorecardReconcileTask.class);

  /**
   * Age at which the task replaces its row with a new row. {@code SYSTEM.TASK} expires a row when
   * its TTL after the TASK_TS of the row is complete. Status updates do not change TASK_TS. Thus
   * the recurring task must move to a new row before the old row expires.
   */
  static final long RENEWAL_AGE_MS = Long.parseLong(TASK_TABLE_TTL) * 1000 / 2;

  @Override
  public TaskResult run(Task.TaskRecord taskRecord) {
    String indexName =
      SchemaUtil.getTableName(taskRecord.getSchemaName(), taskRecord.getTableName());
    Future<TaskResult> reconciliation = VectorIndexTaskRunner.get(taskRecord);
    if (reconciliation != null) {
      if (!reconciliation.isDone()) {
        return new TaskResult(TaskResultCode.SKIPPED, "Reconciling");
      }
      VectorIndexTaskRunner.remove(taskRecord, reconciliation);
      try {
        TaskResult result = reconciliation.get();
        if (result.getResultCode() != TaskResultCode.SKIPPED) {
          return result;
        }
      } catch (Exception e) {
        LOGGER.warn("Reconciliation of vector index {} failed", indexName, e);
      }
    }
    if (VectorIndexTaskRunner.isBackingOff(indexName)) {
      return new TaskResult(TaskResultCode.SKIPPED, "Backing off after failures");
    }
    VectorIndexTaskRunner.submit(taskRecord, () -> reconcile(taskRecord, indexName));
    return new TaskResult(TaskResultCode.SKIPPED, "Awaiting the next reconciliation");
  }

  private TaskResult reconcile(Task.TaskRecord taskRecord, String indexName) {
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {
      try {
        long start = EnvironmentEdgeManager.currentTimeMillis();
        ReconcileOutcome outcome = VectorIndexRebuilder.reconcile(conn, indexName);
        if (outcome == ReconcileOutcome.INDEX_DROPPED) {
          return new TaskResult(TaskResultCode.SUCCESS, "Vector index " + indexName + " dropped");
        }
        if (outcome == ReconcileOutcome.RECONCILED) {
          VectorIndexTaskRunner.succeeded(indexName);
          MetricsIndexerSourceFactory.getInstance().getMetricsVectorIndexSource()
            .updateVectorScorecardReconcileTime(indexName,
              EnvironmentEdgeManager.currentTimeMillis() - start);
        }
      } catch (Throwable t) {
        LOGGER.warn("Reconciliation of vector index {} failed; a later sweep retries it", indexName,
          t);
        VectorIndexTaskRunner.failed(indexName, env.getConfiguration());
      }
      if (
        EnvironmentEdgeManager.currentTimeMillis() - taskRecord.getTimeStamp().getTime()
            >= RENEWAL_AGE_MS
      ) {
        renew(conn, taskRecord);
        return new TaskResult(TaskResultCode.SUCCESS, "Continued in a new task row");
      }
    } catch (Throwable t) {
      LOGGER.warn("Could not renew the reconciliation task of vector index {}", indexName, t);
    }
    return new TaskResult(TaskResultCode.SKIPPED, "Awaiting the next reconciliation");
  }

  /** Adds the next row of the task, unless an earlier attempt to replace this row added it. */
  private static void renew(PhoenixConnection conn, Task.TaskRecord taskRecord) throws Exception {
    for (Task.TaskRecord task : Task.queryTaskTable(conn, null, taskRecord.getSchemaName(),
      taskRecord.getTableName(), taskRecord.getTaskType(), taskRecord.getTenantId(), null)) {
      if (
        task.getTimeStamp().after(taskRecord.getTimeStamp())
          && !PTable.TaskStatus.COMPLETED.toString().equals(task.getStatus())
          && !PTable.TaskStatus.FAILED.toString().equals(task.getStatus())
      ) {
        return;
      }
    }
    ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder().setConn(conn)
      .setTaskType(taskRecord.getTaskType()).setTenantId(taskRecord.getTenantId())
      .setSchemaName(taskRecord.getSchemaName()).setTableName(taskRecord.getTableName())
      .setData(taskRecord.getData()).setPriority(taskRecord.getPriority())
      .setAccessCheckEnabled(false).build());
  }

  /** Starts or collects a reconcile on each sweep of the task table. */
  @Override
  public TaskResult checkCurrentResult(Task.TaskRecord taskRecord) {
    return run(taskRecord);
  }
}
