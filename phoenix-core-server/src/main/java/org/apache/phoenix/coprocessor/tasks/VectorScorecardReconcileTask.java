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

import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResult;
import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResultCode;
import org.apache.phoenix.index.vector.VectorIndexRebuilder;
import org.apache.phoenix.index.vector.VectorIndexRebuilder.ReconcileOutcome;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic server side task managing vector index drift scorecard reconciliation. Evaluates
 * reconciliation intervals and triggers background rebuilds upon detecting drift.
 */
public class VectorScorecardReconcileTask extends BaseTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorScorecardReconcileTask.class);

  @Override
  public TaskResult run(Task.TaskRecord taskRecord) {
    String indexName =
      SchemaUtil.getTableName(taskRecord.getSchemaName(), taskRecord.getTableName());
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {
      if (VectorIndexRebuilder.reconcile(conn, indexName) == ReconcileOutcome.INDEX_DROPPED) {
        return new TaskResult(TaskResultCode.SUCCESS, "Vector index " + indexName + " dropped");
      }
    } catch (Throwable t) {
      LOGGER.warn("Reconciliation of vector index {} failed; the next sweep retries it", indexName,
        t);
    }
    return new TaskResult(TaskResultCode.SKIPPED, "Awaiting the next reconciliation");
  }

  /** Executes periodic reconciliation check on each task table sweep. */
  @Override
  public TaskResult checkCurrentResult(Task.TaskRecord taskRecord) {
    return run(taskRecord);
  }
}
