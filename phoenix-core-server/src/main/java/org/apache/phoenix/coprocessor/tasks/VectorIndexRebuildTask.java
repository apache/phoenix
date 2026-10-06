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
import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResult;
import org.apache.phoenix.coprocessor.TaskRegionObserver.TaskResultCode;
import org.apache.phoenix.index.vector.VectorIndexRebuilder;
import org.apache.phoenix.index.vector.VectorIndexRebuilder.Outcome;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Strings;

/**
 * Server side background task orchestrating asynchronous vector index rebuilds. Enqueued during
 * drift detection or via {@code ALTER INDEX ... REBUILD ASYNC}.
 */
public class VectorIndexRebuildTask extends BaseTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexRebuildTask.class);

  @Override
  public TaskResult run(Task.TaskRecord taskRecord) {
    String indexName =
      SchemaUtil.getTableName(taskRecord.getSchemaName(), taskRecord.getTableName());
    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(env.getConfiguration()).unwrap(PhoenixConnection.class)) {
      boolean manual = false;
      String reason = null;
      if (!Strings.isNullOrEmpty(taskRecord.getData())) {
        JsonNode data = JacksonUtil.getObjectReader().readTree(taskRecord.getData());
        manual = data.path("manual").asBoolean(false);
        reason = data.path("reason").asText(null);
      }
      Outcome outcome = VectorIndexRebuilder.rebuild(conn, indexName, manual,
        manual && Strings.isNullOrEmpty(reason) ? VectorIndexRebuilder.MANUAL_REASON : reason);
      if (outcome == Outcome.IN_PROGRESS) {
        return new TaskResult(TaskResultCode.SKIPPED, "Vector index " + indexName + " is busy");
      }
      return new TaskResult(TaskResultCode.SUCCESS, outcome.name());
    } catch (Throwable t) {
      LOGGER.error("Rebuild of vector index {} failed", indexName, t);
      return new TaskResult(TaskResultCode.FAIL, t.toString());
    }
  }

  /** Retries rebuild execution on subsequent task sweeps if previously busy. */
  @Override
  public TaskResult checkCurrentResult(Task.TaskRecord taskRecord) {
    return run(taskRecord);
  }
}
