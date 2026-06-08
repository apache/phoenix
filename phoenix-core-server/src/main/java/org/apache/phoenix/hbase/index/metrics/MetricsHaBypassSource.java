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
package org.apache.phoenix.hbase.index.metrics;

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * Server-side JMX metrics source for tracking mutations that bypass the cluster-role-based
 * mutation-block gate inside {@code IndexRegionObserver.preBatchMutate}. A bypass occurs when the
 * mutation batch reaches the gate without an associated HA group attribute, so the gate has no
 * haGroupName to evaluate state against and skips the block check entirely.
 * <p>
 * A non-zero counter post-deploy can indicate that some write paths reach the gate without carrying
 * the {@code _HAGroupName} attribute, which in turn means those writes can proceed during a
 * mutation-block window and bypass the safety property the gate exists to enforce. Operators should
 * treat sustained non-zero values as a regression signal.
 */
public interface MetricsHaBypassSource extends BaseSource {

  String METRICS_NAME = "HaBypass";
  String METRICS_CONTEXT = "phoenix";
  String METRICS_DESCRIPTION =
    "Metrics for cluster-role-based mutation-block bypass events on the RegionServer";
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String BYPASSED_MUTATION_BLOCK_COUNT = "bypassedMutationBlockCount";
  String BYPASSED_MUTATION_BLOCK_COUNT_DESC =
    "Counter for mutation batches that reached preBatchMutate without an associated HA group "
      + "(no _HAGroupName attribute), causing the cluster-role-based mutation-block gate to be "
      + "skipped";

  /**
   * Increments the bypass counter. Called from {@code IndexRegionObserver.preBatchMutate} when the
   * resolved {@code Optional<ReplicationLogGroup>} is empty (i.e., the mutation batch carries no HA
   * group attribute and the mutation-block gate cannot be evaluated).
   */
  void incrementBypassedMutationBlockCount();
}
