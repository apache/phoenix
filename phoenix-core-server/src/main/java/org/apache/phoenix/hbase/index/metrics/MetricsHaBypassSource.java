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
 * Server-side JMX metrics source that counts how many mutation batches pass through
 * {@code IndexRegionObserver.preBatchMutate} <em>without</em> a resolvable HA group attribute, so
 * the cluster-role-based mutation-block gate has no haGroupName to evaluate against and is skipped
 * for that batch.
 * <p>
 * This is a <strong>path-coverage detector</strong>, not a safety violation alarm. The counter is
 * incremented in {@code IndexRegionObserver.preBatchMutate} for <em>every</em> mutation batch whose
 * {@code _HAGroupName} attribute cannot be resolved — regardless of whether the cluster-role-based
 * mutation-block feature is enabled, regardless of whether a mutation-block window is currently
 * active for any HA group, and regardless of whether the table being written to is HA-replicated.
 * The "bypass" terminology refers strictly to the gate-evaluation code path being short-circuited;
 * it does <strong>not</strong> imply that any safety property was breached — when the block feature
 * is disabled or no block window is active, there is no property to breach in the first place.
 * <p>
 * Intended operator use:
 * <ul>
 * <li><strong>Path coverage:</strong> baseline rate tells you what fraction of mutations on this
 * RegionServer reach the gate without an HA group attribute. A sustained baseline of zero on an
 * HA-enabled cluster suggests the gate is effectively dead code on that path; a sustained non-zero
 * baseline tells you those write paths exist and need to be either tagged or consciously
 * exempted.</li>
 * <li><strong>Regression signal:</strong> a <em>delta</em> against baseline (especially after a
 * deploy that added a new mutation path) is the actionable signal — it indicates a newly introduced
 * write path is not tagging mutations with {@code _HAGroupName}.</li>
 * </ul>
 * The absolute value alone is not actionable; pair it with deploy markers and the
 * mutation-block-enabled config to interpret correctly.
 */
public interface MetricsHaBypassSource extends BaseSource {

  String METRICS_NAME = "HaBypass";
  String METRICS_CONTEXT = "phoenix";
  String METRICS_DESCRIPTION =
    "Metrics for cluster-role-based mutation-block gate path-coverage on the RegionServer";
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String BYPASSED_MUTATION_BLOCK_COUNT = "bypassedMutationBlockCount";
  String BYPASSED_MUTATION_BLOCK_COUNT_DESC =
    "Path-coverage counter: number of mutation batches that reached preBatchMutate without a "
      + "resolvable _HAGroupName attribute (so the cluster-role-based mutation-block gate had "
      + "nothing to evaluate against and was skipped). Counts the code path being skipped, not "
      + "a safety breach — when the block feature is disabled or no block window is active, "
      + "there is no property to breach. Actionable signal is delta-against-baseline (e.g., a "
      + "spike after a deploy introducing a new mutation path), not absolute value";

  /**
   * Increments the gate-skipped-path counter. Called unconditionally from
   * {@code IndexRegionObserver.preBatchMutate} whenever the resolved
   * {@code Optional<ReplicationLogGroup>} is empty — i.e., the mutation batch carries no
   * {@code _HAGroupName} attribute and the cluster-role-based mutation-block gate cannot be
   * evaluated for it. Independent of whether the mutation-block feature is enabled or any block
   * window is active.
   */
  void incrementBypassedMutationBlockCount();
}
