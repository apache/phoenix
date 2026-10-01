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
 * Server-side JMX metrics source that counts how many client-originated mutation batches pass
 * through {@code IndexRegionObserver.preBatchMutate} <em>without</em> an {@code _HAGroupName}
 * attribute, so the cluster-role-based mutation-block gate has no haGroupName to evaluate against
 * and is skipped for that batch.
 * <p>
 * This is a <strong>path-coverage detector</strong>, not a safety violation alarm. The counter is
 * incremented in {@code IndexRegionObserver.preBatchMutate} only for a genuine client write that
 * reached the write path without attaching {@code _HAGroupName} (batch origin
 * {@code CLIENT_NON_HA}) on a replication-eligible table (the sync-replication master switch is on
 * AND the table is eligible). Standby replay ({@code PHX_REPLAY}) and native-replicated-in
 * ({@code NATIVE_IN}) batches also lack {@code _HAGroupName} but are distinct origins and are
 * <em>excluded</em>, as are system-HA-group state writes (which carry a haGroup, i.e.
 * {@code CLIENT_HA}). The "bypass" terminology refers strictly to the gate-evaluation code path
 * being short-circuited; it does <strong>not</strong> imply that any safety property was breached —
 * the signal is simply "a client missed attaching {@code _HAGroupName}."
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
    "Path-coverage counter: number of client-originated mutation batches that reached "
      + "preBatchMutate on a replication-eligible table without an _HAGroupName attribute "
      + "(origin CLIENT_NON_HA), so the cluster-role-based mutation-block gate had nothing to "
      + "evaluate against and was skipped. Standby-replay and native-replicated-in batches are "
      + "excluded. Counts the code path being skipped, not a safety breach — the signal is that "
      + "a client missed attaching _HAGroupName. Actionable signal is delta-against-baseline "
      + "(e.g., a spike after a deploy introducing a new mutation path), not absolute value";

  /**
   * Increments the gate-skipped-path counter. Called from
   * {@code IndexRegionObserver.preBatchMutate} only for a {@code CLIENT_NON_HA} batch on a
   * replication-eligible table — i.e., a genuine client write that reached the write path without
   * an {@code _HAGroupName} attribute while the sync-replication master switch is on and the table
   * is eligible, so the cluster-role-based mutation-block gate cannot be evaluated for it.
   * Standby-replay ({@code PHX_REPLAY}), native-replicated-in ({@code NATIVE_IN}), and
   * {@code CLIENT_HA} batches are excluded.
   */
  void incrementBypassedMutationBlockCount();
}
