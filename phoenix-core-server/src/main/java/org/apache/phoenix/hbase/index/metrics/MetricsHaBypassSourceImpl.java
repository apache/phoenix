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

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

/**
 * Implementation of {@link MetricsHaBypassSource} backed by a single {@link MutableFastCounter} on
 * the RegionServer JMX registry. The counter is per-RegionServer (not keyed by HA group, by design
 * — a bypass event has no haGroupName to attribute against).
 */
public class MetricsHaBypassSourceImpl extends BaseSourceImpl implements MetricsHaBypassSource {

  private final MutableFastCounter bypassedMutationBlockCount;

  /**
   * Default constructor used by {@link MetricsHaBypassSourceFactory}. Registers the source under
   * the standard {@code HaBypass} JMX context shared with operator dashboards.
   */
  public MetricsHaBypassSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  /**
   * Test-friendly constructor that lets callers override the metrics-registry naming, so unit tests
   * can register an isolated source without colliding with the production singleton.
   * @param metricsName        short name reported to the metrics registry
   * @param metricsDescription human-readable description for the registry
   * @param metricsContext     hadoop-metrics2 context name
   * @param metricsJmxContext  JMX context (typically {@code "RegionServer,sub=" + metricsName})
   */
  public MetricsHaBypassSourceImpl(String metricsName, String metricsDescription,
    String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    bypassedMutationBlockCount = getMetricsRegistry().newCounter(BYPASSED_MUTATION_BLOCK_COUNT,
      BYPASSED_MUTATION_BLOCK_COUNT_DESC, 0L);
  }

  @Override
  public void incrementBypassedMutationBlockCount() {
    bypassedMutationBlockCount.incr();
  }

  /**
   * Test-only accessor returning the current counter value. Public so unit/integration tests
   * outside this package can assert increments without going through JMX read scaffolding.
   */
  public long getBypassedMutationBlockCountForTesting() {
    return bypassedMutationBlockCount.value();
  }
}
