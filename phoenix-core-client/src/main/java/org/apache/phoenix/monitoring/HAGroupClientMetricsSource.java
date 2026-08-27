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
package org.apache.phoenix.monitoring;

import static org.apache.phoenix.monitoring.MetricType.CRR_TRANSITION_COUNT;
import static org.apache.phoenix.monitoring.MetricType.CRR_TRANSITION_DURATION_MS;
import static org.apache.phoenix.monitoring.MetricType.HA_CRR_REFRESH_COUNT;
import static org.apache.phoenix.monitoring.MetricType.HA_FAILOVER_CONNECTION_CREATED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.HA_FAILOVER_CONNECTION_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.HA_FAILOVER_COUNT;
import static org.apache.phoenix.monitoring.MetricType.HA_MUTATION_BLOCKED_COUNT;
import static org.apache.phoenix.monitoring.MetricType.HA_PARALLEL_CONNECTION_CREATED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.HA_PARALLEL_CONNECTION_ERROR_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.HA_PARALLEL_CONNECTION_FALLBACK_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.HA_PARALLEL_TASK_TIMEOUT_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.HA_POLLER_TICK_COUNT;
import static org.apache.phoenix.monitoring.MetricType.HA_POLLER_TICK_FAILURES;
import static org.apache.phoenix.monitoring.MetricType.HA_ROLE_TRANSITION_FAILED_COUNTER;
import static org.apache.phoenix.monitoring.MetricType.HA_STALE_CRR_DETECTED_COUNT;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import javax.management.ObjectName;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.phoenix.metrics.MetricConstants;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;

/**
 * Per-HA-group Hadoop Metrics2 source for the ZK-less HA client's HA/CRR/failover metrics.
 * <p>
 * All groups share one metrics name/context; each instance appends {@code ,haGroup=<quoted-name>}
 * to the JMX context so it registers as a distinct metrics2 source / MBean, and stamps a
 * {@code ha_group} tag ({@link MetricConstants#HA_GROUP_TAG_NAME}) carrying the (unquoted) group
 * name so the series can be sliced per HA group downstream. This mirrors the server-side
 * {@code HAGroupStoreMetricsSourceImpl} tagging pattern; both reference the same tag key.
 * <p>
 * Each metric in {@link #METRIC_TYPES} is a monotonic {@link MutableFastCounter}. Accumulating
 * metrics such as {@code CRR_TRANSITION_DURATION_MS} add total milliseconds via {@link #update},
 * which matches the semantics of the JVM-global {@code GLOBAL_HA_*} counters; those continue to
 * emit unchanged alongside this per-group source (dual emit).
 * <p>
 * The set is intentionally limited to metrics attributable to a single HA group (each emission site
 * has a {@code HighAvailabilityGroup} in scope). The shared parallel-executor pool metrics
 * ({@code HA_PARALLEL_POOL1_*}/{@code HA_PARALLEL_POOL2_*}) and the {@code HA_CRR_CACHE_AGE_MS}
 * gauge are excluded: the pools are JVM-shared and the gauge is not a counter.
 */
public class HAGroupClientMetricsSource extends BaseSourceImpl {

  static final String METRICS_NAME = "HAGroupClient";
  static final String METRICS_DESC = "Phoenix HA Group Client Metrics";
  static final String METRICS_CONTEXT = "phoenix";
  static final String METRICS_JMX_CONTEXT = "Phoenix,sub=" + METRICS_NAME;

  /**
   * The HA metrics attributable to a single HA group; each emission site has a
   * {@code HighAvailabilityGroup} in scope.
   */
  private static final List<MetricType> METRIC_TYPES = ImmutableList.of(HA_FAILOVER_COUNT,
    CRR_TRANSITION_DURATION_MS, HA_FAILOVER_CONNECTION_CREATED_COUNTER,
    HA_FAILOVER_CONNECTION_FAILED_COUNTER, HA_STALE_CRR_DETECTED_COUNT, HA_MUTATION_BLOCKED_COUNT,
    HA_CRR_REFRESH_COUNT, HA_ROLE_TRANSITION_FAILED_COUNTER, CRR_TRANSITION_COUNT,
    HA_PARALLEL_CONNECTION_FALLBACK_COUNTER, HA_PARALLEL_CONNECTION_CREATED_COUNTER,
    HA_PARALLEL_CONNECTION_ERROR_COUNTER, HA_PARALLEL_TASK_TIMEOUT_COUNTER, HA_POLLER_TICK_COUNT,
    HA_POLLER_TICK_FAILURES);

  private final Map<MetricType, MutableFastCounter> counters = new EnumMap<>(MetricType.class);

  public HAGroupClientMetricsSource(String haGroupName) {
    super(METRICS_NAME, METRICS_DESC, METRICS_CONTEXT,
      METRICS_JMX_CONTEXT + ",haGroup=" + ObjectName.quote(haGroupName));
    getMetricsRegistry().tag(
      Interns.info(MetricConstants.HA_GROUP_TAG_NAME, MetricConstants.HA_GROUP_TAG_DESC),
      haGroupName);
    for (MetricType type : METRIC_TYPES) {
      counters.put(type,
        getMetricsRegistry().newCounter(type.columnName(), type.description(), 0L));
    }
  }

  /** Increment the group's counter for the given HA metric type. */
  public void increment(MetricType type) {
    MutableFastCounter counter = counters.get(type);
    if (counter != null) {
      counter.incr();
    }
  }

  /**
   * Add {@code value} to the group's accumulating counter for the given HA metric type (used for
   * {@code CRR_TRANSITION_DURATION_MS}).
   */
  public void update(MetricType type, long value) {
    MutableFastCounter counter = counters.get(type);
    if (counter != null) {
      counter.incr(value);
    }
  }

  /**
   * Detach this source from the metrics system on HA-group close, freeing its JMX-context name in
   * {@link DefaultMetricsSystem} so the same group can register again if later re-created.
   */
  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(metricsJmxContext);
  }

  @VisibleForTesting
  public long getCounterValue(MetricType type) {
    MutableFastCounter counter = counters.get(type);
    return counter == null ? -1L : counter.value();
  }
}
