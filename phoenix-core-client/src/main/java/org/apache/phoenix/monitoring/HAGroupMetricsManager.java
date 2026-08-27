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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.query.QueryServicesOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Central registry of {@link HAGroupClientMetricsSource}, one per HA group name. Each group's
 * source registers with Hadoop's metrics2 {@code DefaultMetricsSystem} tagged
 * {@code haGroup=<name>} so the HA/CRR/failover client metrics can be sliced per HA group alongside
 * the JVM-global {@code GLOBAL_HA_*} counters (which continue to emit unchanged).
 * <p>
 * Sources are created only when global client metrics are enabled (constructing a source registers
 * it with the metrics system). {@link #remove(String)} detaches a group's source on HA-group close.
 */
public class HAGroupMetricsManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(HAGroupMetricsManager.class);

  private static final boolean IS_GLOBAL_METRICS_ENABLED =
    QueryServicesOptions.withDefaults().isGlobalMetricsEnabled();

  private static final Map<String, HAGroupClientMetricsSource> GROUP_SOURCES =
    new ConcurrentHashMap<>();

  private HAGroupMetricsManager() {
  }

  /**
   * Get (registering a metrics2 source on first sight) the metrics source for an HA group. Safe to
   * call repeatedly for the same name. Returns {@code null} for a null/empty group name, when
   * global client metrics are disabled, or when registering the source fails.
   */
  public static HAGroupClientMetricsSource getOrCreate(String haGroupName) {
    if (StringUtils.isEmpty(haGroupName) || !IS_GLOBAL_METRICS_ENABLED) {
      return null;
    }
    HAGroupClientMetricsSource source = GROUP_SOURCES.get(haGroupName);
    if (source != null) {
      return source;
    }
    synchronized (HAGroupMetricsManager.class) {
      source = GROUP_SOURCES.get(haGroupName);
      if (source == null) {
        try {
          source = new HAGroupClientMetricsSource(haGroupName);
        } catch (Exception e) {
          // Constructing a source registers it with DefaultMetricsSystem and can throw (e.g. a
          // MetricsException on a duplicate JMX name). Metrics registration is best-effort and must
          // never break a caller such as HighAvailabilityGroup.init(); swallow and skip this group.
          LOGGER.error("Failed creating HA-group client metrics source for group '{}'", haGroupName,
            e);
          return null;
        }
        GROUP_SOURCES.put(haGroupName, source);
        LOGGER.info("Created HA-group client metrics source for group '{}'", haGroupName);
      }
    }
    return source;
  }

  /**
   * Increment the per-group counter for an HA metric type. Registers the group's source if needed.
   * Metric emission is best-effort and never propagates an exception to the caller's request path.
   */
  public static void increment(String haGroupName, MetricType type) {
    try {
      HAGroupClientMetricsSource source = getOrCreate(haGroupName);
      if (source != null) {
        source.increment(type);
      }
    } catch (Exception e) {
      LOGGER.error("Failed incrementing HA-group metric {} for group '{}'", type, haGroupName, e);
    }
  }

  /**
   * Add a sample to a per-group accumulating HA metric (e.g. {@code CRR_TRANSITION_DURATION_MS}).
   * Registers the group's source if needed. Best-effort; never propagates an exception.
   */
  public static void update(String haGroupName, MetricType type, long value) {
    try {
      HAGroupClientMetricsSource source = getOrCreate(haGroupName);
      if (source != null) {
        source.update(type, value);
      }
    } catch (Exception e) {
      LOGGER.error("Failed updating HA-group metric {} for group '{}'", type, haGroupName, e);
    }
  }

  /**
   * Tear down a group's metrics source on HA-group close, freeing its metrics2 source name so the
   * same group can be re-created later.
   */
  public static void remove(String haGroupName) {
    if (StringUtils.isEmpty(haGroupName)) {
      return;
    }
    synchronized (HAGroupMetricsManager.class) {
      HAGroupClientMetricsSource source = GROUP_SOURCES.remove(haGroupName);
      if (source != null) {
        source.unregister();
        LOGGER.info("Removed HA-group client metrics source for group '{}'", haGroupName);
      }
    }
  }

  @VisibleForTesting
  public static HAGroupClientMetricsSource getIfPresent(String haGroupName) {
    return GROUP_SOURCES.get(haGroupName);
  }
}
