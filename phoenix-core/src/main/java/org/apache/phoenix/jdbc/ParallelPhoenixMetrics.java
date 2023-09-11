/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.jdbc;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.monitoring.AtomicMetric;
import org.apache.phoenix.monitoring.Metric;
import org.apache.phoenix.monitoring.MetricType;

import java.util.EnumMap;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixHAGroupMetrics.HAMetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS;
import static org.apache.phoenix.jdbc.PhoenixHAGroupMetrics.HAMetricType.HA_PARALLEL_COUNT_OPERATIONS;
import static org.apache.phoenix.jdbc.PhoenixHAGroupMetrics.HAMetricType.HA_PARALLEL_USED_OPERATIONS;
import static org.apache.phoenix.monitoring.MetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS_ACTIVE_CLUSTER;
import static org.apache.phoenix.monitoring.MetricType.HA_PARALLEL_COUNT_FAILED_OPERATIONS_STANDBY_CLUSTER;
import static org.apache.phoenix.monitoring.MetricType.HA_PARALLEL_COUNT_OPERATIONS_ACTIVE_CLUSTER;
import static org.apache.phoenix.monitoring.MetricType.HA_PARALLEL_COUNT_OPERATIONS_STANDBY_CLUSTER;
import static org.apache.phoenix.monitoring.MetricType.HA_PARALLEL_COUNT_USED_OPERATIONS_ACTIVE_CLUSTER;
import static org.apache.phoenix.monitoring.MetricType.HA_PARALLEL_COUNT_USED_OPERATIONS_STANDBY_CLUSTER;

public class ParallelPhoenixMetrics extends PhoenixHAGroupMetrics {
    private static List<HAMetricType> TYPES = ImmutableList.of(HA_PARALLEL_COUNT_FAILED_OPERATIONS,HA_PARALLEL_USED_OPERATIONS,HA_PARALLEL_COUNT_OPERATIONS);

    public ParallelPhoenixMetrics() {
        super(TYPES);
    }

    public Metric getActiveClusterOperationCount() {
        return this.get(HA_PARALLEL_COUNT_OPERATIONS,0);
    }

    public Metric getStandbyClusterOperationCount() {
        return this.get(HA_PARALLEL_COUNT_OPERATIONS,1);
    }

    public Metric getActiveClusterFailedOperationCount() {
        return this.get(HA_PARALLEL_COUNT_FAILED_OPERATIONS,0);
    }

    public Metric getStandbyClusterFailedOperationCount() {
        return this.get(HA_PARALLEL_COUNT_FAILED_OPERATIONS,1);
    }

    public Metric getActiveClusterUsedCount() {
        return this.get(HA_PARALLEL_USED_OPERATIONS,0);
    }

    public Metric getStandbyClusterUsedCount() {
        return this.get(HA_PARALLEL_USED_OPERATIONS,1);
    }
}
