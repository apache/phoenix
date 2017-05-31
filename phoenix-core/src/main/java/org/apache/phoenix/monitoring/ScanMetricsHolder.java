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
package org.apache.phoenix.monitoring;

import static org.apache.phoenix.monitoring.MetricType.COUNT_RPC_CALLS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_REMOTE_RPC_CALLS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_MILLS_BETWEEN_NEXTS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_NOT_SERVING_REGION_EXCEPTION;
import static org.apache.phoenix.monitoring.MetricType.COUNT_BYTES_REGION_SERVER_RESULTS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_BYTES_IN_REMOTE_RESULTS;
import static org.apache.phoenix.monitoring.MetricType.COUNT_SCANNED_REGIONS;

import org.apache.hadoop.hbase.client.Scan;

import static org.apache.phoenix.monitoring.MetricType.COUNT_RPC_RETRIES;
import static org.apache.phoenix.monitoring.MetricType.COUNT_REMOTE_RPC_RETRIES;
import static org.apache.phoenix.monitoring.MetricType.COUNT_ROWS_SCANNED;
import static org.apache.phoenix.monitoring.MetricType.COUNT_ROWS_FILTERED;

public class ScanMetricsHolder {

    private final CombinableMetric countOfRPCcalls;
    private final CombinableMetric countOfRemoteRPCcalls;
    private final CombinableMetric sumOfMillisSecBetweenNexts;
    private final CombinableMetric countOfNSRE;
    private final CombinableMetric countOfBytesInResults;
    private final CombinableMetric countOfBytesInRemoteResults;
    private final CombinableMetric countOfRegions;
    private final CombinableMetric countOfRPCRetries;
    private final CombinableMetric countOfRemoteRPCRetries;
    private final CombinableMetric countOfRowsScanned;
    private final CombinableMetric countOfRowsFiltered;

    private static final ScanMetricsHolder NO_OP_INSTANCE =
            new ScanMetricsHolder(new ReadMetricQueue(false), "");

    public static ScanMetricsHolder getInstance(ReadMetricQueue readMetrics, String tableName,
            Scan scan, boolean isRequestMetricsEnabled) {
        if (!isRequestMetricsEnabled) {
            return NO_OP_INSTANCE;
        }
        scan.setScanMetricsEnabled(true);
        return new ScanMetricsHolder(readMetrics, tableName);
    }

    private ScanMetricsHolder(ReadMetricQueue readMetrics, String tableName) {
        countOfRPCcalls = readMetrics.allotMetric(COUNT_RPC_CALLS, tableName);
        countOfRemoteRPCcalls = readMetrics.allotMetric(COUNT_REMOTE_RPC_CALLS, tableName);
        sumOfMillisSecBetweenNexts = readMetrics.allotMetric(COUNT_MILLS_BETWEEN_NEXTS, tableName);
        countOfNSRE = readMetrics.allotMetric(COUNT_NOT_SERVING_REGION_EXCEPTION, tableName);
        countOfBytesInResults =
                readMetrics.allotMetric(COUNT_BYTES_REGION_SERVER_RESULTS, tableName);
        countOfBytesInRemoteResults =
                readMetrics.allotMetric(COUNT_BYTES_IN_REMOTE_RESULTS, tableName);
        countOfRegions = readMetrics.allotMetric(COUNT_SCANNED_REGIONS, tableName);
        countOfRPCRetries = readMetrics.allotMetric(COUNT_RPC_RETRIES, tableName);
        countOfRemoteRPCRetries = readMetrics.allotMetric(COUNT_REMOTE_RPC_RETRIES, tableName);
        countOfRowsScanned = readMetrics.allotMetric(COUNT_ROWS_SCANNED, tableName);
        countOfRowsFiltered = readMetrics.allotMetric(COUNT_ROWS_FILTERED, tableName);
    }

    public CombinableMetric getCountOfRemoteRPCcalls() {
        return countOfRemoteRPCcalls;
    }

    public CombinableMetric getSumOfMillisSecBetweenNexts() {
        return sumOfMillisSecBetweenNexts;
    }

    public CombinableMetric getCountOfNSRE() {
        return countOfNSRE;
    }

    public CombinableMetric getCountOfBytesInRemoteResults() {
        return countOfBytesInRemoteResults;
    }

    public CombinableMetric getCountOfRegions() {
        return countOfRegions;
    }

    public CombinableMetric getCountOfRPCRetries() {
        return countOfRPCRetries;
    }

    public CombinableMetric getCountOfRemoteRPCRetries() {
        return countOfRemoteRPCRetries;
    }

    public CombinableMetric getCountOfRowsFiltered() {
        return countOfRowsFiltered;
    }

    public CombinableMetric getCountOfRPCcalls() {
        return countOfRPCcalls;
    }

    public CombinableMetric getCountOfBytesInResults() {
        return countOfBytesInResults;
    }

    public CombinableMetric getCountOfRowsScanned() {
        return countOfRowsScanned;
    }

}
