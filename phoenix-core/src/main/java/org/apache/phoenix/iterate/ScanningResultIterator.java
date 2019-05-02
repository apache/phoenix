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
package org.apache.phoenix.iterate;

import static org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics.COUNT_OF_ROWS_FILTERED_KEY;
import static org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics.COUNT_OF_ROWS_SCANNED_KEY;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_BYTES_IN_REMOTE_RESULTS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_BYTES_REGION_SERVER_RESULTS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_MILLS_BETWEEN_NEXTS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_NOT_SERVING_REGION_EXCEPTION;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_REMOTE_RPC_CALLS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_REMOTE_RPC_RETRIES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_ROWS_FILTERED;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_ROWS_SCANNED;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_RPC_CALLS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_RPC_RETRIES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HBASE_COUNT_SCANNED_REGIONS;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SCAN_BYTES;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.phoenix.monitoring.CombinableMetric;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ServerUtil;

public class ScanningResultIterator implements ResultIterator {
    private final ResultScanner scanner;
    private final Scan scan;
    private final ScanMetricsHolder scanMetricsHolder;
    boolean scanMetricsUpdated;
    boolean scanMetricsEnabled;

    // These metric names are how HBase refers them
    // Since HBase stores these strings as static final, we are using the same here
    static final String RPC_CALLS_METRIC_NAME = "RPC_CALLS";
    static final String REMOTE_RPC_CALLS_METRIC_NAME = "REMOTE_RPC_CALLS";
    static final String MILLIS_BETWEEN_NEXTS_METRIC_NAME = "MILLIS_BETWEEN_NEXTS";
    static final String NOT_SERVING_REGION_EXCEPTION_METRIC_NAME = "NOT_SERVING_REGION_EXCEPTION";
    static final String BYTES_IN_RESULTS_METRIC_NAME = "BYTES_IN_RESULTS";
    static final String BYTES_IN_REMOTE_RESULTS_METRIC_NAME = "BYTES_IN_REMOTE_RESULTS";
    static final String REGIONS_SCANNED_METRIC_NAME = "REGIONS_SCANNED";
    static final String RPC_RETRIES_METRIC_NAME = "RPC_RETRIES";
    static final String REMOTE_RPC_RETRIES_METRIC_NAME = "REMOTE_RPC_RETRIES";

    public ScanningResultIterator(ResultScanner scanner, Scan scan, ScanMetricsHolder scanMetricsHolder) {
        this.scanner = scanner;
        this.scan = scan;
        this.scanMetricsHolder = scanMetricsHolder;
        scanMetricsUpdated = false;
        scanMetricsEnabled = scan.isScanMetricsEnabled();
    }

    @Override
    public void close() throws SQLException {
        // close the scanner so that metrics are available
        scanner.close();
        updateMetrics();
    }

    private void changeMetric(CombinableMetric metric, Long value) {
        if(value != null) {
            metric.change(value);
        }
    }

    private void changeMetric(GlobalClientMetrics metric, Long value) {
        if(value != null) {
            metric.update(value);
        }
    }

    private void updateMetrics() {

        if (scanMetricsEnabled && !scanMetricsUpdated) {
            ScanMetrics scanMetrics = scan.getScanMetrics();
            Map<String, Long> scanMetricsMap = scanMetrics.getMetricsMap();
            scanMetricsHolder.setScanMetricMap(scanMetricsMap);

            changeMetric(scanMetricsHolder.getCountOfRPCcalls(),
                    scanMetricsMap.get(RPC_CALLS_METRIC_NAME));
            changeMetric(scanMetricsHolder.getCountOfRemoteRPCcalls(),
                    scanMetricsMap.get(REMOTE_RPC_CALLS_METRIC_NAME));
            changeMetric(scanMetricsHolder.getSumOfMillisSecBetweenNexts(),
                    scanMetricsMap.get(MILLIS_BETWEEN_NEXTS_METRIC_NAME));
            changeMetric(scanMetricsHolder.getCountOfNSRE(),
                    scanMetricsMap.get(NOT_SERVING_REGION_EXCEPTION_METRIC_NAME));
            changeMetric(scanMetricsHolder.getCountOfBytesInResults(),
                    scanMetricsMap.get(BYTES_IN_RESULTS_METRIC_NAME));
            changeMetric(scanMetricsHolder.getCountOfBytesInRemoteResults(),
                    scanMetricsMap.get(BYTES_IN_REMOTE_RESULTS_METRIC_NAME));
            changeMetric(scanMetricsHolder.getCountOfRegions(),
                    scanMetricsMap.get(REGIONS_SCANNED_METRIC_NAME));
            changeMetric(scanMetricsHolder.getCountOfRPCRetries(),
                    scanMetricsMap.get(RPC_RETRIES_METRIC_NAME));
            changeMetric(scanMetricsHolder.getCountOfRemoteRPCRetries(),
                    scanMetricsMap.get(REMOTE_RPC_RETRIES_METRIC_NAME));
            changeMetric(scanMetricsHolder.getCountOfRowsScanned(),
                    scanMetricsMap.get(COUNT_OF_ROWS_SCANNED_KEY));
            changeMetric(scanMetricsHolder.getCountOfRowsFiltered(),
                    scanMetricsMap.get(COUNT_OF_ROWS_FILTERED_KEY));

            changeMetric(GLOBAL_SCAN_BYTES,
                    scanMetricsMap.get(BYTES_IN_RESULTS_METRIC_NAME));
            changeMetric(GLOBAL_HBASE_COUNT_RPC_CALLS,
                    scanMetricsMap.get(RPC_CALLS_METRIC_NAME));
            changeMetric(GLOBAL_HBASE_COUNT_REMOTE_RPC_CALLS
                    , scanMetricsMap.get(REMOTE_RPC_CALLS_METRIC_NAME));
            changeMetric(GLOBAL_HBASE_COUNT_MILLS_BETWEEN_NEXTS,
                    scanMetricsMap.get(MILLIS_BETWEEN_NEXTS_METRIC_NAME));
            changeMetric(GLOBAL_HBASE_COUNT_NOT_SERVING_REGION_EXCEPTION,
                    scanMetricsMap.get(NOT_SERVING_REGION_EXCEPTION_METRIC_NAME));
            changeMetric(GLOBAL_HBASE_COUNT_BYTES_REGION_SERVER_RESULTS,
                    scanMetricsMap.get(BYTES_IN_RESULTS_METRIC_NAME));
            changeMetric(GLOBAL_HBASE_COUNT_BYTES_IN_REMOTE_RESULTS,
                    scanMetricsMap.get(BYTES_IN_REMOTE_RESULTS_METRIC_NAME));
            changeMetric(GLOBAL_HBASE_COUNT_SCANNED_REGIONS,
                    scanMetricsMap.get(REGIONS_SCANNED_METRIC_NAME));
            changeMetric(GLOBAL_HBASE_COUNT_RPC_RETRIES,
                    scanMetricsMap.get(RPC_RETRIES_METRIC_NAME));
            changeMetric(GLOBAL_HBASE_COUNT_REMOTE_RPC_RETRIES,
                    scanMetricsMap.get(REMOTE_RPC_RETRIES_METRIC_NAME));
            changeMetric(GLOBAL_HBASE_COUNT_ROWS_SCANNED,
                    scanMetricsMap.get(COUNT_OF_ROWS_SCANNED_KEY));
            changeMetric(GLOBAL_HBASE_COUNT_ROWS_FILTERED,
                    scanMetricsMap.get(COUNT_OF_ROWS_FILTERED_KEY));

            scanMetricsUpdated = true;
        }

    }

    @Override
    public Tuple next() throws SQLException {
        try {
            Result result = scanner.next();
            if (result == null) {
                close(); // Free up resources early
                return null;
            }
            // TODO: use ResultTuple.setResult(result)?
            // Need to create a new one if holding on to it (i.e. OrderedResultIterator)
            return new ResultTuple(result);
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }

    @Override
    public void explain(List<String> planSteps) {
    }

    @Override
    public String toString() {
        return "ScanningResultIterator [scanner=" + scanner + "]";
    }

    public ResultScanner getScanner() {
        return scanner;
    }
}
