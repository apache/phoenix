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

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_SCAN_BYTES;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
    static final String COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME = "ROWS_SCANNED";
    static final String COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME = "ROWS_FILTERED";
    static final String GLOBAL_BYTES_IN_RESULTS_METRIC_NAME = "BYTES_IN_RESULTS";

    public ScanningResultIterator(ResultScanner scanner, Scan scan, ScanMetricsHolder scanMetricsHolder) {
        this.scanner = scanner;
        this.scan = scan;
        this.scanMetricsHolder = scanMetricsHolder;
        scanMetricsUpdated = false;
        scanMetricsEnabled = scan.isScanMetricsEnabled();
    }

    @Override
    public void close() throws SQLException {
        getScanMetrics();
        scanner.close();
    }

    private void getScanMetrics() {

        if (!scanMetricsUpdated && scanMetricsEnabled) {
            Map<String, Long> scanMetricsMap = scan.getScanMetrics().getMetricsMap();
            scanMetricsHolder.getCountOfRPCcalls().change(scanMetricsMap.get(RPC_CALLS_METRIC_NAME));
            scanMetricsHolder.getCountOfRemoteRPCcalls().change(scanMetricsMap.get(REMOTE_RPC_CALLS_METRIC_NAME));
            scanMetricsHolder.getSumOfMillisSecBetweenNexts().change(scanMetricsMap.get(MILLIS_BETWEEN_NEXTS_METRIC_NAME));
            scanMetricsHolder.getCountOfNSRE().change(scanMetricsMap.get(NOT_SERVING_REGION_EXCEPTION_METRIC_NAME));
            scanMetricsHolder.getCountOfBytesInResults().change(scanMetricsMap.get(BYTES_IN_RESULTS_METRIC_NAME));
            scanMetricsHolder.getCountOfBytesInRemoteResults().change(scanMetricsMap.get(BYTES_IN_REMOTE_RESULTS_METRIC_NAME));
            scanMetricsHolder.getCountOfRegions().change(scanMetricsMap.get(REGIONS_SCANNED_METRIC_NAME));
            scanMetricsHolder.getCountOfRPCRetries().change(scanMetricsMap.get(RPC_RETRIES_METRIC_NAME));
            scanMetricsHolder.getCountOfRemoteRPCRetries().change(scanMetricsMap.get(REMOTE_RPC_RETRIES_METRIC_NAME));
            scanMetricsHolder.getCountOfRowsScanned().change(scanMetricsMap.get(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME));
            scanMetricsHolder.getCountOfRowsFiltered().change(scanMetricsMap.get(COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME));

            GLOBAL_SCAN_BYTES.update(scanMetricsMap.get(GLOBAL_BYTES_IN_RESULTS_METRIC_NAME));
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
