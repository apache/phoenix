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

/**
 * Central place where we keep track of all the internal
 * phoenix metrics that we track.
 * 
 */
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.phoenix.query.QueryServicesOptions;

public class PhoenixMetrics {
    private static final boolean isMetricsEnabled = QueryServicesOptions.withDefaults().isMetricsEnabled();

    public static boolean isMetricsEnabled() {
        return isMetricsEnabled;
    }

    public enum SizeMetric {
        MUTATION_BATCH_SIZE("CumulativeBatchSizesOfMutations", "Cumulative batch sizes of mutations"),
        MUTATION_BYTES("CumulativeMutationSize", "Cumulative size of mutations in bytes"),
        MUTATION_COMMIT_TIME("CumulativeMutationTime", "Cumulative time it took to send mutations"),
        QUERY_TIME("QueryTime", "Cumulative query times"),
        PARALLEL_SCANS("CumulativeNumberOfParallelScans", "Cumulative number of scans executed that were executed in parallel"),
        SCAN_BYTES("CumulativeScanBytesSize", "Cumulative number of bytes read by scans"),
        SPOOL_FILE_SIZE("CumulativeSpoolFilesSize", "Cumulative size of spool files created in bytes"),
        MEMORY_MANAGER_BYTES("CumulativeBytesAllocated", "Cumulative number of bytes allocated by the memory manager"),
        MEMORY_WAIT_TIME("CumulativeMemoryWaitTime", "Cumulative number of milliseconds threads needed to wait for memory to be allocated through memory manager"),
        TASK_QUEUE_WAIT_TIME("CumulativeTaskQueueWaitTime", "Cumulative time in milliseconds tasks had to wait in the queue of the thread pool executor"),
        TASK_END_TO_END_TIME("CumulativeTaskEndToEndTime", "Cumulative time in milliseconds spent by tasks from creation to completion"),
        TASK_EXECUTION_TIME("CumulativeTaskExecutionTime", "Cumulative time in milliseconds tasks took to execute");

        private final SizeStatistic metric;

        private SizeMetric(String metricName, String metricDescription) {
            metric = new SizeStatistic(metricName, metricDescription);
        }

        public void update(long value) {
            if (isMetricsEnabled) {
                metric.add(value);
            }
        }
        
        // exposed for testing.
        public Metric getMetric() {
            return metric;
        }
        
        @Override
        public String toString() {
            return metric.toString();
        }
    }

    public enum CountMetric {
        MUTATION_COUNT("NumMutationCounter", "Counter for number of mutation statements"),
        QUERY_COUNT("NumQueryCounter", "Counter for number of queries"),
        TASK_COUNT("NumberOfTasksCounter", "Counter for number of tasks submitted to the thread pool executor"),
        REJECTED_TASK_COUNT("RejectedTasksCounter", "Counter for number of tasks that were rejected by the thread pool executor"),
        QUERY_TIMEOUT("QueryTimeoutCounter", "Number of times query timed out"),
        FAILED_QUERY("QueryFailureCounter", "Number of times query failed"),
        NUM_SPOOL_FILE("NumSpoolFilesCounter", "Number of spool files created");

        private final Counter metric;

        private CountMetric(String metricName, String metricDescription) {
            metric = new Counter(metricName, metricDescription);
        }

        public void increment() {
            if (isMetricsEnabled) {
                metric.increment();
            }
        }
        
        // exposed for testing.
        public Metric getMetric() {
            return metric;
        }
        
        @Override
        public String toString() {
            return metric.toString();
        }
    }
    
    public static Collection<Metric> getMetrics() {
        List<Metric> metrics = new ArrayList<>();
        for (SizeMetric s : SizeMetric.values()) {
            metrics.add(s.metric);
        }
        for (CountMetric s : CountMetric.values()) {
            metrics.add(s.metric);
        }
        return metrics;
    }

}    