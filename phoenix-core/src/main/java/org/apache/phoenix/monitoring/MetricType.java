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

public enum MetricType {

    MUTATION_BATCH_SIZE("Batch sizes of mutations"),
    MUTATION_BYTES("Size of mutations in bytes"),
    MUTATION_COMMIT_TIME("Time it took to commit mutations"),
    QUERY_TIME("Query times"),
    NUM_PARALLEL_SCANS("Number of scans that were executed in parallel"),
    SCAN_BYTES("Number of bytes read by scans"),
    MEMORY_CHUNK_BYTES("Number of bytes allocated by the memory manager"),
    MEMORY_WAIT_TIME("Number of milliseconds threads needed to wait for memory to be allocated through memory manager"),
    MUTATION_SQL_COUNTER("Counter for number of mutation sql statements"),
    SELECT_SQL_COUNTER("Counter for number of sql queries"),
    TASK_QUEUE_WAIT_TIME("Time in milliseconds tasks had to wait in the queue of the thread pool executor"),
    TASK_END_TO_END_TIME("Time in milliseconds spent by tasks from creation to completion"),
    TASK_EXECUTION_TIME("Time in milliseconds tasks took to execute"),
    TASK_EXECUTED_COUNTER("Counter for number of tasks submitted to the thread pool executor"),
    TASK_REJECTED_COUNTER("Counter for number of tasks that were rejected by the thread pool executor"),
    QUERY_TIMEOUT_COUNTER("Number of times query timed out"),
    QUERY_FAILED_COUNTER("Number of times query failed"),
    SPOOL_FILE_SIZE("Size of spool files created in bytes"),
    SPOOL_FILE_COUNTER("Number of spool files created"),
    CACHE_REFRESH_SPLITS_COUNTER("Number of times cache was refreshed because of splits"),
    WALL_CLOCK_TIME_MS("Wall clock time elapsed for the overall query execution"),
    RESULT_SET_TIME_MS("Wall clock time elapsed for reading all records using resultSet.next()"),
    OPEN_PHOENIX_CONNECTIONS_COUNTER("Number of open phoenix connections"),
    QUERY_SERVICES_COUNTER("Number of ConnectionQueryServicesImpl instantiated"),
    HCONNECTIONS_COUNTER("Number of HConnections created by phoenix driver"),
    PHOENIX_CONNECTIONS_THROTTLED_COUNTER("Number of client Phoenix connections prevented from opening " +
                                              "because there are already too many to that target cluster."),
    PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER("Number of requests for Phoenix connections, whether successful or not.");
    
    private final String description;

    private MetricType(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }

}
