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

	NO_OP_METRIC("no", "No op metric"),
	// mutation (write) related metrics 
    MUTATION_BATCH_SIZE("ms", "Number of mutations in the batch"),
    MUTATION_BYTES("mb", "Size of mutations in bytes"),
    MUTATION_COMMIT_TIME("mt", "Time it took to commit a batch of mutations"),
    MUTATION_BATCH_FAILED_SIZE("mfs", "Number of mutations that failed to be committed"),
    MUTATION_SQL_COUNTER("msc", "Counter for number of mutation sql statements"),
    // query (read) related metrics
    QUERY_TIME("qt", "Query times"),
    QUERY_TIMEOUT_COUNTER("qo", "Number of times query timed out"),
    QUERY_FAILED_COUNTER("qf", "Number of times query failed"),
    NUM_PARALLEL_SCANS("ps", "Number of scans that were executed in parallel"),
    SCAN_BYTES("sb", "Number of bytes read by scans"),
    SELECT_SQL_COUNTER("sc", "Counter for number of sql queries"),
    // task metrics
    TASK_QUEUE_WAIT_TIME("tw", "Time in milliseconds tasks had to wait in the queue of the thread pool executor"),
    TASK_END_TO_END_TIME("tee", "Time in milliseconds spent by tasks from creation to completion"),
    TASK_EXECUTION_TIME("tx", "Time in milliseconds tasks took to execute"),
    TASK_EXECUTED_COUNTER("te", "Counter for number of tasks submitted to the thread pool executor"),
    TASK_REJECTED_COUNTER("tr", "Counter for number of tasks that were rejected by the thread pool executor"),
    // spool metrics
    SPOOL_FILE_SIZE("ss", "Size of spool files created in bytes"),
    SPOOL_FILE_COUNTER("sn", "Number of spool files created"),
    // misc metrics
    MEMORY_CHUNK_BYTES("mc", "Number of bytes allocated by the memory manager"),
    MEMORY_WAIT_TIME("mw", "Number of milliseconds threads needed to wait for memory to be allocated through memory manager"),
    CACHE_REFRESH_SPLITS_COUNTER("cr", "Number of times cache was refreshed because of splits"),
    WALL_CLOCK_TIME_MS("tq", "Wall clock time elapsed for the overall query execution"),
    RESULT_SET_TIME_MS("tn", "Wall clock time elapsed for reading all records using resultSet.next()"),
    OPEN_PHOENIX_CONNECTIONS_COUNTER("o", "Number of open phoenix connections"),
    QUERY_SERVICES_COUNTER("cqs", "Number of ConnectionQueryServicesImpl instantiated"),
    HCONNECTIONS_COUNTER("h", "Number of HConnections created by phoenix driver"),
    PHOENIX_CONNECTIONS_THROTTLED_COUNTER("ct", "Number of client Phoenix connections prevented from opening " +
                                              "because there are already too many to that target cluster."),
    PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER("ca","Number of requests for Phoenix connections, whether successful or not."),
    // hbase metrics
    COUNT_RPC_CALLS("rp", "Number of RPC calls"),
    COUNT_REMOTE_RPC_CALLS("rr", "Number of remote RPC calls"),
    COUNT_MILLS_BETWEEN_NEXTS("n", "Sum of milliseconds between sequential next calls"),
    COUNT_NOT_SERVING_REGION_EXCEPTION("nsr", "Number of NotServingRegionException caught"),
    COUNT_BYTES_REGION_SERVER_RESULTS("rs", "Number of bytes in Result objects from region servers"),
    COUNT_BYTES_IN_REMOTE_RESULTS("rrs", "Number of bytes in Result objects from remote region servers"),
    COUNT_SCANNED_REGIONS("rg", "Number of regions scanned"),
    COUNT_RPC_RETRIES("rpr", "Number of RPC retries"),
    COUNT_REMOTE_RPC_RETRIES("rrr", "Number of remote RPC retries"),
    COUNT_ROWS_SCANNED("ws", "Number of rows scanned"),
    COUNT_ROWS_FILTERED("wf", "Number of rows filtered");
	
    private final String description;
    private final String shortName;

    private MetricType(String shortName, String description) {
    	this.shortName = shortName;
        this.description = description;
    }

    public String description() {
        return description;
    }
    
    public String shortName() {
        return shortName;
    }

}
