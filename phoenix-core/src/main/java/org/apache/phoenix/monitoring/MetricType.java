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

import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;


/**
 * Keeping {@link LogLevel#OFF} for metrics which are calculated globally only and doesn't need to be logged in SYSTEM.LOG 
 */
public enum MetricType {

	NO_OP_METRIC("no", "No op metric",LogLevel.OFF, PLong.INSTANCE),
	// mutation (write) related metrics 
    MUTATION_BATCH_SIZE("ms", "Number of mutations in the batch",LogLevel.OFF, PLong.INSTANCE),
    MUTATION_BYTES("mb", "Size of mutations in bytes",LogLevel.OFF, PLong.INSTANCE),
    MUTATION_COMMIT_TIME("mt", "Time it took to commit a batch of mutations",LogLevel.OFF, PLong.INSTANCE),
    MUTATION_BATCH_FAILED_SIZE("mfs", "Number of mutations that failed to be committed",LogLevel.OFF, PLong.INSTANCE),
    MUTATION_SQL_COUNTER("msc", "Counter for number of mutation sql statements",LogLevel.OFF, PLong.INSTANCE),
    INDEX_COMMIT_FAILURE_SIZE("p3s", "Number of mutations that failed in phase 3", LogLevel.OFF, PLong.INSTANCE),
    // query (read) related metrics
    QUERY_TIME("qt", "Query times",LogLevel.OFF, PLong.INSTANCE),
    QUERY_TIMEOUT_COUNTER("qo", "Number of times query timed out",LogLevel.DEBUG, PLong.INSTANCE),
    QUERY_FAILED_COUNTER("qf", "Number of times query failed",LogLevel.DEBUG, PLong.INSTANCE),
    NUM_PARALLEL_SCANS("ps", "Number of scans that were executed in parallel",LogLevel.DEBUG, PLong.INSTANCE),
    SCAN_BYTES("sb", "Number of bytes read by scans",LogLevel.OFF, PLong.INSTANCE),
    SELECT_SQL_COUNTER("sc", "Counter for number of sql queries",LogLevel.OFF, PLong.INSTANCE),
    // task metrics
    TASK_QUEUE_WAIT_TIME("tw", "Time in milliseconds tasks had to wait in the queue of the thread pool executor",LogLevel.DEBUG, PLong.INSTANCE),
    TASK_END_TO_END_TIME("tee", "Time in milliseconds spent by tasks from creation to completion",LogLevel.DEBUG, PLong.INSTANCE),
    TASK_EXECUTION_TIME("tx", "Time in milliseconds tasks took to execute",LogLevel.DEBUG, PLong.INSTANCE),
    TASK_EXECUTED_COUNTER("te", "Counter for number of tasks submitted to the thread pool executor",LogLevel.DEBUG, PLong.INSTANCE),
    TASK_REJECTED_COUNTER("tr", "Counter for number of tasks that were rejected by the thread pool executor",LogLevel.DEBUG, PLong.INSTANCE),
    // spool metrics
    SPOOL_FILE_SIZE("ss", "Size of spool files created in bytes",LogLevel.DEBUG, PLong.INSTANCE),
    SPOOL_FILE_COUNTER("sn", "Number of spool files created",LogLevel.DEBUG, PLong.INSTANCE),
    // misc metrics
    MEMORY_CHUNK_BYTES("mc", "Number of bytes allocated by the memory manager",LogLevel.DEBUG, PLong.INSTANCE),
    MEMORY_WAIT_TIME("mw", "Number of milliseconds threads needed to wait for memory to be allocated through memory manager",LogLevel.DEBUG, PLong.INSTANCE),
    CACHE_REFRESH_SPLITS_COUNTER("cr", "Number of times cache was refreshed because of splits",LogLevel.DEBUG, PLong.INSTANCE),
    WALL_CLOCK_TIME_MS("tq", "Wall clock time elapsed for the overall query execution",LogLevel.INFO, PLong.INSTANCE),
    RESULT_SET_TIME_MS("tn", "Wall clock time elapsed for reading all records using resultSet.next()",LogLevel.INFO, PLong.INSTANCE),
    OPEN_PHOENIX_CONNECTIONS_COUNTER("o", "Number of open phoenix connections",LogLevel.OFF, PLong.INSTANCE),
    OPEN_INTERNAL_PHOENIX_CONNECTIONS_COUNTER("io", "Number of open internal phoenix connections",LogLevel.OFF, PLong.INSTANCE),
    QUERY_SERVICES_COUNTER("cqs", "Number of ConnectionQueryServicesImpl instantiated",LogLevel.OFF, PLong.INSTANCE),
    HCONNECTIONS_COUNTER("h", "Number of HConnections created by phoenix driver",LogLevel.OFF, PLong.INSTANCE),
    PHOENIX_CONNECTIONS_THROTTLED_COUNTER("ct", "Number of client Phoenix connections prevented from opening " +
                                              "because there are already too many to that target cluster.",LogLevel.OFF, PLong.INSTANCE),
    PHOENIX_CONNECTIONS_ATTEMPTED_COUNTER("ca","Number of requests for Phoenix connections, whether successful or not.",LogLevel.OFF, PLong.INSTANCE),
    CLIENT_METADATA_CACHE_MISS_COUNTER("cmcm", "Number of cache misses for the CQSI cache.", LogLevel.DEBUG, PLong.INSTANCE),
    CLIENT_METADATA_CACHE_HIT_COUNTER("cmch", "Number of cache hits for the CQSI cache.", LogLevel.DEBUG, PLong.INSTANCE),
    // hbase metrics
    COUNT_RPC_CALLS("rp", "Number of RPC calls",LogLevel.DEBUG, PLong.INSTANCE),
    COUNT_REMOTE_RPC_CALLS("rr", "Number of remote RPC calls",LogLevel.DEBUG, PLong.INSTANCE),
    COUNT_MILLS_BETWEEN_NEXTS("n", "Sum of milliseconds between sequential next calls",LogLevel.DEBUG, PLong.INSTANCE),
    COUNT_NOT_SERVING_REGION_EXCEPTION("nsr", "Number of NotServingRegionException caught",LogLevel.DEBUG, PLong.INSTANCE),
    COUNT_BYTES_REGION_SERVER_RESULTS("rs", "Number of bytes in Result objects from region servers",LogLevel.DEBUG, PLong.INSTANCE),
    COUNT_BYTES_IN_REMOTE_RESULTS("rrs", "Number of bytes in Result objects from remote region servers",LogLevel.DEBUG, PLong.INSTANCE),
    COUNT_SCANNED_REGIONS("rg", "Number of regions scanned",LogLevel.DEBUG, PLong.INSTANCE),
    COUNT_RPC_RETRIES("rpr", "Number of RPC retries",LogLevel.DEBUG, PLong.INSTANCE),
    COUNT_REMOTE_RPC_RETRIES("rrr", "Number of remote RPC retries",LogLevel.DEBUG, PLong.INSTANCE),
    COUNT_ROWS_SCANNED("ws", "Number of rows scanned",LogLevel.DEBUG, PLong.INSTANCE),
    COUNT_ROWS_FILTERED("wf", "Number of rows filtered",LogLevel.DEBUG,PLong.INSTANCE);
	
    private final String description;
    private final String shortName;
    private LogLevel logLevel;
    private PDataType dataType;

    private MetricType(String shortName, String description, LogLevel logLevel, PDataType dataType) {
    	this.shortName = shortName;
        this.description = description;
        this.logLevel=logLevel;
        this.dataType=dataType;
    }

    public String description() {
        return description;
    }
    
    public String shortName() {
        return shortName;
    }
    
    public LogLevel logLevel() {
        return logLevel;
    }
    
    public PDataType dataType() {
        return dataType;
    }
    
    public String columnName() {
        return name();
    }
    
    public boolean isLoggingEnabled(LogLevel connectionLogLevel){
        return logLevel() != LogLevel.OFF && (logLevel().ordinal() <= connectionLogLevel.ordinal());
    }

    public static String getMetricColumnsDetails() {
        StringBuilder buffer=new StringBuilder();
        for(MetricType metric:MetricType.values()){
            if (metric.logLevel() != LogLevel.OFF) {
                buffer.append(metric.columnName());
                buffer.append(" ");
                buffer.append(metric.dataType.getSqlTypeName());
                buffer.append(",");
            }
        }
        return buffer.toString();
    }
    
}
