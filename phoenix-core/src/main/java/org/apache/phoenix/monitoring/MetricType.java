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
    UPSERT_SQL_COUNTER("uc", "Counter for number of upsert sql queries", LogLevel.OFF, PLong.INSTANCE),
    UPSERT_COMMIT_TIME("ut", "Time it took to commit a batch of upserts", LogLevel.OFF, PLong.INSTANCE),
    UPSERT_MUTATION_BYTES("umb", "Size of mutations in upsert statement in bytes",LogLevel.OFF, PLong.INSTANCE),
    UPSERT_MUTATION_SQL_COUNTER("umsc", "Counter for number of upsert mutations committed",LogLevel.OFF, PLong.INSTANCE),
    UPSERT_BATCH_FAILED_SIZE("ubfs", "Number of upsert mutations in a batch that failed to be committed",
            LogLevel.OFF, PLong.INSTANCE),
    UPSERT_BATCH_FAILED_COUNTER("ubfc", "Number of upsert mutation batches that failed to be committed",
            LogLevel.OFF, PLong.INSTANCE),

    UPSERT_AGGREGATE_SUCCESS_SQL_COUNTER("uassc", "Counter which indicates the total number of upsert Mutations which passed  executeUpdate phase "
            + "(since last commit called) and subsequent conn.commit() are successful.", LogLevel.OFF, PLong.INSTANCE),
    UPSERT_AGGREGATE_FAILURE_SQL_COUNTER("uafsc", "Counter which indicates the total number of upsert Mutations for all statements which failed either  in executeUpdate phase  "
            + "(since last commit called) or subsequent conn.commit() fails", LogLevel.OFF, PLong.INSTANCE),
    UPSERT_SUCCESS_SQL_COUNTER("ussc", "Counter for number of upsert sql queries that successfully"
            + " passed the executeMutation phase, or if autoCommit is true, the total"
            + " number of successful upserts", LogLevel.OFF, PLong.INSTANCE),
    UPSERT_FAILED_SQL_COUNTER("ufsc", "Counter for number of upsert sql queries that"
            + " failed the executeMutation phase, or if autoCommit is true, the total"
            + " number of upsert failures", LogLevel.OFF, PLong.INSTANCE),
    UPSERT_SQL_QUERY_TIME("uqt", "Time taken by upsert sql queries inside executeMutation or if"
            + " autoCommit is true, the total time taken for executeMutation + conn.commit",
            LogLevel.OFF, PLong.INSTANCE),

    // delete-specific metrics updated during executeMutation
    DELETE_SQL_COUNTER("dc", "Counter for number of delete sql queries", LogLevel.OFF, PLong.INSTANCE),
    DELETE_SUCCESS_SQL_COUNTER("dssc", "Counter for number of delete sql queries that successfully"
            + " passed the executeMutation phase, or if autoCommit is true, the total"
            + " number of successful deletes", LogLevel.OFF, PLong.INSTANCE),
    DELETE_AGGREGATE_SUCCESS_SQL_COUNTER("dassc", "Counter which indicates if everything in the executeUpdate phase for all "
            + "statements (since last commit called) and subsequent conn.commit() is successful.", LogLevel.OFF, PLong.INSTANCE),
    DELETE_AGGREGATE_FAILURE_SQL_COUNTER("dafsc", "Counter which indicates  if anything in the executeUpdate phase for any "
            + "statements (since last commit called) or subsequent conn.commit() fails.", LogLevel.OFF, PLong.INSTANCE),
    DELETE_FAILED_SQL_COUNTER("dfsc", "Counter for number of delete sql queries that"
            + " failed the executeMutation phase, or if autoCommit is true, the total"
            + " number of delete failures", LogLevel.OFF, PLong.INSTANCE),
    DELETE_SQL_QUERY_TIME("dqt", "Time taken by delete sql queries inside executeMutation or if"
            + " autoCommit is true, the total time taken for executeMutation + conn.commit",
            LogLevel.OFF, PLong.INSTANCE),

    DELETE_COMMIT_TIME("dt", "Time it took to commit a batch of deletes", LogLevel.OFF, PLong.INSTANCE),
    DELETE_MUTATION_BYTES("dmb", "Size of mutations in delete statement in bytes",LogLevel.OFF, PLong.INSTANCE),
    DELETE_MUTATION_SQL_COUNTER("dmsc", "Counter for number of delete mutations committed",LogLevel.OFF, PLong.INSTANCE),
    DELETE_BATCH_FAILED_SIZE("dbfs", "Number of delete mutations in a batch that failed to be committed",
            LogLevel.OFF, PLong.INSTANCE),
    DELETE_BATCH_FAILED_COUNTER("dbfc", "Number of delete mutation batches that failed to be committed",
            LogLevel.OFF, PLong.INSTANCE),

    // select-specific query (read) metrics updated during executeQuery
    SELECT_SUCCESS_SQL_COUNTER("sss", "Counter for number of select sql queries that successfully"
            + " passed the executeQuery phase", LogLevel.OFF, PLong.INSTANCE),
    SELECT_AGGREGATE_SUCCESS_SQL_COUNTER("sassc","Counter which indicates  if everything in executeQuery"
            + " phase and all rs.next() are successful",LogLevel.OFF, PLong.INSTANCE),
    SELECT_AGGREGATE_FAILURE_SQL_COUNTER("safsc","Counter which indicates if anything in "
            + "executeQuery phase or any of the rs.next() fail",LogLevel.OFF, PLong.INSTANCE),
    SELECT_POINTLOOKUP_SUCCESS_SQL_COUNTER("spls", "Counter for number of point lookup select sql "
            + "queries that succeeded the executeQuery phase", LogLevel.OFF, PLong.INSTANCE),
    SELECT_SCAN_SUCCESS_SQL_COUNTER("sscs", "Counter for number of scan select sql queries "
            + "that succeed the executeQuery phase", LogLevel.OFF, PLong.INSTANCE),
    SELECT_FAILED_SQL_COUNTER("sfsc", "Counter for number of select sql queries that"
            + " failed the executeQuery phase", LogLevel.OFF, PLong.INSTANCE),
    SELECT_POINTLOOKUP_FAILED_SQL_COUNTER("splf", "Counter for number of point lookup select sql "
            + "queries that failed the executeQuery phase", LogLevel.OFF, PLong.INSTANCE),
    SELECT_SCAN_FAILED_SQL_COUNTER("sscf", "Counter for number of scan select sql queries "
            + "that failed the executeQuery phase", LogLevel.OFF, PLong.INSTANCE),
    SELECT_SQL_QUERY_TIME("sqt", "Time taken by select sql queries inside executeQuery",
            LogLevel.OFF, PLong.INSTANCE),

    INDEX_COMMIT_FAILURE_SIZE("p3s", "Number of mutations that failed in phase 3", LogLevel.OFF, PLong.INSTANCE),
    QUERY_POINTLOOKUP_TIMEOUT_COUNTER("qplo", "Number of times the point lookup select query timed out"
            + " when fetching results", LogLevel.DEBUG, PLong.INSTANCE),
    QUERY_SCAN_TIMEOUT_COUNTER("qso", "Number of times the scan select query timed out"
            + " when fetching results", LogLevel.DEBUG, PLong.INSTANCE),
    QUERY_POINTLOOKUP_FAILED_COUNTER("qplf", "Number of times the point lookup select query failed"
            + " when fetching results", LogLevel.DEBUG, PLong.INSTANCE),
    QUERY_SCAN_FAILED_COUNTER("qsf", "Number of times the scan select query failed when fetching"
            + " results", LogLevel.DEBUG, PLong.INSTANCE),
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
