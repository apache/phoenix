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
package org.apache.phoenix.log;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap.Builder;


/*
 * Wrapper for query translator
 */
public class QueryLogger {
    private final ThreadLocal<RingBufferEventTranslator> threadLocalTranslator = new ThreadLocal<>();
    private QueryLoggerDisruptor queryDisruptor;
    private String queryId;
    private LogLevel logLevel;
    private Builder<QueryLogInfo, Object> queryLogBuilder = ImmutableMap.builder();
    private boolean isSynced;
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryLogger.class);
    
    protected QueryLogger(PhoenixConnection connection) {
        this.queryId = UUID.randomUUID().toString();
        this.queryDisruptor = connection.getQueryServices().getQueryDisruptor();
        logLevel = connection.getLogLevel();
        log(QueryLogInfo.QUERY_ID_I, queryId);
        log(QueryLogInfo.START_TIME_I, EnvironmentEdgeManager.currentTimeMillis());
    }

    protected QueryLogger() {
        logLevel = LogLevel.OFF;
    }
    
    private RingBufferEventTranslator getCachedTranslator() {
        RingBufferEventTranslator result = threadLocalTranslator.get();
        if (result == null) {
            result = new RingBufferEventTranslator(queryId);
            threadLocalTranslator.set(result);
        }
        return result;
    }
    
    public static final QueryLogger NO_OP_INSTANCE = new QueryLogger() {
        @Override
        public void log(QueryLogInfo queryLogInfo, Object info) {

        }

        @Override
        public boolean isDebugEnabled() {
            return false;
        }

        @Override
        public boolean isInfoEnabled() {
            return false;
        }

        @Override
        public void sync(
                Map<String, Map<MetricType, Long>> readMetrics, Map<MetricType, Long> overAllMetrics) {

        }
        
        @Override
        public boolean isSynced(){
            return true;
        }
    };

    public static QueryLogger getInstance(PhoenixConnection connection, boolean isSystemTable) {
        if (connection.getLogLevel() == LogLevel.OFF || isSystemTable || ThreadLocalRandom.current()
                .nextDouble() > connection.getLogSamplingRate()) { return NO_OP_INSTANCE; }
        return new QueryLogger(connection);
    }

    /**
     * Add query log in the table, columns will be logged depending upon the connection logLevel 
     */
    public void log(QueryLogInfo queryLogInfo, Object info) {
        try {
            queryLogBuilder.put(queryLogInfo, info);
        } catch (Exception e) {
            LOGGER.warn("Unable to add log info because of " + e.getMessage());
        }
    }
    
    private boolean publishLogs(RingBufferEventTranslator translator) {
        if (queryDisruptor == null) { return false; }
        boolean isLogged = queryDisruptor.tryPublish(translator);
        if (!isLogged && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Unable to write query log in table as ring buffer queue is full!!");
        }
        return isLogged;
    }

    /**
     *  Is debug logging currently enabled?
     *  Call this method to prevent having to perform expensive operations (for example, String concatenation) when the log level is more than debug.
     */
    public boolean isDebugEnabled(){
        return isLevelEnabled(LogLevel.DEBUG);
    }
    
    private boolean isLevelEnabled(LogLevel logLevel){
        return this.logLevel != null && logLevel != LogLevel.OFF ? logLevel.ordinal() <= this.logLevel.ordinal()
                : false;
    }
    
    /**
     * Is Info logging currently enabled?
     * Call this method to prevent having to perform expensive operations (for example, String concatenation) when the log level is more than info.
     * @return
     */
    public boolean isInfoEnabled(){
        return isLevelEnabled(LogLevel.INFO);
    }

    /**
     * Return queryId of the current query logger , needed by the application 
     * to correlate with the logging table.
     * Eg(usage):-
     * StatementContext context = ((PhoenixResultSet)rs).getContext();
     * String queryId = context.getQueryLogger().getQueryId();
     * 
     * @return
     */
    public String getQueryId() {
        return this.queryId;
    }
    

    public void sync(Map<String, Map<MetricType, Long>> readMetrics, Map<MetricType, Long> overAllMetrics) {
        syncBase(readMetrics, overAllMetrics, logLevel);
    }

    public void syncBase(Map<String, Map<MetricType, Long>> readMetrics, Map<MetricType, Long> overAllMetrics, LogLevel logLevel) {
        if (!isSynced) {
            isSynced = true;
            final RingBufferEventTranslator translator = getCachedTranslator();
            translator.setQueryInfo(logLevel, queryLogBuilder.build(), readMetrics, overAllMetrics);
            publishLogs(translator);
        }
    }
    
    /**
     * Is Synced already
     */
    public boolean isSynced(){
        return this.isSynced;
    }
    
}
