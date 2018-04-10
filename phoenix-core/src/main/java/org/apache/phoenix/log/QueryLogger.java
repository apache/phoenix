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

import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;

import com.google.common.collect.ImmutableMap;

import io.netty.util.internal.ThreadLocalRandom;

/*
 * Wrapper for query translator
 */
public class QueryLogger {
    private final ThreadLocal<RingBufferEventTranslator> threadLocalTranslator = new ThreadLocal<>();
    private QueryLoggerDisruptor queryDisruptor;
    private String queryId;
    private Long startTime;
    private LogLevel logLevel;
    private static final Log LOG = LogFactory.getLog(QueryLoggerDisruptor.class);
    
    private QueryLogger(PhoenixConnection connection) {
        this.queryId = UUID.randomUUID().toString();
        this.queryDisruptor = connection.getQueryServices().getQueryDisruptor();
        this.startTime = System.currentTimeMillis();
        logLevel = connection.getLogLevel();
    }
    
    private QueryLogger() {
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
    
    private static final QueryLogger NO_OP_INSTANCE = new QueryLogger() {
        @Override
        public void log(QueryLogState logState, ImmutableMap<QueryLogInfo, Object> map) {

        }
        
        @Override
        public boolean isDebugEnabled(){
            return false;
        }
        
        @Override
        public boolean isInfoEnabled(){
            return false;
        }
    };

    public static QueryLogger getInstance(PhoenixConnection connection, boolean isSystemTable) {
        if (connection.getLogLevel() == LogLevel.OFF || isSystemTable || ThreadLocalRandom.current()
                .nextDouble() > connection.getLogSamplingRate()) { return NO_OP_INSTANCE; }
        return new QueryLogger(connection);
    }

    /**
     * Add query log in the table, columns will be logged depending upon the connection logLevel
     * @param logState State of the query
     * @param map Value of the map should be in format of the corresponding data type 
     */
    public void log(QueryLogState logState, ImmutableMap<QueryLogInfo, Object> map) {
        final RingBufferEventTranslator translator = getCachedTranslator();
        translator.setQueryInfo(logState, map, logLevel);
        publishLogs(translator);
    }
    
    private boolean publishLogs(RingBufferEventTranslator translator) {
        if (queryDisruptor == null) { return false; }
        boolean isLogged = queryDisruptor.tryPublish(translator);
        if (!isLogged && LOG.isDebugEnabled()) {
            LOG.debug("Unable to write query log in table as ring buffer queue is full!!");
        }
        return isLogged;
    }

    /**
     * Start time when the logger was started, if {@link LogLevel#OFF} then it's the current time
     */
    public Long getStartTime() {
        return startTime != null ? startTime : System.currentTimeMillis();
    }
    
    /**
     *  Is debug logging currently enabled?
     *  Call this method to prevent having to perform expensive operations (for example, String concatenation) when the log level is more than debug.
     */
    public boolean isDebugEnabled(){
        return isLevelEnabled(LogLevel.DEBUG);
    }
    
    private boolean isLevelEnabled(LogLevel logLevel){
        return this.logLevel != null ? logLevel.ordinal() <= this.logLevel.ordinal() : false;
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
    
}
