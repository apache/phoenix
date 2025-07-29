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
package org.apache.phoenix.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SQL执行日志记录器，用于记录SQL执行的详细信息
 */
public class SQLLogger {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLLogger.class);
    private static final Logger SQL_EXECUTION_LOGGER = LoggerFactory.getLogger("org.apache.phoenix.jdbc");
    private static final Logger QUERY_TIME_LOGGER = LoggerFactory.getLogger("org.apache.phoenix.query");
    private static final Logger PERFORMANCE_LOGGER = LoggerFactory.getLogger("org.apache.phoenix.monitoring");
    
    private static final AtomicLong queryCounter = new AtomicLong(0);
    private static final Map<String, Long> slowQueryThresholds = new ConcurrentHashMap<>();
    
    // 默认慢查询阈值（毫秒）
    private static final long DEFAULT_SLOW_QUERY_THRESHOLD = 1000;
    
    /**
     * 记录SQL执行开始
     * @param sql SQL语句
     * @param parameters 参数（可选）
     * @return 查询ID
     */
    public static String logQueryStart(String sql, Object... parameters) {
        String queryId = "Q" + queryCounter.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        StringBuilder logMessage = new StringBuilder();
        logMessage.append("SQL_EXECUTION_START [").append(queryId).append("] ");
        logMessage.append("SQL: ").append(sql);
        
        if (parameters != null && parameters.length > 0) {
            logMessage.append(" PARAMETERS: [");
            for (int i = 0; i < parameters.length; i++) {
                if (i > 0) logMessage.append(", ");
                logMessage.append(parameters[i]);
            }
            logMessage.append("]");
        }
        
        SQL_EXECUTION_LOGGER.info(logMessage.toString());
        
        // 存储开始时间
        ThreadLocalQueryContext.setStartTime(startTime);
        ThreadLocalQueryContext.setQueryId(queryId);
        
        return queryId;
    }
    
    /**
     * 记录SQL执行完成
     * @param queryId 查询ID
     * @param rowCount 影响的行数
     * @param success 是否成功
     */
    public static void logQueryEnd(String queryId, int rowCount, boolean success) {
        long endTime = System.currentTimeMillis();
        Long startTime = ThreadLocalQueryContext.getStartTime();
        
        if (startTime != null) {
            long executionTime = endTime - startTime;
            String status = success ? "SUCCESS" : "FAILED";
            
            StringBuilder logMessage = new StringBuilder();
            logMessage.append("SQL_EXECUTION_END [").append(queryId).append("] ");
            logMessage.append("STATUS: ").append(status).append(" ");
            logMessage.append("EXECUTION_TIME: ").append(executionTime).append("ms ");
            logMessage.append("ROWS_AFFECTED: ").append(rowCount);
            
            SQL_EXECUTION_LOGGER.info(logMessage.toString());
            
            // 记录查询时间
            QUERY_TIME_LOGGER.info("QUERY_TIME [{}] {}ms", queryId, executionTime);
            
            // 检查是否为慢查询
            checkSlowQuery(queryId, executionTime);
            
            // 清理ThreadLocal
            ThreadLocalQueryContext.clear();
        }
    }
    
    /**
     * 记录SQL执行异常
     * @param queryId 查询ID
     * @param exception 异常
     */
    public static void logQueryException(String queryId, SQLException exception) {
        long endTime = System.currentTimeMillis();
        Long startTime = ThreadLocalQueryContext.getStartTime();
        
        if (startTime != null) {
            long executionTime = endTime - startTime;
            
            StringBuilder logMessage = new StringBuilder();
            logMessage.append("SQL_EXECUTION_ERROR [").append(queryId).append("] ");
            logMessage.append("EXECUTION_TIME: ").append(executionTime).append("ms ");
            logMessage.append("ERROR: ").append(exception.getMessage());
            logMessage.append(" ERROR_CODE: ").append(exception.getErrorCode());
            logMessage.append(" SQL_STATE: ").append(exception.getSQLState());
            
            SQL_EXECUTION_LOGGER.error(logMessage.toString(), exception);
            
            // 清理ThreadLocal
            ThreadLocalQueryContext.clear();
        }
    }
    
    /**
     * 设置慢查询阈值
     * @param sqlType SQL类型（如SELECT、INSERT等）
     * @param threshold 阈值（毫秒）
     */
    public static void setSlowQueryThreshold(String sqlType, long threshold) {
        slowQueryThresholds.put(sqlType.toUpperCase(), threshold);
    }
    
    /**
     * 检查是否为慢查询
     * @param queryId 查询ID
     * @param executionTime 执行时间
     */
    private static void checkSlowQuery(String queryId, long executionTime) {
        long threshold = DEFAULT_SLOW_QUERY_THRESHOLD;
        
        // 可以根据SQL类型设置不同的阈值
        for (Map.Entry<String, Long> entry : slowQueryThresholds.entrySet()) {
            if (executionTime > entry.getValue()) {
                PERFORMANCE_LOGGER.warn("SLOW_QUERY [{}] {}ms exceeds threshold {}ms for type {}", 
                    queryId, executionTime, entry.getValue(), entry.getKey());
                return;
            }
        }
        
        if (executionTime > threshold) {
            PERFORMANCE_LOGGER.warn("SLOW_QUERY [{}] {}ms exceeds default threshold {}ms", 
                queryId, executionTime, threshold);
        }
    }
    
    /**
     * 记录批量操作
     * @param operation 操作类型
     * @param batchSize 批量大小
     * @param executionTime 执行时间
     */
    public static void logBatchOperation(String operation, int batchSize, long executionTime) {
        PERFORMANCE_LOGGER.info("BATCH_OPERATION {} BATCH_SIZE: {} EXECUTION_TIME: {}ms", 
            operation, batchSize, executionTime);
    }
    
    /**
     * 记录连接信息
     * @param connectionId 连接ID
     * @param operation 操作类型
     * @param details 详细信息
     */
    public static void logConnectionInfo(String connectionId, String operation, String details) {
        SQL_EXECUTION_LOGGER.info("CONNECTION [{}] {}: {}", connectionId, operation, details);
    }
    
    /**
     * ThreadLocal上下文，用于存储查询相关信息
     */
    private static class ThreadLocalQueryContext {
        private static final ThreadLocal<Long> startTime = new ThreadLocal<>();
        private static final ThreadLocal<String> queryId = new ThreadLocal<>();
        
        public static void setStartTime(long time) {
            startTime.set(time);
        }
        
        public static Long getStartTime() {
            return startTime.get();
        }
        
        public static void setQueryId(String id) {
            queryId.set(id);
        }
        
        public static String getQueryId() {
            return queryId.get();
        }
        
        public static void clear() {
            startTime.remove();
            queryId.remove();
        }
    }
} 