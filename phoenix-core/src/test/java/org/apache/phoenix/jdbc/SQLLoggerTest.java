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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * SQLLogger功能测试
 */
@Category(ParallelStatsDisabledTest.class)
public class SQLLoggerTest {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLLoggerTest.class);
    
    @Test
    public void testLogQueryStart() {
        String sql = "SELECT * FROM test_table WHERE id = 1";
        String queryId = SQLLogger.logQueryStart(sql);
        
        assertNotNull("Query ID should not be null", queryId);
        assertTrue("Query ID should start with 'Q'", queryId.startsWith("Q"));
        
        LOGGER.info("Generated query ID: {}", queryId);
    }
    
    @Test
    public void testLogQueryStartWithParameters() {
        String sql = "SELECT * FROM test_table WHERE id = ? AND name = ?";
        Object[] parameters = {1, "test"};
        
        String queryId = SQLLogger.logQueryStart(sql, parameters);
        
        assertNotNull("Query ID should not be null", queryId);
        assertTrue("Query ID should start with 'Q'", queryId.startsWith("Q"));
        
        LOGGER.info("Generated query ID with parameters: {}", queryId);
    }
    
    @Test
    public void testLogQueryEnd() {
        String sql = "SELECT * FROM test_table";
        String queryId = SQLLogger.logQueryStart(sql);
        
        // 模拟查询执行
        try {
            Thread.sleep(100); // 模拟执行时间
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        SQLLogger.logQueryEnd(queryId, 10, true);
        
        LOGGER.info("Logged query end for ID: {}", queryId);
    }
    
    @Test
    public void testLogQueryException() {
        String sql = "SELECT * FROM non_existent_table";
        String queryId = SQLLogger.logQueryStart(sql);
        
        // 模拟查询执行
        try {
            Thread.sleep(50); // 模拟执行时间
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        SQLException exception = new SQLException("Table does not exist", "42S02", 1146);
        SQLLogger.logQueryException(queryId, exception);
        
        LOGGER.info("Logged query exception for ID: {}", queryId);
    }
    
    @Test
    public void testLogBatchOperation() {
        String operation = "BATCH_INSERT";
        int batchSize = 1000;
        long executionTime = 500;
        
        SQLLogger.logBatchOperation(operation, batchSize, executionTime);
        
        LOGGER.info("Logged batch operation: {} with size {} and time {}ms", 
            operation, batchSize, executionTime);
    }
    
    @Test
    public void testLogConnectionInfo() {
        String connectionId = "CONN_001";
        String operation = "CONNECT";
        String details = "User: test, Database: phoenix";
        
        SQLLogger.logConnectionInfo(connectionId, operation, details);
        
        LOGGER.info("Logged connection info: {} {} {}", connectionId, operation, details);
    }
    
    @Test
    public void testSetSlowQueryThreshold() {
        SQLLogger.setSlowQueryThreshold("SELECT", 1000);
        SQLLogger.setSlowQueryThreshold("INSERT", 2000);
        SQLLogger.setSlowQueryThreshold("UPDATE", 1500);
        
        LOGGER.info("Set slow query thresholds for different SQL types");
    }
    
    @Test
    public void testMultipleQueryLogging() {
        // 测试多个查询的日志记录
        String[] queries = {
            "SELECT COUNT(*) FROM users",
            "INSERT INTO users (id, name) VALUES (1, 'test')",
            "UPDATE users SET name = 'updated' WHERE id = 1",
            "DELETE FROM users WHERE id = 1"
        };
        
        for (int i = 0; i < queries.length; i++) {
            String queryId = SQLLogger.logQueryStart(queries[i]);
            
            // 模拟执行时间
            try {
                Thread.sleep(50 + i * 10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            SQLLogger.logQueryEnd(queryId, i + 1, true);
            
            LOGGER.info("Completed query {}: {}", i + 1, queries[i]);
        }
    }
    
    @Test
    public void testSlowQueryDetection() {
        // 设置慢查询阈值
        SQLLogger.setSlowQueryThreshold("SELECT", 100);
        
        String queryId = SQLLogger.logQueryStart("SELECT * FROM large_table");
        
        // 模拟慢查询
        try {
            Thread.sleep(200); // 超过阈值
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        SQLLogger.logQueryEnd(queryId, 1000, true);
        
        LOGGER.info("Tested slow query detection for ID: {}", queryId);
    }
    
    @Test
    public void testQueryWithLongSQL() {
        // 测试长SQL语句的日志记录
        StringBuilder longSql = new StringBuilder();
        longSql.append("SELECT u.id, u.name, u.email, p.title, p.content, c.comment ");
        longSql.append("FROM users u ");
        longSql.append("LEFT JOIN posts p ON u.id = p.user_id ");
        longSql.append("LEFT JOIN comments c ON p.id = c.post_id ");
        longSql.append("WHERE u.status = 'active' ");
        longSql.append("AND p.published_date > '2023-01-01' ");
        longSql.append("ORDER BY u.name, p.published_date DESC ");
        longSql.append("LIMIT 100");
        
        String queryId = SQLLogger.logQueryStart(longSql.toString());
        
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        SQLLogger.logQueryEnd(queryId, 50, true);
        
        LOGGER.info("Tested long SQL query logging for ID: {}", queryId);
    }
    
    @Test
    public void testConcurrentQueryLogging() throws InterruptedException {
        // 测试并发查询日志记录
        int threadCount = 5;
        Thread[] threads = new Thread[threadCount];
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                String queryId = SQLLogger.logQueryStart("SELECT * FROM table_" + threadId);
                
                try {
                    Thread.sleep(50 + threadId * 10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                
                SQLLogger.logQueryEnd(queryId, threadId + 1, true);
                
                LOGGER.info("Thread {} completed query with ID: {}", threadId, queryId);
            });
        }
        
        // 启动所有线程
        for (Thread thread : threads) {
            thread.start();
        }
        
        // 等待所有线程完成
        for (Thread thread : threads) {
            thread.join();
        }
        
        LOGGER.info("Completed concurrent query logging test");
    }
} 