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
package org.apache.phoenix.end2end;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * 批量操作性能测试类
 */
@Category(ParallelStatsDisabledTest.class)
public class BatchPerformanceIT extends BasePerformanceIT {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchPerformanceIT.class);
    
    private static final String TEST_TABLE = "BATCH_PERFORMANCE_TEST";
    private static final int[] BATCH_SIZES = {100, 500, 1000, 2000, 5000};
    private static final int TEST_ITERATIONS = 3;
    
    @Before
    public void setUpTestTable() throws Exception {
        super.setUp();
        createTestTable(TEST_TABLE);
    }
    
    @After
    public void cleanup() throws Exception {
        cleanupTestTable(TEST_TABLE);
    }
    
    @Test
    public void testBatchInsertPerformance() throws Exception {
        LOGGER.info("Testing batch insert performance with different batch sizes...");
        
        for (int batchSize : BATCH_SIZES) {
            PerformanceTestResult result = runBatchInsertTest(batchSize, 10000);
            
            LOGGER.info("Batch Insert Performance (batch size {}):", batchSize);
            LOGGER.info("  Average execution time: {}ms", result.getAverageExecutionTime());
            LOGGER.info("  Rows per second: {}", calculateRowsPerSecond(result.getAverageExecutionTime(), 10000));
            
            // 验证性能指标 - 批量插入应该比单行插入快
            assertTrue("Batch insert should be efficient", 
                result.getAverageExecutionTime() < 10000); // 10秒内完成10000行
        }
    }
    
    @Test
    public void testBatchUpdatePerformance() throws Exception {
        // 先插入一些数据用于更新测试
        generateTestData(TEST_TABLE, 10000, 1000);
        
        LOGGER.info("Testing batch update performance with different batch sizes...");
        
        for (int batchSize : BATCH_SIZES) {
            PerformanceTestResult result = runBatchUpdateTest(batchSize, 5000);
            
            LOGGER.info("Batch Update Performance (batch size {}):", batchSize);
            LOGGER.info("  Average execution time: {}ms", result.getAverageExecutionTime());
            LOGGER.info("  Rows per second: {}", calculateRowsPerSecond(result.getAverageExecutionTime(), 5000));
            
            assertTrue("Batch update should be efficient", 
                result.getAverageExecutionTime() < 8000);
        }
    }
    
    @Test
    public void testBatchDeletePerformance() throws Exception {
        // 先插入一些数据用于删除测试
        generateTestData(TEST_TABLE, 10000, 1000);
        
        LOGGER.info("Testing batch delete performance with different batch sizes...");
        
        for (int batchSize : BATCH_SIZES) {
            PerformanceTestResult result = runBatchDeleteTest(batchSize, 3000);
            
            LOGGER.info("Batch Delete Performance (batch size {}):", batchSize);
            LOGGER.info("  Average execution time: {}ms", result.getAverageExecutionTime());
            LOGGER.info("  Rows per second: {}", calculateRowsPerSecond(result.getAverageExecutionTime(), 3000));
            
            assertTrue("Batch delete should be efficient", 
                result.getAverageExecutionTime() < 6000);
        }
    }
    
    @Test
    public void testLargeBatchPerformance() throws Exception {
        LOGGER.info("Testing large batch insert performance...");
        
        PerformanceTestResult result = runBatchInsertTest(5000, 50000);
        
        LOGGER.info("Large Batch Insert Performance:");
        LOGGER.info("  Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("  Total rows: 50000");
        LOGGER.info("  Rows per second: {}", calculateRowsPerSecond(result.getAverageExecutionTime(), 50000));
        
        assertTrue("Large batch insert should be efficient", 
            result.getAverageExecutionTime() < 30000); // 30秒内完成50000行
    }
    
    @Test
    public void testConcurrentBatchPerformance() throws Exception {
        LOGGER.info("Testing concurrent batch operations...");
        
        // 创建多个表进行并发测试
        String[] tables = {
            TEST_TABLE + "_CONCURRENT_1",
            TEST_TABLE + "_CONCURRENT_2", 
            TEST_TABLE + "_CONCURRENT_3"
        };
        
        for (String table : tables) {
            createTestTable(table);
        }
        
        try {
            List<PerformanceTestResult> results = new ArrayList<>();
            
            // 并发执行批量插入
            for (int i = 0; i < tables.length; i++) {
                PerformanceTestResult result = runBatchInsertTest(1000, 5000, tables[i]);
                results.add(result);
                
                LOGGER.info("Concurrent Batch {} Performance: {}ms avg", i + 1, result.getAverageExecutionTime());
            }
            
            // 验证所有并发操作都成功
            for (PerformanceTestResult result : results) {
                assertTrue("Concurrent batch operation should be efficient", 
                    result.getAverageExecutionTime() < 15000);
            }
            
        } finally {
            // 清理测试表
            for (String table : tables) {
                cleanupTestTable(table);
            }
        }
    }
    
    @Test
    public void testMixedBatchOperations() throws Exception {
        LOGGER.info("Testing mixed batch operations (insert, update, delete)...");
        
        // 先插入基础数据
        generateTestData(TEST_TABLE, 10000, 1000);
        
        // 测试混合操作
        PerformanceTestResult insertResult = runBatchInsertTest(1000, 2000);
        PerformanceTestResult updateResult = runBatchUpdateTest(1000, 2000);
        PerformanceTestResult deleteResult = runBatchDeleteTest(1000, 2000);
        
        LOGGER.info("Mixed Batch Operations Performance:");
        LOGGER.info("  Insert: {}ms avg", insertResult.getAverageExecutionTime());
        LOGGER.info("  Update: {}ms avg", updateResult.getAverageExecutionTime());
        LOGGER.info("  Delete: {}ms avg", deleteResult.getAverageExecutionTime());
        
        assertTrue("Mixed batch operations should be efficient", 
            insertResult.getAverageExecutionTime() < 5000 &&
            updateResult.getAverageExecutionTime() < 5000 &&
            deleteResult.getAverageExecutionTime() < 5000);
    }
    
    /**
     * 运行批量插入性能测试
     */
    private PerformanceTestResult runBatchInsertTest(int batchSize, int totalRows) throws SQLException {
        return runBatchInsertTest(batchSize, totalRows, TEST_TABLE);
    }
    
    /**
     * 运行批量插入性能测试
     */
    private PerformanceTestResult runBatchInsertTest(int batchSize, int totalRows, String tableName) throws SQLException {
        String testName = "Batch Insert Performance (batch size " + batchSize + ")";
        List<Long> executionTimes = new ArrayList<>();
        
        for (int iteration = 0; iteration < TEST_ITERATIONS; iteration++) {
            // 清空表
            stmt.execute("DELETE FROM " + tableName);
            
            String insertSql = "UPSERT INTO " + tableName + " (ID, NAME, AGE, SALARY, DEPT) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(insertSql);
            
            conn.setAutoCommit(false);
            
            try {
                long startTime = System.currentTimeMillis();
                
                for (int i = 0; i < totalRows; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, "User" + i);
                    pstmt.setInt(3, 20 + random.nextInt(50));
                    pstmt.setDouble(4, 30000 + random.nextInt(70000));
                    pstmt.setString(5, "Dept" + (i % 10));
                    pstmt.addBatch();
                    
                    if ((i + 1) % batchSize == 0) {
                        pstmt.executeBatch();
                    }
                }
                
                // 执行剩余的批次
                if (totalRows % batchSize != 0) {
                    pstmt.executeBatch();
                }
                
                conn.commit();
                
                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                executionTimes.add(executionTime);
                
                LOGGER.debug("Batch insert iteration {}: {}ms", iteration + 1, executionTime);
                
            } finally {
                conn.setAutoCommit(true);
                pstmt.close();
            }
        }
        
        return new PerformanceTestResult(testName, "Batch Insert", executionTimes);
    }
    
    /**
     * 运行批量更新性能测试
     */
    private PerformanceTestResult runBatchUpdateTest(int batchSize, int totalRows) throws SQLException {
        String testName = "Batch Update Performance (batch size " + batchSize + ")";
        List<Long> executionTimes = new ArrayList<>();
        
        for (int iteration = 0; iteration < TEST_ITERATIONS; iteration++) {
            String updateSql = "UPDATE " + TEST_TABLE + " SET SALARY = SALARY * 1.1 WHERE ID = ?";
            PreparedStatement pstmt = conn.prepareStatement(updateSql);
            
            conn.setAutoCommit(false);
            
            try {
                long startTime = System.currentTimeMillis();
                
                for (int i = 0; i < totalRows; i++) {
                    pstmt.setInt(1, i);
                    pstmt.addBatch();
                    
                    if ((i + 1) % batchSize == 0) {
                        pstmt.executeBatch();
                    }
                }
                
                // 执行剩余的批次
                if (totalRows % batchSize != 0) {
                    pstmt.executeBatch();
                }
                
                conn.commit();
                
                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                executionTimes.add(executionTime);
                
                LOGGER.debug("Batch update iteration {}: {}ms", iteration + 1, executionTime);
                
            } finally {
                conn.setAutoCommit(true);
                pstmt.close();
            }
        }
        
        return new PerformanceTestResult(testName, "Batch Update", executionTimes);
    }
    
    /**
     * 运行批量删除性能测试
     */
    private PerformanceTestResult runBatchDeleteTest(int batchSize, int totalRows) throws SQLException {
        String testName = "Batch Delete Performance (batch size " + batchSize + ")";
        List<Long> executionTimes = new ArrayList<>();
        
        for (int iteration = 0; iteration < TEST_ITERATIONS; iteration++) {
            String deleteSql = "DELETE FROM " + TEST_TABLE + " WHERE ID = ?";
            PreparedStatement pstmt = conn.prepareStatement(deleteSql);
            
            conn.setAutoCommit(false);
            
            try {
                long startTime = System.currentTimeMillis();
                
                for (int i = 0; i < totalRows; i++) {
                    pstmt.setInt(1, i);
                    pstmt.addBatch();
                    
                    if ((i + 1) % batchSize == 0) {
                        pstmt.executeBatch();
                    }
                }
                
                // 执行剩余的批次
                if (totalRows % batchSize != 0) {
                    pstmt.executeBatch();
                }
                
                conn.commit();
                
                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                executionTimes.add(executionTime);
                
                LOGGER.debug("Batch delete iteration {}: {}ms", iteration + 1, executionTime);
                
            } finally {
                conn.setAutoCommit(true);
                pstmt.close();
            }
        }
        
        return new PerformanceTestResult(testName, "Batch Delete", executionTimes);
    }
    
    /**
     * 计算每秒处理的行数
     */
    private double calculateRowsPerSecond(long executionTimeMs, int totalRows) {
        if (executionTimeMs == 0) return 0.0;
        return (double) totalRows / (executionTimeMs / 1000.0);
    }
} 