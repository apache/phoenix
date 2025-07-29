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

import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * 性能测试基类，提供通用的性能测试功能
 */
@Category(ParallelStatsDisabledTest.class)
public abstract class BasePerformanceIT extends ParallelStatsDisabledIT {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(BasePerformanceIT.class);
    
    protected static final int DEFAULT_WARMUP_ITERATIONS = 3;
    protected static final int DEFAULT_TEST_ITERATIONS = 10;
    protected static final int DEFAULT_DATA_SIZE = 10000;
    
    protected Connection conn;
    protected Statement stmt;
    protected Random random;
    
    @Before
    public void setUp() throws Exception {
        conn = DriverManager.getConnection(getUrl());
        stmt = conn.createStatement();
        random = new Random(42); // 固定种子以确保可重复性
    }
    
    /**
     * 执行性能测试
     * @param testName 测试名称
     * @param sql SQL语句
     * @param iterations 迭代次数
     * @return 性能测试结果
     */
    protected PerformanceTestResult runPerformanceTest(String testName, String sql, int iterations) {
        return runPerformanceTest(testName, sql, iterations, DEFAULT_WARMUP_ITERATIONS);
    }
    
    /**
     * 执行性能测试
     * @param testName 测试名称
     * @param sql SQL语句
     * @param iterations 迭代次数
     * @param warmupIterations 预热迭代次数
     * @return 性能测试结果
     */
    protected PerformanceTestResult runPerformanceTest(String testName, String sql, int iterations, int warmupIterations) {
        LOGGER.info("Starting performance test: {}", testName);
        LOGGER.info("SQL: {}", sql);
        LOGGER.info("Iterations: {}, Warmup iterations: {}", iterations, warmupIterations);
        
        List<Long> executionTimes = new ArrayList<>();
        
        // 预热阶段
        LOGGER.info("Warming up...");
        for (int i = 0; i < warmupIterations; i++) {
            try {
                long startTime = System.currentTimeMillis();
                stmt.execute(sql);
                long endTime = System.currentTimeMillis();
                LOGGER.debug("Warmup iteration {}: {}ms", i + 1, endTime - startTime);
            } catch (SQLException e) {
                LOGGER.warn("Warmup iteration {} failed: {}", i + 1, e.getMessage());
            }
        }
        
        // 实际测试阶段
        LOGGER.info("Running performance test...");
        for (int i = 0; i < iterations; i++) {
            try {
                long startTime = System.currentTimeMillis();
                stmt.execute(sql);
                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                executionTimes.add(executionTime);
                LOGGER.debug("Test iteration {}: {}ms", i + 1, executionTime);
            } catch (SQLException e) {
                LOGGER.error("Test iteration {} failed: {}", i + 1, e.getMessage());
            }
        }
        
        PerformanceTestResult result = new PerformanceTestResult(testName, sql, executionTimes);
        LOGGER.info("Performance test completed: {}", result);
        
        return result;
    }
    
    /**
     * 执行查询性能测试
     * @param testName 测试名称
     * @param sql SQL查询语句
     * @param iterations 迭代次数
     * @return 性能测试结果
     */
    protected PerformanceTestResult runQueryPerformanceTest(String testName, String sql, int iterations) {
        return runQueryPerformanceTest(testName, sql, iterations, DEFAULT_WARMUP_ITERATIONS);
    }
    
    /**
     * 执行查询性能测试
     * @param testName 测试名称
     * @param sql SQL查询语句
     * @param iterations 迭代次数
     * @param warmupIterations 预热迭代次数
     * @return 性能测试结果
     */
    protected PerformanceTestResult runQueryPerformanceTest(String testName, String sql, int iterations, int warmupIterations) {
        LOGGER.info("Starting query performance test: {}", testName);
        LOGGER.info("SQL: {}", sql);
        LOGGER.info("Iterations: {}, Warmup iterations: {}", iterations, warmupIterations);
        
        List<Long> executionTimes = new ArrayList<>();
        List<Integer> rowCounts = new ArrayList<>();
        
        // 预热阶段
        LOGGER.info("Warming up...");
        for (int i = 0; i < warmupIterations; i++) {
            try {
                long startTime = System.currentTimeMillis();
                ResultSet rs = stmt.executeQuery(sql);
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                rs.close();
                long endTime = System.currentTimeMillis();
                LOGGER.debug("Warmup iteration {}: {}ms, {} rows", i + 1, endTime - startTime, rowCount);
            } catch (SQLException e) {
                LOGGER.warn("Warmup iteration {} failed: {}", i + 1, e.getMessage());
            }
        }
        
        // 实际测试阶段
        LOGGER.info("Running query performance test...");
        for (int i = 0; i < iterations; i++) {
            try {
                long startTime = System.currentTimeMillis();
                ResultSet rs = stmt.executeQuery(sql);
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                rs.close();
                long endTime = System.currentTimeMillis();
                long executionTime = endTime - startTime;
                executionTimes.add(executionTime);
                rowCounts.add(rowCount);
                LOGGER.debug("Test iteration {}: {}ms, {} rows", i + 1, executionTime, rowCount);
            } catch (SQLException e) {
                LOGGER.error("Test iteration {} failed: {}", i + 1, e.getMessage());
            }
        }
        
        PerformanceTestResult result = new PerformanceTestResult(testName, sql, executionTimes, rowCounts);
        LOGGER.info("Query performance test completed: {}", result);
        
        return result;
    }
    
    /**
     * 生成测试数据
     * @param tableName 表名
     * @param rowCount 行数
     * @param batchSize 批量大小
     */
    protected void generateTestData(String tableName, int rowCount, int batchSize) throws SQLException {
        LOGGER.info("Generating {} rows of test data for table {}", rowCount, tableName);
        
        String insertSql = "UPSERT INTO " + tableName + " (ID, NAME, AGE, SALARY, DEPT) VALUES (?, ?, ?, ?, ?)";
        PreparedStatement pstmt = conn.prepareStatement(insertSql);
        
        conn.setAutoCommit(false);
        
        try {
            for (int i = 0; i < rowCount; i++) {
                pstmt.setInt(1, i);
                pstmt.setString(2, "User" + i);
                pstmt.setInt(3, 20 + random.nextInt(50));
                pstmt.setDouble(4, 30000 + random.nextInt(70000));
                pstmt.setString(5, "Dept" + (i % 10));
                pstmt.addBatch();
                
                if ((i + 1) % batchSize == 0) {
                    pstmt.executeBatch();
                    LOGGER.debug("Inserted {} rows", i + 1);
                }
            }
            
            // 执行剩余的批次
            if (rowCount % batchSize != 0) {
                pstmt.executeBatch();
            }
            
            conn.commit();
            LOGGER.info("Successfully generated {} rows of test data", rowCount);
        } finally {
            conn.setAutoCommit(true);
            pstmt.close();
        }
    }
    
    /**
     * 创建测试表
     * @param tableName 表名
     */
    protected void createTestTable(String tableName) throws SQLException {
        String createTableSql = "CREATE TABLE " + tableName + " (" +
                "ID INTEGER PRIMARY KEY, " +
                "NAME VARCHAR(50), " +
                "AGE INTEGER, " +
                "SALARY DOUBLE, " +
                "DEPT VARCHAR(20)" +
                ")";
        
        stmt.execute(createTableSql);
        LOGGER.info("Created test table: {}", tableName);
    }
    
    /**
     * 清理测试表
     * @param tableName 表名
     */
    protected void cleanupTestTable(String tableName) throws SQLException {
        try {
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            LOGGER.info("Cleaned up test table: {}", tableName);
        } catch (SQLException e) {
            LOGGER.warn("Failed to cleanup table {}: {}", tableName, e.getMessage());
        }
    }
    
    /**
     * 性能测试结果类
     */
    public static class PerformanceTestResult {
        private final String testName;
        private final String sql;
        private final List<Long> executionTimes;
        private final List<Integer> rowCounts;
        
        public PerformanceTestResult(String testName, String sql, List<Long> executionTimes) {
            this(testName, sql, executionTimes, null);
        }
        
        public PerformanceTestResult(String testName, String sql, List<Long> executionTimes, List<Integer> rowCounts) {
            this.testName = testName;
            this.sql = sql;
            this.executionTimes = executionTimes;
            this.rowCounts = rowCounts;
        }
        
        public String getTestName() {
            return testName;
        }
        
        public String getSql() {
            return sql;
        }
        
        public List<Long> getExecutionTimes() {
            return executionTimes;
        }
        
        public List<Integer> getRowCounts() {
            return rowCounts;
        }
        
        public long getMinExecutionTime() {
            return executionTimes.stream().mapToLong(Long::longValue).min().orElse(0);
        }
        
        public long getMaxExecutionTime() {
            return executionTimes.stream().mapToLong(Long::longValue).max().orElse(0);
        }
        
        public double getAverageExecutionTime() {
            return executionTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        }
        
        public double getStandardDeviation() {
            double mean = getAverageExecutionTime();
            double variance = executionTimes.stream()
                    .mapToDouble(time -> Math.pow(time - mean, 2))
                    .average()
                    .orElse(0.0);
            return Math.sqrt(variance);
        }
        
        public int getTotalRows() {
            if (rowCounts == null) {
                return 0;
            }
            return rowCounts.stream().mapToInt(Integer::intValue).sum();
        }
        
        public double getAverageRowCount() {
            if (rowCounts == null) {
                return 0.0;
            }
            return rowCounts.stream().mapToInt(Integer::intValue).average().orElse(0.0);
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("PerformanceTestResult{");
            sb.append("testName='").append(testName).append('\'');
            sb.append(", iterations=").append(executionTimes.size());
            sb.append(", minTime=").append(getMinExecutionTime()).append("ms");
            sb.append(", maxTime=").append(getMaxExecutionTime()).append("ms");
            sb.append(", avgTime=").append(String.format("%.2f", getAverageExecutionTime())).append("ms");
            sb.append(", stdDev=").append(String.format("%.2f", getStandardDeviation())).append("ms");
            if (rowCounts != null) {
                sb.append(", totalRows=").append(getTotalRows());
                sb.append(", avgRows=").append(String.format("%.2f", getAverageRowCount()));
            }
            sb.append('}');
            return sb.toString();
        }
    }
} 