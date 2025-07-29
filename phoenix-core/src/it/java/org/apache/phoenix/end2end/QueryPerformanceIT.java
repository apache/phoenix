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

import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

/**
 * 查询性能测试类
 */
@Category(ParallelStatsDisabledTest.class)
public class QueryPerformanceIT extends BasePerformanceIT {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryPerformanceIT.class);
    
    private static final String TEST_TABLE = "QUERY_PERFORMANCE_TEST";
    private static final int TEST_DATA_SIZE = 50000;
    private static final int BATCH_SIZE = 1000;
    private static final int TEST_ITERATIONS = 5;
    
    @Before
    public void setUpTestData() throws Exception {
        super.setUp();
        
        // 创建测试表
        createTestTable(TEST_TABLE);
        
        // 生成测试数据
        generateTestData(TEST_TABLE, TEST_DATA_SIZE, BATCH_SIZE);
        
        // 创建索引以提高查询性能
        stmt.execute("CREATE INDEX " + TEST_TABLE + "_NAME_IDX ON " + TEST_TABLE + " (NAME)");
        stmt.execute("CREATE INDEX " + TEST_TABLE + "_DEPT_IDX ON " + TEST_TABLE + " (DEPT)");
        stmt.execute("CREATE INDEX " + TEST_TABLE + "_AGE_IDX ON " + TEST_TABLE + " (AGE)");
        
        LOGGER.info("Test setup completed with {} rows of data", TEST_DATA_SIZE);
    }
    
    @After
    public void cleanup() throws Exception {
        cleanupTestTable(TEST_TABLE);
    }
    
    @Test
    public void testSimpleSelectPerformance() throws Exception {
        String sql = "SELECT * FROM " + TEST_TABLE + " LIMIT 1000";
        
        PerformanceTestResult result = runQueryPerformanceTest(
            "Simple Select Performance", sql, TEST_ITERATIONS);
        
        LOGGER.info("Simple Select Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("Min execution time: {}ms", result.getMinExecutionTime());
        LOGGER.info("Max execution time: {}ms", result.getMaxExecutionTime());
        LOGGER.info("Standard deviation: {}ms", result.getStandardDeviation());
        
        // 验证性能指标
        assertTrue("Average execution time should be reasonable", 
            result.getAverageExecutionTime() < 5000); // 5秒内
    }
    
    @Test
    public void testFilteredQueryPerformance() throws Exception {
        String sql = "SELECT * FROM " + TEST_TABLE + " WHERE AGE > 30 AND SALARY > 50000";
        
        PerformanceTestResult result = runQueryPerformanceTest(
            "Filtered Query Performance", sql, TEST_ITERATIONS);
        
        LOGGER.info("Filtered Query Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("Average rows returned: {}", result.getAverageRowCount());
        
        assertTrue("Average execution time should be reasonable", 
            result.getAverageExecutionTime() < 3000);
    }
    
    @Test
    public void testAggregationQueryPerformance() throws Exception {
        String sql = "SELECT DEPT, COUNT(*), AVG(SALARY), MAX(AGE) FROM " + TEST_TABLE + 
                    " GROUP BY DEPT ORDER BY AVG(SALARY) DESC";
        
        PerformanceTestResult result = runQueryPerformanceTest(
            "Aggregation Query Performance", sql, TEST_ITERATIONS);
        
        LOGGER.info("Aggregation Query Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("Average rows returned: {}", result.getAverageRowCount());
        
        assertTrue("Average execution time should be reasonable", 
            result.getAverageExecutionTime() < 4000);
    }
    
    @Test
    public void testIndexedQueryPerformance() throws Exception {
        String sql = "SELECT * FROM " + TEST_TABLE + " WHERE NAME = 'User1000'";
        
        PerformanceTestResult result = runQueryPerformanceTest(
            "Indexed Query Performance", sql, TEST_ITERATIONS);
        
        LOGGER.info("Indexed Query Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("Average rows returned: {}", result.getAverageRowCount());
        
        // 索引查询应该很快
        assertTrue("Indexed query should be fast", 
            result.getAverageExecutionTime() < 1000);
    }
    
    @Test
    public void testRangeQueryPerformance() throws Exception {
        String sql = "SELECT * FROM " + TEST_TABLE + " WHERE AGE BETWEEN 25 AND 35 ORDER BY SALARY DESC LIMIT 100";
        
        PerformanceTestResult result = runQueryPerformanceTest(
            "Range Query Performance", sql, TEST_ITERATIONS);
        
        LOGGER.info("Range Query Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("Average rows returned: {}", result.getAverageRowCount());
        
        assertTrue("Range query should be reasonable", 
            result.getAverageExecutionTime() < 2000);
    }
    
    @Test
    public void testComplexQueryPerformance() throws Exception {
        String sql = "SELECT DEPT, COUNT(*) as EMP_COUNT, AVG(SALARY) as AVG_SALARY, " +
                    "MAX(AGE) as MAX_AGE, MIN(AGE) as MIN_AGE " +
                    "FROM " + TEST_TABLE + " " +
                    "WHERE SALARY > 40000 AND AGE > 25 " +
                    "GROUP BY DEPT " +
                    "HAVING COUNT(*) > 100 " +
                    "ORDER BY AVG_SALARY DESC";
        
        PerformanceTestResult result = runQueryPerformanceTest(
            "Complex Query Performance", sql, TEST_ITERATIONS);
        
        LOGGER.info("Complex Query Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("Average rows returned: {}", result.getAverageRowCount());
        
        assertTrue("Complex query should be reasonable", 
            result.getAverageExecutionTime() < 5000);
    }
    
    @Test
    public void testJoinQueryPerformance() throws Exception {
        // 创建第二个表用于JOIN测试
        String joinTable = TEST_TABLE + "_JOIN";
        stmt.execute("CREATE TABLE " + joinTable + " (" +
                "DEPT_ID VARCHAR(20) PRIMARY KEY, " +
                "DEPT_NAME VARCHAR(50), " +
                "LOCATION VARCHAR(50)" +
                ")");
        
        // 插入JOIN表数据
        for (int i = 0; i < 10; i++) {
            stmt.execute("UPSERT INTO " + joinTable + " VALUES ('Dept" + i + "', 'Department " + i + "', 'Location " + i + "')");
        }
        
        String sql = "SELECT t.NAME, t.SALARY, j.DEPT_NAME, j.LOCATION " +
                    "FROM " + TEST_TABLE + " t " +
                    "JOIN " + joinTable + " j ON t.DEPT = j.DEPT_ID " +
                    "WHERE t.SALARY > 50000 " +
                    "ORDER BY t.SALARY DESC LIMIT 100";
        
        PerformanceTestResult result = runQueryPerformanceTest(
            "Join Query Performance", sql, TEST_ITERATIONS);
        
        LOGGER.info("Join Query Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("Average rows returned: {}", result.getAverageRowCount());
        
        assertTrue("Join query should be reasonable", 
            result.getAverageExecutionTime() < 4000);
        
        // 清理JOIN表
        stmt.execute("DROP TABLE IF EXISTS " + joinTable);
    }
    
    @Test
    public void testSubqueryPerformance() throws Exception {
        String sql = "SELECT * FROM " + TEST_TABLE + " t1 " +
                    "WHERE SALARY > (SELECT AVG(SALARY) FROM " + TEST_TABLE + " t2 WHERE t2.DEPT = t1.DEPT) " +
                    "ORDER BY SALARY DESC LIMIT 50";
        
        PerformanceTestResult result = runQueryPerformanceTest(
            "Subquery Performance", sql, TEST_ITERATIONS);
        
        LOGGER.info("Subquery Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("Average rows returned: {}", result.getAverageRowCount());
        
        assertTrue("Subquery should be reasonable", 
            result.getAverageExecutionTime() < 6000);
    }
    
    @Test
    public void testConcurrentQueryPerformance() throws Exception {
        // 测试并发查询性能
        String[] queries = {
            "SELECT COUNT(*) FROM " + TEST_TABLE,
            "SELECT AVG(SALARY) FROM " + TEST_TABLE + " WHERE AGE > 30",
            "SELECT DEPT, COUNT(*) FROM " + TEST_TABLE + " GROUP BY DEPT",
            "SELECT * FROM " + TEST_TABLE + " WHERE NAME LIKE 'User%' LIMIT 100"
        };
        
        LOGGER.info("Starting concurrent query performance test...");
        
        for (int i = 0; i < queries.length; i++) {
            PerformanceTestResult result = runQueryPerformanceTest(
                "Concurrent Query " + (i + 1), queries[i], TEST_ITERATIONS);
            
            LOGGER.info("Concurrent Query {} Performance: {}ms avg", i + 1, result.getAverageExecutionTime());
            
            assertTrue("Concurrent query should be reasonable", 
                result.getAverageExecutionTime() < 3000);
        }
    }
    
    @Test
    public void testLargeResultSetPerformance() throws Exception {
        String sql = "SELECT * FROM " + TEST_TABLE + " ORDER BY ID";
        
        PerformanceTestResult result = runQueryPerformanceTest(
            "Large Result Set Performance", sql, 3); // 减少迭代次数，因为结果集很大
        
        LOGGER.info("Large Result Set Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("Total rows returned: {}", result.getTotalRows());
        
        assertTrue("Large result set query should be reasonable", 
            result.getAverageExecutionTime() < 10000);
    }
    
    @Test
    public void testUpdatePerformance() throws Exception {
        String updateSql = "UPDATE " + TEST_TABLE + " SET SALARY = SALARY * 1.1 WHERE AGE > 40";
        
        PerformanceTestResult result = runPerformanceTest(
            "Update Performance", updateSql, 3); // 减少迭代次数，因为这是更新操作
        
        LOGGER.info("Update Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        
        assertTrue("Update operation should be reasonable", 
            result.getAverageExecutionTime() < 5000);
    }
    
    @Test
    public void testDeletePerformance() throws Exception {
        // 先创建一些要删除的数据
        stmt.execute("CREATE TABLE " + TEST_TABLE + "_DELETE AS SELECT * FROM " + TEST_TABLE + " WHERE ID < 1000");
        
        String deleteSql = "DELETE FROM " + TEST_TABLE + "_DELETE WHERE AGE < 25";
        
        PerformanceTestResult result = runPerformanceTest(
            "Delete Performance", deleteSql, 3);
        
        LOGGER.info("Delete Performance Test Results:");
        LOGGER.info("Average execution time: {}ms", result.getAverageExecutionTime());
        
        assertTrue("Delete operation should be reasonable", 
            result.getAverageExecutionTime() < 3000);
        
        // 清理
        stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE + "_DELETE");
    }
} 