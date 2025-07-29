# Phoenix SQL执行日志和性能测试功能

本文档介绍了Phoenix项目中新增的SQL执行日志和性能测试功能。

## 1. SQL执行日志功能

### 1.1 功能概述

新增的SQL执行日志功能提供了详细的SQL执行信息记录，包括：
- SQL语句执行时间
- 影响的行数
- 执行状态（成功/失败）
- 慢查询检测
- 批量操作性能监控
- 连接信息记录

### 1.2 日志配置

日志配置位于 `phoenix-core/src/test/resources/log4j2-test.properties`，包含以下配置：

```properties
# SQL执行日志文件appender
appender.sql.type = File
appender.sql.name = SQLFile
appender.sql.fileName = ${sys:phoenix.log.dir:-logs}/phoenix-sql-execution.log
appender.sql.filePattern = ${sys:phoenix.log.dir:-logs}/phoenix-sql-execution-%d{yyyy-MM-dd}-%i.log.gz

# SQL执行日志配置
logger.sql.name = org.apache.phoenix.jdbc
logger.sql.level = INFO
logger.sql.additivity = false
logger.sql.appenderRef.sql.ref = SQLFile

# 查询执行时间日志
logger.queryTime.name = org.apache.phoenix.query
logger.queryTime.level = INFO
logger.queryTime.additivity = false
logger.queryTime.appenderRef.sql.ref = SQLFile

# 性能监控日志
logger.performance.name = org.apache.phoenix.monitoring
logger.performance.level = INFO
logger.performance.additivity = false
logger.performance.appenderRef.sql.ref = SQLFile
```

### 1.3 日志格式

SQL执行日志包含以下信息：

```
SQL_EXECUTION_START [Q1] SQL: SELECT * FROM users WHERE id = 1 PARAMETERS: [1]
SQL_EXECUTION_END [Q1] STATUS: SUCCESS EXECUTION_TIME: 150ms ROWS_AFFECTED: 1
QUERY_TIME [Q1] 150ms
```

### 1.4 慢查询检测

系统会自动检测慢查询并记录警告：

```
SLOW_QUERY [Q1] 1500ms exceeds default threshold 1000ms
```

可以通过以下方式设置慢查询阈值：

```java
SQLLogger.setSlowQueryThreshold("SELECT", 1000);
SQLLogger.setSlowQueryThreshold("INSERT", 2000);
```

### 1.5 使用示例

```java
// 记录查询开始
String queryId = SQLLogger.logQueryStart("SELECT * FROM users WHERE age > 25");

// 执行查询
ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE age > 25");
int rowCount = 0;
while (rs.next()) {
    rowCount++;
}

// 记录查询结束
SQLLogger.logQueryEnd(queryId, rowCount, true);
```

## 2. 性能测试功能

### 2.1 功能概述

新增的性能测试功能提供了全面的SQL性能测试框架，包括：
- 查询性能测试
- 批量操作性能测试
- 并发性能测试
- 性能指标统计
- 自动化性能验证

### 2.2 测试基类

`BasePerformanceIT` 提供了通用的性能测试功能：

```java
public abstract class BasePerformanceIT extends ParallelStatsDisabledIT {
    // 执行性能测试
    protected PerformanceTestResult runPerformanceTest(String testName, String sql, int iterations);
    
    // 执行查询性能测试
    protected PerformanceTestResult runQueryPerformanceTest(String testName, String sql, int iterations);
    
    // 生成测试数据
    protected void generateTestData(String tableName, int rowCount, int batchSize);
    
    // 创建测试表
    protected void createTestTable(String tableName);
}
```

### 2.3 性能测试结果

`PerformanceTestResult` 类提供详细的性能指标：

```java
public class PerformanceTestResult {
    public long getMinExecutionTime();        // 最小执行时间
    public long getMaxExecutionTime();        // 最大执行时间
    public double getAverageExecutionTime();  // 平均执行时间
    public double getStandardDeviation();     // 标准差
    public int getTotalRows();               // 总行数
    public double getAverageRowCount();       // 平均行数
}
```

### 2.4 查询性能测试

`QueryPerformanceIT` 提供了各种查询类型的性能测试：

```java
@Test
public void testSimpleSelectPerformance() throws Exception {
    String sql = "SELECT * FROM " + TEST_TABLE + " LIMIT 1000";
    PerformanceTestResult result = runQueryPerformanceTest(
        "Simple Select Performance", sql, TEST_ITERATIONS);
    
    assertTrue("Average execution time should be reasonable", 
        result.getAverageExecutionTime() < 5000);
}
```

### 2.5 批量操作性能测试

`BatchPerformanceIT` 提供了批量操作的性能测试：

```java
@Test
public void testBatchInsertPerformance() throws Exception {
    for (int batchSize : BATCH_SIZES) {
        PerformanceTestResult result = runBatchInsertTest(batchSize, 10000);
        
        LOGGER.info("Batch Insert Performance (batch size {}):", batchSize);
        LOGGER.info("  Average execution time: {}ms", result.getAverageExecutionTime());
        LOGGER.info("  Rows per second: {}", calculateRowsPerSecond(result.getAverageExecutionTime(), 10000));
    }
}
```

### 2.6 运行性能测试

#### 2.6.1 运行所有性能测试

```bash
mvn test -Dtest=*PerformanceIT
```

#### 2.6.2 运行特定性能测试

```bash
mvn test -Dtest=QueryPerformanceIT
mvn test -Dtest=BatchPerformanceIT
```

#### 2.6.3 运行单个测试方法

```bash
mvn test -Dtest=QueryPerformanceIT#testSimpleSelectPerformance
```

### 2.7 性能测试配置

可以通过以下方式调整性能测试参数：

```java
// 在测试类中设置
private static final int TEST_ITERATIONS = 10;  // 测试迭代次数
private static final int DEFAULT_WARMUP_ITERATIONS = 3;  // 预热迭代次数
private static final int TEST_DATA_SIZE = 50000;  // 测试数据大小
```

### 2.8 性能测试最佳实践

1. **预热阶段**：在正式测试前进行预热，确保JVM和数据库缓存已预热
2. **多次迭代**：运行多次迭代以获得更准确的性能数据
3. **统计分析**：使用平均值、标准差等统计指标评估性能
4. **环境一致性**：确保测试环境的一致性，避免外部因素影响
5. **监控资源**：在性能测试期间监控CPU、内存、网络等资源使用情况

### 2.9 性能基准

以下是一些性能基准参考：

| 操作类型 | 预期性能 | 说明 |
|---------|---------|------|
| 简单查询 | < 5秒 | 包含LIMIT的简单SELECT |
| 过滤查询 | < 3秒 | 带WHERE条件的查询 |
| 聚合查询 | < 4秒 | GROUP BY、聚合函数 |
| 索引查询 | < 1秒 | 使用索引的查询 |
| 批量插入 | < 10秒/万行 | 批量插入操作 |
| 批量更新 | < 8秒/万行 | 批量更新操作 |
| 批量删除 | < 6秒/万行 | 批量删除操作 |

### 2.10 故障排除

#### 2.10.1 常见问题

1. **测试超时**：增加测试超时时间或减少数据量
2. **内存不足**：减少测试数据量或增加JVM内存
3. **连接超时**：检查数据库连接配置
4. **日志文件过大**：调整日志轮转配置

#### 2.10.2 调试技巧

1. 启用DEBUG日志级别查看详细信息
2. 使用性能分析工具分析瓶颈
3. 检查数据库执行计划
4. 监控系统资源使用情况

## 3. 总结

新增的SQL执行日志和性能测试功能为Phoenix项目提供了：

1. **详细的SQL执行监控**：记录所有SQL执行的详细信息
2. **慢查询检测**：自动识别和警告慢查询
3. **全面的性能测试**：覆盖各种SQL操作类型的性能测试
4. **自动化性能验证**：确保性能指标符合预期
5. **性能基准**：提供性能基准参考

这些功能有助于：
- 监控生产环境中的SQL性能
- 识别性能瓶颈
- 验证性能优化效果
- 确保系统性能符合要求 