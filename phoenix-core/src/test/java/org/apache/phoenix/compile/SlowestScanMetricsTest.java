/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.monitoring.ReadMetricQueue;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Test;

import org.apache.hbase.thirdparty.com.google.gson.JsonObject;

/**
 * Unit test for {@link StatementContext#getTopNSlowestScanMetrics()}. This test validates the logic
 * of computing top N slowest scan metrics in isolation.
 */
public class SlowestScanMetricsTest {

  /**
   * Test that when slowestScanMetricsCount is 0 or negative, an empty list is returned.
   */
  @Test
  public void testWithZeroOrNegativeCount() {
    StatementContext context = createStatementContext(0);
    List<List<JsonObject>> result = context.getTopNSlowestScanMetrics();
    assertTrue(result.isEmpty());

    context = createStatementContext(-1);
    result = context.getTopNSlowestScanMetrics();
    assertTrue(result.isEmpty());
  }

  /**
   * Test with a single scan metrics holder.
   */
  @Test
  public void testWithSingleScanMetric() {
    StatementContext context = createStatementContext(5);
    long millisBetweenNexts = 100L;

    // Add a single scan metric
    ScanMetricsHolder scanMetric = createScanMetricsHolder("TABLE1", millisBetweenNexts);
    context.getSlowestScanMetricsQueue().add(scanMetric);

    List<List<JsonObject>> result = context.getTopNSlowestScanMetrics();

    assertEquals(1, result.size());
    assertEquals(1, result.get(0).size());
    assertEquals("TABLE1", result.get(0).get(0).get("table").getAsString());
    assertEquals(millisBetweenNexts, result.get(0).get(0).get("n").getAsLong());
  }

  /**
   * Test with multiple scan metrics holders - verify the sorted order with slowest one first.
   */
  @Test
  public void testWithMultipleScanMetrics_Sorted() {
    int topN = 3;
    StatementContext context = createStatementContext(topN);

    // Add scan metrics with different times
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE1", 100L));
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE2", 500L));
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE3", 200L));
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE4", 300L));

    List<List<JsonObject>> result = context.getTopNSlowestScanMetrics();

    // Should return top 3 slowest: 500, 300, 200 in this order only.
    assertEquals(topN, result.size());

    // Verify the results are sorted by time in descending order
    assertEquals(500L, result.get(0).get(0).get("n").getAsLong());
    assertEquals(300L, result.get(1).get(0).get("n").getAsLong());
    assertEquals(200L, result.get(2).get(0).get("n").getAsLong());
  }

  /**
   * Test with fewer scan metrics holders than the topN limit.
   */
  @Test
  public void testWithFewerMetricsThanTopN() {
    StatementContext context = createStatementContext(10);

    // Add only 3 metrics when topN is 10
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE1", 100L));
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE2", 300L));
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE3", 200L));

    List<List<JsonObject>> result = context.getTopNSlowestScanMetrics();

    assertEquals(3, result.size());

    // Verify sorting
    assertEquals(300L, result.get(0).get(0).get("n").getAsLong());
    assertEquals(200L, result.get(1).get(0).get("n").getAsLong());
    assertEquals(100L, result.get(2).get(0).get("n").getAsLong());
  }

  /**
   * Test with sub-statement contexts (e.g., subqueries or joins). Verifies that scan metrics from
   * sub-contexts are included and aggregated correctly. The algorithm only returns paths that reach
   * leaf nodes (contexts with no sub-contexts).
   */
  @Test
  public void testWithSubStatementContexts() {
    StatementContext mainContext = createStatementContext(5);
    String mainTableName = "MAIN_TABLE";
    String subTableName = "SUB_TABLE";

    // Add scan metrics holder to main context
    mainContext.getSlowestScanMetricsQueue().add(createScanMetricsHolder(mainTableName, 100L));

    // Create a sub-context (e.g., for a subquery) - this is a leaf node
    StatementContext subContext1 = createStatementContext(5);
    subContext1.getSlowestScanMetricsQueue().add(createScanMetricsHolder(subTableName, 200L));
    mainContext.addSubStatementContext(subContext1);

    List<List<JsonObject>> result = mainContext.getTopNSlowestScanMetrics();

    // Should have 1 result with the full path: main+sub (100+200=300)
    // The algorithm only returns paths to leaf nodes
    assertEquals(1, result.size());

    // Verify the path contains both scan metrics holders
    List<JsonObject> path = result.get(0);
    assertEquals(2, path.size());

    // Verify the metrics
    assertEquals(mainTableName, path.get(0).get("table").getAsString());
    assertEquals(100L, path.get(0).get("n").getAsLong());
    assertEquals(subTableName, path.get(1).get("table").getAsString());
    assertEquals(200L, path.get(1).get("n").getAsLong());
  }

  /**
   * Test with multiple levels of sub-contexts (nested subqueries).
   */
  @Test
  public void testWithNestedSubContexts() {
    StatementContext mainContext = createStatementContext(10);
    String mainTableName = "MAIN";
    String subTableName1 = "SUB1";
    String subTableName2 = "SUB2";

    // Top level statement context
    mainContext.getSlowestScanMetricsQueue().add(createScanMetricsHolder(mainTableName, 50L));

    // First level sub-contexts
    StatementContext subContext11 = createStatementContext(10);
    subContext11.getSlowestScanMetricsQueue().add(createScanMetricsHolder(subTableName1, 100L));
    StatementContext subContext12 = createStatementContext(10);
    subContext12.getSlowestScanMetricsQueue().add(createScanMetricsHolder(subTableName1, 200L));

    // Second level sub-context
    StatementContext subContext2 = createStatementContext(10);
    subContext2.getSlowestScanMetricsQueue().add(createScanMetricsHolder(subTableName2, 150L));

    subContext11.addSubStatementContext(subContext2);
    mainContext.addSubStatementContext(subContext11);
    mainContext.addSubStatementContext(subContext12);

    List<List<JsonObject>> result = mainContext.getTopNSlowestScanMetrics();

    // Should have multiple paths through the tree
    // The longest path would be: main(50) + sub1(100) + sub2(150) = 300ms
    assertEquals(2, result.size());

    // The slowest path should have all three scan metrics
    List<JsonObject> slowestPath = result.get(0);
    assertEquals(3, slowestPath.size());
    assertEquals(mainTableName, slowestPath.get(0).get("table").getAsString());
    assertEquals(50L, slowestPath.get(0).get("n").getAsLong());
    assertEquals(subTableName1, slowestPath.get(1).get("table").getAsString());
    assertEquals(100L, slowestPath.get(1).get("n").getAsLong());
    assertEquals(subTableName2, slowestPath.get(2).get("table").getAsString());
    assertEquals(150L, slowestPath.get(2).get("n").getAsLong());

    // The second slowest path should have two scan metrics
    List<JsonObject> secondSlowestPath = result.get(1);
    assertEquals(2, secondSlowestPath.size());
    assertEquals(mainTableName, secondSlowestPath.get(0).get("table").getAsString());
    assertEquals(50L, secondSlowestPath.get(0).get("n").getAsLong());
    assertEquals(subTableName1, secondSlowestPath.get(1).get("table").getAsString());
    assertEquals(200L, secondSlowestPath.get(1).get("n").getAsLong());
  }

  /**
   * Test with empty sub-context at in between level.
   */
  @Test
  public void testWithEmptySubContextAtInBetweenLevel() {
    StatementContext mainContext = createStatementContext(2);
    String mainTableName = "MAIN";
    String subTableName11 = "SUB11";
    String subTableName21 = "SUB21";
    String subTableName22 = "SUB22";

    // Main statement context
    mainContext.getSlowestScanMetricsQueue().add(createScanMetricsHolder(mainTableName, 100L));

    // First level sub-contexts
    StatementContext subContext11 = createStatementContext(2);
    subContext11.getSlowestScanMetricsQueue().add(createScanMetricsHolder(subTableName11, 200L));
    subContext11.getSlowestScanMetricsQueue().add(createScanMetricsHolder(subTableName11, 250L));
    // Empty sub-context at in between level
    StatementContext subContext12 = createStatementContext(2);

    // Second level sub-contexts
    StatementContext subContext21 = createStatementContext(2);
    subContext21.getSlowestScanMetricsQueue().add(createScanMetricsHolder(subTableName21, 150L));
    StatementContext subContext22 = createStatementContext(2);
    subContext22.getSlowestScanMetricsQueue().add(createScanMetricsHolder(subTableName22, 600L));

    mainContext.addSubStatementContext(subContext11);
    mainContext.addSubStatementContext(subContext12);
    subContext11.addSubStatementContext(subContext21);
    subContext12.addSubStatementContext(subContext22);

    List<List<JsonObject>> result = mainContext.getTopNSlowestScanMetrics();

    assertEquals(2, result.size());

    // Verify the results
    List<JsonObject> slowestPath = result.get(0);
    assertEquals(2, slowestPath.size());
    assertEquals(mainTableName, slowestPath.get(0).get("table").getAsString());
    assertEquals(100L, slowestPath.get(0).get("n").getAsLong());
    assertEquals(subTableName22, slowestPath.get(1).get("table").getAsString());
    assertEquals(600L, slowestPath.get(1).get("n").getAsLong());

    // The second slowest path should have three scan metrics
    List<JsonObject> secondSlowestPath = result.get(1);
    assertEquals(3, secondSlowestPath.size());
    assertEquals(mainTableName, secondSlowestPath.get(0).get("table").getAsString());
    assertEquals(100L, secondSlowestPath.get(0).get("n").getAsLong());
    assertEquals(subTableName11, secondSlowestPath.get(1).get("table").getAsString());
    assertEquals(250L, secondSlowestPath.get(1).get("n").getAsLong());
    assertEquals(subTableName21, secondSlowestPath.get(2).get("table").getAsString());
    assertEquals(150L, secondSlowestPath.get(2).get("n").getAsLong());
  }

  /**
   * Test with multiple ScanMetricsHolders having the same millisBetweenNexts, verifying that
   * eviction works correctly when the underlying TopNTreeMultiMap is full. The TopNTreeMultiMap
   * uses a reverse comparator (larger values sorted first), so it keeps the top N highest values
   * and evicts from the smallest values when full.
   */
  @Test
  public void testEvictionWithDuplicateMillisBetweenNexts() {
    // Create context with capacity for 5 scan metrics
    int topN = 5;
    StatementContext context = createStatementContext(topN);

    // Add scan metrics where two of them have the same millisBetweenNexts (200L)
    // With reverse comparator, these are sorted as: 500L, 400L, 300L, 200L, 200L, 100L
    // When capacity is 5, the smallest values (100L) will be at the "end" of the sorted map
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE1", 500L));
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE2", 400L));
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE3", 300L));
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE4", 200L)); // First with
                                                                                       // 200L
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE5", 200L)); // Second
                                                                                       // with
                                                                                       // 200L
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE6", 100L));

    // Verify we have 5 scan metrics (100L should already be rejected)
    List<List<JsonObject>> result = context.getTopNSlowestScanMetrics();
    assertEquals(5, result.size());

    // Count how many ScanMetricsHolders with 200L are present
    long count200L = result.stream().flatMap(List::stream)
      .filter(json -> Long.valueOf(200L).equals(json.get("n").getAsLong())).count();

    // Both 200L entries should still be there since 100L is the smallest
    assertEquals(2, count200L);

    // Now add a new ScanMetricsHolder with 250L which should cause eviction
    // Since 250L > 200L (smallest in map), and map is full, one of the 200L entries should be
    // evicted
    context.getSlowestScanMetricsQueue().add(createScanMetricsHolder("TABLE7", 250L));

    // Get the updated results
    result = context.getTopNSlowestScanMetrics();

    // Should still have 5 entries total
    assertEquals(5, result.size());

    // Verify the new ScanMetricsHolder with 250L is present
    boolean has250L = result.stream().flatMap(List::stream)
      .anyMatch(json -> Long.valueOf(250L).equals(json.get("n").getAsLong()));
    assertTrue(has250L);

    // Verify that only ONE of the two ScanMetricsHolders with 200L remains
    // (one should have been evicted as it was the last value of the smallest key)
    count200L = result.stream().flatMap(List::stream)
      .filter(json -> Long.valueOf(200L).equals(json.get("n").getAsLong())).count();
    assertEquals(1, count200L);

    // Verify the larger values are still present
    boolean has500L = result.stream().flatMap(List::stream)
      .anyMatch(json -> Long.valueOf(500L).equals(json.get("n").getAsLong()));
    assertTrue(has500L);

    boolean has400L = result.stream().flatMap(List::stream)
      .anyMatch(json -> Long.valueOf(400L).equals(json.get("n").getAsLong()));
    assertTrue(has400L);

    boolean has300L = result.stream().flatMap(List::stream)
      .anyMatch(json -> Long.valueOf(300L).equals(json.get("n").getAsLong()));
    assertTrue(has300L);

    // Verify all remaining entries have millisBetweenNexts >= 200L (the minimum)
    long minMillis = result.stream().flatMap(List::stream)
      .mapToLong(json -> json.get("n").getAsLong()).min().orElse(Long.MAX_VALUE);
    assertEquals(200L, minMillis);
  }

  /**
   * Test with empty slowest scan metrics queue.
   */
  @Test
  public void testWithEmptyQueue() {
    StatementContext context = createStatementContext(5);
    // Don't add any scan metrics

    List<List<JsonObject>> result = context.getTopNSlowestScanMetrics();

    // Should return empty result since the slowest scan metrics queue is empty
    assertEquals(1, result.size());
    assertEquals(0, result.get(0).size());
  }

  /**
   * Helper method to create a StatementContext with a specific slowestScanMetricsCount.
   */
  private StatementContext createStatementContext(int slowestScanMetricsCount) {
    // Mock PhoenixConnection
    PhoenixConnection mockConnection = mock(PhoenixConnection.class);
    ConnectionQueryServices mockQueryServices = mock(ConnectionQueryServices.class);

    // Set up the configuration properties
    Map<String, String> props = new HashMap<>();
    props.put(QueryServices.SLOWEST_SCAN_METRICS_COUNT, String.valueOf(slowestScanMetricsCount));
    ReadOnlyProps readOnlyProps = new ReadOnlyProps(props);

    when(mockConnection.getQueryServices()).thenReturn(mockQueryServices);
    when(mockQueryServices.getProps()).thenReturn(readOnlyProps);
    when(mockConnection.getSlowestScanMetricsCount()).thenReturn(slowestScanMetricsCount);
    when(mockConnection.isRequestLevelMetricsEnabled()).thenReturn(true);
    when(mockConnection.getLogLevel()).thenReturn(LogLevel.INFO);

    // Mock PhoenixStatement
    PhoenixStatement mockStatement = mock(PhoenixStatement.class);
    when(mockStatement.getConnection()).thenReturn(mockConnection);
    when(mockStatement.getParameters()).thenReturn(Collections.emptyList());

    // Create StatementContext
    return new StatementContext(mockStatement, new Scan());
  }

  /**
   * Helper method to create a ScanMetricsHolder with a specific table name and millis between
   * nexts.
   */
  private ScanMetricsHolder createScanMetricsHolder(String tableName, long millisBetweenNexts) {
    ReadMetricQueue readMetricQueue = new ReadMetricQueue(true, LogLevel.DEBUG);
    ScanMetricsHolder holder =
      ScanMetricsHolder.getInstance(readMetricQueue, tableName, new Scan(), LogLevel.DEBUG);
    Map<String, Long> scanMetrics = new HashMap<>();
    scanMetrics.put(ScanMetrics.MILLIS_BETWEEN_NEXTS_METRIC_NAME, millisBetweenNexts);
    holder.setScanMetricMap(scanMetrics);
    return holder;
  }
}
