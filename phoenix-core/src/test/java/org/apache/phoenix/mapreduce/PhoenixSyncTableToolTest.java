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
package org.apache.phoenix.mapreduce;

import static org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil.INVALID_TIME_RANGE_EXCEPTION_MESSAGE;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * Unit tests for PhoenixSyncTableTool command-line parsing and validation.
 */
public class PhoenixSyncTableToolTest extends BaseTest {

  PhoenixSyncTableTool tool;
  private String tableName;
  private String targetCluster;
  private String schema;
  private String tenantId;

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Before
  public void setup() {
    tool = new PhoenixSyncTableTool();
    Configuration conf = HBaseConfiguration.create();
    tool.setConf(conf);
    tool.initializeConfiguration();
    schema = generateUniqueName();
    tableName = generateUniqueName();
    targetCluster = "target-zk1,target-zk2:2181:/hbase";
    tenantId = generateUniqueName();
  }

  @Test
  public void testParseOptionsTimeRangeBothTimesProvided() throws Exception {
    Long startTime = 10L;
    Long endTime = 15L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
  }

  @Test
  public void testParseOptionsTimeRangeBothTimesNull() throws Exception {
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, null, null);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(Long.valueOf(0L), tool.getStartTime());
    // endTime should default to current time - 1 hour
    Long endTime = tool.getEndTime();
    assertNotNull(endTime);
    long expectedEndTime = EnvironmentEdgeManager.currentTimeMillis() - (60 * 60 * 1000);
    assertTrue("End time should be close to (current time - 1 hour)",
      Math.abs(endTime - expectedEndTime) < 10000);
  }

  @Test
  public void testParseOptionsTimeRangeStartTimeOnlyProvided() throws Exception {
    Long startTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, null);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    // endTime should default to current time - 1 hour
    Long endTime = tool.getEndTime();
    assertNotNull(endTime);
    long expectedEndTime = EnvironmentEdgeManager.currentTimeMillis() - (60 * 60 * 1000);
    assertTrue("End time should be close to (current time - 1 hour)",
      Math.abs(endTime - expectedEndTime) < 10000);
  }

  @Test
  public void testParseOptionsTimeRangeEndTimeOnlyProvided() throws Exception {
    Long endTime = 15L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, null, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(Long.valueOf(0L), tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
  }

  @Test
  public void testParseOptionsTimeRangeStartTimeInFuture() throws Exception {
    Long startTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
    Long endTime = EnvironmentEdgeManager.currentTimeMillis() + 200000;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
    tool.populateSyncTableToolAttributes(cmdLine);
  }

  @Test
  public void testParseOptionsTimeRangeEndTimeInFuture() throws Exception {
    Long startTime = EnvironmentEdgeManager.currentTimeMillis();
    Long endTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
    tool.populateSyncTableToolAttributes(cmdLine);
  }

  @Test
  public void testParseOptionsTimeRangeStartTimeNullEndTimeInFuture() throws Exception {
    Long endTime = EnvironmentEdgeManager.currentTimeMillis() + 100000;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, null, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
    tool.populateSyncTableToolAttributes(cmdLine);
  }

  @Test
  public void testParseOptionsTimeRangeStartTimeEqualEndTime() throws Exception {
    Long startTime = 10L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
    tool.populateSyncTableToolAttributes(cmdLine);
  }

  @Test
  public void testParseOptionsTimeRangeStartTimeGreaterThanEndTime() throws Exception {
    Long startTime = 15L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage(INVALID_TIME_RANGE_EXCEPTION_MESSAGE);
    tool.populateSyncTableToolAttributes(cmdLine);
  }

  @Test
  public void testParseOptionsWithSchema() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertEquals(schema, tool.getSchemaName());
  }

  @Test
  public void testParseOptionsWithoutSchema() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(null, tableName, targetCluster, tenantId, startTime, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertNull(tool.getSchemaName());
  }

  @Test
  public void testParseOptionsWithTenantId() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertEquals(tenantId, tool.getTenantId());
  }

  @Test
  public void testParseOptionsWithoutTenantId() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, null, startTime, endTime);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertNull(tool.getTenantId());
  }

  @Test
  public void testParseOptionsWithCustomChunkSize() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    Long chunkSize = 1048576L; // 1MB
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      chunkSize, false, false);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertEquals(chunkSize, tool.getChunkSizeBytes());
  }

  @Test
  public void testParseOptionsWithoutChunkSize() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      null, false, false);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    // Tool should use default chunk size (1GB)
    assertNull(tool.getChunkSizeBytes());
  }

  @Test
  public void testParseOptionsDryRunEnabled() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      null, true, false);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertTrue(tool.isDryRun());
  }

  @Test
  public void testParseOptionsDryRunDisabled() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      null, false, false);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertFalse(tool.isDryRun());
  }

  @Test
  public void testParseOptionsRunForeground() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      null, false, true);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertTrue(tool.isForeground());
  }

  @Test
  public void testParseOptionsRunBackground() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      null, false, false);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertFalse(tool.isForeground());
  }

  @Test
  public void testParseOptionsMissingTableName() throws Exception {
    String[] args = new String[] { "--target-cluster", targetCluster };
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("table-name is a mandatory parameter");
    tool.parseOptions(args);
  }

  @Test
  public void testParseOptionsMissingTargetCluster() throws Exception {
    String[] args = new String[] { "--table-name", tableName };
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("target-cluster is a mandatory parameter");
    tool.parseOptions(args);
  }

  @Test
  public void testDefaultTimeoutConfigurationValues() {
    // Verify that default timeout configuration keys exist and can be retrieved
    Configuration conf = HBaseConfiguration.create();

    // Test that we can retrieve default values from configuration
    long queryTimeout = conf.getLong(QueryServices.SYNC_TABLE_QUERY_TIMEOUT_ATTRIB,
      QueryServicesOptions.DEFAULT_SYNC_TABLE_QUERY_TIMEOUT);
    long rpcTimeout = conf.getLong(QueryServices.SYNC_TABLE_RPC_TIMEOUT_ATTRIB,
      QueryServicesOptions.DEFAULT_SYNC_TABLE_RPC_TIMEOUT);
    long scannerTimeout = conf.getLong(QueryServices.SYNC_TABLE_CLIENT_SCANNER_TIMEOUT_ATTRIB,
      QueryServicesOptions.DEFAULT_SYNC_TABLE_CLIENT_SCANNER_TIMEOUT);
    int rpcRetries = conf.getInt(QueryServices.SYNC_TABLE_RPC_RETRIES_COUNTER,
      QueryServicesOptions.DEFAULT_SYNC_TABLE_RPC_RETRIES_COUNTER);

    // When no custom values are set, configuration returns the defaults
    assertEquals("Query timeout should return default when not configured",
      QueryServicesOptions.DEFAULT_SYNC_TABLE_QUERY_TIMEOUT, queryTimeout);
    assertEquals("RPC timeout should return default when not configured",
      QueryServicesOptions.DEFAULT_SYNC_TABLE_RPC_TIMEOUT, rpcTimeout);
    assertEquals("Scanner timeout should return default when not configured",
      QueryServicesOptions.DEFAULT_SYNC_TABLE_CLIENT_SCANNER_TIMEOUT, scannerTimeout);
    assertEquals("RPC retries should return default when not configured",
      QueryServicesOptions.DEFAULT_SYNC_TABLE_RPC_RETRIES_COUNTER, rpcRetries);
  }

  @Test
  public void testCustomTimeoutConfigurationCanBeSet() {
    // Verify that custom timeout values can be set in configuration
    Configuration conf = HBaseConfiguration.create();
    long customQueryTimeout = 1200000L; // 20 minutes
    long customRpcTimeout = 120000L; // 2 minutes
    long customScannerTimeout = 360000L; // 6 minutes
    int customRpcRetries = 10;

    // Set custom values
    conf.setLong(QueryServices.SYNC_TABLE_QUERY_TIMEOUT_ATTRIB, customQueryTimeout);
    conf.setLong(QueryServices.SYNC_TABLE_RPC_TIMEOUT_ATTRIB, customRpcTimeout);
    conf.setLong(QueryServices.SYNC_TABLE_CLIENT_SCANNER_TIMEOUT_ATTRIB, customScannerTimeout);
    conf.setInt(QueryServices.SYNC_TABLE_RPC_RETRIES_COUNTER, customRpcRetries);

    // Verify custom values can be retrieved
    assertEquals("Should retrieve custom query timeout", customQueryTimeout,
      conf.getLong(QueryServices.SYNC_TABLE_QUERY_TIMEOUT_ATTRIB, -1));
    assertEquals("Should retrieve custom RPC timeout", customRpcTimeout,
      conf.getLong(QueryServices.SYNC_TABLE_RPC_TIMEOUT_ATTRIB, -1));
    assertEquals("Should retrieve custom scanner timeout", customScannerTimeout,
      conf.getLong(QueryServices.SYNC_TABLE_CLIENT_SCANNER_TIMEOUT_ATTRIB, -1));
    assertEquals("Should retrieve custom RPC retries", customRpcRetries,
      conf.getInt(QueryServices.SYNC_TABLE_RPC_RETRIES_COUNTER, -1));
  }

  @Test
  public void testParseOptionsWithNegativeChunkSize() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    Long negativeChunkSize = -1048576L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      negativeChunkSize, false, false);
    CommandLine cmdLine = tool.parseOptions(args);
    exceptionRule.expect(IllegalArgumentException.class);
    exceptionRule.expectMessage("Chunk size must be a positive value");
    tool.populateSyncTableToolAttributes(cmdLine);
  }

  @Test
  public void testParseOptionsWithBothMandatoryOptionsMissing() throws Exception {
    String[] args = new String[] {};
    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("table-name is a mandatory parameter");
    tool.parseOptions(args);
  }

  @Test
  public void testParseOptionsEnableRawScanEnabled() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      null, false, false, true, false);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertTrue("Raw scan should be enabled when flag is present", tool.isRawScan());
  }

  @Test
  public void testParseOptionsEnableRawScanDisabled() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      null, false, false, false, false);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertFalse("Raw scan should be disabled by default when flag is absent", tool.isRawScan());
  }

  @Test
  public void testParseOptionsEnableReadAllVersionsEnabled() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      null, false, false, false, true);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertTrue("Read all versions should be enabled when flag is present",
      tool.isReadAllVersions());
  }

  @Test
  public void testParseOptionsEnableReadAllVersionsDisabled() throws Exception {
    Long startTime = 1L;
    Long endTime = 10L;
    String[] args = getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime,
      null, false, false, false, false);
    CommandLine cmdLine = tool.parseOptions(args);
    tool.populateSyncTableToolAttributes(cmdLine);
    assertEquals(startTime, tool.getStartTime());
    assertEquals(endTime, tool.getEndTime());
    assertFalse("Read all versions should be disabled by default when flag is absent",
      tool.isReadAllVersions());
  }

  @Test
  public void testSyncTableConfiguration() {
    final Configuration conf = new Configuration();

    // Test table name
    String testTableName = "TEST_SYNC_TABLE";
    PhoenixSyncTableTool.setPhoenixSyncTableName(conf, testTableName);
    assertEquals("Should retrieve set table name", testTableName,
      PhoenixSyncTableTool.getPhoenixSyncTableName(conf));

    // Test target ZK quorum
    String testZkQuorum = "testhost1:2181,testhost2:2181:/hbase";
    PhoenixSyncTableTool.setPhoenixSyncTableTargetZkQuorum(conf, testZkQuorum);
    assertEquals("Should retrieve set target ZK quorum", testZkQuorum,
      PhoenixSyncTableTool.getPhoenixSyncTableTargetZkQuorum(conf));

    // Test from time
    Long testFromTime = 1000000L;
    PhoenixSyncTableTool.setPhoenixSyncTableFromTime(conf, testFromTime);
    assertEquals("Should retrieve set from time", testFromTime,
      PhoenixSyncTableTool.getPhoenixSyncTableFromTime(conf));

    // Test to time
    long testToTime = 2000000L;
    PhoenixSyncTableTool.setPhoenixSyncTableToTime(conf, testToTime);
    assertEquals("Should retrieve set to time", testToTime,
      PhoenixSyncTableTool.getPhoenixSyncTableToTime(conf));

    // Test dry run flag
    PhoenixSyncTableTool.setPhoenixSyncTableDryRun(conf, true);
    assertTrue("Should retrieve dry run as true",
      PhoenixSyncTableTool.getPhoenixSyncTableDryRun(conf));

    PhoenixSyncTableTool.setPhoenixSyncTableDryRun(conf, false);
    assertFalse("Should retrieve dry run as false",
      PhoenixSyncTableTool.getPhoenixSyncTableDryRun(conf));

    // Test chunk size bytes
    Long testChunkSize = 5000000L; // 5MB
    PhoenixSyncTableTool.setPhoenixSyncTableChunkSizeBytes(conf, testChunkSize);
    assertEquals("Should retrieve set chunk size", testChunkSize.longValue(),
      PhoenixSyncTableTool.getPhoenixSyncTableChunkSizeBytes(conf));

    // Test default chunk size (1GB)
    final Configuration freshConf = new Configuration();
    long defaultChunkSize = 1024L * 1024L * 1024L; // 1GB
    assertEquals("Should return default chunk size when not set", defaultChunkSize,
      PhoenixSyncTableTool.getPhoenixSyncTableChunkSizeBytes(freshConf));

    // Test raw scan flag
    PhoenixSyncTableTool.setPhoenixSyncTableRawScan(conf, true);
    assertTrue("Should retrieve raw scan as true",
      PhoenixSyncTableTool.getPhoenixSyncTableRawScan(conf));

    PhoenixSyncTableTool.setPhoenixSyncTableRawScan(conf, false);
    assertFalse("Should retrieve raw scan as false",
      PhoenixSyncTableTool.getPhoenixSyncTableRawScan(conf));

    // Test default raw scan (should be false - disabled by default)
    final Configuration rawScanConf = new Configuration();
    assertFalse("Should return default raw scan as false when not set",
      PhoenixSyncTableTool.getPhoenixSyncTableRawScan(rawScanConf));

    // Test read all versions flag
    PhoenixSyncTableTool.setPhoenixSyncTableReadAllVersions(conf, true);
    assertTrue("Should retrieve read all versions as true",
      PhoenixSyncTableTool.getPhoenixSyncTableReadAllVersions(conf));

    PhoenixSyncTableTool.setPhoenixSyncTableReadAllVersions(conf, false);
    assertFalse("Should retrieve read all versions as false",
      PhoenixSyncTableTool.getPhoenixSyncTableReadAllVersions(conf));

    // Test default read all versions (should be false - disabled by default)
    final Configuration readAllVersionsConf = new Configuration();
    assertFalse("Should return default read all versions as false when not set",
      PhoenixSyncTableTool.getPhoenixSyncTableReadAllVersions(readAllVersionsConf));
  }

  /**
   * Creates argument array for PhoenixSyncTableTool
   */
  private static String[] getArgValues(String schema, String tableName, String targetCluster,
    String tenantId, Long startTime, Long endTime) {
    return getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime, null, false,
      false, false, false);
  }

  /**
   * Creates argument array with all optional parameters
   */
  private static String[] getArgValues(String schema, String tableName, String targetCluster,
    String tenantId, Long startTime, Long endTime, Long chunkSize, boolean dryRun,
    boolean runForeground) {
    return getArgValues(schema, tableName, targetCluster, tenantId, startTime, endTime, chunkSize,
      dryRun, runForeground, false, false);
  }

  /**
   * Creates argument array with all optional parameters including raw scan and read all versions
   */
  private static String[] getArgValues(String schema, String tableName, String targetCluster,
    String tenantId, Long startTime, Long endTime, Long chunkSize, boolean dryRun,
    boolean runForeground, boolean enableRawScan, boolean enableReadAllVersions) {
    List<String> args = new ArrayList<>();

    if (schema != null) {
      args.add("--schema");
      args.add(schema);
    }

    args.add("--table-name");
    args.add(tableName);

    args.add("--target-cluster");
    args.add(targetCluster);

    if (tenantId != null) {
      args.add("--tenant-id");
      args.add(tenantId);
    }

    if (startTime != null) {
      args.add("--from-time");
      args.add(String.valueOf(startTime));
    }

    if (endTime != null) {
      args.add("--to-time");
      args.add(String.valueOf(endTime));
    }

    if (chunkSize != null) {
      args.add("--chunk-size");
      args.add(String.valueOf(chunkSize));
    }

    if (dryRun) {
      args.add("--dry-run");
    }

    if (runForeground) {
      args.add("--run-foreground");
    }

    if (enableRawScan) {
      args.add("--raw-scan");
    }

    if (enableReadAllVersions) {
      args.add("--read-all-versions");
    }

    return args.toArray(new String[0]);
  }
}
