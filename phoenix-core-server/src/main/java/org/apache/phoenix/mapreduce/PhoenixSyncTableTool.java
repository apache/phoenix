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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.coprocessor.PhoenixSyncTableRegionScanner;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixMRJobUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.ParseException;

/**
 * A MapReduce tool for verifying and detecting data inconsistencies between Phoenix tables across
 * two HBase clusters (source and target).
 * <h2>Use Case</h2> This tool is designed for replication/migration verification scenarios where
 * data is replicated from a source Phoenix cluster to a target cluster. It efficiently detects
 * which data chunks are out of sync without transferring all the data over the network.
 * <h2>How It Works</h2>
 * <ol>
 * <li><b>Job Setup:</b> The tool creates a MapReduce job that partitions the table into mapper
 * regions based on HBase region boundaries.</li>
 * <li><b>Server-Side Chunking:</b> Each mapper triggers a coprocessor scan on both source and
 * target clusters. The {@link PhoenixSyncTableRegionScanner} coprocessor accumulates rows into
 * chunks (configurable size, default 1GB) and computes an SHA-256 hash of all row data (keys +
 * column families + qualifiers + timestamps + values).</li>
 * <li><b>Hash Comparison:</b> The {@link PhoenixSyncTableMapper} receives chunk metadata (start
 * key, end key, row count, hash) from both clusters and compares the hashes. Matching hashes mean
 * the chunk data is identical; mismatched hashes indicate inconsistency.</li>
 * <li><b>Result Tracking:</b> Results are check pointed to the {@code PHOENIX_SYNC_TABLE_OUTPUT}
 * table, tracking verified chunks, mismatched chunks, and processing progress for resumable
 * operations.</li>
 * </ol>
 * <h2>Usage Example</h2>
 *
 * <pre>
 * hbase org.apache.phoenix.mapreduce.PhoenixSyncTableTool \ --table-name MY_TABLE \
 * --target-cluster target-zk1,target-zk2:2181:/hbase
 * </pre>
 */
public class PhoenixSyncTableTool extends Configured implements Tool {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixSyncTableTool.class);

  private static final Option SCHEMA_NAME_OPTION =
    new Option("s", "schema", true, "Phoenix schema name (optional)");
  private static final Option TABLE_NAME_OPTION =
    new Option("tn", "table-name", true, "Table name (mandatory)");
  private static final Option TARGET_CLUSTER_OPTION =
    new Option("tc", "target-cluster", true, "Target cluster ZooKeeper quorum (mandatory)");
  private static final Option FROM_TIME_OPTION = new Option("ft", "from-time", true,
    "Start time in milliseconds for sync (optional, defaults to 0)");
  private static final Option TO_TIME_OPTION = new Option("tt", "to-time", true,
    "End time in milliseconds for sync (optional, defaults to current time - 1 hour)");
  private static final Option DRY_RUN_OPTION = new Option("dr", "dry-run", false,
    "Dry run mode - only checkpoint inconsistencies, do not repair (optional)");
  private static final Option CHUNK_SIZE_OPTION =
    new Option("cs", "chunk-size", true, "Chunk size in bytes (optional, defaults to 1GB)");
  private static final Option RUN_FOREGROUND_OPTION = new Option("runfg", "run-foreground", false,
    "Run the job in foreground. Default - Runs the job in background.");
  private static final Option TENANT_ID_OPTION =
    new Option("tenant", "tenant-id", true, "Tenant ID for tenant-specific table sync (optional)");
  private static final Option RAW_SCAN_OPTION = new Option("rs", "raw-scan", false,
    "Enable raw scan mode to include delete markers (optional, disabled by default)");
  private static final Option READ_ALL_VERSIONS_OPTION = new Option("rav", "read-all-versions",
    false,
    "Enable reading all cell versions (optional, disabled by default, reads only latest version)");
  private static final Option HELP_OPTION = new Option("h", "help", false, "Help");

  public static final String PHOENIX_SYNC_TABLE_NAME = "phoenix.sync.table.table.name";
  public static final String PHOENIX_SYNC_TABLE_TARGET_ZK_QUORUM = "phoenix.sync.table.target.zk";
  public static final String PHOENIX_SYNC_TABLE_FROM_TIME = "phoenix.sync.table.from.time";
  public static final String PHOENIX_SYNC_TABLE_TO_TIME = "phoenix.sync.table.to.time";
  public static final String PHOENIX_SYNC_TABLE_DRY_RUN = "phoenix.sync.table.dry.run";
  public static final String PHOENIX_SYNC_TABLE_CHUNK_SIZE_BYTES =
    "phoenix.sync.table.chunk.size.bytes";
  public static final long DEFAULT_PHOENIX_SYNC_TABLE_CHUNK_SIZE_BYTES = 1024 * 1024 * 1024; // 1GB
  public static final String PHOENIX_SYNC_TABLE_RAW_SCAN = "phoenix.sync.table.raw.scan";
  public static final String PHOENIX_SYNC_TABLE_READ_ALL_VERSIONS =
    "phoenix.sync.table.read.all.versions";

  private String schemaName;
  private String tableName;
  private String targetZkQuorum;
  private Long startTime;
  private Long endTime;
  private boolean isDryRun;
  private Long chunkSizeBytes;
  private boolean isForeground;
  private String tenantId;
  private boolean isRawScan = false;
  private boolean isReadAllVersions = false;

  private String qTable;
  private String qSchemaName;

  private Configuration configuration;
  private Job job;

  /**
   * Creates an MR job that uses server-side chunking and checksum calculation
   * @return Configured MapReduce job ready for submission
   * @throws Exception if job creation fails
   */
  private Job configureAndCreatePhoenixSyncTableJob(PTableType tableType) throws Exception {
    configureTimeoutsAndRetries(configuration);
    setPhoenixSyncTableToolConfiguration(configuration);
    PhoenixMRJobUtil.updateCapacityQueueInfo(configuration);
    Job job = Job.getInstance(configuration, getJobName());
    job.setMapperClass(PhoenixSyncTableMapper.class);
    job.setJarByClass(PhoenixSyncTableTool.class);
    TableMapReduceUtil.initCredentials(job);
    TableMapReduceUtil.addDependencyJars(job);
    configureInput(job, tableType);
    configureOutput(job);
    obtainTargetClusterTokens(job);
    return job;
  }

  /**
   * Obtains HBase delegation tokens from the target cluster and adds them to the job. This is
   * required for cross-cluster kerberos authentication.
   * @param job The MapReduce job to add tokens
   */
  private void obtainTargetClusterTokens(Job job) throws IOException {
    try {
      Configuration targetConf =
        HBaseConfiguration.createClusterConf(job.getConfiguration(), targetZkQuorum);
      TableMapReduceUtil.initCredentialsForCluster(job, targetConf);
    } catch (IOException e) {
      if (UserProvider.instantiate(job.getConfiguration()).isHBaseSecurityEnabled()) {
        throw e;
      }
      LOGGER.info("Skipping target cluster token acquisition (security not enabled): {}",
        e.getMessage());
    }
  }

  /**
   * Configures timeouts and retry settings for the sync job
   */
  private void configureTimeoutsAndRetries(Configuration configuration) {
    long syncTableQueryTimeoutMs =
      configuration.getLong(QueryServices.SYNC_TABLE_QUERY_TIMEOUT_ATTRIB,
        QueryServicesOptions.DEFAULT_SYNC_TABLE_QUERY_TIMEOUT);
    long syncTableRPCTimeoutMs = configuration.getLong(QueryServices.SYNC_TABLE_RPC_TIMEOUT_ATTRIB,
      QueryServicesOptions.DEFAULT_SYNC_TABLE_RPC_TIMEOUT);
    long syncTableClientScannerTimeoutMs =
      configuration.getLong(QueryServices.SYNC_TABLE_CLIENT_SCANNER_TIMEOUT_ATTRIB,
        QueryServicesOptions.DEFAULT_SYNC_TABLE_CLIENT_SCANNER_TIMEOUT);
    int syncTableRpcRetriesCounter =
      configuration.getInt(QueryServices.SYNC_TABLE_RPC_RETRIES_COUNTER,
        QueryServicesOptions.DEFAULT_SYNC_TABLE_RPC_RETRIES_COUNTER);

    configuration.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      Long.toString(syncTableClientScannerTimeoutMs));
    configuration.set(HConstants.HBASE_RPC_TIMEOUT_KEY, Long.toString(syncTableRPCTimeoutMs));
    configuration.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      Integer.toString(syncTableRpcRetriesCounter));
    configuration.set(MRJobConfig.TASK_TIMEOUT, Long.toString(syncTableQueryTimeoutMs));
  }

  private void setPhoenixSyncTableToolConfiguration(Configuration configuration) {
    setPhoenixSyncTableName(configuration, qTable);
    setPhoenixSyncTableTargetZkQuorum(configuration, targetZkQuorum);
    setPhoenixSyncTableFromTime(configuration, startTime);
    setPhoenixSyncTableToTime(configuration, endTime);
    setPhoenixSyncTableDryRun(configuration, isDryRun);
    setPhoenixSyncTableRawScan(configuration, isRawScan);
    setPhoenixSyncTableReadAllVersions(configuration, isReadAllVersions);
    PhoenixConfigurationUtil.setSplitByStats(configuration, false);
    if (chunkSizeBytes != null) {
      setPhoenixSyncTableChunkSizeBytes(configuration, chunkSizeBytes);
    }
    if (tenantId != null) {
      PhoenixConfigurationUtil.setTenantId(configuration, tenantId);
    }
    PhoenixConfigurationUtil.setCurrentScnValue(configuration, endTime);
    configuration
      .setBooleanIfUnset(PhoenixConfigurationUtil.MAPREDUCE_RANDOMIZE_MAPPER_EXECUTION_ORDER, true);
  }

  private void configureInput(Job job, PTableType tableType) {
    // With below query plan, we get Input split based on region boundary
    String hint = (tableType == PTableType.INDEX) ? "" : "/*+ NO_INDEX */ ";
    String selectStatement = "SELECT " + hint + "1 FROM " + qTable;
    PhoenixMapReduceUtil.setInput(job, DBInputFormat.NullDBWritable.class,
      PhoenixSyncTableInputFormat.class, qTable, selectStatement);
  }

  private void configureOutput(Job job) {
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);
  }

  private String getJobName() {
    StringBuilder jobName = new StringBuilder("PhoenixSyncTableTool");
    if (qSchemaName != null) {
      jobName.append("-").append(qSchemaName);
    }
    jobName.append("-").append(tableName);
    jobName.append("-").append(System.currentTimeMillis());
    return jobName.toString();
  }

  public CommandLine parseOptions(String[] args) throws IllegalStateException {
    Options options = getOptions();
    CommandLineParser parser = DefaultParser.builder().setAllowPartialMatching(false)
      .setStripLeadingAndTrailingQuotes(false).build();
    CommandLine cmdLine = null;
    try {
      cmdLine = parser.parse(options, args);
    } catch (ParseException e) {
      LOGGER.error("Failed to parse command line options. Args: {}. Error: {}",
        Arrays.toString(args), e.getMessage(), e);
      printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
    }

    if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
      printHelpAndExit(options, 0);
    }
    requireOption(cmdLine, TABLE_NAME_OPTION);
    requireOption(cmdLine, TARGET_CLUSTER_OPTION);
    return cmdLine;
  }

  private void requireOption(CommandLine cmdLine, Option option) {
    if (!cmdLine.hasOption(option.getOpt())) {
      throw new IllegalStateException(option.getLongOpt() + " is a mandatory parameter");
    }
  }

  private Options getOptions() {
    Options options = new Options();
    options.addOption(SCHEMA_NAME_OPTION);
    options.addOption(TABLE_NAME_OPTION);
    options.addOption(TARGET_CLUSTER_OPTION);
    options.addOption(FROM_TIME_OPTION);
    options.addOption(TO_TIME_OPTION);
    options.addOption(DRY_RUN_OPTION);
    options.addOption(CHUNK_SIZE_OPTION);
    options.addOption(RUN_FOREGROUND_OPTION);
    options.addOption(TENANT_ID_OPTION);
    options.addOption(RAW_SCAN_OPTION);
    options.addOption(READ_ALL_VERSIONS_OPTION);
    options.addOption(HELP_OPTION);
    return options;
  }

  private void printHelpAndExit(String errorMessage, Options options) {
    System.err.println(errorMessage);
    printHelpAndExit(options, -1);
  }

  private void printHelpAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "/hbase/bin/hbase " + PhoenixSyncTableTool.class.getName();
    formatter.printHelp(cmdLineSyntax,
      "Synchronize a Phoenix table between source and target clusters", options,
      "\nExample:\n" + cmdLineSyntax + " \\\n" + "  --table-name MY_TABLE \\\n"
        + "  --target-cluster <zk_quorum>:2181 \\\n" + "  --dry-run\n",
      true);
    System.exit(exitCode);
  }

  public void populateSyncTableToolAttributes(CommandLine cmdLine) {
    tableName = cmdLine.getOptionValue(TABLE_NAME_OPTION.getOpt());
    targetZkQuorum = cmdLine.getOptionValue(TARGET_CLUSTER_OPTION.getOpt());
    schemaName = cmdLine.getOptionValue(SCHEMA_NAME_OPTION.getOpt());

    if (cmdLine.hasOption(FROM_TIME_OPTION.getOpt())) {
      startTime = Long.valueOf(cmdLine.getOptionValue(FROM_TIME_OPTION.getOpt()));
    } else {
      startTime = 0L;
    }

    if (cmdLine.hasOption(TO_TIME_OPTION.getOpt())) {
      endTime = Long.valueOf(cmdLine.getOptionValue(TO_TIME_OPTION.getOpt()));
    } else {
      // Default endTime, current time - 1 hour
      endTime = EnvironmentEdgeManager.currentTimeMillis() - (60 * 60 * 1000);
    }

    if (cmdLine.hasOption(CHUNK_SIZE_OPTION.getOpt())) {
      chunkSizeBytes = Long.valueOf(cmdLine.getOptionValue(CHUNK_SIZE_OPTION.getOpt()));
      if (chunkSizeBytes <= 0) {
        throw new IllegalArgumentException(
          "Chunk size must be a positive value, got: " + chunkSizeBytes);
      }
    }
    if (cmdLine.hasOption(TENANT_ID_OPTION.getOpt())) {
      tenantId = cmdLine.getOptionValue(TENANT_ID_OPTION.getOpt());
    }
    isDryRun = cmdLine.hasOption(DRY_RUN_OPTION.getOpt());
    isForeground = cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt());
    isRawScan = cmdLine.hasOption(RAW_SCAN_OPTION.getOpt());
    isReadAllVersions = cmdLine.hasOption(READ_ALL_VERSIONS_OPTION.getOpt());
    qTable = SchemaUtil.getQualifiedTableName(schemaName, tableName);
    qSchemaName = SchemaUtil.normalizeIdentifier(schemaName);
    PhoenixMapReduceUtil.validateTimeRange(startTime, endTime, qTable);
    LOGGER.info(
      "PhoenixSyncTableTool configured - Table: {}, Schema: {}, Target: {}, "
        + "StartTime: {}, EndTime: {}, DryRun: {}, ChunkSize: {}, Foreground: {}, TenantId: {}, "
        + "RawScan: {}, ReadAllVersions: {}",
      qTable, qSchemaName, targetZkQuorum, startTime, endTime, isDryRun, chunkSizeBytes,
      isForeground, tenantId, isRawScan, isReadAllVersions);
  }

  /**
   * Creates or verifies the Phoenix sync tool checkpoint table
   */
  private void createSyncOutputTable(Connection connection) throws SQLException {
    PhoenixSyncTableOutputRepository repository = new PhoenixSyncTableOutputRepository(connection);
    repository.createSyncCheckpointTableIfNotExists();
  }

  /**
   * Sets up the table reference and validates it exists and is suitable for sync operations.
   * Validates that the table is not a VIEW or INDEX
   */
  private PTableType validateAndGetTableType() throws SQLException {
    Properties props = new Properties();
    if (tenantId != null) {
      props.setProperty("TenantId", tenantId);
    }
    try (Connection connection = ConnectionUtil.getInputConnection(configuration, props)) {
      PTable pTable = PhoenixMapReduceUtil.validateTableForMRJob(connection, qTable, false, true);
      return pTable.getType();
    }
  }

  private boolean submitPhoenixSyncTableJob() throws Exception {
    if (!isForeground) {
      job.submit();
      LOGGER.info("PhoenixSyncTable Job :{} submitted successfully in background for table {} ",
        job.getJobName(), qTable);
      return true;
    }
    LOGGER.info("Running PhoenixSyncTable job: {} for table:{} in foreground.", job.getJobName(),
      qTable);
    boolean success = job.waitForCompletion(true);
    if (success) {
      LOGGER.info("PhoenixSyncTable job: {} completed for table {}", job.getJobName(), qTable);
    } else {
      LOGGER.error("PhoenixSyncTable job {} failed for table {} to target cluster {}",
        job.getJobName(), qTable, targetZkQuorum);
    }
    Counters counters = job.getCounters();
    if (counters != null) {
      long verifiedMappers =
        counters.findCounter(PhoenixSyncTableMapper.SyncCounters.MAPPERS_VERIFIED).getValue();
      long mismatchedMappers =
        counters.findCounter(PhoenixSyncTableMapper.SyncCounters.MAPPERS_MISMATCHED).getValue();
      long chunksVerified =
        counters.findCounter(PhoenixSyncTableMapper.SyncCounters.CHUNKS_VERIFIED).getValue();
      long chunksMismatched =
        counters.findCounter(PhoenixSyncTableMapper.SyncCounters.CHUNKS_MISMATCHED).getValue();
      long sourceRowsProcessed =
        counters.findCounter(PhoenixSyncTableMapper.SyncCounters.SOURCE_ROWS_PROCESSED).getValue();
      long targetRowsProcessed =
        counters.findCounter(PhoenixSyncTableMapper.SyncCounters.TARGET_ROWS_PROCESSED).getValue();
      LOGGER.info(
        "PhoenixSyncTable job completed, gathered counters are \n Verified Mappers: {}, \n"
          + "Mismatched Mappers: {}, \n Chunks Verified: {}, \n"
          + "Chunks Mismatched: {}, \n Source Rows Processed: {}, \n Target Rows Processed: {}",
        verifiedMappers, mismatchedMappers, chunksVerified, chunksMismatched, sourceRowsProcessed,
        targetRowsProcessed);
    } else {
      LOGGER.warn("Unable to retrieve job counters for table {} - job may have failed "
        + "during initialization", qTable);
    }
    return success;
  }

  @Override
  public int run(String[] args) throws Exception {
    CommandLine cmdLine;
    try {
      cmdLine = parseOptions(args);
    } catch (IllegalStateException e) {
      printHelpAndExit(e.getMessage(), getOptions());
      return -1;
    }
    configuration = HBaseConfiguration.addHbaseResources(getConf());
    try (Connection globalConn = ConnectionUtil.getInputConnection(configuration)) {
      createSyncOutputTable(globalConn);
    }
    populateSyncTableToolAttributes(cmdLine);
    try {
      PTableType tableType = validateAndGetTableType();
      job = configureAndCreatePhoenixSyncTableJob(tableType);
      boolean result = submitPhoenixSyncTableJob();
      return result ? 0 : -1;
    } catch (Exception ex) {
      LOGGER.error(
        "Exception occurred while performing phoenix sync table job for table {} to target {}: {}",
        qTable, targetZkQuorum, ExceptionUtils.getMessage(ex), ex);
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new PhoenixSyncTableTool(), args);
    System.exit(exitCode);
  }

  public static void setPhoenixSyncTableName(Configuration conf, String tableName) {
    Preconditions.checkNotNull(conf);
    conf.set(PHOENIX_SYNC_TABLE_NAME, tableName);
  }

  public static String getPhoenixSyncTableName(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.get(PHOENIX_SYNC_TABLE_NAME);
  }

  public static void setPhoenixSyncTableTargetZkQuorum(Configuration conf, String zkQuorum) {
    Preconditions.checkNotNull(conf);
    conf.set(PHOENIX_SYNC_TABLE_TARGET_ZK_QUORUM, zkQuorum);
  }

  public static String getPhoenixSyncTableTargetZkQuorum(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.get(PHOENIX_SYNC_TABLE_TARGET_ZK_QUORUM);
  }

  public static void setPhoenixSyncTableFromTime(Configuration conf, Long fromTime) {
    Preconditions.checkNotNull(conf);
    conf.setLong(PHOENIX_SYNC_TABLE_FROM_TIME, fromTime);
  }

  public static Long getPhoenixSyncTableFromTime(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.getLong(PHOENIX_SYNC_TABLE_FROM_TIME, -1L);
  }

  public static void setPhoenixSyncTableToTime(Configuration conf, Long toTime) {
    Preconditions.checkNotNull(conf);
    conf.setLong(PHOENIX_SYNC_TABLE_TO_TIME, toTime);
  }

  public static long getPhoenixSyncTableToTime(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.getLong(PHOENIX_SYNC_TABLE_TO_TIME, -1L);
  }

  public static void setPhoenixSyncTableDryRun(Configuration conf, boolean dryRun) {
    Preconditions.checkNotNull(conf);
    conf.setBoolean(PHOENIX_SYNC_TABLE_DRY_RUN, dryRun);
  }

  public static boolean getPhoenixSyncTableDryRun(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.getBoolean(PHOENIX_SYNC_TABLE_DRY_RUN, true);
  }

  public static void setPhoenixSyncTableChunkSizeBytes(Configuration conf, Long chunkSizeBytes) {
    Preconditions.checkNotNull(conf);
    conf.setLong(PHOENIX_SYNC_TABLE_CHUNK_SIZE_BYTES, chunkSizeBytes);
  }

  public static long getPhoenixSyncTableChunkSizeBytes(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.getLong(PHOENIX_SYNC_TABLE_CHUNK_SIZE_BYTES,
      DEFAULT_PHOENIX_SYNC_TABLE_CHUNK_SIZE_BYTES);
  }

  public static void setPhoenixSyncTableRawScan(Configuration conf, boolean rawScan) {
    Preconditions.checkNotNull(conf);
    conf.setBoolean(PHOENIX_SYNC_TABLE_RAW_SCAN, rawScan);
  }

  public static boolean getPhoenixSyncTableRawScan(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.getBoolean(PHOENIX_SYNC_TABLE_RAW_SCAN, false);
  }

  public static void setPhoenixSyncTableReadAllVersions(Configuration conf,
    boolean readAllVersions) {
    Preconditions.checkNotNull(conf);
    conf.setBoolean(PHOENIX_SYNC_TABLE_READ_ALL_VERSIONS, readAllVersions);
  }

  public static boolean getPhoenixSyncTableReadAllVersions(Configuration conf) {
    Preconditions.checkNotNull(conf);
    return conf.getBoolean(PHOENIX_SYNC_TABLE_READ_ALL_VERSIONS, false);
  }

  public Job getJob() {
    return job;
  }

  @VisibleForTesting
  public Long getStartTime() {
    return startTime;
  }

  @VisibleForTesting
  public Long getEndTime() {
    return endTime;
  }

  @VisibleForTesting
  public String getTenantId() {
    return tenantId;
  }

  @VisibleForTesting
  public String getSchemaName() {
    return schemaName;
  }

  @VisibleForTesting
  public Long getChunkSizeBytes() {
    return chunkSizeBytes;
  }

  @VisibleForTesting
  public boolean isDryRun() {
    return isDryRun;
  }

  @VisibleForTesting
  public boolean isForeground() {
    return isForeground;
  }

  @VisibleForTesting
  public boolean isRawScan() {
    return isRawScan;
  }

  @VisibleForTesting
  public boolean isReadAllVersions() {
    return isReadAllVersions;
  }

  @VisibleForTesting
  public void initializeConfiguration() {
    configuration = HBaseConfiguration.addHbaseResources(getConf());
  }
}
