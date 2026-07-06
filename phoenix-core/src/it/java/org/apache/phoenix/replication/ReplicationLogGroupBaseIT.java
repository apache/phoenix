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
package org.apache.phoenix.replication;

import static org.apache.phoenix.hbase.index.IndexRegionObserver.PHOENIX_INDEX_CDC_MUTATION_SERIALIZE;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.getHighAvailibilityGroup;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.query.QueryServices.SYNCHRONOUS_REPLICATION_ENABLED;
import static org.apache.phoenix.replication.ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY;
import static org.apache.phoenix.replication.reader.ReplicationLogReplayService.PHOENIX_REPLICATION_REPLAY_ENABLED;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.expression.function.PartitionIdFunction;
import org.apache.phoenix.jdbc.HABaseIT;
import org.apache.phoenix.jdbc.HighAvailabilityGroup;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.HighAvailabilityTestingUtility;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.replication.reader.ReplicationLogProcessor;
import org.apache.phoenix.replication.tool.LogFileAnalyzer;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared fixture for the replication log-group ITs: cluster setup, per-test HA-group wiring, and
 * the replay/verification helpers. Concrete subclasses supply their own {@code @BeforeClass}
 * calling {@link #setupClusters()} so each runs exactly the tests it declares; nothing here is a
 * {@code @Test}.
 */
public abstract class ReplicationLogGroupBaseIT extends HABaseIT {
  protected static final Logger LOG = LoggerFactory.getLogger(ReplicationLogGroupBaseIT.class);

  @Rule
  public TestName name = new TestName();

  protected Properties clientProps = new Properties();
  protected String haGroupName;
  protected HighAvailabilityGroup haGroup;
  protected ReplicationLogGroup logGroup;

  /**
   * Starts both clusters with the common replication test config. A subclass that needs a
   * cluster-level toggle (e.g. {@code serializeCDCMutations}, read once at IRO {@code start()})
   * sets it on {@code conf1}/{@code conf2} in its own {@code @BeforeClass} before calling this.
   */
  protected static void setupClusters() throws Exception {
    conf1.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, 5);
    // Disable replay on cluster 1
    conf1.setBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, false);
    // Disable replay on cluster 2, we will explicitly replay the log files
    conf2.setBoolean(PHOENIX_REPLICATION_REPLAY_ENABLED, false);
    // Disable writer on cluster 2
    conf2.setBoolean(SYNCHRONOUS_REPLICATION_ENABLED, false);
    CLUSTERS.start();
    DriverManager.registerDriver(PhoenixDriver.INSTANCE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
    CLUSTERS.close();
  }

  @Before
  public void beforeTest() throws Exception {
    LOG.info("Starting test {}", name.getMethodName());
    haGroupName = name.getMethodName();
    clientProps = HighAvailabilityTestingUtility.getHATestProperties();
    clientProps.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);
    CLUSTERS.initClusterRole(haGroupName, HighAvailabilityPolicy.FAILOVER);
    haGroup = getHighAvailibilityGroup(CLUSTERS.getJdbcHAUrl(), clientProps);
    LOG.info("Initialized haGroup {} with URL {}", haGroup, CLUSTERS.getJdbcHAUrl());
    logGroup = getReplicationLogGroup();
  }

  @After
  public void afterTest() throws Exception {
    LOG.info("Starting cleanup for test {}", name.getMethodName());
    logGroup.close();
    LOG.info("Ending cleanup for test {}", name.getMethodName());
  }

  private ReplicationLogGroup getReplicationLogGroup() throws IOException {
    HRegionServer rs = CLUSTERS.getHBaseCluster1().getHBaseCluster().getRegionServer(0);
    return ReplicationLogGroup.get(conf1, rs.getServerName(), haGroupName);
  }

  protected Map<String, List<Mutation>> groupLogsByTable() throws Exception {
    LogFileAnalyzer analyzer = new LogFileAnalyzer();
    // use peer cluster conf
    analyzer.setConf(conf2);
    Path standByLogDir = logGroup.getOrCreatePeerShardManager().getRootDirectoryPath();
    LOG.info("Analyzing log files at {}", standByLogDir);
    String[] args = { "--check", standByLogDir.toString() };
    assertEquals(0, analyzer.run(args));
    return analyzer.groupLogsByTable(standByLogDir.toString());
  }

  protected int getCountForTable(Map<String, List<Mutation>> logsByTable, String tableName)
    throws Exception {
    List<Mutation> mutations = logsByTable.get(tableName);
    return mutations != null ? mutations.size() : 0;
  }

  protected Map<String, Integer> countRecordsByTable() throws Exception {
    LogFileAnalyzer analyzer = new LogFileAnalyzer();
    // use peer cluster conf
    analyzer.setConf(conf2);
    Path standByLogDir = logGroup.getOrCreatePeerShardManager().getRootDirectoryPath();
    return analyzer.countRecordsByTable(standByLogDir.toString());
  }

  /**
   * Counts how many of the given log files actually carry records. A single RS writes one file per
   * replication round and rotates at every round boundary, so a short workload leaves several empty
   * round-files around the one that holds its mutations. Empty files contribute no work to replay,
   * so this -- not the raw file count -- is the true measure of how many files a parallel replay
   * overlaps on. Reads only record counts (no mutation expansion), so it is cheap.
   */
  protected int countFilesWithRecords(List<Path> logFiles) throws Exception {
    LogFileAnalyzer analyzer = new LogFileAnalyzer();
    analyzer.setConf(conf2);
    int populated = 0;
    for (Path logFile : logFiles) {
      if (!analyzer.countRecordsByTable(logFile.toString()).isEmpty()) {
        populated++;
      }
    }
    return populated;
  }

  protected void verifyReplication(Map<String, Integer> expected) throws Exception {
    // first close the logGroup
    logGroup.close();
    Map<String, List<Mutation>> mutationsByTable = groupLogsByTable();
    dumpTableLogCount(mutationsByTable);
    for (Map.Entry<String, Integer> entry : expected.entrySet()) {
      String tableName = entry.getKey();
      int expectedMutationCount = entry.getValue();
      List<Mutation> mutations = mutationsByTable.get(tableName);
      int actualMutationCount = mutations != null ? mutations.size() : 0;
      try {
        if (!tableName.equals(SYSTEM_CATALOG_NAME)) {
          assertEquals(String.format("For table %s", tableName), expectedMutationCount,
            actualMutationCount);
        } else {
          // special handling for syscat
          assertTrue("For SYSCAT", actualMutationCount >= expectedMutationCount);
        }
      } catch (AssertionError e) {
        // create a regular connection
        try (Connection conn = DriverManager.getConnection(CLUSTERS.getJdbcUrl1(haGroup))) {
          TestUtil.dumpTable(conn, TableName.valueOf(tableName));
          throw e;
        }
      }
    }
  }

  protected void dumpTableLogCount(Map<String, List<Mutation>> mutationsByTable) {
    LOG.info("Dump table log count for test {}", name.getMethodName());
    for (Map.Entry<String, List<Mutation>> table : mutationsByTable.entrySet()) {
      LOG.info("#Log entries for {} = {}", table.getKey(), table.getValue().size());
    }
  }

  protected void moveRegionToServer(TableName tableName, ServerName sn) throws Exception {
    HBaseTestingUtility util = CLUSTERS.getHBaseCluster1();
    try (RegionLocator locator = util.getConnection().getRegionLocator(tableName)) {
      String regEN = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
      while (!sn.equals(locator.getAllRegionLocations().get(0).getServerName())) {
        LOG.info("Moving region {} of table {} to server {}", regEN, tableName, sn);
        util.getAdmin().move(Bytes.toBytes(regEN), sn);
        Thread.sleep(100);
      }
      LOG.info("Moved region {} of table {} to server {}", regEN, tableName, sn);
    }
  }

  protected void replayAndVerifyAcrossClusters(List<String> ddlStatements, String... tablesToVerify)
    throws Exception {
    replayAndVerifyAcrossClusters(1, ddlStatements, tablesToVerify);
  }

  /**
   * Creates the schema on cluster 2, replays the replication log, and asserts cross-cluster cell
   * equality for each named table. With {@code parallelism == 1} the log files are replayed
   * sequentially in the calling thread; with {@code parallelism > 1} they are sharded round-robin
   * across that many threads, all sharing one {@link ReplicationLogProcessor}. This simulates
   * multiple region servers draining shard files in parallel within the same replay round, driving
   * overlapping same-row batches through the standby IRO concurrently. Replay order does not affect
   * the result: each {@code (row, ts)} group is an independent reproduction of the active's
   * pre-batch state.
   */
  protected void replayAndVerifyAcrossClusters(int parallelism, List<String> ddlStatements,
    String... tablesToVerify) throws Exception {
    Path standByLogDir = logGroup.getOrCreatePeerShardManager().getRootDirectoryPath();

    // Quiesce the log group before reading: this stops rotation and synchronously closes the
    // writers, so every file has a durable trailer before any reader recovers its lease. Replaying
    // against a still-open group lets lease recovery fence a writer mid-rotation, costing it its
    // trailer. close() is idempotent, so this is a no-op when verifyReplication already closed it.
    logGroup.close();

    // Create the same schema on cluster 2
    try (Connection conn2 = CLUSTERS.getCluster2Connection(haGroup)) {
      for (String ddl : ddlStatements) {
        conn2.createStatement().execute(ddl);
      }
      conn2.commit();
    }

    // Replay replication log on cluster 2
    FileSystem fs = standByLogDir.getFileSystem(conf2);
    List<Path> logFiles = findLogFiles(standByLogDir, fs);
    assertTrue("Should have at least one log file", !logFiles.isEmpty());
    ReplicationLogProcessor processor = ReplicationLogProcessor.get(conf2, haGroupName);
    try {
      if (parallelism <= 1) {
        for (Path logFile : logFiles) {
          LOG.info("Replaying log file: {}", logFile);
          processor.processLogFile(fs, logFile);
        }
      } else {
        // A single populated file would silently degrade parallel replay to the sequential case
        // (empty round-boundary files do no work), so a test that asked for concurrency must have
        // mutations spread across more than one file -- i.e. the workload spanned several rounds.
        // Fail loudly rather than pass with weaker coverage.
        int populated = countFilesWithRecords(logFiles);
        assertTrue("Parallel replay requested but only " + populated + " of " + logFiles.size()
          + " log file(s) carry records; the workload must span multiple rounds", populated > 1);
        replayLogFilesInParallel(processor, fs, logFiles, parallelism);
      }
    } finally {
      processor.close();
    }

    // Verify tables match across clusters at the HBase cell level
    for (String table : tablesToVerify) {
      assertTablesEqualAcrossClusters(table);
    }
  }

  /**
   * Replays the given log files concurrently across {@code parallelism} threads sharing one
   * processor ({@code processLogFile} is stateless apart from the lazily-built, double-checked
   * {@code AsyncConnection}, so concurrent calls are safe). Surfaces the first thread's failure as
   * an {@link AssertionError} so a replay error fails the test rather than being swallowed.
   */
  private void replayLogFilesInParallel(ReplicationLogProcessor processor, FileSystem fs,
    List<Path> logFiles, int parallelism) throws Exception {
    int threads = Math.min(parallelism, logFiles.size());
    CountDownLatch doneSignal = new CountDownLatch(threads);
    AtomicReference<Throwable> firstError = new AtomicReference<>();
    for (int t = 0; t < threads; t++) {
      final int offset = t;
      final int stride = threads;
      Thread worker = new Thread(() -> {
        try {
          for (int i = offset; i < logFiles.size(); i += stride) {
            Path logFile = logFiles.get(i);
            LOG.info("Replaying log file (thread {}): {}", offset, logFile);
            processor.processLogFile(fs, logFile);
          }
        } catch (Throwable e) {
          firstError.compareAndSet(null, e);
        } finally {
          doneSignal.countDown();
        }
      }, "replay-worker-" + t);
      worker.start();
    }
    assertTrue("Ran out of time waiting for parallel replay",
      doneSignal.await(120, TimeUnit.SECONDS));
    if (firstError.get() != null) {
      throw new AssertionError("A parallel replay thread failed", firstError.get());
    }
  }

  /**
   * True for HBase 2.4.18+, 2.5.9+, and 2.6.0+, where atomic upsert with {@code RETURNING *}
   * correctly projects the returned row. Mirrors {@code OnDuplicateKey2IT}.
   */
  protected boolean isSetCorrectResultEnabledOnHBase() {
    String hbaseVersion = VersionInfo.getVersion();
    String[] versionArr = hbaseVersion.split("\\.");
    int majorVersion = Integer.parseInt(versionArr[0]);
    int minorVersion = Integer.parseInt(versionArr[1]);
    int patchVersion = Integer.parseInt(versionArr[2].split("-")[0]);
    if (majorVersion != 2) {
      return majorVersion > 2;
    }
    if (minorVersion >= 6) {
      return true;
    }
    if (minorVersion < 4) {
      return false;
    }
    if (minorVersion == 4) {
      return patchVersion >= 18;
    }
    return patchVersion >= 9;
  }

  protected List<Path> findLogFiles(Path dir, FileSystem fs) throws IOException {
    List<Path> files = new ArrayList<>();
    findLogFilesRecursive(dir, fs, files);
    return files;
  }

  private void findLogFilesRecursive(Path dir, FileSystem fs, List<Path> files) throws IOException {
    if (!fs.exists(dir)) {
      return;
    }
    for (FileStatus status : fs.listStatus(dir)) {
      if (status.isDirectory()) {
        findLogFilesRecursive(status.getPath(), fs, files);
      } else if (status.getPath().getName().endsWith(".plog")) {
        files.add(status.getPath());
      }
    }
  }

  /**
   * Compares an HBase table between the two clusters using {@link Result#compareResults}. Scans
   * both tables with all versions and asserts that every row matches at the cell level.
   */
  protected void assertTablesEqualAcrossClusters(String hbaseTableName) throws Exception {
    TableName tn = TableName.valueOf(hbaseTableName);
    try (
      org.apache.hadoop.hbase.client.Connection hconn1 = ConnectionFactory.createConnection(conf1);
      org.apache.hadoop.hbase.client.Connection hconn2 = ConnectionFactory.createConnection(conf2);
      Table table1 = hconn1.getTable(tn); Table table2 = hconn2.getTable(tn)) {

      Scan scan = new Scan();
      scan.readAllVersions();

      try (ResultScanner scanner1 = table1.getScanner(scan);
        ResultScanner scanner2 = table2.getScanner(scan)) {
        int rowCount = 0;
        while (true) {
          Result r1 = scanner1.next();
          Result r2 = scanner2.next();
          if (r1 == null && r2 == null) {
            break;
          }
          assertNotNull(
            String.format("Table %s: cluster 2 has fewer rows at row %d", hbaseTableName, rowCount),
            r2);
          assertNotNull(
            String.format("Table %s: cluster 1 has fewer rows at row %d", hbaseTableName, rowCount),
            r1);
          try {
            Result.compareResults(r1, r2, true);
          } catch (Exception e) {
            LOG.error("Table {} row {} mismatch. Dumping both tables:", hbaseTableName, rowCount);
            LOG.error("--- Cluster 1 ---");
            TestUtil.dumpTable(table1);
            LOG.error("--- Cluster 2 ---");
            TestUtil.dumpTable(table2);
            fail(String.format("Table %s row %d mismatch: %s", hbaseTableName, rowCount,
              e.getMessage()));
          }
          rowCount++;
        }
        LOG.info("Table {} matches across clusters: {} rows verified", hbaseTableName, rowCount);
      }
    }
  }

  /**
   * Cross-cluster equality for a CDC index physical table. A CDC index rowkey leads with
   * {@code PARTITION_ID()} = the encoded data-table region name ({@code PARTITION_ID_LENGTH}
   * bytes), which is region-local and so differs by design between the active and the standby. The
   * standby regenerates the rowkey with its own partition_id, so a byte-equal compare on the full
   * rowkey (as {@link #assertTablesEqualAcrossClusters} does) would always fail. Everything else is
   * identical: the rowkey suffix (ROW_TIMESTAMP + data PK) and every cell (the index is uncovered,
   * so both sides write the empty cell {@code UNVERIFIED} and there is no post phase to flip it).
   * This compares the rowkey suffix after the partition_id and all cell content except the row.
   */
  protected void assertCDCIndexEqualAcrossClusters(String hbaseTableName) throws Exception {
    final int pidLen = PartitionIdFunction.PARTITION_ID_LENGTH;
    TableName tn = TableName.valueOf(hbaseTableName);
    try (
      org.apache.hadoop.hbase.client.Connection hconn1 = ConnectionFactory.createConnection(conf1);
      org.apache.hadoop.hbase.client.Connection hconn2 = ConnectionFactory.createConnection(conf2);
      Table table1 = hconn1.getTable(tn); Table table2 = hconn2.getTable(tn)) {

      Scan scan = new Scan();
      scan.readAllVersions();
      try (ResultScanner scanner1 = table1.getScanner(scan);
        ResultScanner scanner2 = table2.getScanner(scan)) {
        int rowCount = 0;
        while (true) {
          Result r1 = scanner1.next();
          Result r2 = scanner2.next();
          if (r1 == null && r2 == null) {
            break;
          }
          assertNotNull(String.format("CDC index %s: cluster 2 has fewer rows at row %d",
            hbaseTableName, rowCount), r2);
          assertNotNull(String.format("CDC index %s: cluster 1 has fewer rows at row %d",
            hbaseTableName, rowCount), r1);

          byte[] rk1 = r1.getRow();
          byte[] rk2 = r2.getRow();
          assertTrue(String.format("CDC index %s row %d: rowkey shorter than partition_id",
            hbaseTableName, rowCount), rk1.length >= pidLen && rk2.length >= pidLen);
          assertArrayEquals(
            String.format("CDC index %s row %d: rowkey suffix after partition_id differs",
              hbaseTableName, rowCount),
            Arrays.copyOfRange(rk1, pidLen, rk1.length),
            Arrays.copyOfRange(rk2, pidLen, rk2.length));

          List<Cell> cells1 = r1.listCells();
          List<Cell> cells2 = r2.listCells();
          assertEquals(
            String.format("CDC index %s row %d: cell count differs", hbaseTableName, rowCount),
            cells1.size(), cells2.size());
          for (int i = 0; i < cells1.size(); i++) {
            Cell c1 = cells1.get(i);
            Cell c2 = cells2.get(i);
            String where =
              String.format("CDC index %s row %d cell %d", hbaseTableName, rowCount, i);
            assertTrue(where + ": family differs", CellUtil.matchingFamily(c1, c2));
            assertTrue(where + ": qualifier differs", CellUtil.matchingQualifier(c1, c2));
            assertEquals(where + ": timestamp differs", c1.getTimestamp(), c2.getTimestamp());
            assertEquals(where + ": type differs", c1.getType(), c2.getType());
            assertTrue(where + ": value differs", CellUtil.matchingValue(c1, c2));
          }
          rowCount++;
        }
        LOG.info("CDC index {} matches across clusters (partition_id-stripped): {} rows verified",
          hbaseTableName, rowCount);
      }
    }
  }

  /**
   * Asserts the CDC index's {@code _IDX_PRE_} serialized-payload column is present iff
   * {@code serializeCDCMutations} is enabled on the cluster. Scans the standby (cluster 2) copy,
   * which is the regenerated one, so a present payload also proves the standby reproduced it.
   */
  protected void assertCDCIndexPayloadMatchesConfig(String hbaseTableName) throws Exception {
    boolean serialize = conf1.getBoolean(PHOENIX_INDEX_CDC_MUTATION_SERIALIZE, false);
    TableName tn = TableName.valueOf(hbaseTableName);
    int payloadCells = 0;
    try (
      org.apache.hadoop.hbase.client.Connection hconn = ConnectionFactory.createConnection(conf2);
      Table table = hconn.getTable(tn)) {
      Scan scan = new Scan();
      scan.readAllVersions();
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result r = scanner.next(); r != null; r = scanner.next()) {
          for (Cell c : r.listCells()) {
            if (CellUtil.matchingQualifier(c, QueryConstants.CDC_INDEX_PRE_MUTATIONS_CQ_BYTES)) {
              payloadCells++;
            }
          }
        }
      }
    }
    if (serialize) {
      assertTrue("serializeCDCMutations=true but CDC index " + hbaseTableName
        + " carries no _IDX_PRE_ payload cell on the standby", payloadCells > 0);
    } else {
      assertEquals("serializeCDCMutations=false but CDC index " + hbaseTableName
        + " carries _IDX_PRE_ payload cells on the standby", 0, payloadCells);
    }
  }
}
