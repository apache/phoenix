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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.phoenix.iterate.TestingMapReduceParallelScanGrouper;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.PhoenixTestingInputFormat;
import org.apache.phoenix.mapreduce.index.PhoenixIndexDBWritable;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class TableSnapshotReadsMapReduceIT extends BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableSnapshotReadsMapReduceIT.class);

  private static final String STOCK_NAME = "STOCK_NAME";
  private static final String RECORDING_YEAR = "RECORDING_YEAR";
  private static final String RECORDINGS_QUARTER = "RECORDINGS_QUARTER";
  private static final String MAX_RECORDING = "MAX_RECORDING";
  private final static String SNAPSHOT_NAME = "FOO";
  private static final String FIELD1 = "FIELD1";
  private static final String FIELD2 = "FIELD2";
  private static final String FIELD3 = "FIELD3";
  private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s ( " +
      " FIELD1 VARCHAR NOT NULL , FIELD2 VARCHAR , FIELD3 INTEGER CONSTRAINT pk PRIMARY KEY (FIELD1 ))";
  private static final String UPSERT = "UPSERT into %s values (?, ?, ?)";
  private static final String CREATE_STOCK_TABLE =
          "CREATE TABLE IF NOT EXISTS %s ( " + STOCK_NAME + " VARCHAR NOT NULL , " + RECORDING_YEAR
                  + "  INTEGER NOT  NULL,  " + RECORDINGS_QUARTER + " "
                  + " DOUBLE array[] CONSTRAINT pk PRIMARY KEY ( " + STOCK_NAME + ", "
                  + RECORDING_YEAR + " )) " + "SPLIT ON ('AA')";
  private static final String CREATE_STOCK_STATS_TABLE =
          "CREATE TABLE IF NOT EXISTS %s(" + STOCK_NAME + " VARCHAR NOT NULL , " + MAX_RECORDING
                  + " DOUBLE CONSTRAINT pk PRIMARY KEY (" + STOCK_NAME + " ))";
  private static List<List<Object>> result;
  private long timestamp;
  private String tableName;
  private Job job;
  private Path tmpDir;
  private Configuration conf;
  private static final Random RANDOM = new Random();
  private Boolean isSnapshotRestoreDoneExternally;

  public TableSnapshotReadsMapReduceIT(Boolean isSnapshotRestoreDoneExternally) {
    this.isSnapshotRestoreDoneExternally = isSnapshotRestoreDoneExternally;
  }

  @Parameterized.Parameters
  public static synchronized Collection<Boolean> snapshotRestoreDoneExternallyParams() {
    return Arrays.asList(true, false);
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
      Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
      setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
      getUtility().getAdmin().balancerSwitch(false, true);
  }

  @Before
  public void before() throws SQLException, IOException {
    // create table
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      tableName = generateUniqueName();
      conn.createStatement().execute(String.format(CREATE_TABLE, tableName));
      conn.commit();
    }
    // configure Phoenix M/R job to read snapshot
    conf = getUtility().getConfiguration();
    job = Job.getInstance(conf);
    tmpDir = getUtility().getRandomDir();
  }

  @Test
  public void testMapReduceSnapshots() throws Exception {
    PhoenixMapReduceUtil.setInput(job,PhoenixIndexDBWritable.class,
            SNAPSHOT_NAME, tableName, tmpDir, null, FIELD1, FIELD2, FIELD3);
    configureJob(job, tableName, null, null, false);
  }

  @Test
  public void testMapReduceSnapshotsMultiRegion() throws Exception {
    PhoenixMapReduceUtil.setInput(job,PhoenixIndexDBWritable.class,
            SNAPSHOT_NAME, tableName, tmpDir, null, FIELD1, FIELD2, FIELD3);
    configureJob(job, tableName, null, null, true);
  }

  @Test
  public void testMapReduceSnapshotsWithCondition() throws Exception {
    PhoenixMapReduceUtil.setInput(job,PhoenixIndexDBWritable.class,
            SNAPSHOT_NAME, tableName, tmpDir, FIELD3 + " > 0001", FIELD1, FIELD2, FIELD3);
    configureJob(job, tableName, null, "FIELD3 > 0001", false);
  }

  @Test
  public void testMapReduceSnapshotWithLimit() throws Exception {
    String inputQuery = "SELECT * FROM " + tableName + " ORDER BY FIELD2 LIMIT 1";
    PhoenixMapReduceUtil.setInput(job,PhoenixIndexDBWritable.class,
            SNAPSHOT_NAME, tableName, tmpDir, inputQuery);
    configureJob(job, tableName, inputQuery, null, false);
  }

  @Test
  public void testSnapshotMapReduceJobNotImpactingTableMapReduceJob() throws Exception {
    //Submitting and asserting successful Map Reduce Job over snapshots
    PhoenixMapReduceUtil
            .setInput(job, PhoenixIndexDBWritable.class, SNAPSHOT_NAME, tableName, tmpDir, null,
                    FIELD1, FIELD2, FIELD3);
    configureJob(job, tableName, null, null, false);

    //Asserting that snapshot name is set in configuration
    Configuration config = job.getConfiguration();
    Assert.assertEquals("Correct snapshot name not found in configuration", SNAPSHOT_NAME,
            config.get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY));

    TestingMapReduceParallelScanGrouper.clearNumCallsToGetRegionBoundaries();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // create table
      tableName = generateUniqueName();
      conn.createStatement().execute(String.format(CREATE_TABLE, tableName));
      conn.commit();

      //Submitting next map reduce job over table and making sure that it does not fail with
      // any wrong snapshot properties set in common configurations which are
      // used across all jobs.
      job = createAndTestJob(conn);
    }
    //Asserting that snapshot name is no more set in common shared configuration
    config = job.getConfiguration();
    Assert.assertNull("Snapshot name is not null in Configuration",
            config.get(PhoenixConfigurationUtil.SNAPSHOT_NAME_KEY));
  }

  private Job createAndTestJob(Connection conn)
          throws SQLException, IOException, InterruptedException, ClassNotFoundException {
    String stockTableName = generateUniqueName();
    String stockStatsTableName = generateUniqueName();
    conn.createStatement().execute(String.format(CREATE_STOCK_TABLE, stockTableName));
    conn.createStatement().execute(String.format(CREATE_STOCK_STATS_TABLE, stockStatsTableName));
    conn.commit();
    final Configuration conf = ((PhoenixConnection) conn).getQueryServices().getConfiguration();
    Job job = Job.getInstance(conf);
    PhoenixMapReduceUtil.setInput(job, MapReduceIT.StockWritable.class, PhoenixTestingInputFormat.class,
            stockTableName, null, STOCK_NAME, RECORDING_YEAR, "0." + RECORDINGS_QUARTER);
    testJob(conn, job, stockTableName, stockStatsTableName);
    return job;
  }

  private void testJob(Connection conn, Job job, String stockTableName, String stockStatsTableName)
          throws SQLException, InterruptedException, IOException, ClassNotFoundException {
    assertEquals("Failed to reset getRegionBoundaries counter for scanGrouper", 0,
            TestingMapReduceParallelScanGrouper.getNumCallsToGetRegionBoundaries());
    upsertData(conn, stockTableName);

    // only run locally, rather than having to spin up a MiniMapReduce cluster and lets us use breakpoints
    job.getConfiguration().set("mapreduce.framework.name", "local");

    setOutput(job, stockStatsTableName);

    job.setMapperClass(MapReduceIT.StockMapper.class);
    job.setReducerClass(MapReduceIT.StockReducer.class);
    job.setOutputFormatClass(PhoenixOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(MapReduceIT.StockWritable.class);

    // run job and assert if success
    assertTrue("Job didn't complete successfully! Check logs for reason.", job.waitForCompletion(true));
  }

  /**
   * Custom output setting because output upsert statement setting is broken (PHOENIX-2677)
   *
   * @param job to update
   */
  private void setOutput(Job job, String stockStatsTableName) {
    final Configuration configuration = job.getConfiguration();
    PhoenixConfigurationUtil.setOutputTableName(configuration, stockStatsTableName);
    configuration.set(PhoenixConfigurationUtil.UPSERT_STATEMENT, "UPSERT into " + stockStatsTableName +
            " (" + STOCK_NAME + ", " + MAX_RECORDING + ") values (?,?)");
    job.setOutputFormatClass(PhoenixOutputFormat.class);
  }

  private void configureJob(Job job, String tableName, String inputQuery, String condition, boolean shouldSplit) throws Exception {
    try {
      upsertAndSnapshot(tableName, shouldSplit, job.getConfiguration());
      result = new ArrayList<>();

      job.setMapperClass(TableSnapshotMapper.class);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setOutputFormatClass(NullOutputFormat.class);

      Assert.assertTrue(job.waitForCompletion(true));

      // verify the result, should match the values at the corresponding timestamp
      Properties props = new Properties();
      props.setProperty("CurrentSCN", Long.toString(timestamp));

      StringBuilder selectQuery = new StringBuilder("SELECT * FROM " + tableName);
      if (condition != null) {
        selectQuery.append(" WHERE " + condition);
      }

      if (inputQuery == null)
        inputQuery = selectQuery.toString();

      ResultSet rs = DriverManager.getConnection(getUrl(), props).createStatement().executeQuery(inputQuery);

      for (List<Object> r : result) {
        assertTrue("No data stored in the table!", rs.next());
        int i = 0;
        String field1 = rs.getString(i + 1);
        assertEquals("Got the incorrect value for field1", r.get(i++), field1);
        String field2 = rs.getString(i + 1);
        assertEquals("Got the incorrect value for field2", r.get(i++), field2);
        int field3 = rs.getInt(i + 1);
        assertEquals("Got the incorrect value for field3", r.get(i++), field3);
      }

      assertFalse("Should only have stored" + result.size() + "rows in the table for the timestamp!", rs.next());
      assertRestoreDirCount(conf, tmpDir.toString(), 1);
    } finally {
      deleteSnapshotIfExists(SNAPSHOT_NAME);
    }
  }

  private void upsertData(Connection conn, String stockTableName) throws SQLException {
    PreparedStatement stmt = conn.prepareStatement(String.format(UPSERT, stockTableName));
    upsertData(stmt, "AAPL", 2009, new Double[]{85.88, 91.04, 88.5, 90.3});
    upsertData(stmt, "AAPL", 2008, new Double[]{75.88, 81.04, 78.5, 80.3});
    conn.commit();
  }

  private void upsertData(PreparedStatement stmt, String name, int year, Double[] data) throws SQLException {
    int i = 1;
    stmt.setString(i++, name);
    stmt.setInt(i++, year);
    Array recordings = new PhoenixArray.PrimitiveDoublePhoenixArray(PDouble.INSTANCE, data);
    stmt.setArray(i++, recordings);
    stmt.execute();
  }

  private void upsertData(String tableName) throws SQLException {
    Connection conn = DriverManager.getConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(String.format(UPSERT, tableName));
    upsertData(stmt, "AAAA", "JHHD", 37);
    upsertData(stmt, "BBBB", "JSHJ", 224);
    upsertData(stmt, "CCCC", "SSDD", 15);
    upsertData(stmt, "PPPP", "AJDG", 53);
    upsertData(stmt, "SSSS", "HSDG", 59);
    upsertData(stmt, "XXXX", "HDPP", 22);
    conn.commit();
  }

  private void upsertDataBeforeSplit(String tableName) throws SQLException {
    Connection conn = DriverManager.getConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(String.format(UPSERT, tableName));
    upsertData(stmt, "CCCC", "SSDD", RANDOM.nextInt());
    for (int i = 0; i < 100; i++) {
      upsertData(stmt, "AAAA" + i, "JHHA" + i, RANDOM.nextInt());
      upsertData(stmt, "0000" + i, "JHHB" + i, RANDOM.nextInt());
      upsertData(stmt, "9999" + i, "JHHC" + i, RANDOM.nextInt());
      upsertData(stmt, "BBBB" + i, "JSHJ" + i, RANDOM.nextInt());
      upsertData(stmt, "BBBB1" + i, "JSHK" + i, RANDOM.nextInt());
      upsertData(stmt, "BBBB2" + i, "JSHL" + i, RANDOM.nextInt());
      upsertData(stmt, "CCCC1" + i, "SSDE" + i, RANDOM.nextInt());
      upsertData(stmt, "CCCC2" + i, "SSDF" + i, RANDOM.nextInt());
      upsertData(stmt, "PPPP" + i, "AJDH" + i, RANDOM.nextInt());
      upsertData(stmt, "SSSS" + i, "HSDG" + i, RANDOM.nextInt());
      upsertData(stmt, "XXXX" + i, "HDPP" + i, RANDOM.nextInt());
    }
    conn.commit();
  }

  private void upsertData(PreparedStatement stmt, String field1, String field2, int field3) throws SQLException {
    stmt.setString(1, field1);
    stmt.setString(2, field2);
    stmt.setInt(3, field3);
    stmt.execute();
  }

  private void upsertAndSnapshot(String tableName, boolean shouldSplit, Configuration configuration) throws Exception {
    if (shouldSplit) {
      // having very few rows in table doesn't really help much with splitting case.
      // we should upsert large no of rows as a prerequisite to splitting
      upsertDataBeforeSplit(tableName);
    } else {
      upsertData(tableName);
    }

    TableName hbaseTableName = TableName.valueOf(tableName);
    try (Connection conn = DriverManager.getConnection(getUrl());
         Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {

      if (shouldSplit) {
        splitTableSync(admin, hbaseTableName, Bytes.toBytes("CCCC"), 2);
      }

      snapshotCreateSync(hbaseTableName, admin, SNAPSHOT_NAME);

      List<SnapshotDescription> snapshots = admin.listSnapshots();
      Assert.assertEquals(tableName, snapshots.get(0).getTable());

      // Capture the snapshot timestamp to use as SCN while reading the table later
      // Assigning the timestamp value here will make tests less flaky
      timestamp = System.currentTimeMillis();

      // upsert data after snapshot
      PreparedStatement stmt = conn.prepareStatement(String.format(UPSERT, tableName));
      upsertData(stmt, "DDDD", "SNFB", 45);
      conn.commit();
      if (isSnapshotRestoreDoneExternally) {
        //Performing snapshot restore which will be used during scans
        Path rootDir = new Path(configuration.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(configuration);
        Path restoreDir = new Path(configuration.get(PhoenixConfigurationUtil.RESTORE_DIR_KEY));
        RestoreSnapshotHelper.copySnapshotForScanner(configuration, fs, rootDir, restoreDir, SNAPSHOT_NAME);
        PhoenixConfigurationUtil.setMRSnapshotManagedExternally(configuration, true);
      }
    }
  }

  private void snapshotCreateSync(TableName hbaseTableName,
      Admin admin, String snapshotName) throws IOException, InterruptedException {
    boolean isSnapshotCreated = false;
    SnapshotDescription snapshotDescription =
      new SnapshotDescription(snapshotName);
    // 3 retries while creating snapshot. if all 3 retries exhausted, we have
    // some valid issue.
    for (int i = 0; i < 3; i++) {
      if (isSnapshotCreated) {
        break;
      }
      if (i > 0) {
        LOGGER.info("Retry count {} for snapshot creation", i);
      }
      try {
        admin.snapshot(snapshotName, hbaseTableName);
      } catch (Exception e) {
        LOGGER.info("Snapshot creation failure for {}", snapshotName, e);
        continue;
      }
      // verify if snapshot was created in 10s
      for (int j = 0; j < 10; j++) {
        Thread.sleep(1000);
        try {
          if (admin.isSnapshotFinished(snapshotDescription)) {
            isSnapshotCreated = true;
            break;
          }
        } catch (Exception e) {
          LOGGER.error("Snapshot creation failed.", e);
          break;
        }
      }
    }
    if (!isSnapshotCreated) {
      throw new IOException("Snapshot creation failed for " + snapshotName);
    }
  }

  private void deleteSnapshotIfExists(String snapshotName) throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl());
         Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      List<SnapshotDescription> snapshotDescriptions = admin.listSnapshots();
      boolean isSnapshotPresent = false;
      if (CollectionUtils.isNotEmpty(snapshotDescriptions)) {
        for (SnapshotDescription snapshotDescription : snapshotDescriptions) {
          if (snapshotName.equals(snapshotDescription.getName())) {
            isSnapshotPresent = true;
            break;
          }
        }
      }
      // delete snapshot only if exists and it is not corrupted
      if (isSnapshotPresent) {
        admin.deleteSnapshot(snapshotName);
      } else {
        LOGGER.info("Snapshot {} does not exist. Possibly corrupted due to region movements.",
          snapshotName);
      }
    }
  }

  /**
   * Making sure that restore temp directory is not having multiple sub directories
   * for same snapshot restore.
   * @param conf
   * @param restoreDir
   * @param expectedCount
   * @throws IOException
   */
  private void assertRestoreDirCount(Configuration conf, String restoreDir, int expectedCount)
          throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] subDirectories = fs.listStatus(new Path(restoreDir));
    assertNotNull(subDirectories);
    if (isSnapshotRestoreDoneExternally) {
      //Snapshot Restore to be deleted externally by the caller
      assertEquals(expectedCount, subDirectories.length);
    } else {
      //Snapshot Restore already deleted internally
      assertEquals(0, subDirectories.length);
    }
  }

  public static class TableSnapshotMapper extends Mapper<NullWritable, PhoenixIndexDBWritable, ImmutableBytesWritable, NullWritable> {

    @Override
    protected void map(NullWritable key, PhoenixIndexDBWritable record, Context context)
        throws IOException, InterruptedException {
      final List<Object> values = record.getValues();
      result.add(values);

      // write dummy data
      context.write(new ImmutableBytesWritable(UUID.randomUUID().toString().getBytes()),
          NullWritable.get());
    }
  }

}
