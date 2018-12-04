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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.PhoenixIndexDBWritable;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableSnapshotReadsMapReduceIT extends BaseUniqueNamesOwnClusterIT {

  private static final Logger logger = LoggerFactory.getLogger(TableSnapshotReadsMapReduceIT.class);

  private final static String SNAPSHOT_NAME = "FOO";
  private static final String FIELD1 = "FIELD1";
  private static final String FIELD2 = "FIELD2";
  private static final String FIELD3 = "FIELD3";
  private String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s ( " +
      " FIELD1 VARCHAR NOT NULL , FIELD2 VARCHAR , FIELD3 INTEGER CONSTRAINT pk PRIMARY KEY (FIELD1 ))";
  private String UPSERT = "UPSERT into %s values (?, ?, ?)";

  private static List<List<Object>> result;
  private long timestamp;
  private String tableName;
  private Job job;
  private Path tmpDir;
  private Configuration conf;

  @BeforeClass
  public static void doSetup() throws Exception {
      Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
      setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Before
  public void before() throws SQLException, IOException {
    // create table
    Connection conn = DriverManager.getConnection(getUrl());
    tableName = generateUniqueName();
    conn.createStatement().execute(String.format(CREATE_TABLE, tableName));
    conn.commit();

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

  private void configureJob(Job job, String tableName, String inputQuery, String condition, boolean shouldSplit) throws Exception {
    try {
      upsertAndSnapshot(tableName, shouldSplit);
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
    } finally {
      deleteSnapshot(tableName);
    }
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

  private void upsertData(PreparedStatement stmt, String field1, String field2, int field3) throws SQLException {
    stmt.setString(1, field1);
    stmt.setString(2, field2);
    stmt.setInt(3, field3);
    stmt.execute();
  }

  private void upsertAndSnapshot(String tableName, boolean shouldSplit) throws Exception {
    upsertData(tableName);

    TableName hbaseTableName = TableName.valueOf(tableName);
    Connection conn = DriverManager.getConnection(getUrl());
    Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();

    if (shouldSplit) {
      splitTableSync(admin, hbaseTableName, "BBBB".getBytes(), 2);
    }

    admin.snapshot(SNAPSHOT_NAME, hbaseTableName);

    List<SnapshotDescription> snapshots = admin.listSnapshots();
    Assert.assertEquals(tableName, snapshots.get(0).getTable());

    // Capture the snapshot timestamp to use as SCN while reading the table later
    // Assigning the timestamp value here will make tests less flaky
    timestamp = System.currentTimeMillis();

    // upsert data after snapshot
    PreparedStatement stmt = conn.prepareStatement(String.format(UPSERT, tableName));
    upsertData(stmt, "DDDD", "SNFB", 45);
    conn.commit();
  }

  private void splitTableSync(Admin admin, TableName hbaseTableName,
                              byte[] splitPoint , int expectedRegions) throws IOException, InterruptedException {
    admin.split(hbaseTableName, splitPoint);
    for (int i = 0; i < 100; i++) {
      List<HRegionInfo> hRegionInfoList = admin.getTableRegions(hbaseTableName);
      if (hRegionInfoList.size() >= expectedRegions) {
        break;
      }
      logger.info("Sleeping for 1000 ms while waiting for " + hbaseTableName.getNameAsString() + " to split");
      Thread.sleep(1000);
    }
  }

  private void deleteSnapshot(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Admin admin =
                        conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();) {
            admin.deleteSnapshot(SNAPSHOT_NAME);
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
