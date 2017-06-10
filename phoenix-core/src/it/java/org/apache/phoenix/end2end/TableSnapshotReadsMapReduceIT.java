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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.PhoenixIndexDBWritable;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.junit.*;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableSnapshotReadsMapReduceIT extends ParallelStatsDisabledIT {
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


  @Test
  public void testMapReduceSnapshots() throws Exception {
    // create table
    Connection conn = DriverManager.getConnection(getUrl());
    tableName = generateUniqueName();
    conn.createStatement().execute(String.format(CREATE_TABLE, tableName));
    conn.commit();

    // configure Phoenix M/R job to read snapshot
    final Configuration conf = getUtility().getConfiguration();
    Job job = Job.getInstance(conf);
    Path tmpDir = getUtility().getRandomDir();

    PhoenixMapReduceUtil.setInput(job,PhoenixIndexDBWritable.class,SNAPSHOT_NAME,tableName,tmpDir, null, FIELD1, FIELD2, FIELD3);

    // configure and test job
    configureJob(job, tableName, null, null);
  }

  @Test
  public void testMapReduceSnapshotsWithCondition() throws Exception {
    // create table
    Connection conn = DriverManager.getConnection(getUrl());
    tableName = generateUniqueName();
    conn.createStatement().execute(String.format(CREATE_TABLE, tableName));
    conn.commit();

    // configure Phoenix M/R job to read snapshot
    final Configuration conf = getUtility().getConfiguration();
    Job job = Job.getInstance(conf);
    Path tmpDir = getUtility().getRandomDir();
    PhoenixMapReduceUtil.setInput(job,PhoenixIndexDBWritable.class,SNAPSHOT_NAME,tableName,tmpDir, FIELD3 + " > 0001", FIELD1, FIELD2, FIELD3);

    // configure and test job
    configureJob(job, tableName, null, "FIELD3 > 0001");

  }

  @Test
  public void testMapReduceSnapshotWithLimit() throws Exception {
    // create table
    Connection conn = DriverManager.getConnection(getUrl());
    tableName = generateUniqueName();
    conn.createStatement().execute(String.format(CREATE_TABLE, tableName));
    conn.commit();

    // configure Phoenix M/R job to read snapshot
    final Configuration conf = getUtility().getConfiguration();
    Job job = Job.getInstance(conf);
    Path tmpDir = getUtility().getRandomDir();
    // Running limit with order by on non pk column
    String inputQuery = "SELECT * FROM " + tableName + " ORDER BY FIELD2 LIMIT 1";
    PhoenixMapReduceUtil.setInput(job,PhoenixIndexDBWritable.class,SNAPSHOT_NAME,tableName,tmpDir,inputQuery);

    // configure and test job
    configureJob(job, tableName, inputQuery, null);
  }

  private void configureJob(Job job, String tableName, String inputQuery, String condition) throws Exception {
    try {
      upsertAndSnapshot(tableName);
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
      deleteSnapshotAndTable(tableName);
    }
  }

  private void upsertData(String tableName) throws SQLException {
    Connection conn = DriverManager.getConnection(getUrl());
    PreparedStatement stmt = conn.prepareStatement(String.format(UPSERT, tableName));
    upsertData(stmt, "CCCC", "SSDD", 0001);
    upsertData(stmt, "CCCC", "HDHG", 0005);
    upsertData(stmt, "BBBB", "JSHJ", 0002);
    upsertData(stmt, "AAAA", "JHHD", 0003);
    conn.commit();
    timestamp = System.currentTimeMillis();
  }

  private void upsertData(PreparedStatement stmt, String field1, String field2, int field3) throws SQLException {
    stmt.setString(1, field1);
    stmt.setString(2, field2);
    stmt.setInt(3, field3);
    stmt.execute();
  }

  public void upsertAndSnapshot(String tableName) throws Exception {
    upsertData(tableName);

    Connection conn = DriverManager.getConnection(getUrl());
    HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
    admin.snapshot(SNAPSHOT_NAME, TableName.valueOf(tableName));
    // call flush to create new files in the region
    admin.flush(tableName);

    List<HBaseProtos.SnapshotDescription> snapshots = admin.listSnapshots();
    Assert.assertEquals(tableName, snapshots.get(0).getTable());

    // upsert data after snapshot
    PreparedStatement stmt = conn.prepareStatement(String.format(UPSERT, tableName));
    upsertData(stmt, "DDDD", "SNFB", 0004);
    conn.commit();
  }

  public void deleteSnapshotAndTable(String tableName) throws Exception {
    Connection conn = DriverManager.getConnection(getUrl());
    HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
    admin.deleteSnapshot(SNAPSHOT_NAME);

    conn.createStatement().execute("DROP TABLE " + tableName);
    conn.close();

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
