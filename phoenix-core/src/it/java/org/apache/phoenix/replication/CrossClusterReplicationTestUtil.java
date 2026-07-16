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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.util.TestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared cross-cluster helpers for replication ITs (extracted from ReplicationLogGroupIT). */
public final class CrossClusterReplicationTestUtil {

  private static final Logger LOG =
    LoggerFactory.getLogger(CrossClusterReplicationTestUtil.class);

  private CrossClusterReplicationTestUtil() {
  }

  /** Recursively collect every ".plog" file under {@code dir}; empty list if {@code dir} absent. */
  public static List<Path> findLogFiles(Path dir, FileSystem fs) throws IOException {
    List<Path> files = new ArrayList<>();
    findLogFilesRecursive(dir, fs, files);
    return files;
  }

  private static void findLogFilesRecursive(Path dir, FileSystem fs, List<Path> files)
    throws IOException {
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
   * Assert the given HBase table has cell-identical rows (all versions) on cluster 1 ({@code conf1})
   * and cluster 2 ({@code conf2}). Dumps both tables on the first mismatch before failing.
   */
  public static void assertTablesEqualAcrossClusters(Configuration conf1, Configuration conf2,
    String hbaseTableName) throws Exception {
    TableName tn = TableName.valueOf(hbaseTableName);
    try (Connection hconn1 = ConnectionFactory.createConnection(conf1);
      Connection hconn2 = ConnectionFactory.createConnection(conf2);
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
}
