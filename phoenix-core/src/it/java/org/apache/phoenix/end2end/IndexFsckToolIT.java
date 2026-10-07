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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.index.fsck.Finding;
import org.apache.phoenix.mapreduce.index.fsck.GlobalIndexFsckProvider;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckTool;
import org.apache.phoenix.mapreduce.index.fsck.RepairAction;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.mapreduce.index.fsck.Severity;
import org.apache.phoenix.mapreduce.index.fsck.VerifyFindings;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.IndexScrutiny;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** End-to-end integration tests for global index verification, fsck, and repair. */
@Category(ParallelStatsDisabledTest.class)
public class IndexFsckToolIT extends ParallelStatsDisabledIT {

  static Report run(int expectedStatus, String... args) throws Exception {
    IndexFsckTool tool = new IndexFsckTool();
    tool.setConf(config);
    tool.setOutStream(new PrintStream(new ByteArrayOutputStream()));
    assertEquals(String.join(" ", args), expectedStatus, tool.run(args));
    return tool.getLastReport();
  }

  /** Finds the first report finding matching the specified rule and optional source table. */
  static Finding finding(Report report, String rule, String source) {
    return report.getFindings().stream()
      .filter(f -> f.getRule().equals(rule)
        && (source == null || source.equals(f.getDetails().get("source"))))
      .findFirst().orElseThrow(
        () -> new AssertionError(rule + " from " + source + " not in " + report.getFindings()));
  }

  static long count(Report report, String rule, String source) {
    return ((Number) finding(report, rule, source).getDetails().get("count")).longValue();
  }

  static long errors(Report report) {
    return report.getCount(Severity.ERROR);
  }

  /**
   * Dumps all raw cell versions and delete markers from the physical table for assertion checking.
   */
  static List<String> rawCells(PhoenixConnection conn, PTable table) throws Exception {
    Scan scan = new Scan();
    scan.setRaw(true);
    scan.readAllVersions();
    List<String> cells = new ArrayList<>();
    try (Table hTable = conn.getQueryServices().getTable(table.getPhysicalName().getBytes());
      ResultScanner scanner = hTable.getScanner(scan)) {
      for (Result result : scanner) {
        for (Cell cell : result.rawCells()) {
          cells.add(cell.toString() + "/" + Bytes.toStringBinary(CellUtil.cloneValue(cell)));
        }
      }
    }
    return cells;
  }

  @Test
  public void testVerifyAndRepairGlobalIndex() throws Exception {
    String dataTable = generateUniqueName();
    String indexTable = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + dataTable
        + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexTable + " ON " + dataTable + " (VAL1) INCLUDE (VAL2)");
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + dataTable + " VALUES (?, ?, ?)")) {
        for (int i = 1; i <= 4; i++) {
          ps.setInt(1, i);
          ps.setInt(2, i * 10);
          ps.setInt(3, i * 100);
          ps.execute();
        }
      }
      conn.commit();
      // Inject synthetic orphan index entries without corresponding data rows
      try (PreparedStatement ps =
        conn.prepareStatement("UPSERT INTO " + indexTable + " VALUES (?, ?, ?)")) {
        for (int i : new int[] { 5, 6 }) {
          ps.setInt(1, i * 10);
          ps.setInt(2, i);
          ps.setInt(3, i * 100);
          ps.execute();
        }
      }
      conn.commit();
      run(0, "verify", "-dt", dataTable, "-it", indexTable);

      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable data = pconn.getTable(dataTable);
      PTable index = pconn.getTable(indexTable);
      IndexMaintainer maintainer = index.getIndexMaintainer(data, pconn);
      byte[] emptyFamily = maintainer.getEmptyKeyValueFamily().copyBytesIfNecessary();
      byte[] emptyQualifier = maintainer.getEmptyKeyValueQualifier();
      try (
        Table indexHTable = pconn.getQueryServices().getTable(index.getPhysicalName().getBytes())) {
        Result[] rows = new Result[7];
        try (ResultScanner scanner = indexHTable.getScanner(new Scan())) {
          for (Result r : scanner) {
            byte[] dataKey =
              maintainer.buildDataRowKey(new ImmutableBytesWritable(r.getRow()), null);
            rows[(Integer) PInteger.INSTANCE.toObject(dataKey)] = r;
          }
        }
        // Inject inconsistencies: missing index row, invalid covered value, and verified/
        // unverified orphans.
        try (
          Table dataHTable = pconn.getQueryServices().getTable(data.getPhysicalName().getBytes())) {
          Result row1 = dataHTable.get(new Get(PInteger.INSTANCE.toBytes(1)));
          Put put = new Put(PInteger.INSTANCE.toBytes(7));
          for (Cell cell : row1.rawCells()) {
            put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
              CellUtil.cloneValue(cell));
          }
          dataHTable.put(put);
        }
        for (Cell cell : rows[2].rawCells()) {
          if (!CellUtil.matchingQualifier(cell, emptyQualifier)) {
            Put put = new Put(rows[2].getRow());
            put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
              cell.getTimestamp(), PInteger.INSTANCE.toBytes(99999));
            indexHTable.put(put);
          }
        }
        for (int i : new int[] { 5, 6 }) {
          Put put = new Put(rows[i].getRow());
          put.addColumn(emptyFamily, emptyQualifier, rows[i].rawCells()[0].getTimestamp(),
            i == 5 ? QueryConstants.VERIFIED_BYTES : QueryConstants.UNVERIFIED_BYTES);
          indexHTable.put(put);
        }
        getUtility().getAdmin().flush(TableName.valueOf(index.getPhysicalName().getBytes()));

        Report verify = run(1, "verify", "-dt", dataTable, "-it", indexTable);
        assertEquals(1, count(verify, VerifyFindings.MISSING, "data"));
        assertEquals(1, count(verify, VerifyFindings.INVALID, "data"));
        assertEquals(1, count(verify, VerifyFindings.ORPHAN_VERIFIED, "index"));
        assertEquals(Severity.INFO,
          finding(verify, VerifyFindings.ORPHAN_UNVERIFIED, "index").getSeverity());
        assertEquals(1, count(verify, VerifyFindings.ORPHAN_UNVERIFIED, "index"));

        Report fsck = run(0, "fsck", "-dt", dataTable, "-it", indexTable);
        assertEquals(Severity.WARN, finding(fsck, VerifyFindings.LAST_VERIFY, null).getSeverity());

        // Verify dry run planning without executing mutations
        List<String> before = rawCells(pconn, index);
        Report dryRun = run(1, "repair", "-dt", dataTable, "-it", indexTable);
        assertTrue(dryRun.getRepairPlan().isDryRun());
        assertEquals(
          Arrays.asList(GlobalIndexFsckProvider.REBUILD_INDEX_ROWS,
            GlobalIndexFsckProvider.DELETE_ORPHAN_ROWS),
          dryRun.getRepairPlan().getActions().stream().map(RepairAction::getAction)
            .collect(Collectors.toList()));
        assertTrue(dryRun.getRepairPlan().getActions().stream()
          .allMatch(a -> a.getStatus() == RepairAction.Status.PLANNED));
        assertEquals(before, rawCells(pconn, index));

        Report repair = run(0, "repair", "-dt", dataTable, "-it", indexTable, "--confirm");
        RepairAction deleteOrphans =
          repair.getRepairPlan().get(GlobalIndexFsckProvider.DELETE_ORPHAN_ROWS);
        assertEquals(RepairAction.Status.EXECUTED, deleteOrphans.getStatus());
        assertEquals(1L, deleteOrphans.getDetails().get("deletedOrphanRows"));
        assertEquals(0, errors(repair));

        assertTrue(indexHTable.get(new Get(rows[5].getRow())).isEmpty());
        assertFalse("An unverified orphan is left to read repair",
          indexHTable.get(new Get(rows[6].getRow())).isEmpty());
        for (String cell : rawCells(pconn, index)) {
          assertFalse(cell, cell.contains("/DeleteFamily/") || cell.contains("/DeleteColumn/"));
        }
      }
      Report after = run(0, "verify", "-dt", dataTable, "-it", indexTable);
      assertEquals(0, errors(after));
      assertEquals(1, count(after, VerifyFindings.ORPHAN_UNVERIFIED, "index"));
      // Scrutiny scan triggers read repair to prune the remaining unverified orphan
      assertEquals(5, IndexScrutiny.scrutinizeIndex(conn, dataTable, indexTable));
    }
  }

  /**
   * Verifies that disabled indexes are rejected for row verification to prevent undesired state
   * transitions.
   */
  @Test
  public void testDisabledIndexIsNotVerified() throws Exception {
    String dataTable = generateUniqueName();
    String indexTable = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + dataTable + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER)");
      conn.createStatement().execute("CREATE INDEX " + indexTable + " ON " + dataTable + " (VAL1)");
      conn.createStatement().execute("ALTER INDEX " + indexTable + " ON " + dataTable + " DISABLE");
      for (String command : new String[] { "verify", "repair" }) {
        Report report = run(1, command, "-dt", dataTable, "-it", indexTable, "--confirm");
        finding(report, GlobalIndexFsckProvider.ROWS_NOT_VERIFIED, null);
        finding(report, GlobalIndexFsckProvider.INDEX_DISABLED, null);
      }
      assertEquals(PIndexState.DISABLE,
        conn.unwrap(PhoenixConnection.class).getTableNoCache(indexTable).getIndexState());
    }
  }

  @Test
  public void testUnsupportedIndexesAndArguments() throws Exception {
    String dataTable = generateUniqueName();
    String localIndex = generateUniqueName();
    String vectorIndex = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + dataTable
        + " (ID VARCHAR NOT NULL PRIMARY KEY, VAL1 INTEGER, V VECTOR(FLOAT, 4))");
      conn.createStatement()
        .execute("CREATE LOCAL INDEX " + localIndex + " ON " + dataTable + " (VAL1)");
      conn.createStatement().execute("CREATE VECTOR INDEX " + vectorIndex + " ON " + dataTable
        + " (V) WITH (algorithm = 'IVF', metric = 'L2', lists = 2, sample_size = 10)");
    }
    assertEquals(null, run(-1, "verify", "-dt", dataTable, "-it", localIndex));
    run(-1, "inspect", "-dt", dataTable, "-it", localIndex);
    run(-1, "-dt", dataTable, "-it", vectorIndex);
    run(-1, "verify", "extra", "-dt", dataTable, "-it", vectorIndex);

    IndexScrutinyTool scrutiny = new IndexScrutinyTool();
    scrutiny.setConf(config);
    assertEquals(-1, scrutiny.run(new String[] { "-dt", dataTable, "-it", vectorIndex }));
  }
}
