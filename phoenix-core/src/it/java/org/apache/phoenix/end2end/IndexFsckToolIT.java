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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.index.IndexTool.IndexVerifyType;
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

/** End-to-end tests of the verify, fsck, and repair commands on global indexes. */
@Category(ParallelStatsDisabledTest.class)
public class IndexFsckToolIT extends ParallelStatsDisabledIT {

  static Report run(int expectedStatus, String... args) throws Exception {
    IndexFsckTool tool = new IndexFsckTool();
    tool.setConf(config);
    tool.setOutStream(new PrintStream(new ByteArrayOutputStream()));
    assertEquals(String.join(" ", args), expectedStatus, tool.run(args));
    return tool.getLastReport();
  }

  /**
   * Returns the first finding of the report with the given rule and, if {@code source} is not null,
   * with that source. The test fails if no finding matches.
   */
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
   * Returns all raw cells of the physical table, with all versions and delete markers, as strings
   * that a test can compare.
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
      // Write orphan index rows, which have no data rows
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
        // Make these defects: a data row without an index row, an index row with an invalid
        // covered value, one verified orphan, and one unverified orphan.
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

        // A dry run plans the repair actions but does not change the index table
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
      // A scrutiny scan starts read repair, which deletes the remaining unverified orphan
      assertEquals(5, IndexScrutiny.scrutinizeIndex(conn, dataTable, indexTable));
    }
  }

  /**
   * Verifies that verify and repair do not verify the rows of a disabled index, because an
   * IndexTool run changes the index state to BUILDING. The index must stay DISABLE.
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

  /**
   * Delays the write of a data row until the repair scanner of {@code -fi -v AFTER -do} reads the
   * data rows of its task. This simulates a write in flight at the scan boundary. Such a write
   * commits its data row before it marks its index row verified.
   */
  public static class WriteInFlightObserver implements RegionCoprocessor, RegionObserver {
    static final AtomicReference<Put> PENDING = new AtomicReference<>();

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
      InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
      Put pending = PENDING.getAndSet(null);
      if (pending != null) {
        c.getEnvironment().getRegion().put(pending);
      }
      return hasNext;
    }
  }

  /**
   * Verifies that {@code -do} keeps a verified index row whose data row commits after the repair
   * scanner reads the data rows and before it reads the index rows again.
   */
  @Test
  public void testDeleteOrphansKeepsRowOfWriteInFlight() throws Exception {
    String dataTable = generateUniqueName();
    String indexTable = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + dataTable
        + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexTable + " ON " + dataTable + " (VAL1) INCLUDE (VAL2)");
      conn.createStatement().execute("UPSERT INTO " + dataTable + " VALUES (1, 10, 100)");
      conn.commit();
      // Write and verify the index row of data row 2 before its data row exists
      conn.createStatement().execute("UPSERT INTO " + indexTable + " VALUES (10, 2, 100)");
      conn.commit();
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
      PTable data = pconn.getTable(dataTable);
      PTable index = pconn.getTable(indexTable);
      IndexMaintainer maintainer = index.getIndexMaintainer(data, pconn);
      TableName dataName = TableName.valueOf(data.getPhysicalName().getBytes());
      try (Table dataHTable = pconn.getQueryServices().getTable(dataName.getName());
        Table indexHTable = pconn.getQueryServices().getTable(index.getPhysicalName().getBytes())) {
        Result orphan = null;
        try (ResultScanner scanner = indexHTable.getScanner(new Scan())) {
          for (Result r : scanner) {
            byte[] dataKey =
              maintainer.buildDataRowKey(new ImmutableBytesWritable(r.getRow()), null);
            if ((Integer) PInteger.INSTANCE.toObject(dataKey) == 2) {
              orphan = r;
            }
          }
        }
        long ts = orphan.rawCells()[0].getTimestamp();
        Put verified = new Put(orphan.getRow());
        verified.addColumn(maintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
          maintainer.getEmptyKeyValueQualifier(), ts, QueryConstants.VERIFIED_BYTES);
        indexHTable.put(verified);
        Put pending = new Put(PInteger.INSTANCE.toBytes(2));
        for (Cell cell : dataHTable.get(new Get(PInteger.INSTANCE.toBytes(1))).rawCells()) {
          pending.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), ts,
            CellUtil.cloneValue(cell));
        }
        Admin admin = getUtility().getAdmin();
        admin.modifyTable(TableDescriptorBuilder.newBuilder(admin.getDescriptor(dataName))
          .setCoprocessor(WriteInFlightObserver.class.getName()).build());
        WriteInFlightObserver.PENDING.set(pending);
        try {
          IndexToolIT.runIndexTool(false, null, dataTable, indexTable, null, 0,
            IndexVerifyType.AFTER, "-fi", "-do");
          assertNull("The data row was written during the repair",
            WriteInFlightObserver.PENDING.get());
        } finally {
          WriteInFlightObserver.PENDING.set(null);
        }
        assertFalse(dataHTable.get(new Get(PInteger.INSTANCE.toBytes(2))).isEmpty());
        assertFalse("The index row of the write in flight is kept",
          indexHTable.get(new Get(orphan.getRow())).isEmpty());
      }
      assertEquals(2, IndexScrutiny.scrutinizeIndex(conn, dataTable, indexTable));
    }
  }

  /**
   * Verifies that case-sensitive names reach IndexTool with their case, and that IndexScrutinyTool
   * resolves a case-sensitive index in the default schema when it checks for a vector index.
   */
  @Test
  public void testCaseSensitiveNames() throws Exception {
    String schema = generateUniqueName().toLowerCase();
    String dataTable = generateUniqueName().toLowerCase();
    String indexTable = generateUniqueName().toLowerCase();
    String plainTable = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      for (String qDataTable : new String[] { "\"" + schema + "\".\"" + dataTable + "\"",
        plainTable }) {
        conn.createStatement().execute("CREATE TABLE " + qDataTable
          + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER)");
        conn.createStatement().execute(
          "CREATE INDEX \"" + indexTable + "\" ON " + qDataTable + " (VAL1) INCLUDE (VAL2)");
        conn.createStatement().execute("UPSERT INTO " + qDataTable + " VALUES (1, 10, 100)");
        conn.commit();
      }
    }
    Report verify = run(0, "verify", "-s", "\"" + schema + "\"", "-dt", "\"" + dataTable + "\"",
      "-it", "\"" + indexTable + "\"");
    assertEquals(verify.toText(), 0, verify.getFindings().size());

    IndexScrutinyTool scrutiny = new IndexScrutinyTool();
    // Give the tool a copy, because the tool sets its scan timestamp in its configuration
    scrutiny.setConf(new Configuration(config));
    // The queries of IndexScrutinyTool do not quote names, so only its job setup can succeed. The
    // job setup resolves the index in the same way as the check for a vector index.
    scrutiny.run(new String[] { "-dt", plainTable, "-it", indexTable, "-t",
      String.valueOf(Long.MAX_VALUE), "-run-foreground", "-src", "DATA_TABLE_SOURCE" });
    assertEquals(1, scrutiny.getJobs().size());
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
