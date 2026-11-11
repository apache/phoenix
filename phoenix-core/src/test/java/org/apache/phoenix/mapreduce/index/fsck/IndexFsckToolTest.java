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
package org.apache.phoenix.mapreduce.index.fsck;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult.PhaseResult;
import org.apache.phoenix.mapreduce.index.IndexTool.IndexVerifyType;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.mapreduce.index.fsck.RowKeyFormatter.KeyFormat;
import org.apache.phoenix.mapreduce.index.fsck.ivf.IvfIndexFsckProvider;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests of {@link IndexFsckTool}: provider resolution, row key format, findings, retry,
 * command line validation, and repair rounds.
 */
public class IndexFsckToolTest {

  @Test
  public void testProviderDispatchGlobal() throws Exception {
    PTable globalIndex = new PTableImpl.Builder().setType(PTableType.INDEX)
      .setIndexType(IndexType.GLOBAL).setName(PNameFactory.newName("IDX_GLOBAL")).build();

    IndexFsckProvider provider = IndexFsckProviders.forIndex(globalIndex);
    assertNotNull(provider);
    assertTrue(provider instanceof GlobalIndexFsckProvider);
    assertFalse(provider instanceof IvfIndexFsckProvider);
  }

  @Test
  public void testProviderDispatchUncoveredGlobal() throws Exception {
    PTable uncoveredIndex =
      new PTableImpl.Builder().setType(PTableType.INDEX).setIndexType(IndexType.UNCOVERED_GLOBAL)
        .setName(PNameFactory.newName("IDX_UNCOVERED")).build();

    IndexFsckProvider provider = IndexFsckProviders.forIndex(uncoveredIndex);
    assertNotNull(provider);
    assertTrue(provider instanceof GlobalIndexFsckProvider);
    assertFalse(provider instanceof IvfIndexFsckProvider);
  }

  @Test
  public void testProviderDispatchIvfVector() throws Exception {
    PTable ivfIndex =
      new PTableImpl.Builder().setType(PTableType.INDEX).setIndexType(IndexType.VECTOR_GLOBAL)
        .setVectorIndexAlgorithm("IVF").setName(PNameFactory.newName("IDX_IVF")).build();

    IndexFsckProvider provider = IndexFsckProviders.forIndex(ivfIndex);
    assertNotNull(provider);
    assertTrue(provider instanceof IvfIndexFsckProvider);
  }

  @Test
  public void testProviderDispatchUnsupportedAlgorithm() throws Exception {
    PTable unknownAlgoIndex = new PTableImpl.Builder().setType(PTableType.INDEX)
      .setIndexType(IndexType.VECTOR_GLOBAL).setVectorIndexAlgorithm("UNKNOWN_ALGO")
      .setName(PNameFactory.newName("IDX_UNKNOWN")).build();

    try {
      IndexFsckProviders.forIndex(unknownAlgoIndex);
      fail("Expected UnsupportedOperationException for unknown algorithm");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("UNKNOWN_ALGO"));
    }
  }

  @Test
  public void testProviderDispatchMissingAlgorithm() throws Exception {
    PTable noAlgoIndex = new PTableImpl.Builder().setType(PTableType.INDEX)
      .setIndexType(IndexType.VECTOR_GLOBAL).setName(PNameFactory.newName("IDX_NO_ALGO")).build();

    try {
      IndexFsckProviders.forIndex(noAlgoIndex);
      fail("Expected IllegalArgumentException for missing algorithm");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("no algorithm"));
    }
  }

  @Test
  public void testProviderDispatchUnsupportedIndexTypeLocal() throws Exception {
    PTable localIndex = new PTableImpl.Builder().setType(PTableType.INDEX)
      .setIndexType(IndexType.LOCAL).setName(PNameFactory.newName("IDX_LOCAL")).build();

    try {
      IndexFsckProviders.forIndex(localIndex);
      fail("Expected UnsupportedOperationException for LOCAL index");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("LOCAL"));
    }
  }

  @Test
  public void testProviderDispatchNonIndexTable() throws Exception {
    PTable dataTable = new PTableImpl.Builder().setType(PTableType.TABLE)
      .setName(PNameFactory.newName("DATA_TBL")).build();

    try {
      IndexFsckProviders.forIndex(dataTable);
      fail("Expected UnsupportedOperationException for non-index table");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("TABLE"));
    }
  }

  @Test
  public void testProviderDispatchNull() {
    try {
      IndexFsckProviders.forIndex(null);
      fail("Expected IllegalArgumentException for null table");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testRowKeyFormatterHex() {
    byte[] key = new byte[] { 0x01, 0x02, (byte) 0xFE, (byte) 0xFF };
    String formatted = RowKeyFormatter.format(key, null, KeyFormat.HEX);
    assertEquals("0102feff", formatted);
  }

  @Test
  public void testRowKeyFormatterDecoded() throws Exception {
    PColumn col1 = new PColumnImpl(PNameFactory.newName("K1"), null, PVarchar.INSTANCE, null, null,
      false, 0, SortOrder.ASC, 0, null, false, null, false, false, null, 0);
    PColumn col2 = new PColumnImpl(PNameFactory.newName("K2"), null, PInteger.INSTANCE, null, null,
      false, 1, SortOrder.ASC, 0, null, false, null, false, false, null, 0);

    List<PColumn> columns = new ArrayList<>();
    columns.add(col1);
    columns.add(col2);

    RowKeySchemaBuilder builder = new RowKeySchemaBuilder(2);
    builder.addField(col1, false, SortOrder.ASC);
    builder.addField(col2, false, SortOrder.ASC);
    RowKeySchema schema = builder.build();

    PTable table = org.mockito.Mockito.mock(PTable.class);
    org.mockito.Mockito.when(table.getRowKeySchema()).thenReturn(schema);
    org.mockito.Mockito.when(table.getPKColumns()).thenReturn(columns);
    org.mockito.Mockito.when(table.getBucketNum()).thenReturn(null);

    // Make a composite key: a VARCHAR with a zero byte separator, then a 4-byte INTEGER
    byte[] part1 = PVarchar.INSTANCE.toBytes("ABC");
    byte[] part2 = PInteger.INSTANCE.toBytes(123);
    byte[] rowKey = new byte[part1.length + 1 + part2.length];
    System.arraycopy(part1, 0, rowKey, 0, part1.length);
    rowKey[part1.length] = 0x00; // separator after the VARCHAR value
    System.arraycopy(part2, 0, rowKey, part1.length + 1, part2.length);

    String formatted = RowKeyFormatter.format(rowKey, table, KeyFormat.DECODED);
    assertTrue(formatted.contains("K1=ABC"));
    assertTrue(formatted.contains("K2=123"));
  }

  /**
   * Tests that the counters of a verification phase map to findings with the correct severities. A
   * phase without defects gives no findings.
   */
  @Test
  public void testVerifyFindingsMapping() {
    PhaseResult phase = new PhaseResult();
    phase.setMissingIndexRowCount(3);
    phase.setInvalidIndexRowCount(2);
    phase.setExtraVerifiedIndexRowCount(5);
    phase.setExtraUnverifiedIndexRowCount(7);
    phase.setUnverifiedIndexRowCount(11);
    phase.setBeyondMaxLookBackMissingIndexRowCount(1);
    phase.setBeyondMaxLookBackInvalidIndexRowCount(1);
    List<Finding> findings = VerifyFindings.fromPhase(phase, "index");
    assertEquals(6, findings.size());
    assertFinding(findings.get(0), VerifyFindings.MISSING, Severity.ERROR, 3);
    assertFinding(findings.get(1), VerifyFindings.INVALID, Severity.ERROR, 2);
    assertFinding(findings.get(2), VerifyFindings.ORPHAN_VERIFIED, Severity.ERROR, 5);
    assertFinding(findings.get(3), VerifyFindings.ORPHAN_UNVERIFIED, Severity.INFO, 7);
    assertFinding(findings.get(4), VerifyFindings.UNVERIFIED, Severity.INFO, 11);
    assertFinding(findings.get(5), VerifyFindings.BEYOND_LOOKBACK, Severity.INFO, 2);
    assertEquals("index", findings.get(0).getDetails().get("source"));
    assertTrue(VerifyFindings.fromPhase(new PhaseResult(), "data").isEmpty());
  }

  private static void assertFinding(Finding finding, String rule, Severity severity, long count) {
    assertEquals(rule, finding.getRule());
    assertEquals(severity, finding.getSeverity());
    assertEquals(count, finding.getDetails().get("count"));
  }

  /** Tests that a step is retried after transient failures, and then succeeds. */
  @Test
  public void testRetryRecoversFromTransientFailures() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    String result = Retry.call("step", () -> {
      if (attempts.incrementAndGet() < 3) {
        throw new IOException("transient");
      }
      return "done";
    }, 0);
    assertEquals("done", result);
    assertEquals(3, attempts.get());
  }

  /**
   * Tests that a step that fails on each attempt runs one time plus {@code Retry.RETRIES} retries,
   * and then throws the last failure.
   */
  @Test
  public void testRetryGivesUpWhenRetriesAreExhausted() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    try {
      Retry.call("step", () -> {
        throw new IOException("attempt " + attempts.incrementAndGet());
      }, 0);
      fail("Expected the last failure");
    } catch (IOException e) {
      assertEquals("attempt " + (Retry.RETRIES + 1), e.getMessage());
    }
    assertEquals(Retry.RETRIES + 1, attempts.get());
  }

  /**
   * Tests that an interrupt is not retried, and that Retry sets the interrupt status of the thread
   * again.
   */
  @Test
  public void testRetryStopsOnInterrupt() throws Exception {
    for (Exception interrupt : new Exception[] { new InterruptedException(),
      new IOException(new InterruptedIOException()),
      new IOException(new ClosedByInterruptException()) }) {
      AtomicInteger attempts = new AtomicInteger();
      try {
        Retry.call("step", () -> {
          attempts.incrementAndGet();
          throw interrupt;
        }, 0);
        fail("Expected " + interrupt);
      } catch (Exception e) {
        assertEquals(interrupt, e);
      }
      assertEquals(1, attempts.get());
      assertTrue("The interrupt status is restored", Thread.interrupted());
    }
  }

  /**
   * Tests that socket and connect timeouts are retried and leave the interrupt status clear. These
   * timeouts are {@link InterruptedIOException}s, but they are not interrupts.
   */
  @Test
  public void testRetryRetriesTimeouts() throws Exception {
    for (Exception timeout : new Exception[] { new IOException(new SocketTimeoutException()),
      new ConnectTimeoutException("connect") }) {
      AtomicInteger attempts = new AtomicInteger();
      try {
        Retry.call("step", () -> {
          attempts.incrementAndGet();
          throw timeout;
        }, 0);
        fail("Expected " + timeout);
      } catch (Exception e) {
        assertEquals(timeout, e);
      }
      assertEquals(timeout.toString(), Retry.RETRIES + 1, attempts.get());
      assertFalse("The interrupt status is clear", Thread.interrupted());
    }
  }

  /** Tests that unrecoverable failures are not retried, so the step runs one time only. */
  @Test
  public void testRetryGivesUpOnUnrecoverableFailures() throws Exception {
    for (Exception failure : new Exception[] { new IllegalArgumentException("bad"),
      new RuntimeException(new TableNotFoundException("T")),
      new IOException(new AccessDeniedException("denied")),
      new DoNotRetryIOException("server says no") }) {
      AtomicInteger attempts = new AtomicInteger();
      try {
        Retry.call("step", () -> {
          attempts.incrementAndGet();
          throw failure;
        }, 0);
        fail("Expected " + failure);
      } catch (Exception e) {
        assertEquals(failure, e);
      }
      assertEquals(failure.toString(), 1, attempts.get());
    }
  }

  @Test
  public void testCommandLineValidation() throws Exception {
    IndexFsckTool tool = new IndexFsckTool();
    tool.setConf(new Configuration());
    assertEquals(-1, tool.run(new String[0]));
    assertEquals(-1, tool.run(new String[] { "check", "-dt", "T", "-it", "I" }));
    assertEquals(-1, tool.run(new String[] { "verify", "-dt", "T" }));
    assertEquals(-1, tool.run(new String[] { "verify", "now", "-dt", "T", "-it", "I" }));
    assertEquals(-1, tool.run(new String[] { "inspect", "-dt", "T", "-it", "I" }));
    assertEquals(-1, tool.run(new String[] { "verify", "-dt", "T", "-it", "I", "-x" }));
    assertEquals(-1, tool.run(new String[] { "verify", "-dt", "T", "-it", "I", "-st", "now" }));
    assertEquals(-1, tool.run(new String[] { "verify", "-dt", "T", "-it", "I", "-et", "1e3" }));
    assertEquals(-1, tool.run(new String[] { "verify", "-dt", "T", "-it", "I", "-of", "XML" }));
    assertEquals(-1, tool.run(new String[] { "verify", "-dt", "T", "-it", "I", "-k", "RAW" }));
    assertEquals(0, tool.run(new String[] { "-h" }));
  }

  private static IndexFsckContext context(String schema, String table, String index) {
    PTable data = Mockito.mock(PTable.class);
    Mockito.when(data.getSchemaName()).thenReturn(PNameFactory.newName(schema));
    Mockito.when(data.getTableName()).thenReturn(PNameFactory.newName(table));
    Mockito.when(data.getName())
      .thenReturn(PNameFactory.newName(SchemaUtil.getTableName(schema, table)));
    PTable idx = Mockito.mock(PTable.class);
    Mockito.when(idx.getTableName()).thenReturn(PNameFactory.newName(index));
    Mockito.when(idx.getName())
      .thenReturn(PNameFactory.newName(SchemaUtil.getTableName(schema, index)));
    return new IndexFsckContext(null, new Configuration(), data, idx, null, null, null,
      KeyFormat.DECODED, true);
  }

  private static Finding rows(String rule, long count) {
    Map<String, Object> details = new HashMap<>();
    details.put("count", count);
    return new Finding(Severity.ERROR, VerifyFindings.SCOPE, rule, rule, details);
  }

  /**
   * Tests that repair starts another round while the number of rows in error decreases, also if the
   * number of error findings does not decrease. Repair stops after a round without progress.
   */
  @Test
  public void testRepairContinuesWhileErrorRowsFall() throws Exception {
    List<List<Finding>> rounds = Arrays.asList(
      Arrays.asList(rows(VerifyFindings.MISSING, 10), rows(VerifyFindings.ORPHAN_VERIFIED, 5)),
      Arrays.asList(rows(VerifyFindings.MISSING, 3), rows(VerifyFindings.ORPHAN_VERIFIED, 2)),
      Arrays.asList(rows(VerifyFindings.MISSING, 1),
        new Finding(Severity.ERROR, "TABLE", "MISSING_COPROCESSOR", "no coprocessor"),
        new Finding(Severity.WARN, "TABLE", "INDEX_DISABLED", "disabled")),
      Arrays.asList(rows(VerifyFindings.MISSING, 1),
        new Finding(Severity.ERROR, "TABLE", "MISSING_COPROCESSOR", "no coprocessor")));
    AtomicInteger round = new AtomicInteger();
    GlobalIndexFsckProvider provider = new GlobalIndexFsckProvider() {
      @Override
      protected List<Finding> repairRound(IndexFsckContext context, RepairPlan plan, int n) {
        assertEquals(round.incrementAndGet(), n);
        return new ArrayList<>(rounds.get(n - 1));
      }
    };
    Report report = provider.repair(context("S", "T", "I"));
    assertEquals(4, round.get());
    assertEquals(rounds.get(3), report.getFindings());
  }

  /**
   * Tests the two IndexTool passes of a repair round. The pass from the data table uses
   * {@code -v BOTH}, which rewrites only the rows that fail verification. The pass from the index
   * table uses {@code -v AFTER -fi -do} to delete orphans. IndexTool gets quoted names, so the
   * names keep their case.
   */
  @Test
  public void testRepairPassesAndIndexToolArguments() throws Exception {
    IndexFsckContext context = context("s", "dt", "ix");
    List<String> passes = new ArrayList<>();
    GlobalIndexFsckProvider provider = new GlobalIndexFsckProvider() {
      @Override
      protected Counters runIndexToolOnce(IndexFsckContext ctx, IndexVerifyType verifyType,
        boolean fromIndex) {
        passes.add(String.join(" ", indexToolArgs(ctx, verifyType, fromIndex)));
        return new Counters();
      }
    };
    RepairPlan plan = new RepairPlan(false);
    provider.repairRows(context, plan, 1);
    assertEquals(Arrays.asList("-s \"s\" -dt \"dt\" -it \"ix\" -v BOTH -runfg",
      "-s \"s\" -dt \"dt\" -it \"ix\" -v AFTER -fi -do -runfg"), passes);
    assertTrue(
      plan.getActions().stream().allMatch(a -> a.getStatus() == RepairAction.Status.EXECUTED));
    assertEquals(Arrays.asList("-dt", "\"T\"", "-it", "\"I\"", "-v", "ONLY", "-runfg"),
      GlobalIndexFsckProvider.indexToolArgs(context("", "T", "I"), IndexVerifyType.ONLY, false));
  }

  /**
   * Tests that an interrupt of a repair pass ends the repair and sets the interrupt status of the
   * thread. The repair does not start another IndexTool run after the interrupt.
   */
  @Test
  public void testRepairPassStopsOnInterrupt() throws Exception {
    AtomicInteger runs = new AtomicInteger();
    GlobalIndexFsckProvider provider = new GlobalIndexFsckProvider() {
      @Override
      protected Counters runIndexToolOnce(IndexFsckContext ctx, IndexVerifyType verifyType,
        boolean fromIndex) throws Exception {
        runs.incrementAndGet();
        throw new IOException(new InterruptedIOException());
      }
    };
    try {
      provider.repairRows(context("S", "T", "I"), new RepairPlan(false), 1);
      fail("Expected the interrupt");
    } catch (IOException e) {
      assertTrue(e.getCause() instanceof InterruptedIOException);
    }
    assertEquals(1, runs.get());
    assertTrue("The interrupt status is restored", Thread.interrupted());
  }

  /**
   * Tests the failure of an IndexTool run. If the job is still active, the wait for the job ended
   * early. Then fsck kills the job and returns a failure that is not retried, because a retry can
   * ignore an interrupt. The interrupt status stays clear. If the job completed or did not start,
   * the failure can be retried.
   */
  @Test
  public void testIndexToolFailureOfUnfinishedJob() throws Exception {
    List<String> args = Arrays.asList("-v", "ONLY");
    Job running = Mockito.mock(Job.class);
    Mockito.when(running.getJobID()).thenReturn(new JobID("fsck", 1));
    Mockito.when(running.isComplete()).thenReturn(false);
    Exception failure = GlobalIndexFsckProvider.indexToolFailure(null, running, args, -1);
    assertTrue(failure.toString(), failure instanceof DoNotRetryIOException);
    Mockito.verify(running).killJob();
    AtomicInteger attempts = new AtomicInteger();
    try {
      Retry.call("step", () -> {
        attempts.incrementAndGet();
        throw failure;
      }, 0);
      fail("Expected the failure");
    } catch (DoNotRetryIOException e) {
      assertEquals(failure, e);
    }
    assertEquals(1, attempts.get());
    assertFalse("The interrupt status is clear", Thread.interrupted());

    Job completed = Mockito.mock(Job.class);
    Mockito.when(completed.getJobID()).thenReturn(new JobID("fsck", 2));
    Mockito.when(completed.isComplete()).thenReturn(true);
    assertTrue(GlobalIndexFsckProvider.indexToolFailure(null, completed, args,
      -1) instanceof IllegalStateException);
    Job unsubmitted = Mockito.mock(Job.class);
    assertTrue(GlobalIndexFsckProvider.indexToolFailure(null, unsubmitted, args,
      -1) instanceof IllegalStateException);
    Mockito.verify(completed, Mockito.never()).killJob();
    Mockito.verify(unsubmitted, Mockito.never()).killJob();
  }

  private static Result resultRow(long ts, String index, String region, byte[] qualifier,
    long value) {
    byte[] row = Bytes.toBytes(ts + "|" + index + "|" + region + "|start|stop");
    List<Cell> cells = new ArrayList<>();
    cells.add(new KeyValue(row, IndexVerificationResultRepository.RESULT_TABLE_COLUMN_FAMILY,
      qualifier, Bytes.toBytes(Long.toString(value))));
    return Result.create(cells);
  }

  /**
   * Tests that the last verification combines the latest run from the data table, which finds
   * missing and invalid rows, with the latest run from the index table, which ran after it.
   */
  @Test
  public void testLastVerificationCombinesDataAndIndexRuns() {
    List<Result> results = Arrays.asList(
      resultRow(50, "IDX", "DATA,,1.a.",
        IndexVerificationResultRepository.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES, 7),
      resultRow(100, "IDX", "DATA,,1.a.",
        IndexVerificationResultRepository.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES, 3),
      resultRow(100, "IDX", "DATA,k,2.b.",
        IndexVerificationResultRepository.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES, 1),
      resultRow(200, "IDX", "IDX,,3.c.",
        IndexVerificationResultRepository.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT_BYTES, 9),
      resultRow(300, "OTHER", "OTHER,,4.d.",
        IndexVerificationResultRepository.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES, 5));
    Finding finding = VerifyFindings.lastVerification(results, Bytes.toBytes("IDX"));
    assertEquals(Severity.WARN, finding.getSeverity());
    assertTrue(finding.getMessage(), finding.getMessage().contains(" 4 missing, invalid"));
    assertEquals(100L, ((Map<?, ?>) finding.getDetails().get("data")).get("scanMaxTs"));
    assertEquals(200L, ((Map<?, ?>) finding.getDetails().get("index")).get("scanMaxTs"));

    finding = VerifyFindings.lastVerification(results.subList(3, 4), Bytes.toBytes("IDX"));
    assertEquals(Severity.INFO, finding.getSeverity());
    assertFalse(finding.getDetails().containsKey("data"));
    assertEquals(null,
      VerifyFindings.lastVerification(results.subList(4, 5), Bytes.toBytes("IDX")));

    // The result of a run that repaired and then verified comes from its after phase. This applies
    // to the data pass and to the index pass. The before phase of the index pass counts the
    // orphans that the pass deleted.
    List<Result> repaired = Arrays.asList(
      resultRow(500, "IDX", "IDX,,3.c.",
        IndexVerificationResultRepository.BEFORE_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES, 4),
      resultRow(500, "IDX", "IDX,,3.c.",
        IndexVerificationResultRepository.AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES, 9),
      resultRow(500, "IDX", "IDX,,3.c.",
        IndexVerificationResultRepository.AFTER_REPAIR_EXTRA_VERIFIED_INDEX_ROW_COUNT_BYTES, 0),
      resultRow(400, "IDX", "DATA,,1.a.",
        IndexVerificationResultRepository.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES, 3),
      resultRow(400, "IDX", "DATA,,1.a.",
        IndexVerificationResultRepository.AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES, 3),
      resultRow(400, "IDX", "DATA,,1.a.",
        IndexVerificationResultRepository.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES, 0),
      resultRow(400, "IDX", "DATA,k,2.b.",
        IndexVerificationResultRepository.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_BYTES, 2),
      resultRow(400, "IDX", "DATA,k,2.b.",
        IndexVerificationResultRepository.AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES, 0));
    finding = VerifyFindings.lastVerification(repaired, Bytes.toBytes("IDX"));
    assertEquals(finding.getMessage(), Severity.INFO, finding.getSeverity());
    Map<?, ?> data = (Map<?, ?>) finding.getDetails().get("data");
    assertEquals(3L, ((Map<?, ?>) data.get("before")).get("missing"));
    assertEquals(0L, ((Map<?, ?>) data.get("after")).get("missing"));
    Map<?, ?> index = (Map<?, ?>) finding.getDetails().get("index");
    assertEquals(500L, index.get("scanMaxTs"));
    assertEquals(4L, ((Map<?, ?>) index.get("before")).get("orphanVerified"));
    assertEquals(0L, ((Map<?, ?>) index.get("after")).get("orphanVerified"));

    // A row that the repair cannot fix fails both phases, and the summary counts it one time
    repaired = Arrays.asList(
      resultRow(400, "IDX", "DATA,,1.a.",
        IndexVerificationResultRepository.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES, 1),
      resultRow(400, "IDX", "DATA,,1.a.",
        IndexVerificationResultRepository.AFTER_REBUILD_VALID_INDEX_ROW_COUNT_BYTES, 0),
      resultRow(400, "IDX", "DATA,,1.a.",
        IndexVerificationResultRepository.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT_BYTES, 1));
    finding = VerifyFindings.lastVerification(repaired, Bytes.toBytes("IDX"));
    assertEquals(Severity.WARN, finding.getSeverity());
    assertTrue(finding.getMessage(), finding.getMessage().contains(" 1 missing, invalid"));
  }
}
