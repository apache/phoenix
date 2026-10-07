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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult.PhaseResult;
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
import org.junit.Test;

/**
 * Unit tests for {@link IndexFsckTool}, provider resolution, row key formatting, findings, and
 * retry mechanics.
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

    // Build composite key with null-terminated VARCHAR and 4-byte INTEGER
    byte[] part1 = PVarchar.INSTANCE.toBytes("ABC");
    byte[] part2 = PInteger.INSTANCE.toBytes(123);
    byte[] rowKey = new byte[part1.length + 1 + part2.length];
    System.arraycopy(part1, 0, rowKey, 0, part1.length);
    rowKey[part1.length] = 0x00; // separator
    System.arraycopy(part2, 0, rowKey, part1.length + 1, part2.length);

    String formatted = RowKeyFormatter.format(rowKey, table, KeyFormat.DECODED);
    assertTrue(formatted.contains("K1=ABC"));
    assertTrue(formatted.contains("K2=123"));
  }

  /**
   * Tests mapping from verification phase counters to structured findings with appropriate
   * severities.
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

  /** Tests that transient exceptions trigger exponential backoff retry and eventual success. */
  @Test
  public void testRetryRecoversFromTransientFailures() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    String result = Retry.call("step", () -> {
      if (attempts.incrementAndGet() < 3) {
        throw new IOException("transient");
      }
      return "done";
    });
    assertEquals("done", result);
    assertEquals(3, attempts.get());
  }

  /** Tests that non-transient exceptions terminate immediately without retry. */
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
        });
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
    assertEquals(0, tool.run(new String[] { "-h" }));
  }
}
