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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer.CellDriftCounts;
import org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer.ChunkRepairRequest;
import org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer.ChunkRepairResult;
import org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer.DriftCounters;
import org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer.RowDriftInfo;
import org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer.RowMirrorStatus;
import org.apache.phoenix.mapreduce.PhoenixSyncTableChunkRepairer.TargetRowRecord;
import org.junit.Test;

/**
 * Unit tests for the pure-data inner classes of {@link PhoenixSyncTableChunkRepairer}.
 * Orchestration paths (repair/dryRun walks, scan construction, batch flushing) are covered by
 * {@code PhoenixSyncTableToolIT}; this file pins the data-class invariants so a regression there
 * fails as a localized unit-test failure rather than a counter mismatch many layers up.
 */
public class PhoenixSyncTableChunkRepairerTest {

  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] CF = Bytes.toBytes("0");
  private static final byte[] CF2 = Bytes.toBytes("1");
  private static final byte[] Q_NAME = Bytes.toBytes("NAME");
  private static final byte[] Q_VALUE = Bytes.toBytes("NAME_VALUE");
  private static final byte[] V_ALICE = Bytes.toBytes("alice");

  @Test
  public void columnKeyEqualsHonorsByteArrayContent() {
    PhoenixSyncTableChunkRepairer.ColumnKey a =
      new PhoenixSyncTableChunkRepairer.ColumnKey(CF, Q_NAME);
    PhoenixSyncTableChunkRepairer.ColumnKey b =
      new PhoenixSyncTableChunkRepairer.ColumnKey(Bytes.toBytes("0"), Bytes.toBytes("NAME"));
    assertEquals("Distinct byte[] copies with the same content must be equal", a, b);
    assertEquals("hashCode must match equality", a.hashCode(), b.hashCode());
  }

  @Test
  public void columnKeyDistinguishesFamilyAndQualifier() {
    PhoenixSyncTableChunkRepairer.ColumnKey nameInCf =
      new PhoenixSyncTableChunkRepairer.ColumnKey(CF, Q_NAME);
    PhoenixSyncTableChunkRepairer.ColumnKey nameInOtherCf =
      new PhoenixSyncTableChunkRepairer.ColumnKey(CF2, Q_NAME);
    PhoenixSyncTableChunkRepairer.ColumnKey valueInCf =
      new PhoenixSyncTableChunkRepairer.ColumnKey(CF, Q_VALUE);
    assertNotEquals("Same qualifier in different family must not collide", nameInCf, nameInOtherCf);
    assertNotEquals("Same family with different qualifier must not collide", nameInCf, valueInCf);
  }

  @Test
  public void columnKeyEqualsRejectsForeignType() {
    PhoenixSyncTableChunkRepairer.ColumnKey k =
      new PhoenixSyncTableChunkRepairer.ColumnKey(CF, Q_NAME);
    assertNotEquals(k, "not-a-key");
    assertNotEquals(k, null);
  }

  @Test
  public void columnKeyOfCellMatchesExplicitConstruction() {
    Cell cell = new KeyValue(ROW, CF, Q_NAME, 100L, V_ALICE);
    assertEquals(new PhoenixSyncTableChunkRepairer.ColumnKey(CF, Q_NAME),
      PhoenixSyncTableChunkRepairer.ColumnKey.of(cell));
  }

  @Test
  public void wouldShadowReturnsFalseOnEmptyRecord() {
    TargetRowRecord rec = new TargetRowRecord();
    assertFalse("No tombstones recorded ⇒ no shadow",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 100L, V_ALICE)));
  }

  @Test
  public void pointDeleteShadowsExactTimestampOnly() {
    TargetRowRecord rec = new TargetRowRecord();
    rec.record(new KeyValue(ROW, CF, Q_NAME, 100L, Type.Delete));

    assertTrue("Point Delete shadows a Put at exactly ts == 100",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 100L, V_ALICE)));
    assertFalse("Point Delete must NOT shadow a Put at ts == 99",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 99L, V_ALICE)));
    assertFalse("Point Delete must NOT shadow a Put at ts == 101",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 101L, V_ALICE)));
    assertFalse("Point Delete must NOT shadow a different qualifier",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_VALUE, 100L, V_ALICE)));
  }

  @Test
  public void deleteColumnShadowsAllPutsAtOrBelowMarker() {
    TargetRowRecord rec = new TargetRowRecord();
    rec.record(new KeyValue(ROW, CF, Q_NAME, 100L, Type.DeleteColumn));

    assertTrue(rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 50L, V_ALICE)));
    assertTrue("DeleteColumn shadows Put at exactly the marker ts",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 100L, V_ALICE)));
    assertFalse("DeleteColumn must NOT shadow Puts above the marker",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 101L, V_ALICE)));
    assertFalse("DeleteColumn must NOT shadow a different qualifier",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_VALUE, 50L, V_ALICE)));
  }

  @Test
  public void deleteColumnUpperBoundUsesMaxAcrossMultipleMarkers() {
    TargetRowRecord rec = new TargetRowRecord();
    rec.record(new KeyValue(ROW, CF, Q_NAME, 100L, Type.DeleteColumn));
    rec.record(new KeyValue(ROW, CF, Q_NAME, 200L, Type.DeleteColumn));
    rec.record(new KeyValue(ROW, CF, Q_NAME, 50L, Type.DeleteColumn));

    assertTrue("Upper bound must collapse to the max marker (200)",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 200L, V_ALICE)));
    assertFalse(rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 201L, V_ALICE)));
  }

  @Test
  public void deleteFamilyShadowsAllQualifiersAtOrBelowMarker() {
    TargetRowRecord rec = new TargetRowRecord();
    rec.record(new KeyValue(ROW, CF, null, 100L, Type.DeleteFamily));

    assertTrue("DeleteFamily covers any qualifier in CF at ts <= 100",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 50L, V_ALICE)));
    assertTrue(rec.wouldShadow(new KeyValue(ROW, CF, Q_VALUE, 100L, V_ALICE)));
    assertFalse(rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 101L, V_ALICE)));
    assertFalse("DeleteFamily must NOT span a different family",
      rec.wouldShadow(new KeyValue(ROW, CF2, Q_NAME, 50L, V_ALICE)));
  }

  @Test
  public void deleteFamilyVersionShadowsAllQualifiersAtExactTs() {
    TargetRowRecord rec = new TargetRowRecord();
    rec.record(new KeyValue(ROW, CF, null, 100L, Type.DeleteFamilyVersion));

    assertTrue(rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 100L, V_ALICE)));
    assertTrue(rec.wouldShadow(new KeyValue(ROW, CF, Q_VALUE, 100L, V_ALICE)));
    assertFalse("DFV must NOT cover other timestamps",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 99L, V_ALICE)));
    assertFalse(rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 101L, V_ALICE)));
    assertFalse(rec.wouldShadow(new KeyValue(ROW, CF2, Q_NAME, 100L, V_ALICE)));
  }

  @Test
  public void wouldShadowTrueIfAnyTombstoneSubtypeMatches() {
    TargetRowRecord rec = new TargetRowRecord();
    // Point Delete on (CF, NAME) at ts=100; DeleteFamily on a different family at ts=999.
    rec.record(new KeyValue(ROW, CF, Q_NAME, 100L, Type.Delete));
    rec.record(new KeyValue(ROW, CF2, null, 999L, Type.DeleteFamily));

    assertTrue("Match on point-delete arm wins regardless of other arms",
      rec.wouldShadow(new KeyValue(ROW, CF, Q_NAME, 100L, V_ALICE)));
    assertTrue("Match on family arm wins regardless of other arms",
      rec.wouldShadow(new KeyValue(ROW, CF2, Q_NAME, 500L, V_ALICE)));
  }

  @Test
  public void targetPutTimestampsBetweenIsExclusiveOnBothEnds() {
    TargetRowRecord rec = new TargetRowRecord();
    rec.record(new KeyValue(ROW, CF, Q_NAME, 300L, V_ALICE));
    rec.record(new KeyValue(ROW, CF, Q_NAME, 600L, V_ALICE));
    rec.record(new KeyValue(ROW, CF, Q_NAME, 900L, V_ALICE));

    Set<Long> hidden = rec.targetPutTimestampsBetween(CF, Q_NAME, 300L, 900L);
    assertEquals("Bounds are exclusive on both ends — only 600 falls strictly between",
      Collections.singleton(600L), hidden);
  }

  @Test
  public void targetPutTimestampsBetweenEmptyWhenNoPutsForColumn() {
    TargetRowRecord rec = new TargetRowRecord();
    rec.record(new KeyValue(ROW, CF, Q_NAME, 500L, V_ALICE));
    assertTrue("Different qualifier ⇒ empty set, not null",
      rec.targetPutTimestampsBetween(CF, Q_VALUE, 0L, Long.MAX_VALUE).isEmpty());
  }

  @Test
  public void targetPutTimestampsBetweenSkipsTombstones() {
    TargetRowRecord rec = new TargetRowRecord();
    rec.record(new KeyValue(ROW, CF, Q_NAME, 500L, V_ALICE));
    rec.record(new KeyValue(ROW, CF, Q_NAME, 600L, Type.Delete));
    Set<Long> puts = rec.targetPutTimestampsBetween(CF, Q_NAME, 0L, Long.MAX_VALUE);
    assertEquals("Tombstone cells must not be reported as Put timestamps",
      Collections.singleton(500L), puts);
  }

  @Test
  public void driftCountersStartZeroAndAccumulateCellDrift() {
    DriftCounters d = new DriftCounters();
    assertEquals(0, d.cellsMissingOnTarget);
    assertEquals(0, d.cellsExtraOnTarget);
    assertEquals(0, d.cellsDifferentOnTarget);

    d.addCellDrift(new CellDriftCounts(2, 3, 5));
    d.addCellDrift(new CellDriftCounts(1, 0, 4));
    assertEquals(3, d.cellsMissingOnTarget);
    assertEquals(3, d.cellsExtraOnTarget);
    assertEquals(9, d.cellsDifferentOnTarget);
  }

  @Test
  public void driftCountersToLogStringIncludesEveryCounter() {
    DriftCounters d = new DriftCounters();
    d.rowsMissingOnTarget = 1;
    d.rowsExtraOnTarget = 2;
    d.rowsDifferentOnTarget = 3;
    d.rowsCannotRepair = 4;
    d.cellsMissingOnTarget = 5;
    d.cellsExtraOnTarget = 6;
    d.cellsDifferentOnTarget = 7;

    String log = d.toLogString();
    assertTrue(log.contains("rowsMissingOnTarget=1"));
    assertTrue(log.contains("rowsExtraOnTarget=2"));
    assertTrue(log.contains("rowsDifferentOnTarget=3"));
    assertTrue(log.contains("rowsCannotRepair=4"));
    assertTrue(log.contains("cellsMissingOnTarget=5"));
    assertTrue(log.contains("cellsExtraOnTarget=6"));
    assertTrue(log.contains("cellsDifferentOnTarget=7"));
  }

  @Test
  public void completedReturnsRepairedWhenNoRowCannotRepair() {
    DriftCounters d = new DriftCounters();
    d.rowsMissingOnTarget = 5;
    d.cellsExtraOnTarget = 2;
    ChunkRepairResult result = ChunkRepairResult.completed(d);
    assertEquals(ChunkRepairResult.Status.REPAIRED, result.status);
    assertEquals(d, result.drift);
    assertNotNull(result.endTime);
    assertEquals("Successful completion ⇒ no failure", null, result.failure);
  }

  @Test
  public void completedReturnsUnrepairableWhenRowCannotRepair() {
    DriftCounters d = new DriftCounters();
    d.rowsCannotRepair = 1;
    ChunkRepairResult result = ChunkRepairResult.completed(d);
    assertEquals(ChunkRepairResult.Status.UNREPAIRABLE, result.status);
    assertEquals(null, result.failure);
  }

  @Test
  public void failedReturnsRepairFailedAndCarriesException() {
    DriftCounters d = new DriftCounters();
    d.rowsMissingOnTarget = 1;
    d.rowsCannotRepair = 1;
    IOException cause = new IOException("simulated");
    ChunkRepairResult result = ChunkRepairResult.failed(d, cause);
    assertEquals("REPAIR_FAILED beats UNREPAIRABLE regardless of drift counters",
      ChunkRepairResult.Status.REPAIR_FAILED, result.status);
    assertEquals(cause, result.failure);
    assertNotNull(result.endTime);
  }

  @Test
  public void cellDriftCountsNoneIsAllZero() {
    assertEquals(0, CellDriftCounts.NONE.missing);
    assertEquals(0, CellDriftCounts.NONE.extra);
    assertEquals(0, CellDriftCounts.NONE.different);
  }

  @Test
  public void rowDriftInfoNoneIsZeroDriftAndRepairable() {
    assertEquals(CellDriftCounts.NONE, RowDriftInfo.NONE.cells);
    assertFalse(RowDriftInfo.NONE.rowCannotRepair);
  }

  @Test
  public void rowMirrorStatusEnumeratesAllThreeOutcomes() {
    assertEquals(3, RowMirrorStatus.values().length);
  }

  @Test
  public void chunkRepairRequestPreservesEveryField() {
    byte[] srcStart = Bytes.toBytes("a");
    byte[] srcEnd = Bytes.toBytes("m");
    byte[] tgtStart = Bytes.toBytes("a");
    byte[] tgtEnd = Bytes.toBytes("z");
    Timestamp verifyStart = new Timestamp(123456L);
    ChunkRepairRequest req = new ChunkRepairRequest(srcStart, srcEnd, tgtStart, tgtEnd, false, true,
      42L, 99L, verifyStart, true);
    assertEquals(srcStart, req.sourceStart);
    assertEquals(srcEnd, req.sourceEnd);
    assertEquals(tgtStart, req.targetStart);
    assertEquals(tgtEnd, req.targetEnd);
    assertFalse(req.targetStartInclusive);
    assertTrue(req.targetEndInclusive);
    assertEquals(42L, req.verifySourceRows);
    assertEquals(99L, req.verifyTargetRows);
    assertEquals(verifyStart, req.verifyStartTime);
    assertTrue(req.dryRun);
  }
}
