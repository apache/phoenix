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
package org.apache.phoenix.hbase.index;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.InvalidClusterRoleTransitionException;
import org.apache.phoenix.exception.MutationBlockedIOException;
import org.apache.phoenix.exception.ReplicationFailoverInProgressException;
import org.apache.phoenix.exception.ReplicationLogGroupUnavailableException;
import org.apache.phoenix.hbase.index.IndexRegionObserver.BatchOrigin;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSource;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceImpl;
import org.apache.phoenix.jdbc.ClusterType;
import org.apache.phoenix.jdbc.HAGroupStateListener;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.replication.ReplicationLogGroup;
import org.apache.phoenix.replication.ReplicationLogGroup.Status;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for the role/policy decision in {@link IndexRegionObserver#getReplicationLogGroup}:
 * which live write batches get a replication log writer, which commit locally without one, and
 * which are rejected as split-brain. Also covers the WAL-restore path for PARALLEL groups.
 */
public class IndexRegionObserverReplicationLogGroupTest {

  @Rule
  public TemporaryFolder localFolder = new TemporaryFolder();
  @Rule
  public TemporaryFolder peerFolder = new TemporaryFolder();
  @Rule
  public TestName name = new TestName();

  private Configuration conf;
  private ServerName serverName;
  private RegionCoprocessorEnvironment env;
  private HRegion region;
  private HAGroupStoreManager haGroupStoreManager;
  private String haGroupName;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    conf.setInt(ReplicationLogGroup.REPLICATION_LOG_RINGBUFFER_SIZE_KEY, 32);
    conf.setLong(ReplicationLogGroup.REPLICATION_LOG_SYNC_TIMEOUT_KEY, 1000);
    serverName = ServerName.valueOf("test", 60010, EnvironmentEdgeManager.currentTimeMillis());
    env = mock(RegionCoprocessorEnvironment.class);
    when(env.getConfiguration()).thenReturn(conf);
    when(env.getServerName()).thenReturn(serverName);
    region = mock(HRegion.class);
    when(region.getMemStoreFlushSize()).thenReturn(128L * 1024 * 1024);
    when(env.getRegion()).thenReturn(region);
    haGroupStoreManager = mock(HAGroupStoreManager.class);
    haGroupName = name.getMethodName();
  }

  private void stubRecord(HAGroupState state, HighAvailabilityPolicy policy) throws IOException {
    HAGroupStoreRecord record = new HAGroupStoreRecord(null, haGroupName, state, 0,
      policy.toString(), "peerZKUrl", "clusterUrl", "peerClusterUrl",
      new Path(localFolder.getRoot().toString()).toUri().toString(),
      new Path(peerFolder.getRoot().toString()).toUri().toString(), 0L);
    doReturn(Optional.of(record)).when(haGroupStoreManager)
      .getEffectiveHAGroupStoreRecord(anyString());
  }

  private ReplicationLogGroup resolve(IndexRegionObserver iro, BatchOrigin origin,
    Optional<String> group) throws IOException {
    return iro.getReplicationLogGroup(env, haGroupStoreManager, origin, group);
  }

  private ReplicationLogGroup resolveClientHa() throws IOException {
    return resolve(new IndexRegionObserver("T", true), BatchOrigin.CLIENT_HA,
      Optional.of(haGroupName));
  }

  /** No writer was constructed: a live group subscribes to LOCAL role changes in init(). */
  private void verifyNoWriterCreated() throws Exception {
    verify(haGroupStoreManager, never()).subscribeToTargetState(anyString(),
      any(HAGroupState.class), any(ClusterType.class), any(HAGroupStateListener.class));
  }

  private void assertParallelSkips(HAGroupState state) throws Exception {
    stubRecord(state, HighAvailabilityPolicy.PARALLEL);
    ReplicationLogGroup logGroup = resolveClientHa();
    try {
      assertEquals("PARALLEL write must not ship through the replication log", Status.SKIP,
        logGroup.getStatus());
      assertEquals(haGroupName, logGroup.getHAGroupName());
      // Cached: later batches do not re-read the store.
      assertSame(logGroup, resolveClientHa());
      verify(haGroupStoreManager, times(1)).getEffectiveHAGroupStoreRecord(haGroupName);
      verifyNoWriterCreated();
    } finally {
      logGroup.close();
    }
  }

  @Test
  public void testParallelOnActiveSkips() throws Exception {
    // PARALLEL clients write both clusters themselves, so the active must not ship the batch
    // through the replication log (no append, no pre-image, no WAL-key stamp).
    assertParallelSkips(HAGroupState.ACTIVE_IN_SYNC);
  }

  @Test
  public void testParallelOnStandbySkips() throws Exception {
    assertParallelSkips(HAGroupState.STANDBY);
  }

  @Test
  public void testFailoverOnActiveReplicates() throws Exception {
    stubRecord(HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER);
    ReplicationLogGroup logGroup = resolveClientHa();
    try {
      assertEquals(Status.REPLICATING, logGroup.getStatus());
      assertEquals(haGroupName, logGroup.getHAGroupName());
    } finally {
      logGroup.close();
    }
  }

  /**
   * A group created during the failover gate whose peer writer cannot be created fails its init
   * with the failover exception (its STORE_AND_FORWARD fallback is rejected). Nothing was written
   * yet, so the live write path reports it as blocked, not as a divergence.
   */
  @Test
  public void testGroupCreationFailingDuringFailoverIsBlocked() throws Exception {
    // A regular file where the peer replication directory should be: the peer writer cannot start.
    String peerUrl = new Path(peerFolder.newFile("not-a-dir").toString()).toUri().toString();
    HAGroupStoreRecord record =
      new HAGroupStoreRecord(null, haGroupName, HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, 0,
        HighAvailabilityPolicy.FAILOVER.toString(), "peerZKUrl", "clusterUrl", "peerClusterUrl",
        new Path(localFolder.getRoot().toString()).toUri().toString(), peerUrl, 0L);
    doReturn(Optional.of(record)).when(haGroupStoreManager)
      .getEffectiveHAGroupStoreRecord(anyString());
    doThrow(new InvalidClusterRoleTransitionException("Cannot transition",
      HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, HAGroupState.ACTIVE_NOT_IN_SYNC))
        .when(haGroupStoreManager).setHAGroupStatusToStoreAndForward(haGroupName);
    try {
      resolveClientHa();
      fail("Creating the group during the failover must fail");
    } catch (MutationBlockedIOException expected) {
      assertTrue(expected.getMessage(),
        expected.getMessage().contains("blocked during a failover"));
    }
    // The init failure took the STORE_AND_FORWARD fallback, which the failover rejected.
    verify(haGroupStoreManager, atLeastOnce()).setHAGroupStatusToStoreAndForward(haGroupName);
  }

  @Test
  public void testFailoverOnStandbyThrows() throws Exception {
    stubRecord(HAGroupState.STANDBY, HighAvailabilityPolicy.FAILOVER);
    try {
      resolveClientHa();
      fail("FAILOVER write on a non-active cluster must be rejected as split-brain");
    } catch (IOException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().contains("not active"));
    }
    verifyNoWriterCreated();
  }

  @Test
  public void testMissingRecordFails() throws Exception {
    // With no record the policy is unknown, so resolution fails rather than committing.
    doReturn(Optional.empty()).when(haGroupStoreManager)
      .getEffectiveHAGroupStoreRecord(anyString());
    try {
      resolveClientHa();
      fail("a missing HA group record must not silently skip replication");
    } catch (IOException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().contains("empty group store record"));
    }
    verifyNoWriterCreated();
  }

  @Test
  public void testNonClientHaOriginsReturnNoneWithoutLookup() throws Exception {
    IndexRegionObserver iro = new IndexRegionObserver("T", true);
    assertSame(ReplicationLogGroup.NONE, resolve(iro, BatchOrigin.CLIENT_NON_HA, Optional.empty()));
    assertSame(ReplicationLogGroup.NONE, resolve(iro, BatchOrigin.NATIVE_IN, Optional.empty()));
    assertSame(ReplicationLogGroup.NONE, resolve(iro, BatchOrigin.PHX_REPLAY, Optional.empty()));
    verifyNoInteractions(haGroupStoreManager);
  }

  @Test
  public void testNonReplicatedTableReturnsNoneWithoutLookup() throws Exception {
    IndexRegionObserver iro = new IndexRegionObserver("T", false);
    assertSame(ReplicationLogGroup.NONE,
      resolve(iro, BatchOrigin.CLIENT_HA, Optional.of(haGroupName)));
    verifyNoInteractions(haGroupStoreManager);
  }

  /**
   * A WAL edit stamped with a PARALLEL group (e.g. written before PARALLEL skipped the log) is
   * resolved at the drain to the PARALLEL no-op group: dropped, not re-shipped or recorded, and no
   * writer is created.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testPreWALRestoreSkipsParallelGroup() throws Exception {
    stubRecord(HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.PARALLEL);
    // Prime the cache with the PARALLEL no-op group; the drain resolves through the same
    // (server, HA group) cache entry.
    ReplicationLogGroup logGroup = resolveClientHa();
    try {
      String table = name.getMethodName();
      ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
      when(ctx.getEnvironment()).thenReturn(env);
      RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
      Path edits = new Path("/recovered.edits/1");
      long before = divergenceCount(table);
      // The record isn't readable here (no store client), so the drain decides from the group.
      IndexRegionObserver iro = new IndexRegionObserver(table, true) {
        @Override
        Boolean isDemotedAtStart(RegionCoprocessorEnvironment e, String haGroup) {
          return null;
        }
      };

      // The no-op group's append throws, so getting through the file proves nothing was shipped.
      iro.preReplayWALs(ctx, info, edits);
      iro.preWALRestore(ctx, info, walKey(table), edit(cell(CF, Bytes.toBytes("q"), 10)));
      iro.postReplayWALs(ctx, info, edits);

      assertEquals(before, divergenceCount(table));
      assertSame(logGroup, resolveClientHa());
      verifyNoWriterCreated();
    } finally {
      logGroup.close();
    }
  }

  private static final byte[] ROW = Bytes.toBytes("r");
  private static final byte[] CF = Bytes.toBytes("0");
  private static final byte[] LOCAL_INDEX_CF =
    Bytes.toBytes(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX + "0");

  private static Cell cell(byte[] family, byte[] qualifier, long ts) {
    return new KeyValue(ROW, family, qualifier, ts, Bytes.toBytes("v"));
  }

  private static Cell preImageCell(long ts) {
    return cell(WALEdit.METAFAMILY, IndexRegionObserver.PRE_IMAGE_WAL_QUALIFIER, ts);
  }

  private static WALEdit edit(Cell... cells) {
    WALEdit edit = new WALEdit();
    for (Cell c : cells) {
      edit.add(c);
    }
    return edit;
  }

  private long divergenceCount(String table) {
    MetricsIndexerSource source = MetricsIndexerSourceFactory.getInstance().getIndexerSource();
    return ((MetricsIndexerSourceImpl) source).getMetricsRegistry()
      .getCounter(MetricsIndexerSource.REPLICATION_DIVERGENCE + "." + table, 0).value();
  }

  /**
   * An observer for {@code table} whose WAL-restore drains resolve to {@code logGroup}. Whether the
   * open started demoted is unknown (the record can't be read), so drains decide from the group.
   */
  private IndexRegionObserver observerWithGroup(String table, ReplicationLogGroup logGroup) {
    return new IndexRegionObserver(table, true) {
      @Override
      ReplicationLogGroup resolveRestoreGroup(RegionCoprocessorEnvironment e, String haGroup) {
        return logGroup;
      }

      @Override
      Boolean isDemotedAtStart(RegionCoprocessorEnvironment e, String haGroup) {
        return null;
      }
    };
  }

  /** A group for {@code name} whose cluster is not active. */
  private static ReplicationLogGroup notActiveGroup(String name) {
    ReplicationLogGroup logGroup = mock(ReplicationLogGroup.class);
    when(logGroup.getStatus()).thenReturn(Status.NOT_ACTIVE);
    when(logGroup.getHAGroupName()).thenReturn(name);
    return logGroup;
  }

  /**
   * Like {@link #observerWithGroup}, and records each divergence record's {reason, minTs, maxTs}.
   * Resolves the given groups in turn for successive drains, then keeps the last.
   */
  private IndexRegionObserver recordingObserver(String table, List<Object[]> records,
    ReplicationLogGroup... groups) {
    Iterator<ReplicationLogGroup> resolved = Arrays.asList(groups).iterator();
    return new IndexRegionObserver(table, true) {
      private ReplicationLogGroup last;

      @Override
      ReplicationLogGroup resolveRestoreGroup(RegionCoprocessorEnvironment e, String haGroup) {
        if (resolved.hasNext()) {
          last = resolved.next();
        }
        return last;
      }

      @Override
      Boolean isDemotedAtStart(RegionCoprocessorEnvironment e, String haGroup) {
        return null;
      }

      @Override
      void logDivergence(String reason, String haGroup, RegionInfo region, ServerName rs,
        long minTs, long maxTs, Throwable cause) {
        records.add(new Object[] { reason, minTs, maxTs });
        super.logDivergence(reason, haGroup, region, rs, minTs, maxTs, cause);
      }
    };
  }

  private WALKey walKey(String table) {
    WALKey logKey = mock(WALKey.class);
    when(logKey.getExtendedAttributes()).thenReturn(Collections.singletonMap(
      BaseScannerRegionObserverConstants.HA_GROUP_NAME_ATTRIB, Bytes.toBytes(haGroupName)));
    when(logKey.getTableName()).thenReturn(TableName.valueOf(table));
    return logKey;
  }

  /** A WAL key with sequence id {@code seqId} that embeds watermark {@code watermark}. */
  private WALKey walKey(String table, long seqId, long watermark) {
    Map<String, byte[]> attrs = new HashMap<>();
    attrs.put(BaseScannerRegionObserverConstants.HA_GROUP_NAME_ATTRIB, Bytes.toBytes(haGroupName));
    attrs.put(IndexRegionObserver.REPL_WATERMARK_ATTR, Bytes.toBytes(watermark));
    WALKey logKey = mock(WALKey.class);
    when(logKey.getExtendedAttributes()).thenReturn(attrs);
    when(logKey.getTableName()).thenReturn(TableName.valueOf(table));
    when(logKey.getSequenceId()).thenReturn(seqId);
    return logKey;
  }

  private ReplicationLogGroup replicatingGroup() {
    ReplicationLogGroup logGroup = mock(ReplicationLogGroup.class);
    when(logGroup.getStatus()).thenReturn(Status.REPLICATING);
    when(logGroup.isReplicated()).thenReturn(true);
    when(logGroup.getHAGroupName()).thenReturn(haGroupName);
    return logGroup;
  }

  private void restore(IndexRegionObserver iro, ObserverContext<RegionCoprocessorEnvironment> ctx,
    RegionInfo info, String table, long seqId, long watermark) throws IOException {
    iro.preWALRestore(ctx, info, walKey(table, seqId, watermark),
      edit(cell(CF, Bytes.toBytes("q"), seqId)));
  }

  /**
   * With replication off for the table, the open, close and replay hooks do no work: they never
   * read the environment, so they neither drain nor count the region.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreHooksDoNothingWhenTableNotReplicated() throws Exception {
    IndexRegionObserver iro = new IndexRegionObserver(name.getMethodName(), false);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    WALEdit logEdit = mock(WALEdit.class);
    Path edits = new Path("/recovered.edits/1");

    iro.preReplayWALs(ctx, null, edits);
    iro.preWALRestore(ctx, null, walKey(name.getMethodName()), logEdit);
    iro.postReplayWALs(ctx, null, edits);
    iro.postOpen(ctx);
    iro.postClose(ctx, false);

    verifyNoInteractions(ctx, logEdit);
  }

  /**
   * On a replicated table, a recovered edit without an HA group (a non-HA client write or a standby
   * replay write) is neither re-shipped nor counted: the edit is never read, and the file ends with
   * nothing to drain or record.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreSkipsEditWithoutHAGroup() throws Exception {
    String table = name.getMethodName();
    IndexRegionObserver iro = new IndexRegionObserver(table, true);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");
    WALKey logKey = mock(WALKey.class);
    when(logKey.getExtendedAttributes()).thenReturn(Collections.emptyMap());
    WALEdit logEdit = mock(WALEdit.class);
    long divergences = divergenceCount(table);

    iro.preReplayWALs(ctx, info, edits);
    iro.preWALRestore(ctx, info, logKey, logEdit);
    iro.postReplayWALs(ctx, info, edits);

    verifyNoInteractions(logEdit, region);
    verify(logKey, never()).getSequenceId();
    assertEquals(divergences, divergenceCount(table));
  }

  /**
   * Replay buffers recovered edits and re-ships them at the end of the file in one sync, skipping
   * those a later edit's watermark covers.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreShipsUncoveredEditsInOneSyncPerFile() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    IndexRegionObserver iro = observerWithGroup(table, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");

    iro.preReplayWALs(ctx, info, edits);
    restore(iro, ctx, info, table, 10, 5);
    restore(iro, ctx, info, table, 11, 5);
    // W=10 says 10 was replicated; 11 and 12 are not covered.
    restore(iro, ctx, info, table, 12, 10);
    verify(logGroup, never()).append(anyString(), anyLong(), anyList(), anyMap());
    iro.postReplayWALs(ctx, info, edits);

    verify(logGroup, never()).append(anyString(), eq(10L), anyList(), anyMap());
    verify(logGroup).append(anyString(), eq(11L), anyList(), anyMap());
    verify(logGroup).append(anyString(), eq(12L), anyList(), anyMap());
    verify(logGroup, times(1)).sync();
    // The watermark stays in the WAL key: it is never shipped.
    ArgumentCaptor<Map<String, byte[]>> shipped = ArgumentCaptor.forClass(Map.class);
    verify(logGroup, times(2)).append(anyString(), anyLong(), anyList(), shipped.capture());
    for (Map<String, byte[]> attrs : shipped.getAllValues()) {
      assertFalse(attrs.containsKey(IndexRegionObserver.REPL_WATERMARK_ATTR));
      assertTrue(attrs.containsKey(BaseScannerRegionObserverConstants.HA_GROUP_NAME_ATTRIB));
    }

    // A watermark seen in an earlier file still covers edits replayed later.
    iro.preReplayWALs(ctx, info, edits);
    restore(iro, ctx, info, table, 8, 5);
    iro.postReplayWALs(ctx, info, edits);
    verify(logGroup, never()).append(anyString(), eq(8L), anyList(), anyMap());
    verify(logGroup, times(1)).sync();
  }

  /**
   * An open that starts demoted stays demoted when the cluster is promoted mid-open: its edits were
   * written before the demotion, so every drain records them and none resolves a group or ships.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDemotedRestoreStaysDemotedAfterPromotion() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    List<Object[]> records = new ArrayList<>();
    AtomicInteger resolutions = new AtomicInteger();
    AtomicInteger recordReads = new AtomicInteger();
    IndexRegionObserver iro = new IndexRegionObserver(table, true) {
      @Override
      ReplicationLogGroup resolveRestoreGroup(RegionCoprocessorEnvironment e, String haGroup) {
        resolutions.incrementAndGet();
        return logGroup;
      }

      @Override
      Boolean isDemotedAtStart(RegionCoprocessorEnvironment e, String haGroup) {
        // Demoted at the start, active after the promotion.
        return recordReads.getAndIncrement() == 0;
      }

      @Override
      void logDivergence(String reason, String haGroup, RegionInfo region, ServerName rs,
        long minTs, long maxTs, Throwable cause) {
        records.add(new Object[] { reason, minTs, maxTs });
      }
    };
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");

    // The group now resolves REPLICATING (promoted), across two files.
    iro.preReplayWALs(ctx, info, edits);
    restore(iro, ctx, info, table, 10, 5);
    iro.postReplayWALs(ctx, info, edits);
    iro.preReplayWALs(ctx, info, edits);
    restore(iro, ctx, info, table, 11, 5);
    iro.postReplayWALs(ctx, info, edits);

    verify(logGroup, never()).append(anyString(), anyLong(), anyList(), anyMap());
    assertEquals(0, resolutions.get());
    assertEquals("read once, when the buffer switched to the group", 1, recordReads.get());
    assertEquals(2, records.size());
    assertArrayEquals(new Object[] { "DEMOTED_RESTORE", 10L, 10L }, records.get(0));
    assertArrayEquals(new Object[] { "DEMOTED_RESTORE", 11L, 11L }, records.get(1));
  }

  /**
   * A region's edits can carry different HA groups, as on the live path. The buffer drains on each
   * group change, and each group is shipped to its own log or recorded on its own.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreHandlesEditsOfDifferentHAGroups() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup groupA = replicatingGroup();
    when(groupA.getHAGroupName()).thenReturn("A");
    ReplicationLogGroup notActiveB = notActiveGroup("B");
    List<Object[]> records = new ArrayList<>();
    IndexRegionObserver iro = new IndexRegionObserver(table, true) {
      @Override
      ReplicationLogGroup resolveRestoreGroup(RegionCoprocessorEnvironment e, String haGroup) {
        return haGroup.equals("A") ? groupA : notActiveB;
      }

      @Override
      Boolean isDemotedAtStart(RegionCoprocessorEnvironment e, String haGroup) {
        return null;
      }

      @Override
      void logDivergence(String reason, String haGroup, RegionInfo region, ServerName rs,
        long minTs, long maxTs, Throwable cause) {
        records.add(new Object[] { reason, haGroup, minTs, maxTs });
      }
    };
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");

    iro.preReplayWALs(ctx, info, edits);
    restore(iro, ctx, info, table, "A", 10);
    restore(iro, ctx, info, table, "B", 11);
    // Drained at the change: A's edit shipped, B's not yet recorded.
    verify(groupA).append(anyString(), eq(10L), anyList(), anyMap());
    verify(groupA, times(1)).sync();
    assertTrue(records.isEmpty());
    restore(iro, ctx, info, table, "A", 12);
    restore(iro, ctx, info, table, "B", 13);
    iro.postReplayWALs(ctx, info, edits);

    verify(groupA).append(anyString(), eq(12L), anyList(), anyMap());
    verify(groupA, times(2)).sync();
    verify(notActiveB, never()).append(anyString(), anyLong(), anyList(), anyMap());
    assertEquals(2, records.size());
    assertArrayEquals(new Object[] { "DEMOTED_RESTORE", "B", 11L, 11L }, records.get(0));
    assertArrayEquals(new Object[] { "DEMOTED_RESTORE", "B", 13L, 13L }, records.get(1));
  }

  private void restore(IndexRegionObserver iro, ObserverContext<RegionCoprocessorEnvironment> ctx,
    RegionInfo info, String table, String haGroup, long seqId) throws IOException {
    WALKey logKey = mock(WALKey.class);
    when(logKey.getExtendedAttributes()).thenReturn(Collections.singletonMap(
      BaseScannerRegionObserverConstants.HA_GROUP_NAME_ATTRIB, Bytes.toBytes(haGroup)));
    when(logKey.getTableName()).thenReturn(TableName.valueOf(table));
    when(logKey.getSequenceId()).thenReturn(seqId);
    iro.preWALRestore(ctx, info, logKey, edit(cell(CF, Bytes.toBytes("q"), seqId)));
  }

  /**
   * An open that started active and is demoted between drains ships the first drain and records the
   * second; the region opens.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreRecordsWhenDemotedBetweenDrains() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    List<Object[]> records = new ArrayList<>();
    IndexRegionObserver iro =
      recordingObserver(table, records, logGroup, notActiveGroup(haGroupName));
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");

    iro.preReplayWALs(ctx, info, edits);
    restore(iro, ctx, info, table, 10, 5);
    iro.postReplayWALs(ctx, info, edits);
    iro.preReplayWALs(ctx, info, edits);
    restore(iro, ctx, info, table, 11, 5);
    iro.postReplayWALs(ctx, info, edits);

    verify(logGroup).append(anyString(), eq(10L), anyList(), anyMap());
    verify(logGroup, never()).append(anyString(), eq(11L), anyList(), anyMap());
    assertEquals(1, records.size());
    assertArrayEquals(new Object[] { "DEMOTED_RESTORE", 11L, 11L }, records.get(0));
  }

  /**
   * A failure the failover caused (the S&F switch rejected during the failover) is recorded and the
   * region opens: failing the open would keep the region unavailable until the failover ends.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreFailoverFailureRecordsAndOpens() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    doThrow(new ReplicationFailoverInProgressException("fenced")).when(logGroup).sync();
    List<Object[]> records = new ArrayList<>();
    IndexRegionObserver iro = recordingObserver(table, records, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");

    iro.preReplayWALs(ctx, info, edits);
    restore(iro, ctx, info, table, 10, 5);
    iro.postReplayWALs(ctx, info, edits);

    assertEquals(1, records.size());
    assertArrayEquals(new Object[] { "RESTORE_FAILED", 10L, 10L }, records.get(0));
    verify(logGroup, times(1)).sync();
  }

  /** The group is resolved once per drain, not per edit. */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreResolvesGroupOncePerDrain() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    AtomicInteger resolutions = new AtomicInteger();
    IndexRegionObserver iro = new IndexRegionObserver(table, true) {
      @Override
      ReplicationLogGroup resolveRestoreGroup(RegionCoprocessorEnvironment e, String haGroup) {
        resolutions.incrementAndGet();
        return logGroup;
      }

      @Override
      Boolean isDemotedAtStart(RegionCoprocessorEnvironment e, String haGroup) {
        return null;
      }
    };
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");

    iro.preReplayWALs(ctx, info, edits);
    restore(iro, ctx, info, table, 10, 5);
    restore(iro, ctx, info, table, 11, 5);
    restore(iro, ctx, info, table, 12, 5);
    assertEquals(0, resolutions.get());
    iro.postReplayWALs(ctx, info, edits);

    assertEquals(1, resolutions.get());
    verify(logGroup, times(1)).sync();
  }

  private static byte[] catalogRow(String tenant, String schema, String table, String column,
    String family) {
    byte[] sep = new byte[] { QueryConstants.SEPARATOR_BYTE };
    return Bytes.add(new byte[][] { Bytes.toBytes(tenant), sep, Bytes.toBytes(schema), sep,
      Bytes.toBytes(table), sep, Bytes.toBytes(column), sep, Bytes.toBytes(family) });
  }

  /** Restores one edit per row of {@code table} and returns the rows of the cells it ships. */
  @SuppressWarnings("unchecked")
  private List<String> shippedRows(String table, Cell... rowCells) throws IOException {
    ReplicationLogGroup logGroup = replicatingGroup();
    IndexRegionObserver iro = observerWithGroup(table, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");
    iro.preReplayWALs(ctx, info, edits);
    long seqId = 10;
    for (Cell c : rowCells) {
      iro.preWALRestore(ctx, info, walKey(table, seqId++, 5), edit(c));
    }
    iro.postReplayWALs(ctx, info, edits);
    ArgumentCaptor<List<Cell>> shipped = ArgumentCaptor.forClass(List.class);
    verify(logGroup, atLeastOnce()).append(anyString(), anyLong(), shipped.capture(), anyMap());
    List<String> rows = new ArrayList<>();
    for (List<Cell> cells : shipped.getAllValues()) {
      for (Cell c : cells) {
        rows.add(Bytes.toStringBinary(CellUtil.cloneRow(c)));
      }
    }
    return rows;
  }

  /**
   * Recovered SYSTEM.CATALOG edits are filtered like live writes: rows that are not tenant-owned
   * (no leading tenant id) are not re-shipped.
   */
  @Test
  public void testRestoreSkipsCatalogRowsTheLivePathDoesNotShip() throws Exception {
    byte[] tenantRow = catalogRow("t1", "S", "V", "", "");
    byte[] globalRow = catalogRow("", "S", "T", "", "");
    List<String> rows = shippedRows("SYSTEM.CATALOG",
      new KeyValue(tenantRow, CF, Bytes.toBytes("q"), 10, Bytes.toBytes("v")),
      new KeyValue(globalRow, CF, Bytes.toBytes("q"), 11, Bytes.toBytes("v")));
    assertEquals(Collections.singletonList(Bytes.toStringBinary(tenantRow)), rows);
  }

  /**
   * One recovered SYSTEM.CATALOG edit holding a tenant row and a global row, each with its
   * pre-image: only the tenant row and its pre-image are re-shipped.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreFiltersCatalogRowsWithinOneEdit() throws Exception {
    String table = "SYSTEM.CATALOG";
    byte[] tenantRow = catalogRow("t1", "S", "V", "", "");
    byte[] globalRow = catalogRow("", "S", "T", "", "");
    ReplicationLogGroup logGroup = replicatingGroup();
    IndexRegionObserver iro = observerWithGroup(table, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");

    iro.preReplayWALs(ctx, info, edits);
    iro.preWALRestore(ctx, info, walKey(table, 10, 5),
      edit(new KeyValue(tenantRow, CF, Bytes.toBytes("q"), 10, Bytes.toBytes("v")),
        new KeyValue(globalRow, CF, Bytes.toBytes("q"), 10, Bytes.toBytes("v")),
        new KeyValue(tenantRow, WALEdit.METAFAMILY, IndexRegionObserver.PRE_IMAGE_WAL_QUALIFIER, 10,
          Bytes.toBytes("pre")),
        new KeyValue(globalRow, WALEdit.METAFAMILY, IndexRegionObserver.PRE_IMAGE_WAL_QUALIFIER, 10,
          Bytes.toBytes("pre"))));
    iro.postReplayWALs(ctx, info, edits);

    ArgumentCaptor<List<Cell>> shipped = ArgumentCaptor.forClass(List.class);
    verify(logGroup).append(anyString(), eq(10L), shipped.capture(), anyMap());
    List<Cell> cells = shipped.getValue();
    assertEquals(2, cells.size());
    for (Cell c : cells) {
      assertArrayEquals(tenantRow, CellUtil.cloneRow(c));
    }
    assertTrue(cells.stream().anyMatch(c -> CellUtil.matchingFamily(c, WALEdit.METAFAMILY)));
  }

  /**
   * A demoted cluster replaying only global SYSTEM.CATALOG rows records nothing: the live path
   * never ships them, so they cannot have diverged.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDemotedRestoreOfFilteredRowsRecordsNothing() throws Exception {
    String table = "SYSTEM.CATALOG";
    List<Object[]> records = new ArrayList<>();
    IndexRegionObserver iro = recordingObserver(table, records, notActiveGroup(haGroupName));
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");

    iro.preReplayWALs(ctx, info, edits);
    iro.preWALRestore(ctx, info, walKey(table, 10, 5),
      edit(new KeyValue(catalogRow("", "S", "T", "", ""), CF, Bytes.toBytes("q"), 10,
        Bytes.toBytes("v"))));
    iro.postReplayWALs(ctx, info, edits);

    assertTrue(records.isEmpty());
  }

  /**
   * Recovered SYSTEM.CHILD_LINK edits are filtered like live writes: a global row is re-shipped
   * only if it links to a tenant view.
   */
  @Test
  public void testRestoreSkipsChildLinkRowsTheLivePathDoesNotShip() throws Exception {
    byte[] linkToTenantView = catalogRow("", "S", "T", "t1", "V");
    byte[] globalLink = catalogRow("", "S", "T", "", "V2");
    byte[] childTable = new byte[] { PTable.LinkType.CHILD_TABLE.getSerializedValue() };
    List<String> rows = shippedRows("SYSTEM.CHILD_LINK",
      new KeyValue(linkToTenantView, CF, PhoenixDatabaseMetaData.LINK_TYPE_BYTES, 10, childTable),
      new KeyValue(globalLink, CF, PhoenixDatabaseMetaData.LINK_TYPE_BYTES, 11, childTable));
    assertEquals(Collections.singletonList(Bytes.toStringBinary(linkToTenantView)), rows);
  }

  /**
   * A global SYSTEM.CHILD_LINK row too short to parse makes the filter throw: the row is re-shipped
   * and the replay continues.
   */
  @Test
  public void testRestoreReshipsRowTheFilterCannotParse() throws Exception {
    byte[] shortRow = Bytes.add(new byte[] { QueryConstants.SEPARATOR_BYTE }, Bytes.toBytes("S"));
    byte[] childTable = new byte[] { PTable.LinkType.CHILD_TABLE.getSerializedValue() };
    List<String> rows = shippedRows("SYSTEM.CHILD_LINK",
      new KeyValue(shortRow, CF, PhoenixDatabaseMetaData.LINK_TYPE_BYTES, 10, childTable));
    assertEquals(Collections.singletonList(Bytes.toStringBinary(shortRow)), rows);
  }

  /** A watermark attribute that is not 8 bytes counts as none: the edit is re-shipped. */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreReshipsEditWithMalformedWatermark() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    IndexRegionObserver iro = observerWithGroup(table, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Map<String, byte[]> attrs = new HashMap<>();
    attrs.put(BaseScannerRegionObserverConstants.HA_GROUP_NAME_ATTRIB, Bytes.toBytes(haGroupName));
    attrs.put(IndexRegionObserver.REPL_WATERMARK_ATTR, Bytes.toBytes(20));
    WALKey logKey = mock(WALKey.class);
    when(logKey.getExtendedAttributes()).thenReturn(attrs);
    when(logKey.getTableName()).thenReturn(TableName.valueOf(table));
    when(logKey.getSequenceId()).thenReturn(10L);
    Path edits = new Path("/recovered.edits/1");

    iro.preReplayWALs(ctx, info, edits);
    iro.preWALRestore(ctx, info, logKey, edit(cell(CF, Bytes.toBytes("q"), 10)));
    iro.postReplayWALs(ctx, info, edits);

    verify(logGroup).append(anyString(), eq(10L), anyList(), anyMap());
    verify(logGroup, times(1)).sync();
  }

  /**
   * An edit that could make replay flush the memstore drains the buffer first, so no buffered edit
   * is flushed unshipped.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreDrainsBeforeAReplayFlush() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    IndexRegionObserver iro = observerWithGroup(table, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();

    restore(iro, ctx, info, table, 10, 5);
    verify(logGroup, never()).sync();
    when(region.getMemStoreHeapSize()).thenReturn(128L * 1024 * 1024 - 10);
    restore(iro, ctx, info, table, 11, 5);
    verify(logGroup).append(anyString(), eq(10L), anyList(), anyMap());
    verify(logGroup).append(anyString(), eq(11L), anyList(), anyMap());
    verify(logGroup, times(1)).sync();
  }

  /**
   * On a demoted cluster too, an edit that could make replay flush the memstore records the buffer
   * first: a retried open could skip the flushed file, and its record would be lost.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDemotedRestoreRecordsBeforeAReplayFlush() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup notActive = notActiveGroup(haGroupName);
    List<Object[]> records = new ArrayList<>();
    IndexRegionObserver iro = recordingObserver(table, records, notActive);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();

    restore(iro, ctx, info, table, 10, 5);
    assertTrue(records.isEmpty());
    when(region.getMemStoreHeapSize()).thenReturn(128L * 1024 * 1024 - 10);
    restore(iro, ctx, info, table, 11, 5);
    assertEquals(1, records.size());
    assertArrayEquals(new Object[] { "DEMOTED_RESTORE", 10L, 11L }, records.get(0));
  }

  /** The flush guard counts the off-heap memstore too. */
  @Test
  @SuppressWarnings("unchecked")
  public void testFlushGuardCountsOffHeapMemStore() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    IndexRegionObserver iro = observerWithGroup(table, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();

    restore(iro, ctx, info, table, 10, 5);
    when(region.getMemStoreOffHeapSize()).thenReturn(128L * 1024 * 1024 - 10);
    restore(iro, ctx, info, table, 11, 5);
    verify(logGroup, times(1)).sync();
  }

  /**
   * The flush guard counts the edit twice (its memstore overhead is unknown): an edit that fits
   * once but not twice still drains first.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testFlushGuardCountsTheEditTwice() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    IndexRegionObserver iro = observerWithGroup(table, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    WALEdit next = edit(cell(CF, Bytes.toBytes("q"), 11));
    long flushSize = 128L * 1024 * 1024;

    restore(iro, ctx, info, table, 10, 5);
    when(region.getMemStoreHeapSize()).thenReturn(flushSize - next.heapSize() * 3 / 2);
    iro.preWALRestore(ctx, info, walKey(table, 11, 5), next);
    verify(logGroup, times(1)).sync();
  }

  /**
   * With no readable HA record (no store client here), the real lookup returns null instead of
   * throwing out of the restore hook, so drains decide from the group's state.
   */
  @Test
  public void testIsDemotedAtStartIsNullWhenTheRecordCannotBeRead() {
    assertNull(
      new IndexRegionObserver(name.getMethodName(), true).isDemotedAtStart(env, haGroupName));
  }

  /** What the HA group record says about this cluster at the start of an open. */
  @Test
  public void testIsDemotedFromTheRecord() throws Exception {
    stubRecord(HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.FAILOVER);
    assertEquals(false, IndexRegionObserver.isDemoted(haGroupStoreManager, haGroupName));
    stubRecord(HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY, HighAvailabilityPolicy.FAILOVER);
    assertEquals("the failover gate is still active", false,
      IndexRegionObserver.isDemoted(haGroupStoreManager, haGroupName));
    stubRecord(HAGroupState.STANDBY_TO_ACTIVE, HighAvailabilityPolicy.FAILOVER);
    assertEquals(true, IndexRegionObserver.isDemoted(haGroupStoreManager, haGroupName));
    stubRecord(HAGroupState.STANDBY, HighAvailabilityPolicy.FAILOVER);
    assertEquals(true, IndexRegionObserver.isDemoted(haGroupStoreManager, haGroupName));
    stubRecord(HAGroupState.STANDBY, HighAvailabilityPolicy.PARALLEL);
    assertEquals("a PARALLEL group's edits are never shipped, so never recorded", false,
      IndexRegionObserver.isDemoted(haGroupStoreManager, haGroupName));
    doReturn(Optional.empty()).when(haGroupStoreManager)
      .getEffectiveHAGroupStoreRecord(anyString());
    assertNull(IndexRegionObserver.isDemoted(haGroupStoreManager, haGroupName));
  }

  /**
   * An edit without an HA group is not buffered, but it can still make replay flush the memstore,
   * so it drains the buffered edits first.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testEditWithoutHAGroupDrainsBeforeAReplayFlush() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    IndexRegionObserver iro = observerWithGroup(table, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    WALKey noGroupKey = mock(WALKey.class);
    when(noGroupKey.getExtendedAttributes()).thenReturn(Collections.emptyMap());

    restore(iro, ctx, info, table, 10, 5);
    iro.preWALRestore(ctx, info, noGroupKey, edit(cell(CF, Bytes.toBytes("q"), 11)));
    verify(logGroup, never()).sync();
    when(region.getMemStoreHeapSize()).thenReturn(128L * 1024 * 1024 - 10);
    iro.preWALRestore(ctx, info, noGroupKey, edit(cell(CF, Bytes.toBytes("q"), 12)));
    verify(logGroup).append(anyString(), eq(10L), anyList(), anyMap());
    verify(logGroup, times(1)).sync();
  }

  /** The buffer drains once it holds the configured bytes, without waiting for the file end. */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreDrainsAtTheByteCap() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    IndexRegionObserver iro = observerWithGroup(table, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();

    byte[] big = new byte[(int) IndexRegionObserver.DEFAULT_RESTORE_SYNC_BATCH_BYTES];
    iro.preWALRestore(ctx, info, walKey(table, 10, 5),
      edit(new KeyValue(ROW, CF, Bytes.toBytes("q"), 10, big)));
    verify(logGroup).append(anyString(), eq(10L), anyList(), anyMap());
    verify(logGroup, times(1)).sync();
  }

  /**
   * A file HBase moves aside skips postReplayWALs: its buffered edits ship at the next file's
   * preReplayWALs, or in postOpen for the last file, where a failure is recorded but cannot fail
   * the open.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreShipsMovedAsideFileAtNextFileOrPostOpen() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup logGroup = replicatingGroup();
    IndexRegionObserver iro = observerWithGroup(table, logGroup);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    when(env.getRegionInfo()).thenReturn(info);
    long before = divergenceCount(table);

    iro.preReplayWALs(ctx, info, new Path("/recovered.edits/1"));
    restore(iro, ctx, info, table, 10, 5);
    iro.preReplayWALs(ctx, info, new Path("/recovered.edits/2"));
    verify(logGroup).append(anyString(), eq(10L), anyList(), anyMap());
    verify(logGroup, times(1)).sync();

    restore(iro, ctx, info, table, 11, 5);
    doThrow(new IOException("peer down")).when(logGroup).sync();
    try {
      iro.postOpen(ctx);
      verify(logGroup).append(anyString(), eq(11L), anyList(), anyMap());
      assertEquals("recorded, not thrown", before + 1, divergenceCount(table));
    } finally {
      iro.postClose(ctx, false);
    }
  }

  @Test
  public void testDataTimestampRangeSkipsPreImageAndLocalIndexCells() {
    List<Cell> cells = Arrays.asList(cell(CF, Bytes.toBytes("q"), 20), preImageCell(5),
      cell(LOCAL_INDEX_CF, Bytes.toBytes("q"), 1), cell(CF, Bytes.toBytes("q2"), 30),
      preImageCell(99));
    long[] range = IndexRegionObserver.dataTimestampRange(cells);
    assertEquals(20, range[0]);
    assertEquals(30, range[1]);
  }

  /**
   * A demoted cluster replays its own formerly-ACTIVE edits without re-shipping them, so each
   * recovered-edits file with such edits logs one divergence record.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDemotedRestoreLogsOneRecordPerFile() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup notActive = notActiveGroup(haGroupName);
    IndexRegionObserver iro = observerWithGroup(table, notActive);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");
    long before = divergenceCount(table);

    iro.preReplayWALs(ctx, info, edits);
    iro.preWALRestore(ctx, info, walKey(table), edit(cell(CF, Bytes.toBytes("q"), 10)));
    iro.preWALRestore(ctx, info, walKey(table), edit(cell(CF, Bytes.toBytes("q"), 20)));
    iro.postReplayWALs(ctx, info, edits);
    assertEquals("one record for the file", before + 1, divergenceCount(table));

    // A file with no demoted edits logs nothing.
    iro.preReplayWALs(ctx, info, edits);
    iro.postReplayWALs(ctx, info, edits);
    assertEquals(before + 1, divergenceCount(table));
    verify(notActive, never()).append(anyString(), anyLong(), anyList(), anyMap());
  }

  /**
   * A demoted replay reports only the edits no later watermark covers: the record's window is the
   * uncovered tail, not every replayed edit.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDemotedRestoreReportsOnlyUncoveredEdits() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup notActive = notActiveGroup(haGroupName);
    List<Object[]> records = new ArrayList<>();
    IndexRegionObserver iro = recordingObserver(table, records, notActive);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");

    // Each edit's cell timestamp is its sequence id. W=11 covers 10 and 11; 12 and 13 are not.
    iro.preReplayWALs(ctx, info, edits);
    restore(iro, ctx, info, table, 10, 5);
    restore(iro, ctx, info, table, 11, 5);
    restore(iro, ctx, info, table, 12, 11);
    restore(iro, ctx, info, table, 13, 11);
    iro.postReplayWALs(ctx, info, edits);

    assertEquals(1, records.size());
    assertArrayEquals(new Object[] { "DEMOTED_RESTORE", 12L, 13L }, records.get(0));
    verify(notActive, never()).append(anyString(), anyLong(), anyList(), anyMap());
  }

  /**
   * At the edit cap, the buffered demoted edits are recorded, so a later watermark can no longer
   * cover them: the records over-report but memory stays bounded.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDemotedRestoreRecordsAtTheEditCap() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup notActive = notActiveGroup(haGroupName);
    List<Object[]> records = new ArrayList<>();
    IndexRegionObserver iro = recordingObserver(table, records, notActive);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");
    int cap = IndexRegionObserver.DEFAULT_RESTORE_SYNC_BATCH_EDITS;

    iro.preReplayWALs(ctx, info, edits);
    for (int seqId = 1; seqId <= cap; seqId++) {
      restore(iro, ctx, info, table, seqId, RestoreBuffer.NO_WATERMARK);
    }
    // Would cover every earlier edit, but they were recorded at the cap.
    restore(iro, ctx, info, table, cap + 1, cap);
    iro.postReplayWALs(ctx, info, edits);

    assertEquals(2, records.size());
    assertArrayEquals(new Object[] { "DEMOTED_RESTORE", 1L, (long) cap }, records.get(0));
    assertArrayEquals(new Object[] { "DEMOTED_RESTORE", (long) cap + 1, (long) cap + 1 },
      records.get(1));
  }

  /**
   * A failed re-ship (not because of a failover) is recorded and the region opens: failing the open
   * could lose edits of a file HBase moved aside.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testRestoreFailureIsRecordedAndOpens() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup failing = replicatingGroup();
    EOFException cause = new EOFException("peer down");
    doThrow(cause).when(failing).sync();
    IndexRegionObserver iro = observerWithGroup(table, failing);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");
    long before = divergenceCount(table);

    iro.preReplayWALs(ctx, info, edits);
    iro.preWALRestore(ctx, info, walKey(table), edit(cell(CF, Bytes.toBytes("q"), 10)));
    iro.postReplayWALs(ctx, info, edits);
    assertEquals(before + 1, divergenceCount(table));
  }

  /**
   * HBase skips postReplayWALs for a file it moves aside and continues past (EOF, parse error,
   * skip.errors); the file's demoted edits are logged by the next file's preReplayWALs, or by
   * postOpen for the last file.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDemotedRestoreLoggedWhenFileMovedAside() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup notActive = notActiveGroup(haGroupName);
    IndexRegionObserver iro = observerWithGroup(table, notActive);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    when(env.getRegionInfo()).thenReturn(info);
    Path edits = new Path("/recovered.edits/1");
    long before = divergenceCount(table);

    iro.preReplayWALs(ctx, info, edits);
    iro.preWALRestore(ctx, info, walKey(table), edit(cell(CF, Bytes.toBytes("q"), 10)));
    // Moved aside: no postReplayWALs. The next file logs it.
    iro.preReplayWALs(ctx, info, new Path("/recovered.edits/2"));
    assertEquals(before + 1, divergenceCount(table));

    // The last file is moved aside too: postOpen logs it.
    iro.preWALRestore(ctx, info, walKey(table), edit(cell(CF, Bytes.toBytes("q"), 20)));
    iro.postOpen(ctx);
    try {
      assertEquals(before + 2, divergenceCount(table));
    } finally {
      iro.postClose(ctx, false);
    }
  }

  /**
   * Resolving the group at the drain can create the writer on an RS restart; a failure there is
   * recorded and the region opens.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testGroupResolutionFailureIsRecorded() throws Exception {
    String table = name.getMethodName();
    EOFException cause = new EOFException("peer writer create failed");
    IndexRegionObserver iro = new IndexRegionObserver(table, true) {
      @Override
      ReplicationLogGroup resolveRestoreGroup(RegionCoprocessorEnvironment e, String haGroup)
        throws IOException {
        throw cause;
      }

      @Override
      Boolean isDemotedAtStart(RegionCoprocessorEnvironment e, String haGroup) {
        return null;
      }
    };
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");
    long before = divergenceCount(table);

    iro.preReplayWALs(ctx, info, edits);
    iro.preWALRestore(ctx, info, walKey(table), edit(cell(CF, Bytes.toBytes("q"), 10)));
    iro.postReplayWALs(ctx, info, edits);
    assertEquals(before + 1, divergenceCount(table));
  }

  /**
   * A RuntimeException from the drain (e.g. resolving the group) is recorded like any other
   * failure; escaping the hook unchecked would abort the RS.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDrainRuntimeExceptionIsRecorded() throws Exception {
    String table = name.getMethodName();
    NullPointerException cause = new NullPointerException("zkUrl cannot be null");
    IndexRegionObserver iro = new IndexRegionObserver(table, true) {
      @Override
      ReplicationLogGroup resolveRestoreGroup(RegionCoprocessorEnvironment e, String haGroup) {
        throw cause;
      }

      @Override
      Boolean isDemotedAtStart(RegionCoprocessorEnvironment e, String haGroup) {
        return null;
      }
    };
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");
    long before = divergenceCount(table);

    iro.preReplayWALs(ctx, info, edits);
    iro.preWALRestore(ctx, info, walKey(table), edit(cell(CF, Bytes.toBytes("q"), 10)));
    iro.postReplayWALs(ctx, info, edits);
    assertEquals(before + 1, divergenceCount(table));
  }

  /**
   * The drain calls checkUsable before re-shipping: a group that is closed or being replaced after
   * a cancelled failover refuses the edits; they are recorded and the region opens.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testDrainChecksGroupIsUsable() throws Exception {
    String table = name.getMethodName();
    ReplicationLogGroup stopped = replicatingGroup();
    ReplicationLogGroupUnavailableException cause =
      new ReplicationLogGroupUnavailableException("being replaced");
    doThrow(cause).when(stopped).checkUsable();
    IndexRegionObserver iro = observerWithGroup(table, stopped);
    ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
    when(ctx.getEnvironment()).thenReturn(env);
    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(table)).build();
    Path edits = new Path("/recovered.edits/1");
    long before = divergenceCount(table);

    iro.preReplayWALs(ctx, info, edits);
    iro.preWALRestore(ctx, info, walKey(table), edit(cell(CF, Bytes.toBytes("q"), 10)));
    iro.postReplayWALs(ctx, info, edits);
    verify(stopped, never()).append(anyString(), anyLong(), anyList(), anyMap());
    assertEquals(before + 1, divergenceCount(table));
  }
}
