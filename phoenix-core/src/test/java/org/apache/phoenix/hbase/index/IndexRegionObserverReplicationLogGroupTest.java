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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.hbase.index.IndexRegionObserver.BatchOrigin;
import org.apache.phoenix.jdbc.ClusterType;
import org.apache.phoenix.jdbc.HAGroupStateListener;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.replication.ReplicationLogGroup;
import org.apache.phoenix.replication.ReplicationLogGroup.Status;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

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
   * A WAL edit stamped with a PARALLEL group (e.g. written before PARALLEL skipped the log) must
   * not be re-shipped on WAL restore, and must not create a writer.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testPreWALRestoreSkipsParallelGroup() throws Exception {
    stubRecord(HAGroupState.ACTIVE_IN_SYNC, HighAvailabilityPolicy.PARALLEL);
    // Prime the cache with the PARALLEL no-op group; preWALRestore resolves through the same
    // (server, HA group) cache entry.
    ReplicationLogGroup logGroup = resolveClientHa();
    try {
      ObserverContext<RegionCoprocessorEnvironment> ctx = mock(ObserverContext.class);
      when(ctx.getEnvironment()).thenReturn(env);
      WALKey logKey = mock(WALKey.class);
      when(logKey.getExtendedAttributes()).thenReturn(Collections.singletonMap(
        BaseScannerRegionObserverConstants.HA_GROUP_NAME_ATTRIB, Bytes.toBytes(haGroupName)));
      WALEdit logEdit = mock(WALEdit.class);

      new IndexRegionObserver("T", true).preWALRestore(ctx, null, logKey, logEdit);

      // A re-ship would read the edit's cells; no interaction proves preWALRestore skipped it.
      verifyNoInteractions(logEdit);
      assertSame(logGroup, resolveClientHa());
    } finally {
      logGroup.close();
    }
  }
}
