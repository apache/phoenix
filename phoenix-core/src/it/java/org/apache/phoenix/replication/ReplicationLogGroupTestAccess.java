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

import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.STORE_AND_FORWARD;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC;

import java.io.IOException;
import org.apache.hadoop.fs.Path;

/**
 * Integration-test-only access shim for the package-visible mode controls on
 * {@link ReplicationLogGroup}. Lets an IT in another package flip a live writer SYNC -&gt;
 * STORE_AND_FORWARD deterministically, exercising the REAL {@link StoreAndForwardModeImpl} (local
 * 'out' log + forwarder + periodic ACTIVE_NOT_IN_SYNC persistence) without having to injure a real
 * peer HDFS to provoke the sync failure that triggers store-and-forward in production. Lives in the
 * test source tree, so no production code is modified.
 */
public final class ReplicationLogGroupTestAccess {

  private ReplicationLogGroupTestAccess() {
  }

  /**
   * Flip a live writer from SYNC to STORE_AND_FORWARD and fence the swap through sync boundaries.
   * The event handler reads the mode field in processPendingSyncs *after* completing a sync's
   * future, so we issue two syncs: the first lets the handler observe the new mode and run
   * StoreAndForwardModeImpl.onEnter (open the local 'out' log, start the forwarder); the second
   * runs entirely under the store-and-forward mode impl, guaranteeing any subsequent append is
   * buffered locally rather than written straight to the peer.
   * @return true if the mode was SYNC and is now STORE_AND_FORWARD; false if it was not SYNC.
   */
  public static boolean forceStoreAndForward(ReplicationLogGroup logGroup) throws IOException {
    boolean swapped = logGroup.checkAndSetMode(SYNC, STORE_AND_FORWARD);
    logGroup.sync();
    logGroup.sync();
    return swapped;
  }

  /** True if the writer's current mode is STORE_AND_FORWARD. */
  public static boolean isStoreAndForward(ReplicationLogGroup logGroup) {
    return logGroup.getMode() == STORE_AND_FORWARD;
  }

  /**
   * The peer (standby) 'in' directory this writer forwards to — the exact path a replay instance on
   * the peer cluster must read.
   */
  public static Path peerStandbyDir(ReplicationLogGroup logGroup) throws IOException {
    return logGroup.getOrCreatePeerShardManager().getRootDirectoryPath();
  }
}
