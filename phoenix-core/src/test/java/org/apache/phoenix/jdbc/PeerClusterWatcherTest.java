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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.query.QueryServices.HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * Unit test for {@link PeerClusterWatcher} concurrency behavior.
 */
public class PeerClusterWatcherTest {

  /**
   * A visible/blind transition and its listener notification must be delivered atomically: while
   * one transition is mid-notification, a concurrent opposite transition must not deliver its own
   * notification, otherwise the two can reorder and leave the replayer's state disagreeing with the
   * peer's actual visibility.
   */
  @Test(timeout = 30000)
  public void testVisibilityTransitionsAreSerializedWithTheirNotifications() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS, 0L);

    CountDownLatch visibleEntered = new CountDownLatch(1);
    CountDownLatch releaseVisible = new CountDownLatch(1);
    CountDownLatch blindNotified = new CountDownLatch(1);

    PeerClusterWatcher.PeerStateListener listener = new PeerClusterWatcher.PeerStateListener() {
      @Override
      public void onPeerStateChanged(HAGroupStoreRecord peerRecord, Stat stat) {
      }

      @Override
      public void onPeerVisible() {
        visibleEntered.countDown();
        try {
          releaseVisible.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      @Override
      public void onPeerBlind() {
        blindNotified.countDown();
      }
    };

    PeerClusterWatcher watcher = new PeerClusterWatcher(conf, "g", "ns", listener);
    try {
      Thread visible = new Thread(watcher::setVisible, "setVisible");
      visible.start();
      assertTrue("setVisible should enter its notification",
        visibleEntered.await(5, TimeUnit.SECONDS));

      Thread blind = new Thread(watcher::setBlind, "setBlind");
      blind.start();
      assertFalse("setBlind delivered its notification while setVisible was still mid-notification",
        blindNotified.await(2, TimeUnit.SECONDS));

      releaseVisible.countDown();
      assertTrue("setBlind should deliver once setVisible completes",
        blindNotified.await(5, TimeUnit.SECONDS));
      visible.join();
      blind.join();
    } finally {
      watcher.close();
    }
  }

  /**
   * A blank peer URL means there is no peer to watch, so reconcile reports the peer as visible
   * ("nothing to be blind about") without building any cache.
   */
  @Test(timeout = 30000)
  public void testBlankPeerUrlIsReportedVisible() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS, 0L);

    CountDownLatch visible = new CountDownLatch(1);
    PeerClusterWatcher.PeerStateListener listener = new PeerClusterWatcher.PeerStateListener() {
      @Override
      public void onPeerStateChanged(HAGroupStoreRecord peerRecord, Stat stat) {
      }

      @Override
      public void onPeerVisible() {
        visible.countDown();
      }

      @Override
      public void onPeerBlind() {
      }
    };

    PeerClusterWatcher watcher = new PeerClusterWatcher(conf, "g", "ns", listener);
    try {
      watcher.reconfigure("");
      assertTrue("blank peer URL should be reported visible", visible.await(5, TimeUnit.SECONDS));
      assertFalse("no peer cache should be built for a blank URL", watcher.hasPeerCache());
    } finally {
      watcher.close();
    }
  }

  /**
   * close() must be idempotent and must stop further reconciliation: a second close() is a no-op,
   * and a post-close reconfigure neither throws nor builds a cache.
   */
  @Test(timeout = 30000)
  public void testCloseIsIdempotentAndStopsReconcile() throws Exception {
    Configuration conf = new Configuration();
    // Retry enabled (non-zero) so close() must tear down the retry executor cleanly, even though
    // lazy scheduling means no periodic retry is armed until the watcher is configured with a peer.
    conf.setLong(HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS, 1L);

    PeerClusterWatcher.PeerStateListener listener = new PeerClusterWatcher.PeerStateListener() {
      @Override
      public void onPeerStateChanged(HAGroupStoreRecord peerRecord, Stat stat) {
      }

      @Override
      public void onPeerVisible() {
      }

      @Override
      public void onPeerBlind() {
      }
    };

    PeerClusterWatcher watcher = new PeerClusterWatcher(conf, "g", "ns", listener);
    watcher.close();
    watcher.close(); // second close is a no-op
    watcher.reconfigure(""); // ignored after close
    assertFalse("closed watcher must not build a peer cache", watcher.hasPeerCache());
  }

  /**
   * The periodic retry is armed lazily: a watcher that is never configured with a peer (or only a
   * blank URL) must arm no retry, so the common "constructed but never used" path starts no
   * background thread. The positive path (a real peer arms the retry and rebuilds the cache once
   * the peer ZK returns) is covered by
   * {@code HAGroupStoreClientIT#testPeerCacheRetryCreatesCacheAfterPeerZkReturns}.
   */
  @Test(timeout = 30000)
  public void testRetryIsNotArmedUntilConfiguredWithPeer() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS, 1L);

    PeerClusterWatcher.PeerStateListener listener = new PeerClusterWatcher.PeerStateListener() {
      @Override
      public void onPeerStateChanged(HAGroupStoreRecord peerRecord, Stat stat) {
      }

      @Override
      public void onPeerVisible() {
      }

      @Override
      public void onPeerBlind() {
      }
    };

    PeerClusterWatcher watcher = new PeerClusterWatcher(conf, "g", "ns", listener);
    try {
      assertFalse("a never-configured watcher must arm no retry", watcher.isRetryScheduled());
      watcher.reconfigure(""); // blank: still nothing to watch
      assertFalse("a blank peer URL must arm no retry", watcher.isRetryScheduled());
    } finally {
      watcher.close();
    }
  }
}
