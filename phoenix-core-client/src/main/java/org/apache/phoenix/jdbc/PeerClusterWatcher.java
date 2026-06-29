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

import static org.apache.phoenix.jdbc.PhoenixHAAdmin.toPath;
import static org.apache.phoenix.query.QueryServices.HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.MoreExecutors;

/**
 * Watches one peer cluster's {@link HAGroupStoreRecord} over the peer's ZooKeeper, for a single HA
 * group on one RegionServer. Owns the peer cache + admin and a background retry, and reports peer
 * state changes (de-duplicated by znode version, with one forced redelivery after a reconnect) and
 * visible&lt;-&gt;blind transitions. Thread-safe; the (possibly blocking) cache build runs on the
 * caller's thread for the initial {@link #reconfigure} and on the retry executor afterwards, never
 * holding {@link #stateLock}. Listener callbacks fire outside the lock.
 */
final class PeerClusterWatcher implements Closeable {

  /**
   * Sink for what the watcher observes; implemented by {@link HAGroupStoreClient}. Callbacks run
   * while the watcher holds {@code transitionLock} (outside {@code stateLock}), so implementations
   * must not re-enter the watcher (e.g. {@code reconfigure} / {@code close}) and should offload
   * blocking work.
   */
  interface PeerStateListener {
    /**
     * Current peer HA record. May be redelivered once after reconnect even if the znode version did
     * not change; consumers must tolerate duplicate same-state delivery.
     */
    void onPeerStateChanged(HAGroupStoreRecord peerRecord, Stat stat);

    /** Peer connectivity is visible again; this is not a peer HA state transition. */
    void onPeerVisible();

    /** Peer connectivity is unavailable; this is not a peer HA state transition. */
    void onPeerBlind();
  }

  private enum Visibility {
    UNKNOWN,
    VISIBLE,
    BLIND
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PeerClusterWatcher.class);
  private static final long RETRY_WARN_EVERY_N_ATTEMPTS = 10L;

  private final Configuration conf;
  private final String haGroupName;
  private final String namespace;
  private final PeerStateListener listener;
  private final long initTimeoutMs;
  private final long retryIntervalSec;

  // Serializes a whole reconcile (close/build/publish) so the constructor's synchronous reconcile
  // and the retry executor never build concurrently. Held only by ensureConnection;
  // stateLock is taken briefly inside it for field access (ordering is always
  // reconcileLock -> stateLock).
  private final Object reconcileLock = new Object();
  private final Object stateLock = new Object();
  // Serializes a visibility transition with its notification so concurrent transitions (peer event
  // thread vs reconcile/retry executor) cannot reorder the notifications they deliver.
  private final Object transitionLock = new Object();
  private String peerZkUrl; // desired peer; blank = none
  private PhoenixHAAdmin admin;
  private PathChildrenCache cache;
  private int lastDeliveredVersion = -1;
  private long peerCacheRetryAttempts = 0L;
  private volatile boolean watcherClosed = false;
  private ScheduledExecutorService retryExecutor;
  private boolean retryScheduled = false;
  private volatile Visibility visibility = Visibility.UNKNOWN;

  PeerClusterWatcher(Configuration conf, String haGroupName, String namespace,
    PeerStateListener listener) {
    this.conf = conf;
    this.haGroupName = haGroupName;
    this.namespace = namespace;
    this.listener = listener;
    this.initTimeoutMs =
      conf.getLong(HAGroupStoreClient.PHOENIX_HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS,
        HAGroupStoreClient.DEFAULT_HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS);
    this.retryIntervalSec = conf.getLong(HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS,
      DEFAULT_HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS);
    // Create the executor up front (no worker thread starts until the first task is submitted) so
    // reconfigureAsync can run off the caller's thread. The periodic retry is scheduled lazily on
    // the first reconfigure with a peer; a watcher that is never configured never starts a thread.
    this.retryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "PeerClusterWatcher-" + haGroupName);
      t.setDaemon(true);
      return t;
    });
  }

  /** Set/change/clear the peer and reconcile the connection synchronously. */
  void reconfigure(String url) {
    synchronized (stateLock) {
      if (watcherClosed) {
        return;
      }
      peerZkUrl = url;
      maybeScheduleRetry();
    }
    ensureConnection();
  }

  // Schedule the periodic retry once, the first time the watcher has a peer to watch, so a watcher
  // that is never configured starts no thread. Call under stateLock.
  private void maybeScheduleRetry() {
    if (
      retryScheduled || watcherClosed || retryIntervalSec <= 0 || StringUtils.isBlank(peerZkUrl)
    ) {
      return;
    }
    long initialDelaySec = ThreadLocalRandom.current().nextLong(1, retryIntervalSec + 1);
    retryExecutor.scheduleAtFixedRate(this::retryIfBlind, initialDelaySec, retryIntervalSec,
      TimeUnit.SECONDS);
    retryScheduled = true;
  }

  /** Reconcile off the caller's thread; used from the Curator event thread. */
  void reconfigureAsync(String url) {
    ScheduledExecutorService ex = retryExecutor;
    if (ex == null) {
      reconfigure(url);
      return;
    }
    try {
      ex.execute(() -> reconfigure(url));
    } catch (RejectedExecutionException e) {
      LOGGER.debug("Peer reconfigure skipped for HA group {}: watcher closing", haGroupName);
    }
  }

  /** Current peer record, or null when the peer is not visible. */
  HAGroupStoreRecord getCurrentPeerRecord() {
    // Read the cache under the lock: close() nulls the field and closes the cache only after
    // acquiring this lock, so the O(1) in-memory read here always sees a live cache.
    synchronized (stateLock) {
      return HAGroupStoreCacheUtil.recordAndStatAt(cache, toPath(haGroupName)).getLeft();
    }
  }

  /** True when the peer is not currently visible (unknown or lost). */
  boolean isBlind() {
    return visibility != Visibility.VISIBLE;
  }

  /** Whether the peer cache is currently built and live. */
  @VisibleForTesting
  boolean hasPeerCache() {
    synchronized (stateLock) {
      return cache != null;
    }
  }

  /** Whether the periodic retry has been armed; scheduled lazily on the first configured peer. */
  @VisibleForTesting
  boolean isRetryScheduled() {
    synchronized (stateLock) {
      return retryScheduled;
    }
  }

  /** Blocking, event-free rebuild of the peer cache (mirrors {@code PathChildrenCache.rebuild}). */
  void rebuild() {
    PathChildrenCache c;
    synchronized (stateLock) {
      c = cache;
    }
    if (c != null) {
      try {
        c.rebuild();
      } catch (Exception e) {
        LOGGER.error("Peer cache rebuild failed for HA group {}", haGroupName, e);
      }
    }
  }

  @Override
  public void close() {
    ScheduledExecutorService ex;
    synchronized (stateLock) {
      watcherClosed = true;
      ex = retryExecutor;
      retryExecutor = null;
    }
    if (ex != null) {
      MoreExecutors.shutdownAndAwaitTermination(ex, 5, TimeUnit.SECONDS);
    }
    closeConnection();
  }

  private void retryIfBlind() {
    boolean needsBuild;
    String desiredUrl;
    long attempt = 0L;
    synchronized (stateLock) {
      needsBuild = !watcherClosed && StringUtils.isNotBlank(peerZkUrl) && cache == null;
      desiredUrl = peerZkUrl;
      if (needsBuild) {
        attempt = ++peerCacheRetryAttempts;
      }
    }
    if (needsBuild) {
      if (attempt == 1L || attempt % RETRY_WARN_EVERY_N_ATTEMPTS == 0L) {
        LOGGER.warn("Retrying peer cache build for HA group {} with peer ZK URL {} (attempt {})",
          haGroupName, desiredUrl, attempt);
      } else {
        LOGGER.debug("Retrying peer cache build for HA group {} with peer ZK URL {} (attempt {})",
          haGroupName, desiredUrl, attempt);
      }
      ensureConnection();
    }
  }

  /**
   * Reconcile the live connection to the desired peer URL. Serialized by {@link #reconcileLock} so
   * builds never overlap; the (possibly blocking) build runs without holding {@link #stateLock}.
   */
  private void ensureConnection() {
    synchronized (reconcileLock) {
      String desiredUrl;
      synchronized (stateLock) {
        if (watcherClosed) {
          return;
        }
        desiredUrl = peerZkUrl;
        if (
          StringUtils.isNotBlank(desiredUrl) && cache != null
            && StringUtils.equals(desiredUrl, admin.getZkUrl())
        ) {
          return; // already connected to this peer
        }
      }
      closeConnection();
      if (StringUtils.isBlank(desiredUrl)) {
        setVisible(); // no peer configured: nothing to be blind about
        return;
      }
      PhoenixHAAdmin newAdmin = null;
      PathChildrenCache newCache = null;
      try {
        newAdmin = new PhoenixHAAdmin(desiredUrl, conf, namespace);
        newCache = HAGroupStoreCacheUtil.startCache(newAdmin.getCurator(), peerCacheListener(),
          initTimeoutMs);
      } catch (Exception e) {
        LOGGER.error("Unable to build peer cache for HA group {}", haGroupName, e);
      }
      if (newCache == null) {
        closeAdminQuietly(newAdmin);
        setBlind();
        return;
      }
      // close() can race the lock-free build above; publish only while still open, else discard.
      boolean watcherStillOpen;
      synchronized (stateLock) {
        watcherStillOpen = !watcherClosed;
        if (watcherStillOpen) {
          admin = newAdmin;
          cache = newCache;
          peerCacheRetryAttempts = 0L;
        }
      }
      if (watcherStillOpen) {
        setVisible();
      } else {
        closeCacheQuietly(newCache);
        closeAdminQuietly(newAdmin);
      }
    }
  }

  private PathChildrenCacheListener peerCacheListener() {
    return (client, event) -> {
      switch (event.getType()) {
        case CHILD_ADDED:
        case CHILD_UPDATED:
          deliver(HAGroupStoreCacheUtil.recordAndStat(event.getData()), false);
          break;
        case CONNECTION_RECONNECTED:
          onReconnected();
          break;
        case CONNECTION_SUSPENDED:
        case CONNECTION_LOST:
          setBlind();
          break;
        default:
          break;
      }
    };
  }

  private void onReconnected() {
    // Force one redelivery of the current peer record after a reconnect: while the peer ZK
    // connection was down we may have missed CHILD_UPDATED events, so we re-deliver to guarantee no
    // peer state transition is dropped across the disconnect window. Subscribers tolerate the
    // duplicate (per the PeerStateListener contract). Snapshot under the lock so a concurrent
    // close() cannot turn this into a read off a closed cache; deliver() runs outside the lock and
    // no-ops if empty.
    Pair<HAGroupStoreRecord, Stat> snapshot;
    synchronized (stateLock) {
      lastDeliveredVersion = -1; // bypass the de-dup check so the forced redelivery is not skipped
      snapshot = HAGroupStoreCacheUtil.recordAndStatAt(cache, toPath(haGroupName));
    }
    setVisible();
    deliver(snapshot, true);
  }

  private void deliver(Pair<HAGroupStoreRecord, Stat> recordAndStat, boolean forced) {
    HAGroupStoreRecord record = recordAndStat.getLeft();
    if (record == null || !Objects.equals(record.getHaGroupName(), haGroupName)) {
      return;
    }
    Stat stat = recordAndStat.getRight();
    synchronized (stateLock) {
      int version = stat != null ? stat.getVersion() : -1;
      if (!forced && version <= lastDeliveredVersion) {
        return; // duplicate or stale peer event
      }
      lastDeliveredVersion = version;
    }
    listener.onPeerStateChanged(record, stat);
  }

  // decide-and-notify runs under transitionLock so a concurrent opposite transition cannot reorder
  // its notification with this one; the watcherClosed/visibility check is under stateLock, and the
  // listener callback runs outside stateLock.
  @VisibleForTesting
  void setVisible() {
    synchronized (transitionLock) {
      synchronized (stateLock) {
        if (watcherClosed || visibility == Visibility.VISIBLE) {
          return;
        }
        visibility = Visibility.VISIBLE;
      }
      LOGGER.info("Peer visible for HA group {}", haGroupName);
      listener.onPeerVisible();
    }
  }

  @VisibleForTesting
  void setBlind() {
    synchronized (transitionLock) {
      String url;
      synchronized (stateLock) {
        if (watcherClosed || visibility == Visibility.BLIND) {
          return;
        }
        visibility = Visibility.BLIND;
        url = peerZkUrl;
      }
      LOGGER.warn("Peer not visible for HA group {} (peer ZK {}); peer ZK may be unreachable",
        haGroupName, url);
      listener.onPeerBlind();
    }
  }

  private void closeConnection() {
    PathChildrenCache c;
    PhoenixHAAdmin a;
    synchronized (stateLock) {
      c = cache;
      cache = null;
      a = admin;
      admin = null;
      lastDeliveredVersion = -1;
    }
    closeCacheQuietly(c);
    closeAdminQuietly(a);
  }

  private void closeCacheQuietly(PathChildrenCache c) {
    if (c != null) {
      try {
        c.close();
      } catch (IOException e) {
        LOGGER.warn("Failed to close peer cache for HA group {}", haGroupName, e);
      }
    }
  }

  private void closeAdminQuietly(PhoenixHAAdmin a) {
    if (a != null) {
      try {
        a.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close peer admin for HA group {}", haGroupName, e);
      }
    }
  }
}
