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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;

/**
 * Helpers shared by the local and peer {@link PathChildrenCache}s backing
 * {@link HAGroupStoreClient} and {@link PeerClusterWatcher}: parsing node data into a record and
 * building a started cache.
 */
final class HAGroupStoreCacheUtil {

  private HAGroupStoreCacheUtil() {
  }

  /**
   * Parse a node's data into (record, stat): (null, null) when absent; a null record (stat still
   * returned) when present but unparseable.
   */
  static Pair<HAGroupStoreRecord, Stat> recordAndStat(ChildData childData) {
    if (childData == null) {
      return Pair.of(null, null);
    }
    return Pair.of(HAGroupStoreRecord.fromJson(childData.getData()).orElse(null),
      childData.getStat());
  }

  /** Read the current (record, stat) for {@code path} from {@code cache}. */
  static Pair<HAGroupStoreRecord, Stat> recordAndStatAt(PathChildrenCache cache, String path) {
    return cache == null ? Pair.of(null, null) : recordAndStat(cache.getCurrentData(path));
  }

  /**
   * Build and start a cache, waiting up to {@code timeoutMs} for the initial load. The supplied
   * {@code listener} receives every cache event; this method releases its initial-load latch on the
   * {@code INITIALIZED} event in a {@code finally}, after the listener returns, so a listener that
   * throws while handling {@code INITIALIZED} cannot strand startup. Returns the started cache, or
   * null (closed) if it did not initialize within {@code timeoutMs}.
   */
  static PathChildrenCache startCache(CuratorFramework curator, PathChildrenCacheListener listener,
    long timeoutMs) throws Exception {
    PathChildrenCache cache = new PathChildrenCache(curator, ZKPaths.PATH_SEPARATOR, true);
    try {
      CountDownLatch initialized = new CountDownLatch(1);
      cache.getListenable().addListener((c, e) -> {
        // Always release the latch on INITIALIZED, even if the caller listener throws while
        // handling it; otherwise startCache would time out and return null for a cache that
        // actually initialized.
        try {
          listener.childEvent(c, e);
        } finally {
          if (e.getType() == PathChildrenCacheEvent.Type.INITIALIZED) {
            initialized.countDown();
          }
        }
      });
      cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
      if (initialized.await(timeoutMs, TimeUnit.MILLISECONDS)) {
        return cache;
      }
      cache.close();
      return null;
    } catch (Exception e) {
      try {
        cache.close();
      } catch (IOException ignore) {
        // best effort
      }
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw e;
    }
  }
}
