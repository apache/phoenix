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
package org.apache.phoenix.monitoring;

import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Maintains a queue of {@link ScanMetricsHolder} instances, one instance per HBase scan created by
 * Phoenix client. The class exposes an iterator which returns ScanMetricsHolder instances in
 * reverse order of insertion as the last inserted instance is more likely to correspond to the
 * slowest HBase scan.
 * <p>
 * The insertion of ScanMetricsHolder instances to the queue is thread-safe, so multiple parallel
 * scans can concurrently add their ScanMetricsHolder instances to the queue.
 */
public class SlowestScanMetricsQueue {

  /**
   * A no-op implementation that ignores all additions and returns an empty iterator.
   */
  public static final SlowestScanMetricsQueue NOOP_SLOWEST_SCAN_METRICS_QUEUE =
    new SlowestScanMetricsQueue() {
      @Override
      public void add(ScanMetricsHolder scanMetricsHolder) {
      }

      @Override
      public Iterator<ScanMetricsHolder> getIterator() {
        return Collections.emptyIterator();
      }
    };

  private final Deque<ScanMetricsHolder> slowestScanMetricsQueue;

  /**
   * Creates a new SlowestScanMetricsQueue with an empty concurrent deque.
   */
  public SlowestScanMetricsQueue() {
    this.slowestScanMetricsQueue = new ConcurrentLinkedDeque<>();
  }

  /**
   * Adds a {@link ScanMetricsHolder} instance to the queue. This method is thread-safe and can be
   * called concurrently by multiple threads.
   * @param scanMetricsHolder the scan metrics holder to add to the queue
   */
  public void add(ScanMetricsHolder scanMetricsHolder) {
    this.slowestScanMetricsQueue.add(scanMetricsHolder);
  }

  /**
   * Returns an iterator that traverses the queue in reverse order of insertion (LIFO). The most
   * recently added {@link ScanMetricsHolder} will be returned first.
   * @return an iterator over the scan metrics holders in reverse insertion order
   */
  public Iterator<ScanMetricsHolder> getIterator() {
    return this.slowestScanMetricsQueue.descendingIterator();
  }
}
