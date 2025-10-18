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

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

public class SlowestScanReadMetricsQueue {

  public static final SlowestScanReadMetricsQueue NOOP_SLOWEST_SCAN_READ_METRICS_QUEUE =
    new SlowestScanReadMetricsQueue() {
      @Override
      public void add(ScanMetricsGroup scanMetricsGroup) {
      }

      @Override
      public ScanMetricsGroup aggregate() {
        return ScanMetricsGroup.EMPTY_SCAN_METRICS_GROUP;
      }
    };

  private final Deque<ScanMetricsGroup> slowestScanReadMetricsQueue;

  public SlowestScanReadMetricsQueue() {
    this.slowestScanReadMetricsQueue = new ConcurrentLinkedDeque<>();
  }

  public void add(ScanMetricsGroup scanMetricsGroup) {
    this.slowestScanReadMetricsQueue.add(scanMetricsGroup);
  }

  public ScanMetricsGroup aggregate() {
    ScanMetricsGroup slowestScanMetricsGroup = null;
    long maxMillisBetweenNexts = Long.MIN_VALUE;
    while (!slowestScanReadMetricsQueue.isEmpty()) {
      ScanMetricsGroup scanMetricsGroup = slowestScanReadMetricsQueue.poll();
      long currentMillisBetweenNexts = scanMetricsGroup.getSumOfMillisSecBetweenNexts();
      if (slowestScanMetricsGroup == null || currentMillisBetweenNexts > maxMillisBetweenNexts) {
        slowestScanMetricsGroup = scanMetricsGroup;
        maxMillisBetweenNexts = currentMillisBetweenNexts;
      }
    }

    if (slowestScanMetricsGroup == null) {
      return ScanMetricsGroup.EMPTY_SCAN_METRICS_GROUP;
    }
    return slowestScanMetricsGroup;
  }
}
