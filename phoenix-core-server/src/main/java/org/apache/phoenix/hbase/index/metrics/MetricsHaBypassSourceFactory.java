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
package org.apache.phoenix.hbase.index.metrics;

/**
 * Factory for the per-RegionServer {@link MetricsHaBypassSource} singleton. Unlike the per-haGroup
 * factories elsewhere in the codebase, the bypass counter is a single global (per-JVM,
 * per-RegionServer) counter, so this factory holds one eagerly-initialized instance rather than a
 * {@code ConcurrentHashMap} keyed on haGroupName.
 */
public final class MetricsHaBypassSourceFactory {

  private static final MetricsHaBypassSource INSTANCE = new MetricsHaBypassSourceImpl();

  private MetricsHaBypassSourceFactory() {
  }

  /**
   * Returns the process-wide {@link MetricsHaBypassSource} singleton. The instance is initialized
   * eagerly at class-load time, so this method is thread-safe without any additional
   * synchronization.
   * @return the singleton {@link MetricsHaBypassSource} for this RegionServer JVM
   */
  public static MetricsHaBypassSource getInstance() {
    return INSTANCE;
  }
}
