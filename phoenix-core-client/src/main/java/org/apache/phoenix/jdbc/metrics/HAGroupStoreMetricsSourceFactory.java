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
package org.apache.phoenix.jdbc.metrics;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for process-lifetime HAGroupStore metric sources.
 * <p>
 * Creates one source lazily per HA group and intentionally retains it for the JVM lifetime so
 * cumulative counters survive client replacement. HA-group names are expected to be a small, stable
 * set within a process.
 */
public final class HAGroupStoreMetricsSourceFactory {

  private static final ConcurrentHashMap<String, HAGroupStoreMetricsSource> SOURCES =
    new ConcurrentHashMap<>();

  private HAGroupStoreMetricsSourceFactory() {
  }

  public static HAGroupStoreMetricsSource getInstanceForHAGroup(String haGroupName) {
    return SOURCES.computeIfAbsent(haGroupName, HAGroupStoreMetricsSourceImpl::new);
  }
}
