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
package org.apache.phoenix.compat.hbase;

import java.util.Map;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

public class CompatScanMetrics {
  private CompatScanMetrics() {
    // Not to be instantiated
  }

  public static boolean supportsFineGrainedReadMetrics() {
    return false;
  }

  public static Long getFsReadTime(Map<String, Long> scanMetrics) {
    return 0L;
  }

  public static Long getFsReadTime(ScanMetrics scanMetrics) {
    return 0L;
  }

  public static Long getBytesReadFromFs(Map<String, Long> scanMetrics) {
    return 0L;
  }

  public static Long getBytesReadFromFs(ScanMetrics scanMetrics) {
    return 0L;
  }

  public static Long getBytesReadFromMemstore(Map<String, Long> scanMetrics) {
    return 0L;
  }

  public static Long getBytesReadFromMemstore(ScanMetrics scanMetrics) {
    return 0L;
  }

  public static Long getBytesReadFromBlockCache(Map<String, Long> scanMetrics) {
    return 0L;
  }

  public static Long getBytesReadFromBlockCache(ScanMetrics scanMetrics) {
    return 0L;
  }

  public static Long getBlockReadOpsCount(Map<String, Long> scanMetrics) {
    return 0L;
  }

  public static Long getBlockReadOpsCount(ScanMetrics scanMetrics) {
    return 0L;
  }

  public static Long getRpcScanProcessingTime(Map<String, Long> scanMetrics) {
    return 0L;
  }

  public static Long getRpcScanQueueWaitTime(Map<String, Long> scanMetrics) {
    return 0L;
  }
}
