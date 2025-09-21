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
package org.apache.phoenix.coprocessor;

import java.util.Map;
import org.apache.hadoop.hbase.monitoring.ThreadLocalServerSideScanMetrics;
import org.apache.phoenix.iterate.HBaseScanMetrics;

public class DataTableScanMetrics {
  private final long fsReadTimeInMs;
  private final long bytesReadFromFS;
  private final long bytesReadFromMemstore;
  private final long bytesReadFromBlockcache;
  private final long blockReadOps;

  protected DataTableScanMetrics(long fsReadTimeInMs, long bytesReadFromFS,
    long bytesReadFromMemstore, long bytesReadFromBlockcache, long blockReadOps) {
    this.fsReadTimeInMs = fsReadTimeInMs;
    this.bytesReadFromFS = bytesReadFromFS;
    this.bytesReadFromMemstore = bytesReadFromMemstore;
    this.bytesReadFromBlockcache = bytesReadFromBlockcache;
    this.blockReadOps = blockReadOps;
  }

  public long getFsReadTimeInMs() {
    return fsReadTimeInMs;
  }

  public long getBytesReadFromFS() {
    return bytesReadFromFS;
  }

  public long getBytesReadFromMemstore() {
    return bytesReadFromMemstore;
  }

  public long getBytesReadFromBlockcache() {
    return bytesReadFromBlockcache;
  }

  public long getBlockReadOps() {
    return blockReadOps;
  }

  public static class Builder {
    protected long fsReadTimeInMs = 0;
    protected long bytesReadFromFS = 0;
    protected long bytesReadFromMemstore = 0;
    protected long bytesReadFromBlockcache = 0;
    protected long blockReadOps = 0;

    public Builder setFsReadTimeInMs(long fsReadTimeInMs) {
      this.fsReadTimeInMs = fsReadTimeInMs;
      return this;
    }

    public Builder setBytesReadFromFS(long bytesReadFromFS) {
      this.bytesReadFromFS = bytesReadFromFS;
      return this;
    }

    public Builder setBytesReadFromMemstore(long bytesReadFromMemstore) {
      this.bytesReadFromMemstore = bytesReadFromMemstore;
      return this;
    }

    public Builder setBytesReadFromBlockcache(long bytesReadFromBlockcache) {
      this.bytesReadFromBlockcache = bytesReadFromBlockcache;
      return this;
    }

    public Builder setBlockReadOps(long blockReadOps) {
      this.blockReadOps = blockReadOps;
      return this;
    }

    public DataTableScanMetrics build() {
      return new DataTableScanMetrics(fsReadTimeInMs, bytesReadFromFS, bytesReadFromMemstore,
        bytesReadFromBlockcache, blockReadOps);
    }
  }

  public static void buildDataTableScanMetrics(Map<String, Long> scanMetrics, Builder builder) {
    builder.setFsReadTimeInMs(scanMetrics.get(HBaseScanMetrics.FS_READ_TIME_METRIC_NAME))
      .setBytesReadFromFS(scanMetrics.get(HBaseScanMetrics.BYTES_READ_FROM_FS_METRIC_NAME))
      .setBytesReadFromMemstore(
        scanMetrics.get(HBaseScanMetrics.BYTES_READ_FROM_MEMSTORE_METRIC_NAME))
      .setBytesReadFromBlockcache(
        scanMetrics.get(HBaseScanMetrics.BYTES_READ_FROM_BLOCK_CACHE_METRIC_NAME))
      .setBlockReadOps(scanMetrics.get(HBaseScanMetrics.BLOCK_READ_OPS_COUNT_METRIC_NAME));
  }

  public void populateThreadLocalServerSideScanMetrics() {
    ThreadLocalServerSideScanMetrics.addFsReadTime(fsReadTimeInMs);
    ThreadLocalServerSideScanMetrics.addBytesReadFromFs(bytesReadFromFS);
    ThreadLocalServerSideScanMetrics.addBytesReadFromMemstore(bytesReadFromMemstore);
    ThreadLocalServerSideScanMetrics.addBytesReadFromBlockCache(bytesReadFromBlockcache);
    ThreadLocalServerSideScanMetrics.addBlockReadOpsCount(blockReadOps);
  }
}
