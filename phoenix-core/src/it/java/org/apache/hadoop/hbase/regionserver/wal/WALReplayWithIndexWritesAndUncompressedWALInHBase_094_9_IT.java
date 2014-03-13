/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.util.ConfigUtil;

/**
 * Do the WAL Replay test but with the WALEditCodec, rather than an {@link IndexedHLogReader}, but
 * still with compression
 */
public class WALReplayWithIndexWritesAndUncompressedWALInHBase_094_9_IT extends WALReplayWithIndexWritesAndCompressedWALIT {

  @Override
  protected void configureCluster() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    setDefaults(conf);
    LOG.info("Setting HLog impl to indexed log reader");
    conf.set(IndexManagementUtil.HLOG_READER_IMPL_KEY, IndexedHLogReader.class.getName());

    // disable WAL compression
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    // set replication required parameter
    ConfigUtil.setReplicationConfigIfAbsent(conf);
  }
}