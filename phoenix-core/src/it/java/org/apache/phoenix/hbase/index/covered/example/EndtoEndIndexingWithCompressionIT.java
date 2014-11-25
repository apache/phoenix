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
package org.apache.phoenix.hbase.index.covered.example;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.phoenix.hbase.index.IndexTestingUtils;
import org.apache.phoenix.hbase.index.Indexer;
import org.junit.BeforeClass;

/**
 * Test secondary indexing from an end-to-end perspective (client to server to index table).
 */


public class EndtoEndIndexingWithCompressionIT extends EndToEndCoveredIndexingIT {

  @BeforeClass
  public static void setupCluster() throws Exception {
    //add our codec and enable WAL compression
    Configuration conf = UTIL.getConfiguration();
    setUpConfigForMiniCluster(conf);
    IndexTestingUtils.setupConfig(conf);
    // disable version checking, so we can test against whatever version of HBase happens to be
    // installed (right now, its generally going to be SNAPSHOT versions).
    conf.setBoolean(Indexer.CHECK_VERSION_CONF_KEY, false);
    conf.set(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY,
    IndexedWALEditCodec.class.getName());
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    //start the mini-cluster
    UTIL.startMiniCluster();
  }
}