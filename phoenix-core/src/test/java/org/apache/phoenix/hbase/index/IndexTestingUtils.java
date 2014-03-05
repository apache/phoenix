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
package org.apache.phoenix.hbase.index;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.util.Bytes;



/**
 * Utility class for testing indexing
 */
public class IndexTestingUtils {

  private static final Log LOG = LogFactory.getLog(IndexTestingUtils.class);
  private static final String MASTER_INFO_PORT_KEY = "hbase.master.info.port";
  private static final String RS_INFO_PORT_KEY = "hbase.regionserver.info.port";
  
  private IndexTestingUtils() {
    // private ctor for util class
  }

  public static void setupConfig(Configuration conf) {
      conf.setInt(MASTER_INFO_PORT_KEY, -1);
      conf.setInt(RS_INFO_PORT_KEY, -1);
    // setup our codec, so we get proper replay/write
      conf.set(WALCellCodec.WAL_CELL_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());
  }
  /**
   * Verify the state of the index table between the given key and time ranges against the list of
   * expected keyvalues.
   * @throws IOException
   */
  @SuppressWarnings("javadoc")
  public static void verifyIndexTableAtTimestamp(HTable index1, List<KeyValue> expected,
      long start, long end, byte[] startKey, byte[] endKey) throws IOException {
    LOG.debug("Scanning " + Bytes.toString(index1.getTableName()) + " between times (" + start
        + ", " + end + "] and keys: [" + Bytes.toString(startKey) + ", " + Bytes.toString(endKey)
        + "].");
    Scan s = new Scan(startKey, endKey);
    // s.setRaw(true);
    s.setMaxVersions();
    s.setTimeRange(start, end);
    List<KeyValue> received = new ArrayList<KeyValue>();
    ResultScanner scanner = index1.getScanner(s);
    for (Result r : scanner) {
      received.addAll(r.list());
      LOG.debug("Received: " + r.list());
    }
    scanner.close();
    assertEquals("Didn't get the expected kvs from the index table!", expected, received);
  }

  public static void verifyIndexTableAtTimestamp(HTable index1, List<KeyValue> expected, long ts,
      byte[] startKey) throws IOException {
    IndexTestingUtils.verifyIndexTableAtTimestamp(index1, expected, ts, startKey, HConstants.EMPTY_END_ROW);
  }

  public static void verifyIndexTableAtTimestamp(HTable index1, List<KeyValue> expected, long start,
      byte[] startKey, byte[] endKey) throws IOException {
    verifyIndexTableAtTimestamp(index1, expected, start, start + 1, startKey, endKey);
  }
}
