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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixCoprocessorSourceFactory;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixMasterSource;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class CDCStream2IT extends CDCBaseIT {

  private static final MetricsPhoenixMasterSource METRICS_SOURCE =
    MetricsPhoenixCoprocessorSourceFactory.getInstance().getPhoenixMasterSource();

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(60 * 60)); // An hour
    props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
    props.put("hbase.coprocessor.master.classes", TestPhoenixMasterObserver.class.getName());
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Test
  public void testPartitionMetadataWithRetries() throws Exception {
    Connection conn = newConnection();
    String tableName = generateUniqueName();
    createTableAndEnableCDC(conn, tableName, false);

    assertEquals("Post split partition update failures should be 0 initially", 0,
      METRICS_SOURCE.getPostSplitPartitionUpdateFailureCount());
    assertEquals("Post merge partition update failures should be 0 initially", 0,
      METRICS_SOURCE.getPostMergePartitionUpdateFailureCount());

    // Perform a split operation - this will fail 24 times before succeeding
    TestUtil.splitTable(conn, tableName, Bytes.toBytes("m"));

    // Verify split metric is 24
    assertEquals("Post split partition update failures should be 24 after retries", 24,
      METRICS_SOURCE.getPostSplitPartitionUpdateFailureCount());

    List<HRegionLocation> regions = TestUtil.getAllTableRegions(conn, tableName);

    // Perform a merge operation - this will fail 15 times before succeeding
    TestUtil.mergeTableRegions(conn, tableName, regions.stream().map(HRegionLocation::getRegion)
      .map(RegionInfo::getEncodedName).collect(Collectors.toList()));

    // Verify merge metric is 15
    assertEquals("Post merge partition update failures should be 15 after retries", 15,
      METRICS_SOURCE.getPostMergePartitionUpdateFailureCount());

    ResultSet rs = conn.createStatement()
      .executeQuery("SELECT * FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'");
    List<PartitionMetadata> mergedDaughter = new ArrayList<>();
    List<PartitionMetadata> splitParents = new ArrayList<>();
    while (rs.next()) {
      PartitionMetadata pm = new PartitionMetadata(rs);
      if (pm.startKey == null && pm.endKey == null && pm.endTime == 0) {
        mergedDaughter.add(pm);
      }
      if (pm.startKey != null || pm.endKey != null) {
        splitParents.add(pm);
      }
    }
    assertEquals(2, mergedDaughter.size());
    assertEquals(2, splitParents.size());
    assertEquals(mergedDaughter.get(0).startTime, mergedDaughter.get(1).startTime);
    assertEquals(mergedDaughter.get(0).endTime, mergedDaughter.get(1).endTime);
    assertEquals(mergedDaughter.get(0).partitionId, mergedDaughter.get(1).partitionId);
    assertTrue(mergedDaughter.stream()
      .anyMatch(d -> Objects.equals(d.parentPartitionId, splitParents.get(0).partitionId)));
    assertTrue(mergedDaughter.stream()
      .anyMatch(d -> Objects.equals(d.parentPartitionId, splitParents.get(1).partitionId)));
    for (PartitionMetadata splitDaughter : splitParents) {
      Assert.assertEquals(mergedDaughter.get(0).startTime, splitDaughter.endTime);
    }
  }

}
