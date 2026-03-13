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

import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_RETRY_PAUSE_MS;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class IndexToolForNonTxGlobalIndexEventualIT extends IndexToolForNonTxGlobalIndexIT {

  public IndexToolForNonTxGlobalIndexEventualIT(boolean mutable, boolean singleCell) {
    super(mutable, singleCell);
    if (indexDDLOptions.trim().isEmpty()) {
      indexDDLOptions = " CONSISTENCY=EVENTUAL";
    } else {
      indexDDLOptions = " CONSISTENCY=EVENTUAL," + indexDDLOptions;
    }
  }

  @Override
  protected void waitForEventualConsistency() throws Exception {
    Thread.sleep(15000);
  }

  @BeforeClass
  public static synchronized void setup() throws Exception {
    Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(12);
    serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
    serverProps.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB,
      Long.toString(5));
    serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
      QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
    serverProps.put(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS, Long.toString(8));
    serverProps.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Long.toString(MAX_LOOKBACK_AGE));
    serverProps.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(2));
    serverProps.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(1));
    serverProps.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB,
      Long.toString(0));
    serverProps.put("hbase.regionserver.rpc.retry.interval", Long.toString(0));
    serverProps.put("hbase.procedure.remote.dispatcher.delay.msec", Integer.toString(0));
    serverProps.put(INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS, Integer.toString(2000));
    serverProps.put(INDEX_CDC_CONSUMER_RETRY_PAUSE_MS, Integer.toString(5));
    serverProps.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Integer.toString(-1));
    Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(5);
    clientProps.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
    clientProps.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
    clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
    clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
    clientProps.put(QueryServices.SERVER_SIDE_IMMUTABLE_INDEXES_ENABLED_ATTRIB,
      Boolean.TRUE.toString());
    destroyDriver();
    setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
      new ReadOnlyProps(clientProps.entrySet().iterator()));
    getUtility().getConfiguration().set(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER, "1");
  }

  @Parameterized.Parameters(name = "mutable={0}, singleCellIndex={1}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(
      new Object[][] { { true, true }, { true, false }, { false, true }, { false, false } });
  }
}
