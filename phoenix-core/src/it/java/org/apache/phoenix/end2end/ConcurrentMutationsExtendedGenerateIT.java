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

import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_BATCH_SIZE;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS;
import static org.apache.phoenix.hbase.index.IndexRegionObserver.PHOENIX_INDEX_CDC_MUTATION_SERIALIZE;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class ConcurrentMutationsExtendedGenerateIT extends ConcurrentMutationsExtendedIT {

  public ConcurrentMutationsExtendedGenerateIT(boolean uncovered, boolean eventual) {
    super(uncovered, eventual);
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(10);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(MAX_LOOKBACK_AGE));
    props.put("hbase.rowlock.wait.duration", "100");
    props.put("phoenix.index.concurrent.wait.duration.ms", "10");
    props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(2));
    props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(1));
    props.put(INDEX_CDC_CONSUMER_BATCH_SIZE, Integer.toString(4500));
    props.put(INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS, Integer.toString(2500));
    // props.put(INDEX_CDC_CONSUMER_RETRY_PAUSE_MS, Integer.toString(200));
    props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
    props.put(PHOENIX_INDEX_CDC_MUTATION_SERIALIZE, Boolean.FALSE.toString());
    props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Integer.toString(-1));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Parameterized.Parameters(name = "uncovered={0}, eventual={1}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { false, true }, { true, true } });
  }
}
