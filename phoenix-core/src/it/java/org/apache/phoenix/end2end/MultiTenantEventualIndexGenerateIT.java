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
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_RETRY_PAUSE_MS;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS;
import static org.apache.phoenix.hbase.index.IndexRegionObserver.PHOENIX_INDEX_CDC_MUTATION_SERIALIZE;

import java.util.Map;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class MultiTenantEventualIndexGenerateIT extends MultiTenantEventualIndexIT {

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(12);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(MAX_LOOKBACK_AGE));
    props.put(INDEX_CDC_CONSUMER_BATCH_SIZE, Integer.toString(3500));
    props.put(INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS, Integer.toString(4000));
    props.put(INDEX_CDC_CONSUMER_RETRY_PAUSE_MS, Long.toString(10));
    props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(2));
    props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(1));
    props.put(QueryServices.SERVER_SIDE_IMMUTABLE_INDEXES_ENABLED_ATTRIB, Boolean.TRUE.toString());
    props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
    props.put(PHOENIX_INDEX_CDC_MUTATION_SERIALIZE, Boolean.FALSE.toString());
    props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Integer.toString(-1));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

}
