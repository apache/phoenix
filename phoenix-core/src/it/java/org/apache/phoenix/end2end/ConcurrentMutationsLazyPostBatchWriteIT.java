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

import java.util.Map;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Test class that extends ConcurrentMutationsExtendedIT with lazy post batch write enabled.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ConcurrentMutationsLazyPostBatchWriteIT extends ConcurrentMutationsExtendedIT {

  public ConcurrentMutationsLazyPostBatchWriteIT(boolean uncovered, boolean eventual) {
    super(uncovered, eventual);
    Assume.assumeFalse("Only covered index supports lazy post batch write mode", uncovered);
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(4);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    props.put(IndexRegionObserver.INDEX_LAZY_POST_BATCH_WRITE, "true");
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(MAX_LOOKBACK_AGE));
    props.put("hbase.rowlock.wait.duration", "100");
    props.put("phoenix.index.concurrent.wait.duration.ms", "10");
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }
}
