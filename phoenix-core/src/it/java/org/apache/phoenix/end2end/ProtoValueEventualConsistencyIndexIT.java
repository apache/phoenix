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
import static org.apache.phoenix.hbase.index.IndexRegionObserver.PHOENIX_INDEX_CDC_MUTATION_SERIALIZE;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ProtoValueEventualConsistencyIndexIT extends ProtoValueIndexIT {
  private static final long WAIT_MILLIS = 15_000L;

  public ProtoValueEventualConsistencyIndexIT(boolean columnEncoded, boolean coveredIndex) {
    super(columnEncoded, coveredIndex);
  }

  @Parameterized.Parameters(name = "columnEncoded={0}, coveredIndex={1}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { false, false }, { true, true } });
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(6);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    props.put(INDEX_CDC_CONSUMER_RETRY_PAUSE_MS, Integer.toString(5));
    props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Integer.toString(-1));
    props.put(PHOENIX_INDEX_CDC_MUTATION_SERIALIZE, Boolean.TRUE.toString());
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  @Before
  public void appendEventualConsistency() {
    if (indexDDLOptions.trim().isEmpty()) {
      indexDDLOptions = " CONSISTENCY=EVENTUAL";
    } else {
      indexDDLOptions = " CONSISTENCY=EVENTUAL," + indexDDLOptions;
    }
  }

  @Override
  protected void waitForEventualConsistency() throws Exception {
    Thread.sleep(WAIT_MILLIS);
  }
}
