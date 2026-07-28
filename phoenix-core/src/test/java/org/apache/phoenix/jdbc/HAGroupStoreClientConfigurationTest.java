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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.query.QueryServices.HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS;
import static org.apache.phoenix.query.QueryServices.HA_GROUP_STORE_SYNC_INTERVAL_SECONDS;
import static org.apache.phoenix.query.QueryServices.PHOENIX_HA_LEGACY_CRR_RECONCILIATION_INTERVAL_SECONDS;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class HAGroupStoreClientConfigurationTest {

  @Test
  public void testRequiredIntervalsMustBePositive() {
    assertInvalid(HAGroupStoreClient.PHOENIX_HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS, 0L);
    assertInvalid(HAGroupStoreClient.PHOENIX_HA_GROUP_STORE_CLIENT_INITIALIZATION_TIMEOUT_MS, -1L);
    assertInvalid(HA_GROUP_STORE_SYNC_INTERVAL_SECONDS, 0L);
    assertInvalid(HA_GROUP_STORE_SYNC_INTERVAL_SECONDS, -1L);
  }

  @Test
  public void testOptionalIntervalsMayDisableWork() {
    Configuration conf = new Configuration(false);
    conf.setLong(HA_GROUP_STORE_PEER_CACHE_RETRY_INTERVAL_SECONDS, 0L);
    conf.setLong(PHOENIX_HA_LEGACY_CRR_RECONCILIATION_INTERVAL_SECONDS, -1L);
    HAGroupStoreClient.validateConfiguration(conf);
  }

  private static void assertInvalid(String key, long value) {
    Configuration conf = new Configuration(false);
    conf.setLong(key, value);
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
      () -> HAGroupStoreClient.validateConfiguration(conf));
    assertTrue(error.getMessage().contains(key));
    assertTrue(error.getMessage().contains(String.valueOf(value)));
  }
}
