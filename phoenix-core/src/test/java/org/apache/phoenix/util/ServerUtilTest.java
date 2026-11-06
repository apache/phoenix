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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.junit.Test;

public class ServerUtilTest {

  @Test
  public void testMetadataConfigDefaults() {
    Configuration conf = new Configuration(false);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 150);
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 60000);
    conf.set("some.other.key", "value");

    Configuration metadataConf = ServerUtil.getMetadataConfig(conf);

    int timeoutMs = QueryServicesOptions.DEFAULT_METADATA_SERVER_LOOKUP_TIMEOUT_MS;
    assertEquals(timeoutMs,
      metadataConf.getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1));
    assertEquals(timeoutMs, metadataConf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, -1));
    assertEquals(timeoutMs, metadataConf.getInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, -1));
    assertEquals(timeoutMs,
      metadataConf.getInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, -1));
    assertEquals(timeoutMs, metadataConf.getInt("hbase.client.meta.read.rpc.timeout", -1));
    assertEquals(timeoutMs, metadataConf.getInt("hbase.client.meta.scanner.timeout.period", -1));
    assertEquals("value", metadataConf.get("some.other.key"));
    // backoffs of 0.1+0.2+0.3+0.5+1+2+4 = 8.1s fit in 10s, the next 10s backoff does not
    assertEquals(7, metadataConf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, -1));
    // the source configuration is not modified
    assertEquals(150, conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, -1));
    assertEquals(60000, conf.getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1));
  }

  @Test
  public void testMetadataConfigOverrides() {
    Configuration conf = new Configuration(false);
    conf.setInt(QueryServices.METADATA_SERVER_LOOKUP_TIMEOUT_MS, 2500);

    Configuration metadataConf = ServerUtil.getMetadataConfig(conf);

    assertEquals(2500, metadataConf.getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1));
    assertEquals(2500, metadataConf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, -1));
    assertEquals(2500, metadataConf.getInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, -1));
    // backoffs of 0.1+0.2+0.3+0.5+1 = 2.1s fit in 2.5s
    assertEquals(5, metadataConf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, -1));
  }

  @Test
  public void testMetadataConfigRetriesUseLargerPause() {
    Configuration conf = new Configuration(false);
    conf.setLong(HConstants.HBASE_CLIENT_PAUSE, 1000);
    // backoffs of 1+2+3 = 6s fit in 10s
    assertEquals(3,
      ServerUtil.getMetadataConfig(conf).getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, -1));

    conf.setLong(HConstants.HBASE_CLIENT_PAUSE, 100);
    conf.setLong("hbase.client.pause.server.overloaded", 1000);
    assertEquals(3,
      ServerUtil.getMetadataConfig(conf).getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, -1));
  }

  @Test
  public void testMetadataConfigNonPositiveTimeoutUsesDefault() {
    Configuration conf = new Configuration(false);
    conf.setInt(QueryServices.METADATA_SERVER_LOOKUP_TIMEOUT_MS, 0);

    Configuration metadataConf = ServerUtil.getMetadataConfig(conf);

    assertEquals(QueryServicesOptions.DEFAULT_METADATA_SERVER_LOOKUP_TIMEOUT_MS,
      metadataConf.getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1));
    assertEquals(7, metadataConf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, -1));
  }

  @Test
  public void testRetriesWithinTimeout() {
    assertEquals(0, ServerUtil.getRetriesWithinTimeout(50, 100));
    assertEquals(1, ServerUtil.getRetriesWithinTimeout(100, 100));
    assertEquals(7, ServerUtil.getRetriesWithinTimeout(10000, 100));
    // a larger pause allows fewer retries in the same time
    assertEquals(3, ServerUtil.getRetriesWithinTimeout(10000, 1000));
    // a zero pause must not loop forever
    assertEquals(13, ServerUtil.getRetriesWithinTimeout(1000, 0));
  }

  @Test
  public void testMetadataLookupConnectionUsesMetadataConfig() {
    Configuration conf = new Configuration(false);
    conf.setInt(QueryServices.METADATA_SERVER_LOOKUP_TIMEOUT_MS, 2500);

    Configuration connConf = ServerUtil.ConnectionFactory
      .getTypeSpecificConfiguration(ServerUtil.ConnectionType.METADATA_LOOKUP_CONNECTION, conf);

    assertEquals(2500, connConf.getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, -1));
  }
}
