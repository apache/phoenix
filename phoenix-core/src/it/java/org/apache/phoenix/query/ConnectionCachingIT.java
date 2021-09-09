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
package org.apache.phoenix.query;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.end2end.ParallelStatsEnabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(ParallelStatsEnabledTest.class)
@RunWith(Parameterized.class)
public class ConnectionCachingIT extends ParallelStatsEnabledIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionCachingIT.class);

  @Parameters(name= "phoenix.scanner.lease.renew.enabled={0}")
  public static synchronized Iterable<String> data() {
    return Arrays.asList("true", "false");
  }

  private String leaseRenewal;

  public ConnectionCachingIT(String leaseRenewalValue) {
    this.leaseRenewal = leaseRenewalValue;
  }

  @Test
  public void test() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    props.put("phoenix.scanner.lease.renew.enabled", leaseRenewal);

    // The test driver works correctly, the real one doesn't.
    String url = getUrl();
    url = url.replace(";" + PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM, "");
    LOGGER.info("URL to use is: {}", url);

    Connection conn = DriverManager.getConnection(url, props);
    long before = getNumCachedConnections(conn);
    for (int i = 0; i < 10_000; i++) {
      Connection c = DriverManager.getConnection(url, props);
      c.close();
    }
    Thread.sleep(QueryServicesOptions.DEFAULT_RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS / 2);
    long after = getNumCachedConnections(conn);
    for (int i = 0; i < 6; i++) {
      LOGGER.info("Found {} connections cached", after);
      if (after <= before) {
        break;
      }
      Thread.sleep(QueryServicesOptions.DEFAULT_RUN_RENEW_LEASE_FREQUENCY_INTERVAL_MILLISECONDS / 2);
      after = getNumCachedConnections(conn);
    }
    assertTrue("Saw " + before + " connections, but ended with " + after, after <= before);
  }

  long getNumCachedConnections(Connection conn) throws Exception {
    PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
    ConnectionQueryServices cqs = pConn.getQueryServices();
    // For whatever reason, we sometimes get a delegate here, and sometimes the real thing.
    if (cqs instanceof DelegateConnectionQueryServices) {
      cqs = ((DelegateConnectionQueryServices) cqs).getDelegate();
    }
    assertTrue("ConnectionQueryServices was a " + cqs.getClass(), cqs instanceof ConnectionQueryServicesImpl);
    ConnectionQueryServicesImpl cqsi = (ConnectionQueryServicesImpl) cqs;
    long cachedConnections = 0L;
    for (LinkedBlockingQueue<WeakReference<PhoenixConnection>> queue : cqsi.getCachedConnections()) {
      cachedConnections += queue.size();
    }
    return cachedConnections;
  }
}
