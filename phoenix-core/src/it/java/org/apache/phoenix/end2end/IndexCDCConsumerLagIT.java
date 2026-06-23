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

import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_LAG_SAMPLE_INTERVAL_MS;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_POLL_INTERVAL_MS;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_RETRY_PAUSE_MS;
import static org.apache.phoenix.hbase.index.IndexCDCConsumer.INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS;
import static org.apache.phoenix.hbase.index.metrics.MetricsIndexCDCConsumerSource.CDC_INDEX_UPDATE_LAG;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexCDCConsumerSourceImpl;
import org.apache.phoenix.hbase.index.metrics.MetricsIndexerSourceFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Verifies the {@code cdcIndexUpdateLag} histogram keeps receiving samples while the consumer is
 * idle.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class IndexCDCConsumerLagIT extends ParallelStatsDisabledIT {

  private static final Logger LOG = LoggerFactory.getLogger(IndexCDCConsumerLagIT.class);

  private static final int TIMESTAMP_BUFFER_MS = 2_000;
  private static final int POLL_INTERVAL_MS = 500;
  private static final int LAG_SAMPLE_INTERVAL_MS = 500;
  // Small retry pause so empty-poll backoff doesn't dominate idle behavior.
  private static final int RETRY_PAUSE_MS = 100;
  // Budget for the consumer to start up and emit its first lag sample. Generous because the
  // consumer waits up to INDEX_CDC_CONSUMER_STARTUP_DELAY_MS (default 10s) and then performs
  // CDC_STREAM / IDX_CDC_TRACKER lookups before its first poll. Sized for slow CI / cold JVM.
  private static final long CONSUMER_STARTUP_BUDGET_MS = 120_000L;
  // Idle window for the flow check. Only need to prove ≥ 1 sample fires; kept short to keep
  // total test runtime low.
  private static final long IDLE_WAIT_MS = 5_000L;
  private static final long MAX_LOOKBACK_AGE = 1_000_000L;

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(10);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Long.toString(MAX_LOOKBACK_AGE));
    props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(2));
    props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(1));
    props.put(QueryServices.SERVER_SIDE_IMMUTABLE_INDEXES_ENABLED_ATTRIB, Boolean.TRUE.toString());
    props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
    props.put(INDEX_CDC_CONSUMER_TIMESTAMP_BUFFER_MS, Integer.toString(TIMESTAMP_BUFFER_MS));
    props.put(INDEX_CDC_CONSUMER_POLL_INTERVAL_MS, Integer.toString(POLL_INTERVAL_MS));
    props.put(INDEX_CDC_CONSUMER_LAG_SAMPLE_INTERVAL_MS, Integer.toString(LAG_SAMPLE_INTERVAL_MS));
    props.put(INDEX_CDC_CONSUMER_RETRY_PAUSE_MS, Integer.toString(RETRY_PAUSE_MS));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  private Connection getConnection() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  private MetricHistogram lagHistogram() {
    MetricsIndexCDCConsumerSourceImpl source =
      (MetricsIndexCDCConsumerSourceImpl) MetricsIndexerSourceFactory.getInstance()
        .getIndexCDCConsumerSource();
    return source.getMetricsRegistry().getHistogram(CDC_INDEX_UPDATE_LAG);
  }

  /**
   * Polls until the lag histogram has at least {@code minCount} samples, or fails after timeout.
   */
  private void awaitMinCount(long minCount, long timeoutMs) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    long observed = 0L;
    while (System.currentTimeMillis() < deadline) {
      observed = lagHistogram().getCount();
      if (observed >= minCount) {
        return;
      }
      Thread.sleep(500L);
    }
    fail("Lag histogram never reached count=" + minCount + " within " + timeoutMs + "ms; observed="
      + observed);
  }

  @Test
  public void testLagMetricKeepsSamplingWhenIdle() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();

    try (Connection conn = getConnection()) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (PK VARCHAR NOT NULL PRIMARY KEY," + " V1 VARCHAR, V2 VARCHAR) COLUMN_ENCODED_BYTES=0");
      conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName
        + "(V1) INCLUDE (V2) CONSISTENCY=EVENTUAL");
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (PK, V1, V2) VALUES ('r1', 'v1', 'd1')");
      conn.commit();
    }

    // Wait for the consumer thread to start and emit its first lag sample. Replaces a fixed
    // settle sleep so the test is robust to slow CI / cold JVM startup.
    awaitMinCount(1L, CONSUMER_STARTUP_BUDGET_MS);

    long countBeforeIdle = lagHistogram().getCount();
    Thread.sleep(IDLE_WAIT_MS);
    long countAfterIdle = lagHistogram().getCount();
    long delta = countAfterIdle - countBeforeIdle;
    LOG.info("Idle window {}ms: countBefore={}, countAfter={}, delta={}", IDLE_WAIT_MS,
      countBeforeIdle, countAfterIdle, delta);

    assertTrue("Histogram count did not advance during idle; delta=" + delta, delta >= 1);
  }
}
