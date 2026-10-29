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
package org.apache.phoenix.iterate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

public class BaseResultIteratorsRetryTest extends BaseConnectionlessQueryTest {

  private static final int MAX_EXPECTED_SUBMITS = 50;

  /**
   * Fails every submitted scan with the supplied exception and counts submissions.
   */
  private static class FailingResultIterators extends BaseResultIterators {
    private final Supplier<SQLException> failure;
    private final AtomicInteger submits = new AtomicInteger();

    FailingResultIterators(QueryPlan plan, Supplier<SQLException> failure) throws SQLException {
      super(plan, null, null, DefaultParallelScanGrouper.getInstance(), plan.getContext().getScan(),
        new HashMap<>(), null);
      this.failure = failure;
    }

    @Override
    protected boolean isSerial() {
      return false;
    }

    @Override
    protected String getName() {
      return "TEST";
    }

    @Override
    protected void submitWork(List<List<Scan>> nestedScans,
      List<List<Pair<Scan, Future<PeekingResultIterator>>>> nestedFutures,
      Queue<PeekingResultIterator> allIterators, int estFlattenedSize, boolean isReverse,
      ParallelScanGrouper scanGrouper, long maxQueryEndTime) throws SQLException {
      assertTrue("Unbounded retries", submits.incrementAndGet() <= MAX_EXPECTED_SUBMITS);
      for (List<Scan> scans : nestedScans) {
        List<Pair<Scan, Future<PeekingResultIterator>>> futures = new ArrayList<>(scans.size());
        for (Scan scan : scans) {
          CompletableFuture<PeekingResultIterator> future = new CompletableFuture<>();
          future.completeExceptionally(failure.get());
          futures.add(new Pair<>(scan, future));
        }
        nestedFutures.add(futures);
      }
    }
  }

  private static int getRetries(Connection conn) throws SQLException {
    return conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration().getInt(
      QueryConstants.HASH_JOIN_CACHE_RETRIES, QueryConstants.DEFAULT_HASH_JOIN_CACHE_RETRIES);
  }

  private static QueryPlan createTableAndGetPlan(Connection conn) throws SQLException {
    String tableName = generateUniqueName();
    conn.createStatement()
      .execute("CREATE TABLE " + tableName + " (K VARCHAR PRIMARY KEY, V VARCHAR)");
    return TestUtil.getOptimizeQueryPlanNoIterator(conn, "SELECT * FROM " + tableName);
  }

  @Test
  public void testStaleRegionBoundaryRetriesAreBounded() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      QueryPlan plan = createTableAndGetPlan(conn);
      FailingResultIterators iterators =
        new FailingResultIterators(plan, StaleRegionBoundaryCacheException::new);
      try {
        iterators.getIterators();
        fail("Expected StaleRegionBoundaryCacheException");
      } catch (StaleRegionBoundaryCacheException e) {
        // expected
      }
      assertEquals(getRetries(conn) + 1, iterators.submits.get());
    }
  }

  @Test
  public void testColumnFamilyNotFoundRetriesAreBounded() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      QueryPlan plan = createTableAndGetPlan(conn);
      plan.getContext().getScan().setAttribute(BaseScannerRegionObserverConstants.LOCAL_INDEX_BUILD,
        new byte[] { 1 });
      FailingResultIterators iterators = new FailingResultIterators(plan,
        () -> new ColumnFamilyNotFoundException(null, null, "L#0"));
      try {
        iterators.getIterators();
        fail("Expected ColumnFamilyNotFoundException");
      } catch (ColumnFamilyNotFoundException e) {
        // expected
      }
      assertEquals(getRetries(conn) + 1, iterators.submits.get());
    }
  }
}
