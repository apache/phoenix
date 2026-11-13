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
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.write.AbstractParallelWriterIndexCommitter;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Concurrent conditional updates on the same row of an indexed table, where some of the updates are
 * no-ops and release the row early.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ConcurrentNoOpAtomicUpsertIndexIT extends BaseTest {
  private static final Logger LOG =
    LoggerFactory.getLogger(ConcurrentNoOpAtomicUpsertIndexIT.class);

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(4);
    props.put(IndexRegionObserver.INDEX_LAZY_POST_BATCH_WRITE, Boolean.TRUE.toString());
    props.put(IndexRegionObserver.PHOENIX_INDEX_CDC_MUTATION_SERIALIZE, Boolean.FALSE.toString());
    // Batches waiting on the parked batch must not time out
    props.put("phoenix.index.concurrent.wait.duration.ms", Integer.toString(30000));
    // More than one batch can be parked in its index write at a time
    props.put(AbstractParallelWriterIndexCommitter.NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY,
      Integer.toString(4));
    setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }

  private final boolean eventual;

  public ConcurrentNoOpAtomicUpsertIndexIT(boolean eventual) {
    this.eventual = eventual;
  }

  @Parameterized.Parameters(name = "eventual={0}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { true }, { false } });
  }

  /** Parks the next write to an armed table until released, and optionally fails it. */
  public static class BlockingIndexWriteObserver extends SimpleRegionObserver {
    private static final Map<String, Object[]> ARMED = new ConcurrentHashMap<>();

    static void arm(String indexName, CountDownLatch reached, CountDownLatch proceed) {
      arm(indexName, reached, proceed, false);
    }

    static void arm(String indexName, CountDownLatch reached, CountDownLatch proceed,
      boolean fail) {
      ARMED.put(indexName, new Object[] { reached, proceed, fail });
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      String tableName = c.getEnvironment().getRegionInfo().getTable().getNameAsString();
      Object[] armed = ARMED.remove(tableName);
      if (armed == null) {
        return;
      }
      ((CountDownLatch) armed[0]).countDown();
      try {
        ((CountDownLatch) armed[1]).await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      if ((Boolean) armed[2]) {
        throw new DoNotRetryIOException("Simulating an index write failure");
      }
    }
  }

  /** Delays every write to the table it is added to, and fails some of them at random. */
  public static class RandomIndexWriteObserver extends SimpleRegionObserver {
    private static volatile int failurePercent = 0;
    private static volatile int maxDelayMs = 0;

    static void configure(int failurePercent, int maxDelayMs) {
      RandomIndexWriteObserver.failurePercent = failurePercent;
      RandomIndexWriteObserver.maxDelayMs = maxDelayMs;
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      try {
        if (maxDelayMs > 0) {
          Thread.sleep(random.nextInt(maxDelayMs + 1));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      if (random.nextInt(100) < failurePercent) {
        throw new DoNotRetryIOException("Simulating a random index write failure");
      }
    }
  }

  private static int conditionalUpdate(Connection conn, String tableName, String writer,
    int expected) throws Exception {
    return conditionalUpdate(conn, tableName, "r1", writer, expected).getFirst();
  }

  /** Returns the update count and the row returned by the update. */
  private static Pair<Integer, BsonDocument> conditionalUpdate(Connection conn, String tableName,
    String rowKey, String writer, int expected) throws Exception {
    BsonDocument condition = new BsonDocument();
    condition.put("$EXPR", new BsonString("#v = :v"));
    condition.put("$VAL", new BsonDocument().append(":v", new BsonInt32(expected)));
    condition.put("$KEYS", new BsonDocument().append("#v", new BsonString("ver")));
    BsonDocument update = new BsonDocument().append("$SET", new BsonDocument()
      .append("ver", new BsonInt32(expected + 1)).append("by", new BsonString(writer)));
    PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + tableName
      + " VALUES (?) ON DUPLICATE KEY UPDATE COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL, '"
      + condition.toJson() + "') THEN BSON_UPDATE_EXPRESSION(COL, '" + update.toJson()
      + "') ELSE COL END");
    ps.setString(1, rowKey);
    Pair<Integer, ResultSet> result =
      ps.unwrap(PhoenixPreparedStatement.class).executeAtomicUpdateReturnRow();
    ResultSet rs = result.getSecond();
    return new Pair<>(result.getFirst(), rs == null ? null : (BsonDocument) rs.getObject(2));
  }

  private static Thread runAsync(String name, String tableName, int expected,
    AtomicReference<Integer> count, AtomicReference<Exception> error) {
    Thread t = new Thread(() -> {
      try (Connection conn = DriverManager.getConnection(getUrl())) {
        conn.setAutoCommit(true);
        count.set(conditionalUpdate(conn, tableName, name, expected));
      } catch (Exception e) {
        error.set(e);
      }
    }, name);
    t.start();
    return t;
  }

  /** Returns true if the error was caused by a simulated index write failure. */
  private static boolean isSimulatedFailure(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains("Simulating")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates the table with row r1 (ver = 1) and its index, and installs the blocking observer.
   * Returns the name of the table whose write is blocked.
   */
  private String createTable(Connection conn, String tableName) throws Exception {
    String indexName = "IDX_" + tableName;
    conn.createStatement().execute("CREATE TABLE " + tableName
      + " (PK1 VARCHAR NOT NULL, COL BSON CONSTRAINT pk PRIMARY KEY(PK1))");
    conn.createStatement()
      .execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName
        + " (BSON_VALUE(COL, 'idxKey', 'VARCHAR')) "
        + "WHERE BSON_VALUE(COL, 'idxKey', 'VARCHAR') IS NOT NULL"
        + (eventual ? " CONSISTENCY = EVENTUAL" : ""));
    PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?)");
    ps.setString(1, "r1");
    ps.setObject(2, RawBsonDocument.parse("{\"idxKey\": \"g1\", \"ver\": 1}"));
    ps.executeUpdate();

    // With only eventually consistent indexes, the CDC index is the synchronous index write
    String blockedTable = eventual ? CDCUtil.getCDCIndexName("CDC_" + tableName) : indexName;
    TestUtil.addCoprocessor(conn, blockedTable, BlockingIndexWriteObserver.class);
    return blockedTable;
  }

  private static BsonDocument getRow(Connection conn, String tableName) throws Exception {
    ResultSet rs =
      conn.createStatement().executeQuery("SELECT COL FROM " + tableName + " WHERE PK1 = 'r1'");
    assertTrue(rs.next());
    return (BsonDocument) rs.getObject(1);
  }

  private static int getVer(Connection conn, String tableName) throws Exception {
    return getRow(conn, tableName).getInt32("ver").getValue();
  }

  private static String getWriter(Connection conn, String tableName) throws Exception {
    return getRow(conn, tableName).getString("by").getValue();
  }

  /**
   * <ol>
   * <li>Batch A: v 1 -> 2, parked in doPre.</li>
   * <li>Batch B: v 1 -> 2, a no-op on A's pending state. Must not return before A completes.</li>
   * <li>Batch C: v 2 -> 3 while A is pending. Must chain on A, not on B.</li>
   * </ol>
   */
  @Test
  public void testConcurrentConditionalUpdatesWithNoOp() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      String blockedTable = createTable(conn, tableName);

      // A: 1 -> 2, parks in doPre with the row lock released
      CountDownLatch aReached = new CountDownLatch(1);
      CountDownLatch aProceed = new CountDownLatch(1);
      BlockingIndexWriteObserver.arm(blockedTable, aReached, aProceed);
      AtomicReference<Integer> aCount = new AtomicReference<>();
      AtomicReference<Exception> aError = new AtomicReference<>();
      Thread a = runAsync("A", tableName, 1, aCount, aError);
      Thread b = null;
      Thread c = null;
      try {
        assertTrue("batch A did not reach the index write", aReached.await(60, TimeUnit.SECONDS));

        // B: 1 -> 2, a no-op on A's pending state
        AtomicReference<Integer> bCount = new AtomicReference<>();
        AtomicReference<Exception> bError = new AtomicReference<>();
        b = runAsync("B", tableName, 1, bCount, bError);
        b.join(3000);
        assertTrue("batch B returned before batch A completed: " + bError.get(), b.isAlive());

        // C: 2 -> 3 while A is still pending
        AtomicReference<Integer> cCount = new AtomicReference<>();
        AtomicReference<Exception> cError = new AtomicReference<>();
        c = runAsync("C", tableName, 2, cCount, cError);
        c.join(3000);
        assertTrue("batch C returned before batch A completed: " + cError.get(), c.isAlive());

        aProceed.countDown();
        a.join(60000);
        b.join(60000);
        c.join(60000);
        assertFalse(a.isAlive());
        assertFalse(b.isAlive());
        assertFalse(c.isAlive());
        assertNull("batch A failed: " + aError.get(), aError.get());
        assertNull("batch B failed: " + bError.get(), bError.get());
        assertNull("batch C failed: " + cError.get(), cError.get());
        assertEquals(Integer.valueOf(1), aCount.get());
        assertEquals(Integer.valueOf(0), bCount.get());
        assertEquals(Integer.valueOf(1), cCount.get());
      } finally {
        aProceed.countDown();
        a.join(60000);
        if (b != null) {
          b.join(60000);
        }
        if (c != null) {
          c.join(60000);
        }
        TestUtil.removeCoprocessor(conn, blockedTable, BlockingIndexWriteObserver.class);
      }

      assertEquals(3, getVer(conn, tableName));
      assertEquals("C", getWriter(conn, tableName));
    }
  }

  /**
   * <ol>
   * <li>Batch A: v 1 -> 2, parked in doPre.</li>
   * <li>Batch X: v 2 -> 3, chained on A, fails in doPre.</li>
   * <li>Batch B: no-op, must skip X, chain on A and wait for A.</li>
   * <li>Batch C: no-op, must also chain on A, not on B.</li>
   * </ol>
   */
  @Test
  public void testNoOpAfterConcurrentFailedBatch() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      String blockedTable = createTable(conn, tableName);

      CountDownLatch aReached = new CountDownLatch(1);
      CountDownLatch aProceed = new CountDownLatch(1);
      BlockingIndexWriteObserver.arm(blockedTable, aReached, aProceed);
      AtomicReference<Integer> aCount = new AtomicReference<>();
      AtomicReference<Exception> aError = new AtomicReference<>();
      Thread a = runAsync("A", tableName, 1, aCount, aError);
      Thread b = null;
      Thread c = null;
      try {
        assertTrue("batch A did not reach the index write", aReached.await(60, TimeUnit.SECONDS));

        IndexRegionObserver.setFailPreIndexUpdatesForTesting(true);
        try {
          conditionalUpdate(conn, tableName, "X", 2);
          fail("batch X should have failed");
        } catch (SQLException e) {
          assertTrue("unexpected error: " + e, isSimulatedFailure(e));
        } finally {
          IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
        }

        AtomicReference<Integer> bCount = new AtomicReference<>();
        AtomicReference<Exception> bError = new AtomicReference<>();
        b = runAsync("B", tableName, 99, bCount, bError);
        b.join(3000);
        assertTrue("batch B returned before batch A completed: " + bError.get(), b.isAlive());

        AtomicReference<Integer> cCount = new AtomicReference<>();
        AtomicReference<Exception> cError = new AtomicReference<>();
        c = runAsync("C", tableName, 99, cCount, cError);
        c.join(3000);
        assertTrue("batch C returned before batch A completed: " + cError.get(), c.isAlive());

        aProceed.countDown();
        a.join(60000);
        b.join(60000);
        c.join(60000);
        assertFalse(a.isAlive());
        assertFalse(b.isAlive());
        assertFalse(c.isAlive());
        assertNull("batch A failed: " + aError.get(), aError.get());
        assertNull("batch B failed: " + bError.get(), bError.get());
        assertNull("batch C failed: " + cError.get(), cError.get());
        assertEquals(Integer.valueOf(1), aCount.get());
        assertEquals(Integer.valueOf(0), bCount.get());
        assertEquals(Integer.valueOf(0), cCount.get());
      } finally {
        IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
        aProceed.countDown();
        a.join(60000);
        if (b != null) {
          b.join(60000);
        }
        if (c != null) {
          c.join(60000);
        }
        TestUtil.removeCoprocessor(conn, blockedTable, BlockingIndexWriteObserver.class);
      }

      assertEquals(2, getVer(conn, tableName));
      assertEquals("A", getWriter(conn, tableName));
    }
  }

  /**
   * <ol>
   * <li>Batch A: v 1 -> 2, parked in doPre.</li>
   * <li>Batch B: v 1 -> 2, a no-op on A's pending state.</li>
   * <li>Batch C: v 1 -> 2, also a no-op on A's pending state.</li>
   * <li>Batch D: v 1 -> 2, must see A's pending state and must not overwrite A.</li>
   * </ol>
   */
  @Test
  public void testNoLostUpdateAfterConcurrentNoOp() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      String blockedTable = createTable(conn, tableName);

      CountDownLatch aReached = new CountDownLatch(1);
      CountDownLatch aProceed = new CountDownLatch(1);
      BlockingIndexWriteObserver.arm(blockedTable, aReached, aProceed);
      Map<String, AtomicReference<Integer>> counts = new ConcurrentHashMap<>();
      Map<String, AtomicReference<Exception>> errors = new ConcurrentHashMap<>();
      Map<String, Thread> threads = new LinkedHashMap<>();
      try {
        for (String name : new String[] { "A", "B", "C", "D" }) {
          counts.put(name, new AtomicReference<>());
          errors.put(name, new AtomicReference<>());
          threads.put(name, runAsync(name, tableName, 1, counts.get(name), errors.get(name)));
          if (name.equals("A")) {
            assertTrue("batch A did not reach the index write",
              aReached.await(60, TimeUnit.SECONDS));
          } else {
            threads.get(name).join(3000);
          }
        }
        aProceed.countDown();
        for (Thread t : threads.values()) {
          t.join(60000);
          assertFalse(t.isAlive());
        }
        // Check the data before the errors so that a lost update is reported even if a batch
        // failed
        assertEquals("batch A failed: " + errors.get("A").get(), Integer.valueOf(1),
          counts.get("A").get());
        assertEquals("A", getWriter(conn, tableName));
        assertEquals(2, getVer(conn, tableName));
        for (String name : threads.keySet()) {
          assertNull("batch " + name + " failed: " + errors.get(name).get(),
            errors.get(name).get());
        }
        assertEquals(Integer.valueOf(0), counts.get("B").get());
        assertEquals(Integer.valueOf(0), counts.get("C").get());
        assertEquals(Integer.valueOf(0), counts.get("D").get());
      } finally {
        aProceed.countDown();
        for (Thread t : threads.values()) {
          t.join(60000);
        }
        TestUtil.removeCoprocessor(conn, blockedTable, BlockingIndexWriteObserver.class);
      }

      assertEquals(2, getVer(conn, tableName));
      assertEquals("A", getWriter(conn, tableName));
    }
  }

  /**
   * <ol>
   * <li>Batch A: v 1 -> 2, parked in doPre.</li>
   * <li>Batch X: v 2 -> 3, chained on A, fails in doPre.</li>
   * <li>Batch D: v 1 -> 2, must see A's pending state and must not overwrite A.</li>
   * </ol>
   */
  @Test
  public void testNoLostUpdateAfterConcurrentFailedBatch() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      String blockedTable = createTable(conn, tableName);

      CountDownLatch aReached = new CountDownLatch(1);
      CountDownLatch aProceed = new CountDownLatch(1);
      BlockingIndexWriteObserver.arm(blockedTable, aReached, aProceed);
      AtomicReference<Integer> aCount = new AtomicReference<>();
      AtomicReference<Exception> aError = new AtomicReference<>();
      Thread a = runAsync("A", tableName, 1, aCount, aError);
      Thread d = null;
      try {
        assertTrue("batch A did not reach the index write", aReached.await(60, TimeUnit.SECONDS));

        IndexRegionObserver.setFailPreIndexUpdatesForTesting(true);
        try {
          conditionalUpdate(conn, tableName, "X", 2);
          fail("batch X should have failed");
        } catch (SQLException e) {
          assertTrue("unexpected error: " + e, isSimulatedFailure(e));
        } finally {
          IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
        }

        AtomicReference<Integer> dCount = new AtomicReference<>();
        AtomicReference<Exception> dError = new AtomicReference<>();
        d = runAsync("D", tableName, 1, dCount, dError);
        d.join(3000);

        aProceed.countDown();
        a.join(60000);
        d.join(60000);
        assertFalse(a.isAlive());
        assertFalse(d.isAlive());
        assertNull("batch A failed: " + aError.get(), aError.get());
        assertEquals(Integer.valueOf(1), aCount.get());
        assertNull("batch D failed: " + dError.get(), dError.get());
        assertEquals("batch D overwrote batch A", Integer.valueOf(0), dCount.get());
      } finally {
        IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
        aProceed.countDown();
        a.join(60000);
        if (d != null) {
          d.join(60000);
        }
        TestUtil.removeCoprocessor(conn, blockedTable, BlockingIndexWriteObserver.class);
      }

      assertEquals(2, getVer(conn, tableName));
      assertEquals("A", getWriter(conn, tableName));
    }
  }

  /**
   * <ol>
   * <li>Batch A: v 1 -> 2, parked in doPre.</li>
   * <li>Batch Y: v 2 -> 3, chained on A, parked in doPre.</li>
   * <li>Batch X: v 3 -> 4, chained on Y, parked in doPre.</li>
   * <li>Y fails, then X fails.</li>
   * <li>Batch D: v 1 -> 2, must skip X and Y, see A's pending state and must not overwrite A.</li>
   * </ol>
   */
  @Test
  public void testNoLostUpdateAfterConsecutiveFailedBatches() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      String blockedTable = createTable(conn, tableName);

      Map<String, AtomicReference<Integer>> counts = new ConcurrentHashMap<>();
      Map<String, AtomicReference<Exception>> errors = new ConcurrentHashMap<>();
      Map<String, CountDownLatch> proceed = new ConcurrentHashMap<>();
      Map<String, Thread> threads = new LinkedHashMap<>();
      try {
        // A, Y and X park in doPre one after the other. Y and X then fail
        String[] names = { "A", "Y", "X" };
        int[] expected = { 1, 2, 3 };
        for (int i = 0; i < names.length; i++) {
          String name = names[i];
          counts.put(name, new AtomicReference<>());
          errors.put(name, new AtomicReference<>());
          proceed.put(name, new CountDownLatch(1));
          CountDownLatch reached = new CountDownLatch(1);
          BlockingIndexWriteObserver.arm(blockedTable, reached, proceed.get(name),
            !name.equals("A"));
          threads.put(name,
            runAsync(name, tableName, expected[i], counts.get(name), errors.get(name)));
          assertTrue("batch " + name + " did not reach the index write",
            reached.await(60, TimeUnit.SECONDS));
        }
        for (String name : new String[] { "Y", "X" }) {
          proceed.get(name).countDown();
          threads.get(name).join(60000);
          assertFalse(threads.get(name).isAlive());
          assertTrue("unexpected error: " + errors.get(name).get(),
            isSimulatedFailure(errors.get(name).get()));
        }

        counts.put("D", new AtomicReference<>());
        errors.put("D", new AtomicReference<>());
        threads.put("D", runAsync("D", tableName, 1, counts.get("D"), errors.get("D")));
        threads.get("D").join(3000);

        proceed.get("A").countDown();
        for (Thread t : threads.values()) {
          t.join(60000);
          assertFalse(t.isAlive());
        }
        assertNull("batch A failed: " + errors.get("A").get(), errors.get("A").get());
        assertEquals(Integer.valueOf(1), counts.get("A").get());
        assertNull("batch D failed: " + errors.get("D").get(), errors.get("D").get());
        assertEquals("batch D overwrote batch A", Integer.valueOf(0), counts.get("D").get());
      } finally {
        for (CountDownLatch latch : proceed.values()) {
          latch.countDown();
        }
        for (Thread t : threads.values()) {
          t.join(60000);
        }
        TestUtil.removeCoprocessor(conn, blockedTable, BlockingIndexWriteObserver.class);
      }

      assertEquals(2, getVer(conn, tableName));
      assertEquals("A", getWriter(conn, tableName));
    }
  }

  /**
   * <ol>
   * <li>Batch A: v 1 -> 2, parked in doPre.</li>
   * <li>Batch X: plain upsert of the full row, which is not tracked as pending, fails in
   * doPre.</li>
   * <li>Batch D: v 1 -> 2, must see A's pending state and must not overwrite A.</li>
   * </ol>
   */
  @Test
  public void testNoLostUpdateAfterUntrackedBatch() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      String blockedTable = createTable(conn, tableName);

      CountDownLatch aReached = new CountDownLatch(1);
      CountDownLatch aProceed = new CountDownLatch(1);
      BlockingIndexWriteObserver.arm(blockedTable, aReached, aProceed);
      AtomicReference<Integer> aCount = new AtomicReference<>();
      AtomicReference<Exception> aError = new AtomicReference<>();
      Thread a = runAsync("A", tableName, 1, aCount, aError);
      Thread d = null;
      try {
        assertTrue("batch A did not reach the index write", aReached.await(60, TimeUnit.SECONDS));

        IndexRegionObserver.setFailPreIndexUpdatesForTesting(true);
        try {
          PreparedStatement ps =
            conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?)");
          ps.setString(1, "r1");
          ps.setObject(2, RawBsonDocument.parse("{\"idxKey\": \"g1\", \"ver\": 7, \"by\": \"X\"}"));
          ps.executeUpdate();
          fail("batch X should have failed");
        } catch (SQLException e) {
          assertTrue("unexpected error: " + e, isSimulatedFailure(e));
        } finally {
          IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
        }

        AtomicReference<Integer> dCount = new AtomicReference<>();
        AtomicReference<Exception> dError = new AtomicReference<>();
        d = runAsync("D", tableName, 1, dCount, dError);
        d.join(3000);
        assertTrue("batch D returned before batch A completed: " + dError.get(), d.isAlive());

        aProceed.countDown();
        a.join(60000);
        d.join(60000);
        assertFalse(a.isAlive());
        assertFalse(d.isAlive());
        assertNull("batch A failed: " + aError.get(), aError.get());
        assertEquals(Integer.valueOf(1), aCount.get());
        assertNull("batch D failed: " + dError.get(), dError.get());
        assertEquals("batch D overwrote batch A", Integer.valueOf(0), dCount.get());
      } finally {
        IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
        aProceed.countDown();
        a.join(60000);
        if (d != null) {
          d.join(60000);
        }
        TestUtil.removeCoprocessor(conn, blockedTable, BlockingIndexWriteObserver.class);
      }

      assertEquals(2, getVer(conn, tableName));
      assertEquals("A", getWriter(conn, tableName));
    }
  }

  /**
   * <ol>
   * <li>Batch A: v 1 -> 2, parked in doPre, then fails.</li>
   * <li>Batch B: v 1 -> 2, a no-op on A's pending state. Must not return A's state, which is never
   * committed, and must be retried after A fails.</li>
   * </ol>
   */
  @Test
  public void testNoOpOnFailedPendingBatch() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      String blockedTable = createTable(conn, tableName);

      CountDownLatch aReached = new CountDownLatch(1);
      CountDownLatch aProceed = new CountDownLatch(1);
      BlockingIndexWriteObserver.arm(blockedTable, aReached, aProceed, true);
      AtomicReference<Integer> aCount = new AtomicReference<>();
      AtomicReference<Exception> aError = new AtomicReference<>();
      Thread a = runAsync("A", tableName, 1, aCount, aError);
      Thread b = null;
      try {
        assertTrue("batch A did not reach the index write", aReached.await(60, TimeUnit.SECONDS));

        AtomicReference<Integer> bCount = new AtomicReference<>();
        AtomicReference<Exception> bError = new AtomicReference<>();
        b = runAsync("B", tableName, 1, bCount, bError);
        b.join(3000);
        assertTrue("batch B returned before batch A completed: " + bError.get(), b.isAlive());

        aProceed.countDown();
        a.join(60000);
        b.join(60000);
        assertFalse(a.isAlive());
        assertFalse(b.isAlive());
        assertTrue("unexpected error: " + aError.get(), isSimulatedFailure(aError.get()));
        assertNull("batch B failed: " + bError.get(), bError.get());
        assertEquals("batch B was a no-op on a state that was never committed", Integer.valueOf(1),
          bCount.get());
      } finally {
        aProceed.countDown();
        a.join(60000);
        if (b != null) {
          b.join(60000);
        }
        TestUtil.removeCoprocessor(conn, blockedTable, BlockingIndexWriteObserver.class);
      }

      assertEquals(2, getVer(conn, tableName));
      assertEquals("B", getWriter(conn, tableName));
    }
  }

  /**
   * <ol>
   * <li>Batch A: inserts row r2 with ON DUPLICATE KEY IGNORE, parked in doPre, then fails.</li>
   * <li>Batch B: inserts row r2 with ON DUPLICATE KEY IGNORE, a no-op on A's pending insert. Must
   * be retried after A fails and insert the row.</li>
   * </ol>
   */
  @Test
  public void testIgnoreOnFailedPendingInsert() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      String blockedTable = createTable(conn, tableName);

      CountDownLatch aReached = new CountDownLatch(1);
      CountDownLatch aProceed = new CountDownLatch(1);
      BlockingIndexWriteObserver.arm(blockedTable, aReached, aProceed, true);
      Map<String, AtomicReference<Exception>> errors = new ConcurrentHashMap<>();
      Map<String, Thread> threads = new LinkedHashMap<>();
      try {
        for (String name : new String[] { "A", "B" }) {
          AtomicReference<Exception> error = new AtomicReference<>();
          errors.put(name, error);
          Thread t = new Thread(() -> {
            try (Connection c = DriverManager.getConnection(getUrl())) {
              c.setAutoCommit(true);
              PreparedStatement ps = c.prepareStatement(
                "UPSERT INTO " + tableName + " VALUES (?, ?) ON DUPLICATE KEY IGNORE");
              ps.setString(1, "r2");
              ps.setObject(2, RawBsonDocument
                .parse("{\"idxKey\": \"g1\", \"ver\": 1, \"by\": \"" + name + "\"}"));
              ps.executeUpdate();
            } catch (Exception e) {
              error.set(e);
            }
          }, name);
          threads.put(name, t);
          t.start();
          if (name.equals("A")) {
            assertTrue("batch A did not reach the index write",
              aReached.await(60, TimeUnit.SECONDS));
          } else {
            t.join(3000);
            assertTrue("batch B returned before batch A completed: " + error.get(), t.isAlive());
          }
        }
        aProceed.countDown();
        for (Thread t : threads.values()) {
          t.join(60000);
          assertFalse(t.isAlive());
        }
        assertTrue("unexpected error: " + errors.get("A").get(),
          isSimulatedFailure(errors.get("A").get()));
        assertNull("batch B failed: " + errors.get("B").get(), errors.get("B").get());
      } finally {
        aProceed.countDown();
        for (Thread t : threads.values()) {
          t.join(60000);
        }
        TestUtil.removeCoprocessor(conn, blockedTable, BlockingIndexWriteObserver.class);
      }

      ResultSet rs =
        conn.createStatement().executeQuery("SELECT COL FROM " + tableName + " WHERE PK1 = 'r2'");
      assertTrue("the row inserted by batch B is missing", rs.next());
      assertEquals("B", ((BsonDocument) rs.getObject(1)).getString("by").getValue());
    }
  }

  /**
   * Many threads run conditional v -> v + 1 updates on a few rows while index writes are delayed
   * and fail at random. Checks that no update is lost, that no failed update is written, and that
   * every row returned by an update was committed.
   */
  @Test
  public void testRandomConcurrentConditionalUpdates() throws Exception {
    final int numRows = 2;
    final int numThreads = 10;
    final int updatesPerThread = 60;
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      String blockedTable = createTable(conn, tableName);
      TestUtil.removeCoprocessor(conn, blockedTable, BlockingIndexWriteObserver.class);
      PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?)");
      for (int i = 0; i < numRows; i++) {
        ps.setString(1, "s" + i);
        ps.setObject(2, RawBsonDocument.parse("{\"idxKey\": \"g1\", \"ver\": 0}"));
        ps.executeUpdate();
      }

      // For each row, the writer of each committed version, and the versions returned to clients
      List<Map<Integer, String>> committed = new ArrayList<>();
      List<Map<Integer, String>> returned = new ArrayList<>();
      for (int i = 0; i < numRows; i++) {
        committed.add(new ConcurrentHashMap<>());
        returned.add(new ConcurrentHashMap<>());
      }
      Map<String, String> duplicateVersions = new ConcurrentHashMap<>();
      AtomicReference<Throwable> unexpectedError = new AtomicReference<>();
      int[] outcomes = new int[3];

      TestUtil.addCoprocessor(conn, blockedTable, RandomIndexWriteObserver.class);
      RandomIndexWriteObserver.configure(25, 50);
      List<Thread> threads = new ArrayList<>();
      try {
        for (int t = 0; t < numThreads; t++) {
          final int threadId = t;
          Thread thread = new Thread(() -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            int[] known = new int[numRows];
            try (Connection c = DriverManager.getConnection(getUrl())) {
              c.setAutoCommit(true);
              for (int i = 0; i < updatesPerThread; i++) {
                int row = random.nextInt(numRows);
                // Mostly use the last version seen, sometimes a stale one, and sometimes guess a
                // version that a pending update may write so that this update chains on it
                int choice = random.nextInt(10);
                int expected = choice < 2 ? Math.max(0, known[row] - 1)
                  : choice < 5 ? known[row]
                  : known[row] + 1 + random.nextInt(2);
                String writer = "T" + threadId + "-" + i;
                Pair<Integer, BsonDocument> result;
                try {
                  result = conditionalUpdate(c, tableName, "s" + row, writer, expected);
                } catch (SQLException e) {
                  // Only a simulated failure, or a failed wait for a previous batch after the
                  // retries, is expected
                  if (
                    !isSimulatedFailure(e)
                      && !e.toString().contains("One of the previous concurrent mutations")
                  ) {
                    throw e;
                  }
                  synchronized (outcomes) {
                    outcomes[2]++;
                  }
                  continue;
                }
                BsonDocument doc = result.getSecond();
                int ver = doc.getInt32("ver").getValue();
                String by = doc.containsKey("by") ? doc.getString("by").getValue() : "";
                if (result.getFirst() == 1) {
                  synchronized (outcomes) {
                    outcomes[0]++;
                  }
                  assertEquals(expected + 1, ver);
                  assertEquals(writer, by);
                  String prev = committed.get(row).putIfAbsent(ver, writer);
                  if (prev != null) {
                    duplicateVersions.put("s" + row + " v" + ver, prev + ", " + writer);
                  }
                } else {
                  synchronized (outcomes) {
                    outcomes[1]++;
                  }
                  assertEquals(Integer.valueOf(0), result.getFirst());
                  returned.get(row).put(ver, by);
                }
                known[row] = ver;
              }
            } catch (Throwable e) {
              LOG.error("Unexpected error in thread " + threadId, e);
              unexpectedError.compareAndSet(null, e);
            }
          }, "stress-" + t);
          threads.add(thread);
          thread.start();
        }
        for (Thread thread : threads) {
          thread.join(600000);
          assertFalse("thread " + thread.getName() + " did not finish", thread.isAlive());
        }
      } finally {
        RandomIndexWriteObserver.configure(0, 0);
        for (Thread thread : threads) {
          thread.join(60000);
        }
        TestUtil.removeCoprocessor(conn, blockedTable, RandomIndexWriteObserver.class);
      }

      LOG.info("Updates: {} succeeded, {} no-ops, {} failed", outcomes[0], outcomes[1],
        outcomes[2]);
      assertNull("unexpected error: " + unexpectedError.get(), unexpectedError.get());
      assertTrue("lost updates: " + duplicateVersions, duplicateVersions.isEmpty());
      assertTrue("no update succeeded", outcomes[0] > 0);
      assertTrue("no update was a no-op", outcomes[1] > 0);
      assertTrue("no update failed", outcomes[2] > 0);

      for (int row = 0; row < numRows; row++) {
        ResultSet rs = conn.createStatement()
          .executeQuery("SELECT COL FROM " + tableName + " WHERE PK1 = 's" + row + "'");
        assertTrue(rs.next());
        BsonDocument doc = (BsonDocument) rs.getObject(1);
        int ver = doc.getInt32("ver").getValue();
        Map<Integer, String> rowCommitted = committed.get(row);
        // Every version from 1 to the final one was committed by exactly one successful update,
        // so no update was lost and no failed update was written
        assertEquals("row s" + row + " versions " + rowCommitted.keySet(), ver,
          rowCommitted.size());
        for (int v = 1; v <= ver; v++) {
          assertTrue("row s" + row + " version " + v + " has no successful update",
            rowCommitted.containsKey(v));
        }
        if (ver > 0) {
          assertEquals(rowCommitted.get(ver), doc.getString("by").getValue());
        }
        // Every row returned by a no-op was committed
        for (Map.Entry<Integer, String> entry : returned.get(row).entrySet()) {
          int v = entry.getKey();
          String expectedWriter = v == 0 ? "" : rowCommitted.get(v);
          assertEquals("row s" + row + " version " + v + " returned by a no-op was not committed",
            expectedWriter, entry.getValue());
        }
      }
    }
  }
}
