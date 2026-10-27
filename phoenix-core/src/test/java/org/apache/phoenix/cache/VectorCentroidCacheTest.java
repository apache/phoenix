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
package org.apache.phoenix.cache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class VectorCentroidCacheTest {

  private static final long MAX_BYTES = 1024L * 1024L;

  private static List<float[]> centroids(float[]... c) {
    return Arrays.asList(c);
  }

  /** Returns a mock connection that serves the given centroid rows and counts each load. */
  private static Connection connectionReturning(List<float[]> rows, AtomicInteger loads)
    throws SQLException {
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenAnswer((Answer<ResultSet>) invocation -> {
      loads.incrementAndGet();
      ResultSet rs = mock(ResultSet.class);
      int[] pos = { -1 };
      when(rs.next()).thenAnswer(i -> ++pos[0] < rows.size());
      when(rs.getBytes(1)).thenAnswer(i -> PVectorFloat.INSTANCE.toBytes(rows.get(pos[0])));
      return rs;
    });
    return conn;
  }

  @Test
  public void testAssignNearestCentroid() {
    CachedCentroids c = new CachedCentroids(
      centroids(new float[] { 0, 0 }, new float[] { 10, 0 }, new float[] { 0, 10 }),
      DistanceMetric.L2);
    assertEquals(1, c.assign(new float[] { 9.5f, 0.5f }));
    assertEquals(2, c.assign(new float[] { 0.5f, 9f }));
    assertEquals(0, c.assign(new float[] { 1f, 1f }));
  }

  @Test
  public void testNearestNInDistanceOrder() {
    CachedCentroids c = new CachedCentroids(centroids(new float[] { 0, 0 }, new float[] { 10, 0 },
      new float[] { 0, 10 }, new float[] { 20, 20 }), DistanceMetric.L2);
    assertArrayEquals(new int[] { 1, 0 }, c.nearest(new float[] { 9.5f, 0.5f }, 2));
    assertArrayEquals(new int[] { 1, 0, 2, 3 }, c.nearest(new float[] { 9.5f, 0.5f }, 10));
  }

  /** Verifies that an INNER_PRODUCT index assigns a vector to a centroid by L2 distance. */
  @Test
  public void testInnerProductAssignsByL2() {
    CachedCentroids c = new CachedCentroids(centroids(new float[] { 9, 0 }, new float[] { 100, 0 }),
      DistanceMetric.INNER_PRODUCT);
    assertEquals(0, c.assign(new float[] { 10, 0 }));
  }

  @Test
  public void testCosineAssignsByAngle() {
    CachedCentroids c = new CachedCentroids(centroids(new float[] { 1, 0 }, new float[] { 0, 1 }),
      DistanceMetric.COSINE);
    assertEquals(0, c.assign(new float[] { 100, 1 }));
    assertEquals(1, c.assign(new float[] { 0.01f, 5 }));
  }

  /** Verifies that packed FLOAT and DOUBLE forms of one vector go to the same centroid. */
  @Test
  public void testAssignPackedFloatAndDoubleVectors() {
    CachedCentroids c = new CachedCentroids(
      centroids(new float[] { 0, 0, 0 }, new float[] { 5, 5, 5 }), DistanceMetric.L2);
    byte[] f = PVectorFloat.INSTANCE.toBytes(new float[] { 4, 4, 4 });
    byte[] d = PVectorDouble.INSTANCE.toBytes(new double[] { 4, 4, 4 });
    assertEquals(1, c.assign(f, 0, f.length, PVectorFloat.INSTANCE));
    assertEquals(1, c.assign(d, 0, d.length, PVectorDouble.INSTANCE));
  }

  /**
   * Verifies that a vector with a DOUBLE element beyond float range routes by its direction. The
   * element narrows to infinity, and assignment uses the largest float of the same sign instead.
   * Without saturation, the distances are infinite or NaN, and the tie decides the centroid.
   */
  @Test
  public void testOutOfFloatRangeElementRoutesByDirection() {
    CachedCentroids cosine = new CachedCentroids(
      centroids(new float[] { 1, 0 }, new float[] { 0, 1 }), DistanceMetric.COSINE);
    byte[] d = PVectorDouble.INSTANCE.toBytes(new double[] { 1, 1e300 });
    assertEquals(1, cosine.assign(d, 0, d.length, PVectorDouble.INSTANCE));
    // nearest() also saturates the query. Without saturation, both distances are NaN and tie,
    // and the tie puts ID 0 first.
    assertArrayEquals(new int[] { 1, 0 },
      cosine.nearest(new float[] { 1, Float.POSITIVE_INFINITY }, 2));

    CachedCentroids l2 = new CachedCentroids(
      centroids(new float[] { 0, 0 }, new float[] { -1e37f, 0 }), DistanceMetric.L2);
    assertEquals(1, l2.assign(new float[] { Float.NEGATIVE_INFINITY, 0 }));
  }

  /**
   * Verifies that nearest() breaks distance ties by ascending centroid ID, as assign() does. Under
   * L2, a saturated component dominates the distance to every ordinary centroid, and the distances
   * tie. A probe for such a vector must start at the list that holds its rows.
   */
  @Test
  public void testNearestBreaksTiesLikeAssign() {
    CachedCentroids l2 = new CachedCentroids(
      centroids(new float[] { 0, 0 }, new float[] { 1, 1 }, new float[] { 2, 2 }),
      DistanceMetric.L2);
    float[] v = { Float.POSITIVE_INFINITY, 0 };
    assertEquals(0, l2.assign(v));
    assertArrayEquals(new int[] { 0 }, l2.nearest(v, 1));
    assertArrayEquals(new int[] { 0, 1 }, l2.nearest(v, 2));
    assertArrayEquals(new int[] { 0, 1, 2 }, l2.nearest(v, 3));
  }

  @Test
  public void testDimensionMismatchRejected() {
    CachedCentroids c = new CachedCentroids(centroids(new float[] { 0, 0 }), DistanceMetric.L2);
    try {
      c.assign(new float[] { 1, 2, 3 });
      fail("Expected a dimension mismatch");
    } catch (IllegalArgumentException expected) {
    }
  }

  /** Verifies that the cache keeps more than one centroid generation of an index. */
  @Test
  public void testGenerationsCoexist() {
    VectorCentroidCache cache = new VectorCentroidCache(MAX_BYTES);
    CachedCentroids g1 = new CachedCentroids(centroids(new float[] { 1 }), DistanceMetric.L2);
    CachedCentroids g2 = new CachedCentroids(centroids(new float[] { 2 }), DistanceMetric.L2);
    cache.put("S.IDX", 100L, g1);
    cache.put("S.IDX", 200L, g2);
    assertSame(g1, cache.getIfPresent("S.IDX", 100L));
    assertSame(g2, cache.getIfPresent("S.IDX", 200L));
    assertNull(cache.getIfPresent("S.IDX", 300L));
  }

  /** Verifies that the cache compares index names with case sensitivity. */
  @Test
  public void testNamesAreCaseSensitive() {
    VectorCentroidCache cache = new VectorCentroidCache(MAX_BYTES);
    cache.put("S.myIdx", 1L, new CachedCentroids(centroids(new float[] { 1 }), DistanceMetric.L2));
    assertNull(cache.getIfPresent("S.MYIDX", 1L));
  }

  @Test
  public void testLoadOnMissThenHit() throws Exception {
    VectorCentroidCache cache = new VectorCentroidCache(MAX_BYTES);
    AtomicInteger loads = new AtomicInteger();
    Connection conn =
      connectionReturning(centroids(new float[] { 0, 0 }, new float[] { 10, 10 }), loads);
    CachedCentroids first = cache.get(conn, "IDX", 7L, DistanceMetric.L2);
    assertEquals(2, first.size());
    assertArrayEquals(new float[] { 10, 10 }, first.getCentroid(1), 0f);
    assertSame(first, cache.get(conn, "IDX", 7L, DistanceMetric.L2));
    assertEquals(1, loads.get());
  }

  /**
   * Verifies that a load that finds no centroids fails and puts no entry in the cache. A later load
   * for the same key can then succeed.
   */
  @Test
  public void testEmptyLoadIsNotCached() throws Exception {
    VectorCentroidCache cache = new VectorCentroidCache(MAX_BYTES);
    AtomicInteger loads = new AtomicInteger();
    try {
      cache.get(connectionReturning(new ArrayList<>(), loads), "IDX", 1L, DistanceMetric.L2);
      fail("Expected an empty generation to fail");
    } catch (SQLException expected) {
    }
    assertNull(cache.getIfPresent("IDX", 1L));
    assertNotNull(cache.get(connectionReturning(centroids(new float[] { 1 }), loads), "IDX", 1L,
      DistanceMetric.L2));
  }

  /**
   * Verifies that concurrent misses for the same key share one load. The load blocks until every
   * caller waits inside the cache, so all callers miss at the same time.
   */
  @Test(timeout = 30000)
  public void testConcurrentMissesLoadOnce() throws Exception {
    VectorCentroidCache cache = new VectorCentroidCache(MAX_BYTES);
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch release = new CountDownLatch(1);
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenAnswer((Answer<ResultSet>) invocation -> {
      loads.incrementAndGet();
      release.await();
      ResultSet rs = mock(ResultSet.class);
      when(rs.next()).thenReturn(true, false);
      when(rs.getBytes(1)).thenReturn(PVectorFloat.INSTANCE.toBytes(new float[] { 3, 4 }));
      return rs;
    });
    int callers = 8;
    CachedCentroids[] results = new CachedCentroids[callers];
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < callers; i++) {
      int caller = i;
      Thread thread = new Thread(() -> {
        try {
          results[caller] = cache.get(conn, "IDX", 1L, DistanceMetric.L2);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
      threads.add(thread);
      thread.start();
    }
    // Wait until each caller blocks, in the load or on the load that a different caller started
    for (Thread thread : threads) {
      while (
        thread.getState() != Thread.State.WAITING && thread.getState() != Thread.State.TIMED_WAITING
      ) {
        assertTrue("Caller finished before the load was released", thread.isAlive());
        Thread.yield();
      }
    }
    release.countDown();
    for (Thread thread : threads) {
      thread.join();
    }
    assertEquals(1, loads.get());
    verify(ps, times(1)).executeQuery();
    for (CachedCentroids result : results) {
      assertSame(results[0], result);
    }
  }

  @Test
  public void testInvalidateDropsEveryGenerationOfOneIndex() {
    VectorCentroidCache cache = new VectorCentroidCache(MAX_BYTES);
    CachedCentroids c = new CachedCentroids(centroids(new float[] { 1 }), DistanceMetric.L2);
    cache.put("A", 1L, c);
    cache.put("A", 2L, c);
    cache.put("B", 1L, c);
    cache.invalidate("A");
    assertNull(cache.getIfPresent("A", 1L));
    assertNull(cache.getIfPresent("A", 2L));
    assertSame(c, cache.getIfPresent("B", 1L));
  }

  /**
   * Verifies that the cache bound applies to the bytes of the centroid vectors, not to the number
   * of models. A model that uses most of the bound stays in the cache. When the total exceeds the
   * bound, the cache evicts the least recently used model.
   */
  @Test
  public void testSizeBoundedByBytes() {
    // A two-dimensional centroid uses 8 bytes, so 10 centroids use 80 bytes and 2 use 16 bytes
    CachedCentroids large =
      new CachedCentroids(Collections.nCopies(10, new float[] { 1, 2 }), DistanceMetric.L2);
    CachedCentroids small =
      new CachedCentroids(Collections.nCopies(2, new float[] { 1, 2 }), DistanceMetric.L2);
    VectorCentroidCache cache = new VectorCentroidCache(100);
    cache.put("A", 1L, large);
    assertSame(large, cache.getIfPresent("A", 1L));
    cache.put("B", 1L, small);
    assertSame(small, cache.getIfPresent("B", 1L));
    assertSame(large, cache.getIfPresent("A", 1L));
    // The total of 112 bytes exceeds the bound, so the cache evicts the least recently used model
    cache.put("C", 1L, small);
    assertNull(cache.getIfPresent("B", 1L));
    assertSame(large, cache.getIfPresent("A", 1L));
    assertSame(small, cache.getIfPresent("C", 1L));
    cache.put("D", 1L, large);
    assertNull(cache.getIfPresent("A", 1L));
    assertEquals(2, cache.size());
  }

  /**
   * Verifies assignment, nearest search, and lookup by ID when the centroid IDs start at a nonzero
   * first ID.
   */
  @Test
  public void testCentroidIdsStartAtFirstId() {
    CachedCentroids c = new CachedCentroids(
      centroids(new float[] { 0, 0 }, new float[] { 10, 0 }, new float[] { 0, 10 }),
      DistanceMetric.L2, 7);
    assertEquals(7, c.getFirstId());
    assertEquals(8, c.assign(new float[] { 9.5f, 0.5f }));
    assertArrayEquals(new int[] { 8, 7 }, c.nearest(new float[] { 9.5f, 0.5f }, 2));
    assertArrayEquals(new float[] { 0, 10 }, c.getCentroid(9), 0f);
  }
}
