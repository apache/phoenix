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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.optimize.DistanceMetric;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;
import org.junit.Test;
import org.mockito.stubbing.Answer;

public class VectorCentroidCacheTest {

  private static List<float[]> centroids(float[]... c) {
    return Arrays.asList(c);
  }

  /**
   * Constructs a mock connection returning the given centroid rows and tracking query invocations.
   */
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

  /** Verifies that inner product indexes assign vectors using Euclidean distance. */
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

  /** Verifies vector decoding across different precision formats prior to assignment. */
  @Test
  public void testAssignPackedFloatAndDoubleVectors() {
    CachedCentroids c = new CachedCentroids(
      centroids(new float[] { 0, 0, 0 }, new float[] { 5, 5, 5 }), DistanceMetric.L2);
    byte[] f = PVectorFloat.INSTANCE.toBytes(new float[] { 4, 4, 4 });
    byte[] d = PVectorDouble.INSTANCE.toBytes(new double[] { 4, 4, 4 });
    assertEquals(1, c.assign(f, 0, f.length, PVectorFloat.INSTANCE));
    assertEquals(1, c.assign(d, 0, d.length, PVectorDouble.INSTANCE));
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

  /** Verifies that distinct centroid generations for an index coexist in cache. */
  @Test
  public void testGenerationsCoexist() {
    VectorCentroidCache cache = new VectorCentroidCache(10);
    CachedCentroids g1 = new CachedCentroids(centroids(new float[] { 1 }), DistanceMetric.L2);
    CachedCentroids g2 = new CachedCentroids(centroids(new float[] { 2 }), DistanceMetric.L2);
    cache.put("S.IDX", 100L, g1);
    cache.put("S.IDX", 200L, g2);
    assertSame(g1, cache.getIfPresent("S.IDX", 100L));
    assertSame(g2, cache.getIfPresent("S.IDX", 200L));
    assertNull(cache.getIfPresent("S.IDX", 300L));
  }

  /** Verifies case sensitive key lookup behavior in the centroid cache. */
  @Test
  public void testNamesAreCaseSensitive() {
    VectorCentroidCache cache = new VectorCentroidCache(10);
    cache.put("S.myIdx", 1L, new CachedCentroids(centroids(new float[] { 1 }), DistanceMetric.L2));
    assertNull(cache.getIfPresent("S.MYIDX", 1L));
  }

  @Test
  public void testLoadOnMissThenHit() throws Exception {
    VectorCentroidCache cache = new VectorCentroidCache(10);
    AtomicInteger loads = new AtomicInteger();
    Connection conn =
      connectionReturning(centroids(new float[] { 0, 0 }, new float[] { 10, 10 }), loads);
    CachedCentroids first = cache.get(conn, "IDX", 7L, DistanceMetric.L2);
    assertEquals(2, first.size());
    assertArrayEquals(new float[] { 10, 10 }, first.getCentroid(1), 0f);
    assertSame(first, cache.get(conn, "IDX", 7L, DistanceMetric.L2));
    assertEquals(1, loads.get());
  }

  /** Verifies that empty centroid lookups fail fast without populating negative cache entries. */
  @Test
  public void testEmptyLoadIsNotCached() throws Exception {
    VectorCentroidCache cache = new VectorCentroidCache(10);
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

  /** Verifies that concurrent cache misses for the same key coalesce into a single load. */
  @Test
  public void testConcurrentMissesLoadOnce() throws Exception {
    VectorCentroidCache cache = new VectorCentroidCache(10);
    AtomicInteger loads = new AtomicInteger();
    CountDownLatch release = new CountDownLatch(1);
    Connection conn = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenAnswer((Answer<ResultSet>) invocation -> {
      loads.incrementAndGet();
      release.await(10, TimeUnit.SECONDS);
      ResultSet rs = mock(ResultSet.class);
      when(rs.next()).thenReturn(true, false);
      when(rs.getBytes(1)).thenReturn(PVectorFloat.INSTANCE.toBytes(new float[] { 3, 4 }));
      return rs;
    });
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      List<Future<CachedCentroids>> futures = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        futures.add(pool.submit(() -> cache.get(conn, "IDX", 1L, DistanceMetric.L2)));
      }
      Thread.sleep(200);
      release.countDown();
      CachedCentroids first = futures.get(0).get(10, TimeUnit.SECONDS);
      for (Future<CachedCentroids> f : futures) {
        assertSame(first, f.get(10, TimeUnit.SECONDS));
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(1, loads.get());
    verify(ps, times(1)).executeQuery();
  }

  @Test
  public void testInvalidateDropsEveryGenerationOfOneIndex() {
    VectorCentroidCache cache = new VectorCentroidCache(10);
    CachedCentroids c = new CachedCentroids(centroids(new float[] { 1 }), DistanceMetric.L2);
    cache.put("A", 1L, c);
    cache.put("A", 2L, c);
    cache.put("B", 1L, c);
    cache.invalidate("A");
    assertNull(cache.getIfPresent("A", 1L));
    assertNull(cache.getIfPresent("A", 2L));
    assertSame(c, cache.getIfPresent("B", 1L));
  }

  @Test
  public void testSizeBoundedByEntryCount() {
    VectorCentroidCache cache = new VectorCentroidCache(2);
    CachedCentroids c = new CachedCentroids(centroids(new float[] { 1 }), DistanceMetric.L2);
    cache.put("A", 1L, c);
    cache.put("A", 2L, c);
    cache.put("A", 3L, c);
    cache.getIfPresent("A", 3L);
    assertEquals(2, cache.size());
  }

  /** Verifies centroid assignment and indexing when IDs start from a non-zero base ID. */
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
