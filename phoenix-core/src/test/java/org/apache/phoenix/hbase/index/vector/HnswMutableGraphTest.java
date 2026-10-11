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
package org.apache.phoenix.hbase.index.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

/**
 * Unit tests for the mutable graph of {@link HnswIndexManager}: the in-memory graph that holds the
 * buffered vector changes of a region until a flush.
 */
public class HnswMutableGraphTest {
  private static final int DIM = 8;
  private static final PTable.VectorIndex VI = new PTable.VectorIndex.Builder().setAlgorithm("HNSW")
    .setDistanceMetric("L2").setDimension(DIM).setHnswM(16).setHnswEfConstruction(100)
    .setHnswAlpha(1.2).setQuantizationType("NONE").build();

  private static HnswIndexManager.MutableGraph graph() {
    return new HnswIndexManager.MutableGraph(VI, HnswSegment.similarityFunction("L2"));
  }

  private static ImmutableBytesPtr key(String key) {
    return new ImmutableBytesPtr(Bytes.toBytes(key));
  }

  private static float[] vector(Random random) {
    float[] v = new float[DIM];
    for (int d = 0; d < DIM; d++) {
      v[d] = (float) random.nextGaussian();
    }
    return v;
  }

  private static float[] point(float x) {
    float[] v = new float[DIM];
    v[0] = x;
    return v;
  }

  // Returns the key nearest to the vector, or null if the search finds no key. The search uses its
  // candidate count as its beam width, so this helper asks for ten candidates
  private static ImmutableBytesPtr nearest(HnswIndexManager.MutableGraph graph, float[] v) {
    Map<ImmutableBytesPtr, Float> scores = new HashMap<>();
    graph.search(VectorizationProvider.getInstance().getVectorTypeSupport().createFloatVector(v),
      10, scores, null, k -> true);
    ImmutableBytesPtr best = null;
    for (Map.Entry<ImmutableBytesPtr, Float> e : scores.entrySet()) {
      if (best == null || e.getValue() > scores.get(best)) {
        best = e.getKey();
      }
    }
    return best;
  }

  /**
   * The test upserts the only live key, or the last key that deletes leave live. The new node, and
   * the nodes that the test inserts after it, must stay reachable. JVector keeps a node that is
   * marked deleted as the entry point, and links a new node only to unmarked nodes. Thus the
   * mutable graph does not mark the nodes of old versions as deleted in JVector.
   */
  @Test
  public void testUpsertOfOnlyLiveKeyStaysReachable() {
    Random random = new Random(1);
    HnswIndexManager.MutableGraph graph = graph();
    graph.upsert(key("k0"), vector(random));
    float[] k0 = vector(random);
    graph.upsert(key("k0"), k0);
    Map<String, float[]> live = new HashMap<>();
    live.put("k0", k0);
    for (int i = 1; i < 12; i++) {
      float[] v = vector(random);
      graph.upsert(key("k" + i), v);
      live.put("k" + i, v);
    }
    for (Map.Entry<String, float[]> e : live.entrySet()) {
      assertEquals(e.getKey(), key(e.getKey()), nearest(graph, e.getValue()));
    }

    // The deletes leave one live key, and then the test upserts that key
    graph = graph();
    for (int i = 0; i < 5; i++) {
      graph.upsert(key("d" + i), vector(random));
    }
    for (int i = 1; i < 5; i++) {
      graph.delete(key("d" + i));
    }
    float[] d0 = vector(random);
    graph.upsert(key("d0"), d0);
    float[] d5 = vector(random);
    graph.upsert(key("d5"), d5);
    assertEquals(key("d0"), nearest(graph, d0));
    assertEquals(key("d5"), nearest(graph, d5));

    // The deletes leave no live key
    graph.delete(key("d0"));
    graph.delete(key("d5"));
    assertNull(nearest(graph, d0));
    float[] d6 = vector(random);
    graph.upsert(key("d6"), d6);
    assertEquals(key("d6"), nearest(graph, d6));
  }

  /**
   * Each vector change adds a graph node, and each deleted key adds an entry. The flush threshold
   * counts these entries, not the changed keys.
   */
  @Test
  public void testSizeCountsGraphNodes() {
    Random random = new Random(2);
    HnswIndexManager.MutableGraph graph = graph();
    for (int i = 0; i < 5; i++) {
      graph.upsert(key("hot"), vector(random));
    }
    assertEquals(1, graph.changes());
    assertEquals(5, graph.size());
    graph.delete(key("gone"));
    assertEquals(2, graph.changes());
    assertEquals(6, graph.size());
    // The nodes of a deleted key stay in the graph as routing nodes until a flush swaps the graph
    graph.delete(key("hot"));
    assertEquals(2, graph.changes());
    assertEquals(7, graph.size());
  }

  /**
   * Searches must not fail while the test deletes every key and then inserts the keys again. The
   * mutable graph does not reset when its last key is deleted, because a search can read the graph
   * at that time. A search over a cleared vector map fails with a NullPointerException.
   */
  @Test
  public void testDeleteOfEveryKeyDuringSearches() throws Exception {
    Random random = new Random(3);
    HnswIndexManager.MutableGraph graph = graph();
    List<float[]> vectors = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      vectors.add(vector(random));
    }
    AtomicBoolean done = new AtomicBoolean();
    ExecutorService pool = Executors.newFixedThreadPool(3);
    try {
      List<Future<?>> searches = new ArrayList<>();
      for (int t = 0; t < 2; t++) {
        searches.add(pool.submit(() -> {
          Random r = new Random();
          while (!done.get()) {
            nearest(graph, vectors.get(r.nextInt(vectors.size())));
          }
          return null;
        }));
      }
      for (int round = 0; round < 300; round++) {
        for (int i = 0; i < vectors.size(); i++) {
          graph.upsert(key("r" + i), vectors.get(i));
        }
        // The last delete leaves no live key
        for (int i = 0; i < vectors.size(); i++) {
          graph.delete(key("r" + i));
        }
      }
      done.set(true);
      for (Future<?> search : searches) {
        search.get(60, TimeUnit.SECONDS);
      }
    } finally {
      done.set(true);
      pool.shutdownNow();
    }
  }

  /**
   * A search that runs while a key changes must find the key in its old or its new version. The
   * search sees the graph as it was at its start, so it can miss the new node. It then finds the
   * key through the old node, which stays as a routing node, and scores the current version.
   */
  @Test
  public void testUpdatedKeyIsFoundDuringSearches() throws Exception {
    Random random = new Random(4);
    HnswIndexManager.MutableGraph graph = graph();
    for (int i = 0; i < 50; i++) {
      float[] v = vector(random);
      for (int d = 0; d < DIM; d++) {
        v[d] *= 5;
      }
      graph.upsert(key("far" + i), v);
    }
    graph.upsert(key("hot"), point(0));
    AtomicBoolean done = new AtomicBoolean();
    AtomicInteger searched = new AtomicInteger();
    AtomicInteger missed = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      List<Future<?>> searches = new ArrayList<>();
      for (int t = 0; t < 2; t++) {
        searches.add(pool.submit(() -> {
          while (!done.get()) {
            searched.incrementAndGet();
            if (!key("hot").equals(nearest(graph, point(0)))) {
              missed.incrementAndGet();
            }
          }
          return null;
        }));
      }
      for (int i = 0; i < 3000; i++) {
        graph.upsert(key("hot"), point(i % 2 == 0 ? 0.01f : -0.01f));
      }
      done.set(true);
      for (Future<?> search : searches) {
        search.get(60, TimeUnit.SECONDS);
      }
    } finally {
      done.set(true);
      pool.shutdownNow();
    }
    // JVector can miss a node while a concurrent insert changes the links of its neighbors. Thus a
    // few searches can miss the key. An update that removes the old version before it adds the new
    // version causes many misses
    assertTrue(missed + " of " + searched + " searches missed the updated key",
      missed.get() * 100 <= searched.get());
  }

  /**
   * Inserts and deletes of different keys run in parallel, and at times the graph has no live
   * nodes. Each live key must stay reachable, and no key can be both live and deleted.
   */
  @Test
  public void testParallelChanges() throws Exception {
    HnswIndexManager.MutableGraph graph = graph();
    int threads = 4;
    Map<String, float[]> last = new ConcurrentHashMap<>();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> writers = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        int id = t;
        writers.add(pool.submit(() -> {
          Random random = new Random(id);
          // Each thread changes one key, because row locks order the changes to one row
          String k = "t" + id;
          for (int i = 0; i < 3000; i++) {
            if (random.nextBoolean()) {
              graph.delete(key(k));
              last.remove(k);
            } else {
              float[] v = vector(random);
              graph.upsert(key(k), v);
              last.put(k, v);
            }
          }
          return null;
        }));
      }
      for (Future<?> writer : writers) {
        writer.get(120, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(last.keySet().size(), graph.live().size());
    for (Map.Entry<String, float[]> e : last.entrySet()) {
      assertEquals(e.getKey(), key(e.getKey()), nearest(graph, e.getValue()));
    }
    for (ImmutableBytesPtr deleted : graph.deleted()) {
      assertFalse(graph.live().containsKey(deleted));
    }
    assertTrue(graph.size() >= graph.changes());
  }
}
