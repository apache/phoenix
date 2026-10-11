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
import static org.junit.Assume.assumeFalse;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.QueryServicesOptions;
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

  private static VectorFloat<?> query(float[] v) {
    return VectorizationProvider.getInstance().getVectorTypeSupport().createFloatVector(v);
  }

  // Returns the key nearest to the vector, or null if the search finds no key. The helper turns the
  // exact search off, so that the tests check the graph links
  private static ImmutableBytesPtr nearest(HnswIndexManager.MutableGraph graph, float[] v) {
    return nearest(graph, v, 0);
  }

  // The search uses its candidate count as its beam width, so this helper asks for ten candidates
  private static ImmutableBytesPtr nearest(HnswIndexManager.MutableGraph graph, float[] v,
    int exactMaxRows) {
    Map<ImmutableBytesPtr, Float> scores = new HashMap<>();
    graph.search(query(v), 10, scores, null, k -> true, exactMaxRows);
    return best(scores);
  }

  private static ImmutableBytesPtr best(Map<ImmutableBytesPtr, Float> scores) {
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
    AtomicInteger searched = new AtomicInteger();
    AtomicInteger missed = new AtomicInteger();
    searchDuringUpdates(0, searched, missed);
    // JVector can miss a node while a concurrent insert changes the links of its neighbors. Thus a
    // few searches can miss the key. An update that removes the old version before it adds the new
    // version causes many misses
    assertTrue(missed + " of " + searched + " searches missed the updated key",
      missed.get() * 100 <= searched.get());
  }

  /**
   * The exact search of a small graph scores the current version of each live key. A key that
   * changes during the search always has one current version. Thus no search misses the key.
   */
  @Test
  public void testUpdatedKeyIsFoundDuringExactSearches() throws Exception {
    AtomicInteger searched = new AtomicInteger();
    AtomicInteger missed = new AtomicInteger();
    searchDuringUpdates(QueryServicesOptions.DEFAULT_HNSW_MUTABLE_EXACT_MAX_ROWS, searched, missed);
    assertTrue(searched.get() > 0);
    assertEquals(missed + " of " + searched + " searches missed the updated key", 0, missed.get());
  }

  // Two threads search for the key "hot" while the key changes many times. The search uses the
  // given limit of the exact search. Counts the searches and the searches that miss the key
  private static void searchDuringUpdates(int exactMaxRows, AtomicInteger searched,
    AtomicInteger missed) throws Exception {
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
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      List<Future<?>> searches = new ArrayList<>();
      for (int t = 0; t < 2; t++) {
        searches.add(pool.submit(() -> {
          while (!done.get()) {
            searched.incrementAndGet();
            if (!key("hot").equals(nearest(graph, point(0), exactMaxRows))) {
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

  // Returns the keys that the search finds with ten candidates
  static Set<ImmutableBytesPtr> search(HnswIndexManager.MutableGraph graph, float[] v,
    int exactMaxRows) {
    Map<ImmutableBytesPtr, Float> scores = new HashMap<>();
    graph.search(query(v), 10, scores, null, k -> true, exactMaxRows);
    return scores.keySet();
  }

  // Makes the changes of a graph whose beam search misses a live row, and returns the vector of
  // the row. The row "twin" repeats the vector of the deleted row "first". JVector picks the layer
  // of each new node from a random sequence with a fixed seed. Thus nodes 82 and 83 are on an upper
  // layer in each graph. The two equal vectors tie on that layer. The search then does not visit
  // the new node on the lowest layer
  static float[] twin(BiConsumer<String, float[]> upsert, Consumer<String> delete) {
    Random random = new Random(1);
    for (int i = 0; i < 82; i++) {
      upsert.accept("b" + i, vector(random));
    }
    float[] probe = vector(random);
    upsert.accept("first", probe);
    delete.accept("first");
    upsert.accept("twin", probe);
    return probe;
  }

  private static HnswIndexManager.MutableGraph twin(float[] probe) {
    HnswIndexManager.MutableGraph graph = graph();
    float[] v = twin((k, x) -> graph.upsert(key(k), x), k -> graph.delete(key(k)));
    System.arraycopy(v, 0, probe, 0, DIM);
    return graph;
  }

  // Returns true if the beam search with ten candidates misses the row "twin". The miss depends on
  // how JVector builds the graph, so the tests that need the miss only assume it
  static boolean beamMissesTwin() {
    float[] probe = new float[DIM];
    return !search(twin(probe), probe, 0).contains(key("twin"));
  }

  /**
   * A new row that repeats the vector of a deleted row can be unreachable for the beam search, at
   * any beam width. The exact search of a small graph finds it, and returns the true top k.
   */
  @Test
  public void testExactSearchFindsRowThatBeamMisses() {
    float[] probe = new float[DIM];
    HnswIndexManager.MutableGraph graph = twin(probe);
    int exactMaxRows = QueryServicesOptions.DEFAULT_HNSW_MUTABLE_EXACT_MAX_ROWS;
    assertTrue(graph.live().size() <= exactMaxRows);
    Map<ImmutableBytesPtr, Float> scores = new HashMap<>();
    graph.search(query(probe), 10, scores, null, k -> true, exactMaxRows);
    assertEquals(key("twin"), best(scores));
    // The result is the true top 10 of the live rows
    List<Map.Entry<ImmutableBytesPtr, float[]>> ranked = new ArrayList<>(graph.live().entrySet());
    ranked
      .sort((a, b) -> Float.compare(distance(a.getValue(), probe), distance(b.getValue(), probe)));
    Set<ImmutableBytesPtr> top = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      top.add(ranked.get(i).getKey());
    }
    assertEquals(top, scores.keySet());
    // The beam miss depends on how JVector builds the graph. Thus the test only assumes it
    for (int topK : new int[] { 10, 64, 100 }) {
      scores.clear();
      graph.search(query(probe), topK, scores, null, k -> true, 0);
      assumeFalse("beam with " + topK + " candidates", scores.containsKey(key("twin")));
    }
  }

  /**
   * A graph with at most the limit of live rows gets the exact search, and a larger graph gets the
   * beam search. A limit of 0 turns the exact search off.
   */
  @Test
  public void testExactSearchLimit() {
    float[] probe = new float[DIM];
    HnswIndexManager.MutableGraph graph = twin(probe);
    int rows = graph.live().size();
    assertTrue(search(graph, probe, rows).contains(key("twin")));
    // The beam miss depends on how JVector builds the graph. Thus the test only assumes it
    assumeFalse(search(graph, probe, 0).contains(key("twin")));
    assertFalse(search(graph, probe, rows - 1).contains(key("twin")));
    assertFalse(search(graph, probe, -1).contains(key("twin")));
  }

  /**
   * The exact search scores only the current version of each live key. It skips the routing nodes
   * of replaced and deleted versions.
   */
  @Test
  public void testExactSearchSkipsRoutingNodes() {
    HnswIndexManager.MutableGraph graph = graph();
    for (int i = 0; i < 20; i++) {
      graph.upsert(key("far" + i), point(100 + i));
    }
    graph.upsert(key("moved"), point(0));
    graph.upsert(key("moved"), point(50));
    graph.upsert(key("gone"), point(0.5f));
    graph.delete(key("gone"));
    Map<ImmutableBytesPtr, Float> scores = new HashMap<>();
    int scored = graph.search(query(point(0)), 10, scores, null, k -> true, 1000);
    assertEquals(graph.live().size(), scored);
    assertFalse(scores.containsKey(key("gone")));
    assertEquals(key("moved"), best(scores));
    float current = HnswSegment.similarityFunction("L2").compare(query(point(0)), query(point(50)));
    assertEquals(current, scores.get(key("moved")), 0f);
    assertEquals(10, scores.size());
  }

  /**
   * The exact search returns only the keys that pass the accept filter and that are not excluded.
   * It returns the nearest of these keys.
   */
  @Test
  public void testExactSearchFilters() {
    HnswIndexManager.MutableGraph graph = graph();
    for (int i = 0; i < 30; i++) {
      graph.upsert(key("k" + (char) ('a' + i % 3) + i), point(i));
    }
    Set<ImmutableBytesPtr> excluded = new HashSet<>();
    excluded.add(key("ka0"));
    excluded.add(key("ka3"));
    Map<ImmutableBytesPtr, Float> scores = new HashMap<>();
    graph.search(query(point(0)), 3, scores, excluded, k -> true, 1000);
    Set<ImmutableBytesPtr> expected = new HashSet<>();
    expected.add(key("kb1"));
    expected.add(key("kc2"));
    expected.add(key("kb4"));
    assertEquals(expected, scores.keySet());

    scores.clear();
    graph.search(query(point(0)), 3, scores, excluded, k -> k[1] != 'b', 1000);
    expected.clear();
    expected.add(key("kc2"));
    expected.add(key("kc5"));
    expected.add(key("ka6"));
    assertEquals(expected, scores.keySet());
  }

  private static float distance(float[] a, float[] b) {
    float sum = 0;
    for (int d = 0; d < DIM; d++) {
      sum += (a[d] - b[d]) * (a[d] - b[d]);
    }
    return sum;
  }
}
