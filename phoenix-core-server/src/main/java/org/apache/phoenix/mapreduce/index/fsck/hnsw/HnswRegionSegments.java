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
package org.apache.phoenix.mapreduce.index.fsck.hnsw;

import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;

/**
 * Segment views and row mappings for an HNSW index covering an HBase region. Coordinates segment
 * stacks (base segments and associated deltas) according to {@link HnswIndexManager} resolution
 * rules, where newer deltas or tombstones supersede entries in older segments within the same
 * stack.
 */
public final class HnswRegionSegments implements AutoCloseable {
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  /** Maximum relative divergence allowed for unquantized vector comparisons. */
  static final double EXACT_TOLERANCE = 1e-6;
  /** Maximum relative divergence allowed when verifying quantized (NVQ) vectors. */
  static final double QUANTIZED_TOLERANCE = 0.02;

  private final byte[] start;
  private final byte[] end;
  private final List<HnswSegment.Descriptor> current;
  // Ordered newest to oldest for stack shadowing resolution
  private final List<Source> sources = new ArrayList<>();

  /** Open segment and its associated graph view. */
  public static final class Source {
    private final HnswSegment.Descriptor descriptor;
    private final HnswSegment segment;
    private final Set<ImmutableBytesPtr> tombstones = new HashSet<>();
    private final OnDiskGraphIndex.View view;
    private final boolean exact;

    Source(HnswSegment.Descriptor descriptor, HnswSegment segment) throws IOException {
      this.descriptor = descriptor;
      this.segment = segment;
      for (byte[] key : segment.getTombstones()) {
        tombstones.add(new ImmutableBytesPtr(key));
      }
      OnDiskGraphIndex graph = segment.graph();
      this.view = graph == null ? null : graph.getView();
      this.exact = graph != null && graph.getFeatureSet().contains(FeatureId.INLINE_VECTORS);
    }

    public HnswSegment.Descriptor getDescriptor() {
      return descriptor;
    }

    public HnswSegment getSegment() {
      return segment;
    }

    long stackTime() {
      return descriptor.isDelta() ? descriptor.baseTime : descriptor.time;
    }

    boolean sameStack(Source other) {
      return stackTime() == other.stackTime()
        && Bytes.equals(descriptor.startKey, other.descriptor.startKey);
    }

    /**
     * Computes relative divergence between the vector stored at the specified ordinal and the
     * expected vector, normalized by vector magnitude.
     */
    public double divergence(int ordinal, float[] vector) {
      VectorFloat<?> v = VTS.createFloatVector(vector);
      double squared;
      if (exact) {
        squared = VectorUtil.squareL2Distance(v, view.getVector(ordinal));
      } else {
        // The Euclidean similarity is 1 / (1 + squared distance)
        squared =
          1 / view.rerankerFor(v, VectorSimilarityFunction.EUCLIDEAN).similarityTo(ordinal) - 1;
      }
      double norm = Math.sqrt(VectorUtil.dotProduct(v, v));
      return Math.sqrt(Math.max(0, squared)) / Math.max(norm, Float.MIN_NORMAL);
    }

    /** Returns true if the vector at the specified ordinal exceeds divergence tolerance. */
    public boolean diverges(int ordinal, float[] vector) {
      return divergence(ordinal, vector) > (exact ? EXACT_TOLERANCE : QUANTIZED_TOLERANCE);
    }

    void close() throws IOException {
      try {
        if (view != null) {
          view.close();
        }
      } finally {
        segment.close();
      }
    }
  }

  /** Association between a segment source and a row ordinal. */
  public static final class Entry {
    private final Source source;
    private final int ordinal;

    Entry(Source source, int ordinal) {
      this.source = source;
      this.ordinal = ordinal;
    }

    public Source getSource() {
      return source;
    }

    public int getOrdinal() {
      return ordinal;
    }
  }

  private HnswRegionSegments(byte[] start, byte[] end, List<HnswSegment.Descriptor> current) {
    this.start = start;
    this.end = end;
    this.current = current;
  }

  /**
   * Opens active segments covering the key range {@code [start, end)}. Segments containing no
   * vectors are omitted.
   */
  public static HnswRegionSegments open(HnswIndexReader reader, List<HnswSegment.Descriptor> all,
    byte[] start, byte[] end) throws IOException {
    HnswRegionSegments region =
      new HnswRegionSegments(start, end, HnswIndexManager.currentSegments(all, start, end));
    try {
      for (HnswSegment.Descriptor d : region.current) {
        if (d.count > 0 || d.isDelta()) {
          region.sources.add(new Source(d, reader.open(d)));
        }
      }
    } catch (IOException | RuntimeException e) {
      region.close();
      throw e;
    }
    return region;
  }

  /** Returns segment descriptors ordered newest to oldest. */
  public List<HnswSegment.Descriptor> getCurrent() {
    return current;
  }

  /** Returns open segment sources ordered newest to oldest. */
  public List<Source> getSources() {
    return sources;
  }

  /**
   * Returns the replay start timestamp for the region. Rows modified after this timestamp are
   * resolved from memstore or WAL rather than segment files.
   */
  public long replayStart() {
    return current.isEmpty() ? 0 : HnswIndexManager.replayStartTime(current);
  }

  /** Resolves all visible entries for a row key across active segment stacks. */
  public List<Entry> lookup(byte[] key) {
    ImmutableBytesPtr ptr = new ImmutableBytesPtr(key);
    List<Entry> entries = new ArrayList<>(1);
    // Tracks stacks where an entry or tombstone has already shadowed older segments
    List<Source> decided = new ArrayList<>(1);
    for (Source source : sources) {
      if (decided.stream().anyMatch(source::sameStack)) {
        continue;
      }
      int ordinal = source.segment.ordinalOf(ptr);
      if (ordinal >= 0) {
        entries.add(new Entry(source, ordinal));
        decided.add(source);
      } else if (source.tombstones.contains(ptr)) {
        decided.add(source);
      }
    }
    return entries;
  }

  /** Returns true if the ordinal is not superseded by a newer entry or tombstone in the stack. */
  public boolean isLive(Source source, int ordinal) {
    ImmutableBytesPtr key = new ImmutableBytesPtr(source.segment.getKey(ordinal));
    for (Source s : sources) {
      if (s == source) {
        return true;
      }
      if (s.sameStack(source) && (s.segment.ordinalOf(key) >= 0 || s.tombstones.contains(key))) {
        return false;
      }
    }
    return true;
  }

  /** Returns the ordinal range {@code [low, high)} belonging to this region's key boundary. */
  public int[] ordinalRange(Source source) {
    HnswSegment segment = source.segment;
    int low = segment.ceiling(start);
    int high = end.length == 0 ? segment.size() : segment.ceiling(end);
    return new int[] { low, Math.max(low, high) };
  }

  @Override
  public void close() throws IOException {
    IOException failure = null;
    for (Source source : sources) {
      try {
        source.close();
      } catch (IOException e) {
        failure = e;
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
