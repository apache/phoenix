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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;

/**
 * The current segments of an HNSW index for the key range of one data region, as the tools see
 * them. The selection of the segments and the masks of their entries use the rules of
 * {@link HnswIndexManager}. Thus the tools and the region server agree on which entries are live. A
 * segment stack is a base segment and its deltas. In a stack, an entry or a tombstone in a newer
 * delta masks the entries of the same row in the older segments. As the region does, the tools use
 * only the segments of the index definition that the region holds in its {@code V#} family. After a
 * split, the lower daughter holds the stack of the parent. The upper daughter holds no segment
 * until its first build.
 */
public final class HnswRegionSegments implements AutoCloseable {
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  /** The relative divergence limit for full vectors. It allows only for float rounding. */
  static final double EXACT_TOLERANCE = 1e-6;
  /** The relative divergence limit for quantized vectors. It allows for the NVQ error. */
  static final double QUANTIZED_TOLERANCE = 0.02;

  private final byte[] start;
  private final byte[] end;
  private final List<HnswSegment.Descriptor> current;
  // Newest first, which is the order that the stack masks need
  private final List<Source> sources = new ArrayList<>();

  /** An open segment and a view of its graph. A close of the source closes the segment. */
  public static final class Source {
    private final HnswSegment.Descriptor descriptor;
    private final HnswSegment segment;
    // The ordinals that an entry or a tombstone in a newer delta of the same stack masks
    private Set<Integer> mask = Collections.emptySet();
    private final OnDiskGraphIndex.View view;
    private final boolean exact;

    Source(HnswSegment.Descriptor descriptor, HnswSegment segment) throws IOException {
      this.descriptor = descriptor;
      this.segment = segment;
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

    /**
     * Returns the L2 distance between the segment vector at the ordinal and the given vector,
     * divided by the norm of the given vector. A segment with full vectors gives the exact
     * distance. A quantized segment gives the distance to its NVQ approximation of the vector.
     */
    public double divergence(int ordinal, float[] vector) {
      VectorFloat<?> v = VTS.createFloatVector(vector);
      double squared;
      if (exact) {
        squared = VectorUtil.squareL2Distance(v, view.getVector(ordinal));
      } else {
        // JVector gives the EUCLIDEAN similarity as 1 / (1 + squared distance)
        squared =
          1 / view.rerankerFor(v, VectorSimilarityFunction.EUCLIDEAN).similarityTo(ordinal) - 1;
      }
      double norm = Math.sqrt(VectorUtil.dotProduct(v, v));
      return Math.sqrt(Math.max(0, squared)) / Math.max(norm, Float.MIN_NORMAL);
    }

    /**
     * Returns true if the segment vector at the ordinal differs from the given vector by more than
     * the limit for the vectors of the segment.
     */
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

  /** A live entry of a row: a segment source and the ordinal of the row in that segment. */
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
   * Opens the current segments for the key range {@code [start, end)}. The method selects them from
   * the segments of the index definition that the region holds, as the region does. A base segment
   * with no vectors gets no source. A delta always gets a source, because its tombstones mask
   * entries of older segments. If an open fails, the method closes the segments that it opened.
   */
  public static HnswRegionSegments open(HnswIndexReader reader, List<HnswSegment.Descriptor> all,
    byte[] start, byte[] end) throws IOException {
    HnswRegionSegments region = new HnswRegionSegments(start, end, HnswIndexManager
      .currentSegments(reader.fenced(HnswIndexReader.held(all, start, end)), start, end));
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
    List<HnswSegment.Descriptor> descriptors = new ArrayList<>(region.sources.size());
    List<HnswSegment> segments = new ArrayList<>(region.sources.size());
    for (Source source : region.sources) {
      descriptors.add(source.descriptor);
      segments.add(source.segment);
    }
    List<Set<Integer>> masks = HnswIndexManager.stackMasks(descriptors, segments);
    for (int i = 0; i < masks.size(); i++) {
      region.sources.get(i).mask = masks.get(i);
    }
    return region;
  }

  /**
   * Lists the segments and opens the current segments for the key range {@code [start, end)}. A
   * server can retire a listed segment before the open. Then the method lists the segments again,
   * as the region manager does. If the list did not change, the method throws the
   * {@link HnswSegment.NotFoundException}.
   */
  public static HnswRegionSegments open(HnswIndexReader reader, byte[] start, byte[] end)
    throws IOException {
    List<HnswSegment.Descriptor> all = reader.listSegments();
    while (true) {
      try {
        return open(reader, all, start, end);
      } catch (HnswSegment.NotFoundException e) {
        List<HnswSegment.Descriptor> again = reader.listSegments();
        if (identities(again).equals(identities(all))) {
          // The segment is still listed, so no server retired it
          throw e;
        }
        all = again;
      }
    }
  }

  private static List<ImmutableBytesPtr> identities(List<HnswSegment.Descriptor> segments) {
    List<ImmutableBytesPtr> identities = new ArrayList<>(segments.size());
    for (HnswSegment.Descriptor d : segments) {
      identities.add(new ImmutableBytesPtr(Bytes.add(d.anchor, d.identity)));
    }
    return identities;
  }

  /**
   * Returns the descriptors of the current segments, newest first. The list includes base segments
   * with no vectors, which have no source.
   */
  public List<HnswSegment.Descriptor> getCurrent() {
    return current;
  }

  /** Returns the open sources, newest first. */
  public List<Source> getSources() {
    return sources;
  }

  /**
   * Returns true if the base segments cover the key range of the region. A region that is not
   * covered, such as the upper daughter of a split before its first build, reads its rows without
   * the index. Thus its query results stay exact.
   */
  public boolean covered() {
    return HnswIndexReader.covered(start, end, current);
  }

  /**
   * Returns the replay start of the region: the oldest replay start of the newest segments of the
   * stacks. A data row that changed at or after this time can be only in the in-memory graph of the
   * region. The region replays these rows when it opens. Thus the segments can omit these rows or
   * hold earlier versions of them. Returns 0 if there are no current segments.
   */
  public long replayStart() {
    return current.isEmpty() ? 0 : HnswIndexManager.replayStartTime(current);
  }

  /** Returns the live entries of a row in all current segments, newest segment first. */
  public List<Entry> lookup(byte[] key) {
    ImmutableBytesPtr ptr = new ImmutableBytesPtr(key);
    List<Entry> entries = new ArrayList<>(1);
    for (Source source : sources) {
      int ordinal = source.segment.ordinalOf(ptr);
      if (ordinal >= 0 && !source.mask.contains(ordinal)) {
        entries.add(new Entry(source, ordinal));
      }
    }
    return entries;
  }

  /** Returns true if no entry or tombstone in a newer delta of the same stack masks the ordinal. */
  public boolean isLive(Source source, int ordinal) {
    return !source.mask.contains(ordinal);
  }

  /**
   * Returns the ordinals {@code [low, high)} of the entries of a source whose row keys are in the
   * key range of the region. The ordinals follow row key order, so these ordinals are contiguous. A
   * wider segment, for example the stack of a split parent, also holds rows of other regions.
   */
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
