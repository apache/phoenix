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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.jbellis.jvector.graph.NodesIterator;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.hnsw.HnswSegment;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.vector.HnswIndexManager;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckContext;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.query.QueryServicesOptions;

/**
 * Diagnostic and introspection commands for HNSW vector indexes. Provides read only analysis of
 * segment catalog metadata, serialized graph structures, and base table vector alignments:
 * <ul>
 * <li>{@code list}: Segment descriptors and payload sizes.</li>
 * <li>{@code coverage}: Region segment resolution, coverage gaps, and retirement candidates.</li>
 * <li>{@code dump [time=T] [nodes] [dot]}: Decoded segment payload inspection and Graphviz
 * export.</li>
 * <li>{@code export <directory> [time=T]}: Raw segment payload binaries and metadata JSON
 * export.</li>
 * <li>{@code search <[v1,...] | @file | key values> [k=K] [ef=EF]}: Client-side federated search
 * with recall metrics against brute-force scan.</li>
 * <li>{@code neighbors <key values> | neighbors time=T <ordinal>}: Graph adjacency analysis across
 * hierarchy levels.</li>
 * <li>{@code lookup <key values> | lookup time=T <ordinal>}: Bidirectional mapping between data row
 * keys and segment ordinals with vector divergence checking.</li>
 * <li>{@code delta}: Region mutation backlog pending server flush and segment incorporation.</li>
 * </ul>
 */
public class HnswIndexInspector {
  public static final String LIST = "list";
  public static final String COVERAGE = "coverage";
  public static final String DUMP = "dump";
  public static final String EXPORT = "export";
  public static final String SEARCH = "search";
  public static final String NEIGHBORS = "neighbors";
  public static final String LOOKUP = "lookup";
  public static final String DELTA = "delta";

  private static final int DEFAULT_K = 10;
  private static final VectorTypeSupport VTS =
    VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final ObjectMapper MAPPER =
    new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  private final IndexFsckContext fsck;
  private final HnswIndexReader reader;
  private final HnswIndexContext hnsw;

  public HnswIndexInspector(IndexFsckContext fsck, HnswIndexReader reader) {
    this.fsck = fsck;
    this.reader = reader;
    this.hnsw = reader.getContext();
  }

  public void inspect(String command, List<String> args, Report report) throws Exception {
    Args parsed = new Args(args);
    switch (command) {
      case LIST:
        list(report);
        break;
      case COVERAGE:
        coverage(report);
        break;
      case DUMP:
        dump(parsed, report);
        break;
      case EXPORT:
        export(parsed, report);
        break;
      case SEARCH:
        search(parsed, report);
        break;
      case NEIGHBORS:
        neighbors(parsed, report);
        break;
      case LOOKUP:
        lookup(parsed, report);
        break;
      case DELTA:
        delta(report);
        break;
      default:
        throw new IllegalArgumentException(
          "Unknown inspect command " + command + "; the " + "commands are "
            + Arrays.asList(LIST, COVERAGE, DUMP, EXPORT, SEARCH, NEIGHBORS, LOOKUP, DELTA));
    }
  }

  /**
   * Command argument parser supporting key-value options ({@code name=value}) and flag keywords.
   */
  static final class Args {
    private static final Set<String> FLAGS = new HashSet<>(Arrays.asList("nodes", "dot"));
    private static final Set<String> NAMES = new HashSet<>(Arrays.asList("k", "ef", "time"));
    final List<String> positional = new ArrayList<>();
    final Map<String, String> options = new HashMap<>();

    Args(List<String> args) {
      for (String arg : args) {
        int eq = arg.indexOf('=');
        if (FLAGS.contains(arg)) {
          options.put(arg, "true");
        } else if (eq > 0 && NAMES.contains(arg.substring(0, eq))) {
          options.put(arg.substring(0, eq), arg.substring(eq + 1));
        } else {
          positional.add(arg);
        }
      }
    }

    boolean has(String option) {
      return options.containsKey(option);
    }

    Long getLong(String option) {
      return options.containsKey(option) ? Long.valueOf(options.get(option)) : null;
    }

    int getInt(String option, int defaultValue) {
      return options.containsKey(option) ? Integer.parseInt(options.get(option)) : defaultValue;
    }
  }

  private void list(Report report) throws IOException {
    Map<ImmutableBytesPtr, Integer> sizes = reader.payloadSizes();
    List<Map<String, Object>> segments = new ArrayList<>();
    for (HnswSegment.Descriptor d : reader.listSegments()) {
      Map<String, Object> s = describe(d);
      s.put("payloadBytes", sizes.get(new ImmutableBytesPtr(d.rowKey)));
      segments.add(s);
    }
    report.putInspection("segments", segments);
  }

  private Map<String, Object> describe(HnswSegment.Descriptor d) {
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("segment", Bytes.toStringBinary(d.rowKey));
    s.put("startKey", key(d.startKey));
    s.put("endKey", key(d.endKey));
    s.put("time", d.time);
    s.put("kind", d.isDelta() ? "delta" : "base");
    if (d.isDelta()) {
      s.put("baseTime", d.baseTime);
    }
    s.put("rows", d.count);
    return s;
  }

  private String key(byte[] key) {
    return fsck.formatRowKey(key, hnsw.getDataTable());
  }

  private void coverage(Report report) throws IOException {
    List<HnswSegment.Descriptor> all = reader.listSegments();
    List<Map<String, Object>> regions = new ArrayList<>();
    for (RegionInfo region : reader.listRegions()) {
      byte[] start = region.getStartKey();
      byte[] end = region.getEndKey();
      List<HnswSegment.Descriptor> current = HnswIndexManager.currentSegments(all, start, end);
      Map<String, Object> r = new LinkedHashMap<>();
      r.put("region", region.getEncodedName());
      r.put("startKey", key(start));
      r.put("endKey", key(end));
      List<String> segments = new ArrayList<>();
      List<String> wider = new ArrayList<>();
      int exactBases = 0;
      int bases = 0;
      for (HnswSegment.Descriptor d : current) {
        segments.add(Bytes.toStringBinary(d.rowKey));
        if (!d.isDelta()) {
          bases++;
          if (d.covers(start, end)) {
            exactBases++;
          } else if (
            Bytes.compareTo(d.startKey, start) <= 0
              && (d.endKey.length == 0 || (end.length > 0 && Bytes.compareTo(d.endKey, end) >= 0))
          ) {
            wider.add(Bytes.toStringBinary(d.rowKey));
          }
        }
      }
      r.put("segments", segments);
      r.put("wider", wider);
      r.put("exact", bases == 1 && exactBases == 1);
      r.put("covered", HnswSegment.covered(start, end, all));
      regions.add(r);
    }
    report.putInspection("regions", regions);
    List<String> retirable = new ArrayList<>();
    for (HnswSegment.Descriptor d : HnswIndexManager.segmentsToRetire(all,
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW)) {
      retirable.add(Bytes.toStringBinary(d.rowKey));
    }
    report.putInspection("retirable", retirable);
  }

  private List<HnswSegment.Descriptor> select(Long time) throws IOException {
    List<HnswSegment.Descriptor> selected = new ArrayList<>();
    for (HnswSegment.Descriptor d : reader.listSegments()) {
      if (time == null || d.time == time) {
        selected.add(d);
      }
    }
    if (selected.isEmpty()) {
      throw new IllegalArgumentException("No segment has time " + time);
    }
    return selected;
  }

  private HnswSegment.Descriptor selectOne(Long time) throws IOException {
    if (time == null) {
      throw new IllegalArgumentException("An ordinal is named with time=<segment time>");
    }
    List<HnswSegment.Descriptor> selected = select(time);
    if (selected.size() > 1) {
      throw new IllegalArgumentException(selected.size() + " segments have time " + time);
    }
    return selected.get(0);
  }

  private void dump(Args args, Report report) throws IOException {
    List<Map<String, Object>> segments = new ArrayList<>();
    for (HnswSegment.Descriptor d : select(args.getLong("time"))) {
      Map<String, Object> s = describe(d);
      byte[] payload = reader.readPayload(d);
      s.put("payloadBytes", payload == null ? null : payload.length);
      if (payload != null && payload.length >= 2 * Bytes.SIZEOF_INT) {
        s.put("graphMagic", String.format("%08x", Bytes.toInt(payload, 0)));
        s.put("graphFormatVersion", Bytes.toInt(payload, Bytes.SIZEOF_INT));
        s.put("trailerMagic",
          String.format("%08x", Bytes.toInt(payload, payload.length - Bytes.SIZEOF_INT)));
      }
      HnswSegment segment = reader.open(d);
      try {
        s.put("tombstones", segment.getTombstones().length);
        OnDiskGraphIndex graph = segment.graph();
        if (graph != null) {
          dumpGraph(segment, graph, args, s);
        }
      } finally {
        segment.close();
      }
      segments.add(s);
    }
    report.putInspection("segments", segments);
  }

  private void dumpGraph(HnswSegment segment, OnDiskGraphIndex graph, Args args,
    Map<String, Object> s) throws IOException {
    s.put("dimension", graph.getDimension());
    s.put("features", new ArrayList<>(graph.getFeatureSet()));
    List<Map<String, Object>> levels = new ArrayList<>();
    for (int level = 0; level <= graph.getMaxLevel(); level++) {
      Map<String, Object> l = new LinkedHashMap<>();
      l.put("level", level);
      l.put("nodes", graph.size(level));
      l.put("maxDegree", graph.getDegree(level));
      l.put("averageDegree", graph.getAverageDegree(level));
      levels.add(l);
    }
    s.put("levels", levels);
    StringBuilder dot = new StringBuilder("digraph segment {\n");
    List<Map<String, Object>> nodes = new ArrayList<>();
    Map<Integer, Integer> histogram = new TreeMap<>();
    try (OnDiskGraphIndex.View view = graph.getView()) {
      s.put("entryNode", view.entryNode().node);
      s.put("entryLevel", view.entryNode().level);
      for (int node = 0; node < segment.size(); node++) {
        List<Integer> neighbors = neighbors(view, 0, node);
        histogram.merge(neighbors.size(), 1, Integer::sum);
        if (args.has("nodes")) {
          Map<String, Object> n = new LinkedHashMap<>();
          n.put("ordinal", node);
          n.put("key", key(segment.getKey(node)));
          n.put("neighbors", neighbors);
          nodes.add(n);
        }
        for (int neighbor : neighbors) {
          dot.append("  ").append(node).append(" -> ").append(neighbor).append(";\n");
        }
      }
    }
    s.put("degreeHistogram", histogram);
    if (args.has("nodes")) {
      s.put("nodes", nodes);
    }
    if (args.has("dot")) {
      s.put("dot", dot.append("}\n").toString());
    }
  }

  private static List<Integer> neighbors(OnDiskGraphIndex.View view, int level, int node) {
    List<Integer> neighbors = new ArrayList<>();
    for (NodesIterator it = view.getNeighborsIterator(level, node); it.hasNext();) {
      neighbors.add(it.nextInt());
    }
    return neighbors;
  }

  private void export(Args args, Report report) throws IOException {
    if (args.positional.size() != 1) {
      throw new IllegalArgumentException("export <directory> [time=T]");
    }
    Path directory = Paths.get(args.positional.get(0));
    Files.createDirectories(directory);
    Map<String, Object> index = new LinkedHashMap<>();
    index.put("dataTable", hnsw.getDataTable().getName().getString());
    index.put("indexTable", hnsw.getIndexTable().getName().getString());
    index.put("metric", hnsw.getMetric());
    index.put("dimension", hnsw.getVectorIndex().getDimension());
    index.put("m", hnsw.getVectorIndex().getHnswM());
    index.put("efConstruction", hnsw.getVectorIndex().getHnswEfConstruction());
    index.put("alpha", hnsw.getVectorIndex().getHnswAlpha());
    index.put("quantization", hnsw.getVectorIndex().getQuantizationType());
    index.put("pqSegments", hnsw.getVectorIndex().getPqSegments());
    List<String> files = new ArrayList<>();
    for (HnswSegment.Descriptor d : select(args.getLong("time"))) {
      String name = Hex.encodeHexString(d.rowKey);
      Map<String, Object> metadata = new LinkedHashMap<>();
      metadata.put("rowKey", name);
      metadata.put("startKey", Hex.encodeHexString(d.startKey));
      metadata.put("endKey", Hex.encodeHexString(d.endKey));
      metadata.put("time", d.time);
      metadata.put("baseTime", d.baseTime);
      metadata.put("rows", d.count);
      byte[] payload = reader.readPayload(d);
      if (payload != null) {
        Path file = directory.resolve(name + ".hnsw");
        Files.write(file, payload);
        files.add(file.toString());
        metadata.put("payload", file.getFileName().toString());
      }
      if (d.isDelta()) {
        HnswSegment segment = reader.open(d);
        try {
          List<String> tombstones = new ArrayList<>();
          for (byte[] tombstone : segment.getTombstones()) {
            tombstones.add(Hex.encodeHexString(tombstone));
          }
          metadata.put("tombstones", tombstones);
        } finally {
          segment.close();
        }
      }
      metadata.put("index", index);
      Path sidecar = directory.resolve(name + ".json");
      Files.write(sidecar, MAPPER.writeValueAsString(metadata).getBytes(StandardCharsets.UTF_8));
      files.add(sidecar.toString());
    }
    report.putInspection("files", files);
  }

  /** Candidate result entry from a segment graph search. */
  private static final class Hit {
    final byte[] key;
    final String segment;
    final int ordinal;
    final float score;

    Hit(byte[] key, String segment, int ordinal, float score) {
      this.key = key;
      this.segment = segment;
      this.ordinal = ordinal;
      this.score = score;
    }
  }

  private void search(Args args, Report report) throws Exception {
    float[] query = query(args.positional);
    int k = args.getInt("k", DEFAULT_K);
    int ef = args.getInt("ef", QueryServicesOptions.DEFAULT_HNSW_EF_SEARCH);
    VectorFloat<?> q = VTS.createFloatVector(query);
    VectorSimilarityFunction similarity = hnsw.getSimilarity();
    Map<ImmutableBytesPtr, Hit> found = new HashMap<>();
    Map<ImmutableBytesPtr, Hit> exact = new HashMap<>();
    long visited = 0;
    long expanded = 0;
    List<HnswSegment.Descriptor> all = reader.listSegments();
    for (RegionInfo region : reader.listRegions()) {
      try (HnswRegionSegments segments =
        HnswRegionSegments.open(reader, all, region.getStartKey(), region.getEndKey())) {
        for (HnswRegionSegments.Source source : segments.getSources()) {
          HnswSegment segment = source.getSegment();
          int[] range = segments.ordinalRange(source);
          if (range[0] == range[1]) {
            continue;
          }
          String name = Bytes.toStringBinary(source.getDescriptor().rowKey);
          Bits accept = ordinal -> ordinal >= range[0] && ordinal < range[1]
            && segments.isLive(source, ordinal);
          SearchResult result = segment.search(q, k, ef, accept);
          visited += result.getVisitedCount();
          expanded += result.getExpandedCount();
          for (SearchResult.NodeScore ns : result.getNodes()) {
            keep(found, new Hit(segment.getKey(ns.node), name, ns.node, ns.score));
          }
          for (double[] node : HnswIndexFsckProvider.bruteForce(segment, q, similarity, k,
            accept)) {
            keep(exact,
              new Hit(segment.getKey((int) node[0]), name, (int) node[0], (float) node[1]));
          }
        }
      }
    }
    List<Hit> results = top(found, k);
    List<Hit> truth = top(exact, k);
    List<byte[]> keys = new ArrayList<>();
    for (Hit hit : results) {
      keys.add(hit.key);
    }
    Map<ImmutableBytesPtr, float[]> base = reader.getVectors(keys);
    Set<ImmutableBytesPtr> truthKeys = new HashSet<>();
    List<Map<String, Object>> bruteForce = new ArrayList<>();
    for (Hit hit : truth) {
      truthKeys.add(new ImmutableBytesPtr(hit.key));
      bruteForce.add(hit(hit, null, similarity, q));
    }
    int hits = 0;
    List<Map<String, Object>> rendered = new ArrayList<>();
    for (Hit hit : results) {
      hits += truthKeys.contains(new ImmutableBytesPtr(hit.key)) ? 1 : 0;
      rendered.add(hit(hit, base.get(new ImmutableBytesPtr(hit.key)), similarity, q));
    }
    report.putInspection("query", query);
    report.putInspection("k", k);
    report.putInspection("ef", ef);
    report.putInspection("results", rendered);
    report.putInspection("bruteForce", bruteForce);
    report.putInspection("recall", truth.isEmpty() ? null : (double) hits / truth.size());
    report.putInspection("visited", visited);
    report.putInspection("expanded", expanded);
  }

  private Map<String, Object> hit(Hit hit, float[] base, VectorSimilarityFunction similarity,
    VectorFloat<?> query) {
    Map<String, Object> h = new LinkedHashMap<>();
    h.put("key", key(hit.key));
    h.put("segment", hit.segment);
    h.put("ordinal", hit.ordinal);
    h.put("score", hit.score);
    if (base != null) {
      h.put("baseScore", similarity.compare(query, VTS.createFloatVector(base)));
    }
    return h;
  }

  private static void keep(Map<ImmutableBytesPtr, Hit> hits, Hit hit) {
    hits.merge(new ImmutableBytesPtr(hit.key), hit, (a, b) -> a.score >= b.score ? a : b);
  }

  private static List<Hit> top(Map<ImmutableBytesPtr, Hit> hits, int k) {
    List<Hit> sorted = new ArrayList<>(hits.values());
    sorted.sort((a, b) -> Float.compare(b.score, a.score));
    return sorted.subList(0, Math.min(k, sorted.size()));
  }

  /** Parses a query vector from literal array format, a file reference, or by data row key. */
  private float[] query(List<String> args) throws Exception {
    if (args.isEmpty()) {
      throw new IllegalArgumentException("search <[v1,v2,...] | @file | data row key values>");
    }
    String first = args.get(0);
    if (first.startsWith("@")) {
      return parseVector(
        new String(Files.readAllBytes(Paths.get(first.substring(1))), StandardCharsets.UTF_8));
    }
    if (first.startsWith("[")) {
      return parseVector(String.join(",", args));
    }
    HnswIndexContext.KeyedVector row = hnsw.readRow(hnsw.parseKey(args));
    if (row == null || row.getVector() == null) {
      throw new IllegalArgumentException("No data row with key " + args + " has a vector");
    }
    return row.getVector();
  }

  static float[] parseVector(String text) {
    String body = text.trim();
    if (!body.startsWith("[") || !body.endsWith("]")) {
      throw new IllegalArgumentException("A vector is written [v1,v2,...]: " + text);
    }
    String[] parts = body.substring(1, body.length() - 1).split("[,\\s]+");
    List<Float> values = new ArrayList<>();
    for (String part : parts) {
      if (!part.isEmpty()) {
        values.add(Float.parseFloat(part));
      }
    }
    float[] vector = new float[values.size()];
    for (int i = 0; i < vector.length; i++) {
      vector[i] = values.get(i);
    }
    return vector;
  }

  /** Callback interface for visiting segment ordinals within region context. */
  private interface OrdinalVisitor {
    void visit(HnswRegionSegments.Source source, int ordinal, HnswRegionSegments region)
      throws IOException;
  }

  /**
   * Resolves target ordinals specified explicitly by timestamp/ordinal or implicitly by data row
   * key, delegating matching entries to {@code visitor}.
   */
  private void ordinals(Args args, Report report, OrdinalVisitor visitor) throws Exception {
    if (args.has("time")) {
      if (args.positional.size() != 1) {
        throw new IllegalArgumentException("time=<segment time> <ordinal>");
      }
      HnswSegment.Descriptor d = selectOne(args.getLong("time"));
      HnswRegionSegments.Source source = new HnswRegionSegments.Source(d, reader.open(d));
      try {
        int ordinal = Integer.parseInt(args.positional.get(0));
        if (ordinal < 0 || ordinal >= source.getSegment().size()) {
          throw new IllegalArgumentException("Segment " + Bytes.toStringBinary(d.rowKey)
            + " has ordinals 0 to " + (source.getSegment().size() - 1));
        }
        visitor.visit(source, ordinal, null);
      } finally {
        source.close();
      }
      return;
    }
    HnswIndexContext.KeyedVector row = hnsw.readRow(hnsw.parseKey(args.positional));
    if (row == null) {
      throw new IllegalArgumentException("No data row has key " + args.positional);
    }
    report.putInspection("key", key(row.getKey()));
    report.putInspection("vector", row.getVector());
    for (RegionInfo region : reader.listRegions()) {
      if (!region.containsRow(row.getKey())) {
        continue;
      }
      report.putInspection("region", region.getEncodedName());
      try (HnswRegionSegments segments = HnswRegionSegments.open(reader, reader.listSegments(),
        region.getStartKey(), region.getEndKey())) {
        for (HnswRegionSegments.Entry entry : segments.lookup(row.getKey())) {
          visitor.visit(entry.getSource(), entry.getOrdinal(), segments);
        }
      }
    }
  }

  private void lookup(Args args, Report report) throws Exception {
    List<Map<String, Object>> entries = new ArrayList<>();
    ordinals(args, report, (source, ordinal, region) -> {
      HnswSegment segment = source.getSegment();
      byte[] key = segment.getKey(ordinal);
      Map<String, Object> e = new LinkedHashMap<>();
      e.put("segment", Bytes.toStringBinary(source.getDescriptor().rowKey));
      e.put("time", source.getDescriptor().time);
      e.put("ordinal", ordinal);
      e.put("key", key(key));
      HnswSegment.Descriptor d = source.getDescriptor();
      e.put("inSegmentRange", Bytes.compareTo(key, d.startKey) >= 0
        && (d.endKey.length == 0 || Bytes.compareTo(key, d.endKey) < 0));
      if (region != null) {
        int[] range = region.ordinalRange(source);
        e.put("inRegion", ordinal >= range[0] && ordinal < range[1]);
        e.put("live", region.isLive(source, ordinal));
      }
      float[] base = reader.getVectors(Arrays.asList(key)).get(new ImmutableBytesPtr(key));
      e.put("baseRowHasVector", base != null);
      if (base != null) {
        e.put("divergence", source.divergence(ordinal, base));
        e.put("diverges", source.diverges(ordinal, base));
      }
      entries.add(e);
    });
    report.putInspection("entries", entries);
  }

  private void neighbors(Args args, Report report) throws Exception {
    List<Map<String, Object>> entries = new ArrayList<>();
    ordinals(args, report, (source, ordinal, region) -> {
      HnswSegment segment = source.getSegment();
      Map<String, Object> e = new LinkedHashMap<>();
      e.put("segment", Bytes.toStringBinary(source.getDescriptor().rowKey));
      e.put("ordinal", ordinal);
      e.put("key", key(segment.getKey(ordinal)));
      OnDiskGraphIndex graph = segment.graph();
      List<Map<String, Object>> forward = new ArrayList<>();
      List<Map<String, Object>> reverse = new ArrayList<>();
      try (OnDiskGraphIndex.View view = graph.getView()) {
        for (int level = 0; level <= graph.getMaxLevel(); level++) {
          if (!view.contains(level, ordinal)) {
            break;
          }
          for (int neighbor : neighbors(view, level, ordinal)) {
            forward.add(edge(segment, level, neighbor));
          }
        }
        for (int node = 0; node < segment.size(); node++) {
          if (neighbors(view, 0, node).contains(ordinal)) {
            reverse.add(edge(segment, 0, node));
          }
        }
      }
      e.put("forward", forward);
      e.put("reverse", reverse);
      entries.add(e);
    });
    report.putInspection("entries", entries);
  }

  private Map<String, Object> edge(HnswSegment segment, int level, int node) {
    Map<String, Object> e = new LinkedHashMap<>();
    e.put("level", level);
    e.put("ordinal", node);
    e.put("key", key(segment.getKey(node)));
    return e;
  }

  private void delta(Report report) throws IOException {
    List<HnswSegment.Descriptor> all = reader.listSegments();
    List<Map<String, Object>> regions = new ArrayList<>();
    for (RegionInfo region : reader.listRegions()) {
      byte[] start = region.getStartKey();
      byte[] end = region.getEndKey();
      List<HnswSegment.Descriptor> current = HnswIndexManager.currentSegments(all, start, end);
      Map<String, Object> r = new LinkedHashMap<>();
      r.put("region", region.getEncodedName());
      if (current.isEmpty()) {
        r.put("segments", 0);
      } else {
        long newest = current.get(0).time;
        long replayStart = HnswIndexManager.replayStartTime(current);
        r.put("segments", current.size());
        r.put("newestSegmentTime", newest);
        r.put("changedRows", reader.changedSince(start, end, newest).size());
        r.put("replayStart", replayStart);
        r.put("replayedRows", reader.changedSince(start, end, replayStart).size());
      }
      regions.add(r);
    }
    report.putInspection("regions", regions);
  }
}
