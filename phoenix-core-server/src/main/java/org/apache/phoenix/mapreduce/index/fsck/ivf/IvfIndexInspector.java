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
package org.apache.phoenix.mapreduce.index.fsck.ivf;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.VectorCentroidCache.CachedCentroids;
import org.apache.phoenix.execute.VectorIndexScanPlan;
import org.apache.phoenix.expression.function.CosineDistanceFunction;
import org.apache.phoenix.expression.function.InnerProductDistanceFunction;
import org.apache.phoenix.expression.function.L2DistanceFunction;
import org.apache.phoenix.index.vector.GenerationSummary;
import org.apache.phoenix.index.vector.KMeansTrainer;
import org.apache.phoenix.index.vector.ScorecardRow;
import org.apache.phoenix.index.vector.VectorIndexScorecard;
import org.apache.phoenix.index.vector.VectorIndexTrainer;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.TaskType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.task.Task.TaskRecord;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.SchemaUtil;

/**
 * The inspect commands for IVF vector indexes.
 * <p>
 * All commands are read only:
 * <ul>
 * <li>{@code generations}: the centroid generations, the rebuild claim, and the vector tasks.</li>
 * <li>{@code centroids}: the centroids of a generation, with norms and nearest centroids.</li>
 * <li>{@code scorecard}: the scorecard against the posting counts, with drift assessments.</li>
 * <li>{@code postings}: a sample of the index rows under one centroid ID.</li>
 * <li>{@code lookup}: the centroid assignments and the index rows of one data row.</li>
 * <li>{@code probe}: the probe order for a query vector, and recall against an exact scan.</li>
 * <li>{@code export}: the index header and the centroid generations.</li>
 * </ul>
 */
public class IvfIndexInspector {
  public static final String GENERATIONS = "generations";
  public static final String CENTROIDS = "centroids";
  public static final String SCORECARD = "scorecard";
  public static final String POSTINGS = "postings";
  public static final String LOOKUP = "lookup";
  public static final String PROBE = "probe";
  public static final String EXPORT = "export";

  /** The number of exact nearest neighbors that the probe command uses to measure recall. */
  static final int RECALL_K = 10;
  private static final int POSTINGS_SAMPLE = 100;
  private static final String CENTROID_COLUMN =
    "\"" + MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME + "\"";

  private final IvfIndexContext ivf;
  private final IvfIndexReader reader;

  public IvfIndexInspector(IvfIndexContext ivf) {
    this.ivf = ivf;
    this.reader = new IvfIndexReader(ivf);
  }

  public void inspect(String command, List<String> args, Report report) throws Exception {
    switch (command) {
      case GENERATIONS:
        generations(report);
        break;
      case CENTROIDS:
        centroids(args.isEmpty() ? activeGeneration() : Long.parseLong(args.get(0)), report);
        break;
      case SCORECARD:
        scorecard(report);
        break;
      case POSTINGS:
        if (args.isEmpty()) {
          throw new IllegalArgumentException("postings <centroid id>");
        }
        postings(Integer.parseInt(args.get(0)), report);
        break;
      case LOOKUP:
        lookup(args, report);
        break;
      case PROBE:
        probe(args, report);
        break;
      case EXPORT:
        export(args, report);
        break;
      default:
        throw new IllegalArgumentException(
          "Unknown inspect command " + command + "; the commands " + "are "
            + Arrays.asList(GENERATIONS, CENTROIDS, SCORECARD, POSTINGS, LOOKUP, PROBE, EXPORT));
    }
  }

  private void generations(Report report) throws SQLException {
    PTable index = ivf.getIndexTable();
    report.putInspection("activeGeneration", ivf.getActiveGeneration());
    report.putInspection("buildingGeneration", ivf.getBuildingGeneration());
    report.putInspection("lists", index.getVectorIvfLists());
    Set<Long> all = new TreeSet<>(reader.listGenerations());
    all.addAll(ivf.getLiveGenerations());
    List<Map<String, Object>> generations = new ArrayList<>();
    for (long generation : all) {
      Map<String, Object> g = new LinkedHashMap<>();
      g.put("generation", generation);
      g.put("role", role(generation));
      List<IvfIndexReader.CentroidRow> rows = reader.loadCentroidRows(generation);
      g.put("centroids", rows.size());
      if (!rows.isEmpty()) {
        g.put("firstId", rows.get(0).getId());
        g.put("lastId", rows.get(rows.size() - 1).getId());
      }
      GenerationSummary summary = reader.loadSummary(generation);
      if (summary != null) {
        g.put("state", summary.getRebuildState());
        g.put("triggerReason", summary.getTriggerReason());
        g.put("requestedLists", summary.getRequestedLists());
        g.put("lastRebuildTime", summary.getLastRebuildTime());
        g.put("lastScorecardUpdate", summary.getLastScorecardUpdate());
      }
      generations.add(g);
    }
    report.putInspection("generations", generations);
    IvfIndexReader.Claim claim = reader.loadClaim();
    if (claim != null) {
      Map<String, Object> c = new LinkedHashMap<>();
      c.put("token", claim.getToken());
      c.put("ageMs", EnvironmentEdgeManager.currentTimeMillis() - claim.getTime());
      report.putInspection("claim", c);
    }
    List<Map<String, Object>> tasks = new ArrayList<>();
    for (TaskType type : Arrays.asList(TaskType.VECTOR_SCORECARD_RECONCILE,
      TaskType.VECTOR_INDEX_REBUILD)) {
      for (TaskRecord task : reader.loadLiveTasks(type)) {
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("type", type);
        t.put("status", task.getStatus());
        t.put("timestamp", task.getTimeStamp().getTime());
        t.put("data", task.getData());
        tasks.add(t);
      }
    }
    report.putInspection("tasks", tasks);
  }

  /** Shows the norm of each centroid, and its nearest other centroid with the distance to it. */
  private void centroids(long generation, Report report) throws SQLException {
    CachedCentroids model = ivf.getModel(generation);
    List<Map<String, Object>> centroids = new ArrayList<>();
    for (int id = model.getFirstId(); id < model.getFirstId() + model.size(); id++) {
      float[] v = model.getCentroid(id);
      Map<String, Object> c = new LinkedHashMap<>();
      c.put("id", id);
      double norm = 0;
      for (float x : v) {
        norm += (double) x * x;
      }
      c.put("norm", Math.sqrt(norm));
      if (model.size() > 1) {
        int[] nearest = model.nearest(v, 2);
        int other = nearest[0] == id ? nearest[1] : nearest[0];
        c.put("nearestId", other);
        c.put("nearestDistance",
          KMeansTrainer.assignmentDistance(ivf.getMetric(), v, model.getCentroid(other)));
      }
      centroids.add(c);
    }
    report.putInspection("generation", generation);
    report.putInspection("centroids", centroids);
  }

  private void scorecard(Report report) throws SQLException {
    long active = activeGeneration();
    Map<Integer, Long> postings = reader.countPostings();
    List<ScorecardRow> stored = reader.loadScorecard(active);
    List<ScorecardRow> live = new ArrayList<>();
    List<Map<String, Object>> rows = new ArrayList<>();
    for (ScorecardRow row : stored) {
      long count = postings.getOrDefault(row.getCentroidId(), 0L);
      live.add(new ScorecardRow(row.getCentroidId(), count, row.getReassignCount()));
      Map<String, Object> r = new LinkedHashMap<>();
      r.put("centroidId", row.getCentroidId());
      r.put("clusterSize", row.getClusterSize());
      r.put("rows", count);
      r.put("reassignCount", row.getReassignCount());
      rows.add(r);
    }
    GenerationSummary summary = reader.loadSummary(active);
    Long lastUpdate = summary == null ? null : summary.getLastScorecardUpdate();
    report.putInspection("generation", active);
    report.putInspection("lastReconciliationAgeMs",
      lastUpdate == null ? null : EnvironmentEdgeManager.currentTimeMillis() - lastUpdate);
    report.putInspection("scorecard", rows);
    report.putInspection("storedAssessment", VectorIndexScorecard
      .assess(stored, ivf.getConnection().getQueryServices().getProps()).toString());
    report.putInspection("liveAssessment", VectorIndexScorecard
      .assess(live, ivf.getConnection().getQueryServices().getProps()).toString());
  }

  /**
   * Shows a sample of the index rows under one centroid ID, read directly from HBase. The report
   * also counts the rows, the unverified rows, and the regions that hold them.
   */
  private void postings(int centroidId, Report report) throws Exception {
    PTable index = ivf.getIndexTable();
    RowKeySchema schema = index.getRowKeySchema();
    int position = centroidPosition(index);
    byte[] family = SchemaUtil.getEmptyColumnFamily(index);
    byte[] qualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(index).getFirst();
    TableName tableName = TableName.valueOf(index.getPhysicalName().getBytes());
    List<Map<String, Object>> sample = new ArrayList<>();
    long rows = 0;
    long unverified = 0;
    Set<String> regions = new LinkedHashSet<>();
    try (Table table = ivf.getConnection().getQueryServices().getTable(tableName.getName());
      Admin admin = ivf.getConnection().getQueryServices().getAdmin();
      RegionLocator locator = admin.getConnection().getRegionLocator(tableName)) {
      for (Scan scan : postingsScans(index, centroidId)) {
        scan.addColumn(family, qualifier);
        try (ResultScanner scanner = table.getScanner(scan)) {
          for (Result result : scanner) {
            if (centroidId != centroidIdOf(schema, position, result.getRow())) {
              continue;
            }
            rows++;
            Cell cell = result.getColumnLatestCell(family, qualifier);
            boolean verified = cell != null
              && Bytes.equals(CellUtil.cloneValue(cell), QueryConstants.VERIFIED_BYTES);
            if (!verified) {
              unverified++;
            }
            HRegionLocation location = locator.getRegionLocation(result.getRow());
            regions.add(location.getRegion().getEncodedName());
            if (sample.size() < POSTINGS_SAMPLE) {
              Map<String, Object> r = new LinkedHashMap<>();
              r.put("key", ivf.getFsckContext().formatRowKey(result.getRow(), index));
              r.put("verified", verified);
              sample.add(r);
            }
          }
        }
      }
    }
    report.putInspection("centroidId", centroidId);
    report.putInspection("generation", ivf.generationOf(centroidId));
    report.putInspection("rows", rows);
    report.putInspection("unverifiedRows", unverified);
    report.putInspection("regions", regions.size());
    report.putInspection("sample", sample);
  }

  /**
   * Shows one data row: its vector, its assignment in each live generation, and the centroid IDs of
   * its index rows. An assignment shows the three nearest centroids and the margin between the
   * nearest two.
   */
  private void lookup(List<String> args, Report report) throws SQLException {
    List<Object> key = parseKey(args);
    float[] vector = readVector(key);
    report.putInspection("key", key);
    report.putInspection("vector", vector);
    if (vector != null) {
      List<Map<String, Object>> assignments = new ArrayList<>();
      for (long generation : ivf.getLiveGenerations()) {
        CachedCentroids model = ivf.getModel(generation);
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("generation", generation);
        a.put("role", role(generation));
        a.put("centroidId", model.assign(vector));
        List<Map<String, Object>> nearest = new ArrayList<>();
        for (int id : model.nearest(vector, 3)) {
          Map<String, Object> n = new LinkedHashMap<>();
          n.put("centroidId", id);
          n.put("distance",
            KMeansTrainer.assignmentDistance(ivf.getMetric(), vector, model.getCentroid(id)));
          nearest.add(n);
        }
        a.put("nearest", nearest);
        if (nearest.size() > 1) {
          a.put("margin",
            (double) nearest.get(1).get("distance") - (double) nearest.get(0).get("distance"));
        }
        assignments.add(a);
      }
      report.putInspection("assignments", assignments);
    }
    report.putInspection("indexRows", indexCentroids(key));
  }

  /**
   * Shows the probe order for a query vector, with the row count of each posting list. The query is
   * a vector literal or the vector of a data row. During a migration, the order interleaves the
   * active and the building generation, as a query does. The command also finds the exact nearest
   * neighbors with a scan of the data table, and shows the recall for each number of probes.
   */
  private void probe(List<String> args, Report report) throws SQLException {
    if (args.isEmpty()) {
      throw new IllegalArgumentException("probe <[v1,v2,...] | data row key values>");
    }
    float[] query = args.get(0).startsWith("[")
      ? parseVector(String.join(",", args))
      : readVector(parseKey(args));
    if (query == null) {
      throw new IllegalArgumentException("The data row has no vector");
    }
    int[] order = ivf.getModel(activeGeneration()).nearest(query, Integer.MAX_VALUE);
    if (ivf.getBuildingGeneration() != null) {
      order = VectorIndexScanPlan.interleave(order,
        ivf.getModel(ivf.getBuildingGeneration()).nearest(query, Integer.MAX_VALUE));
    }
    Map<Integer, Long> postings = reader.countPostings();
    List<Map<String, Object>> probes = new ArrayList<>();
    for (int id : order) {
      Map<String, Object> p = new LinkedHashMap<>();
      p.put("centroidId", id);
      p.put("generation", ivf.generationOf(id));
      p.put("rows", postings.getOrDefault(id, 0L));
      probes.add(p);
    }
    report.putInspection("query", query);
    report.putInspection("probeOrder", probes);

    // For each exact nearest neighbor, find the first probe that reaches one of its index rows
    List<Integer> ranks = new ArrayList<>();
    List<Map<String, Object>> nearest = new ArrayList<>();
    for (List<Object> key : exactNearest(query)) {
      List<Integer> present = indexCentroids(key);
      int rank = -1;
      for (int i = 0; i < order.length && rank < 0; i++) {
        if (present.contains(order[i])) {
          rank = i;
        }
      }
      Map<String, Object> n = new LinkedHashMap<>();
      n.put("key", key);
      n.put("probeRank", rank);
      nearest.add(n);
      ranks.add(rank);
    }
    report.putInspection("exactNearest", nearest);
    List<Double> recall = new ArrayList<>();
    for (int probesUsed = 1; probesUsed <= order.length && !ranks.isEmpty(); probesUsed++) {
      int found = 0;
      for (int rank : ranks) {
        found += rank >= 0 && rank < probesUsed ? 1 : 0;
      }
      recall.add((double) found / ranks.size());
    }
    report.putInspection("recallByProbeCount", recall);
    report.putInspection("probesForFullRecall",
      ranks.contains(-1) || ranks.isEmpty()
        ? null
        : ranks.stream().max(Integer::compare).get() + 1);
  }

  private void export(List<String> args, Report report) throws SQLException {
    PTable index = ivf.getIndexTable();
    report.putInspection("algorithm", index.getVectorIndexAlgorithm());
    report.putInspection("metric", index.getVectorDistanceMetric());
    report.putInspection("dimension", index.getVectorDimension());
    report.putInspection("lists", index.getVectorIvfLists());
    report.putInspection("activeGeneration", ivf.getActiveGeneration());
    report.putInspection("buildingGeneration", ivf.getBuildingGeneration());
    List<Long> generations = new ArrayList<>();
    if (args.isEmpty()) {
      generations.addAll(new TreeSet<>(reader.listGenerations()));
    } else {
      generations.add(Long.parseLong(args.get(0)));
    }
    List<Map<String, Object>> exported = new ArrayList<>();
    for (long generation : generations) {
      exported.add(reader.exportGeneration(generation));
    }
    report.putInspection("generations", exported);
  }

  private long activeGeneration() {
    Long active = ivf.getActiveGeneration();
    if (active == null) {
      throw new IllegalArgumentException("The index is untrained");
    }
    return active;
  }

  private String role(long generation) {
    return Long.valueOf(generation).equals(ivf.getActiveGeneration()) ? "ACTIVE"
      : Long.valueOf(generation).equals(ivf.getBuildingGeneration()) ? "BUILDING"
      : "INACTIVE";
  }

  /** Converts the key arguments into typed values, one for each data key column in sequence. */
  private List<Object> parseKey(List<String> args) {
    List<PColumn> columns = ivf.getDataKeyColumns();
    if (args.size() != columns.size()) {
      List<String> names = new ArrayList<>();
      for (PColumn column : columns) {
        names.add(column.getName().getString());
      }
      throw new IllegalArgumentException("A data row is named by values for " + names);
    }
    List<Object> key = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      key.add(columns.get(i).getDataType().toObject(args.get(i)));
    }
    return key;
  }

  private float[] readVector(List<Object> key) throws SQLException {
    try (PreparedStatement ps = ivf.getConnection()
      .prepareStatement("SELECT " + ivf.getVectorExpression() + " FROM "
        + SchemaUtil.getEscapedFullTableName(ivf.getDataTable().getName().getString()) + " WHERE "
        + ivf.getDataKeyPredicate())) {
      bind(ps, key, 1);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new IllegalArgumentException("No data row has key " + key);
        }
        return VectorIndexTrainer.toFloats(rs.getObject(1));
      }
    }
  }

  /** Returns the live centroid IDs under which the index has a row for the data row key. */
  private List<Integer> indexCentroids(List<Object> key) throws SQLException {
    List<Integer> ids = new ArrayList<>();
    for (long generation : ivf.getLiveGenerations()) {
      CachedCentroids model = ivf.getModel(generation);
      for (int id = model.getFirstId(); id < model.getFirstId() + model.size(); id++) {
        ids.add(id);
      }
    }
    if (ids.isEmpty()) {
      return ids;
    }
    StringBuilder sql = new StringBuilder("SELECT ").append(CENTROID_COLUMN).append(" FROM ")
      .append(SchemaUtil.getEscapedFullTableName(ivf.getIndexTable().getName().getString()))
      .append(" WHERE ").append(CENTROID_COLUMN).append(" IN (");
    for (int i = 0; i < ids.size(); i++) {
      sql.append(i == 0 ? "?" : ",?");
    }
    sql.append(")");
    for (PColumn column : ivf.getDataKeyColumns()) {
      sql.append(" AND ")
        .append(SchemaUtil.getEscapedFullColumnName(IndexUtil.getIndexColumnName(column)))
        .append(" = ?");
    }
    List<Integer> present = new ArrayList<>();
    try (PreparedStatement ps = ivf.getConnection().prepareStatement(sql.toString())) {
      int i = 1;
      for (int id : ids) {
        ps.setInt(i++, id);
      }
      bind(ps, key, i);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          present.add(rs.getInt(1));
        }
      }
    }
    return present;
  }

  /** Returns the keys of the RECALL_K exact nearest neighbors, from a scan of the data table. */
  private List<List<Object>> exactNearest(float[] query) throws SQLException {
    List<PColumn> columns = ivf.getDataKeyColumns();
    StringBuilder select = new StringBuilder();
    for (PColumn column : columns) {
      select.append(select.length() == 0 ? "" : ", ")
        .append(SchemaUtil.getEscapedFullColumnName(column.getName().getString()));
    }
    String expr = ivf.getVectorExpression();
    String function;
    switch (ivf.getMetric()) {
      case COSINE:
        function = CosineDistanceFunction.NAME;
        break;
      case INNER_PRODUCT:
        function = InnerProductDistanceFunction.NAME;
        break;
      default:
        function = L2DistanceFunction.NAME;
    }
    Float[] boxed = new Float[query.length];
    for (int i = 0; i < query.length; i++) {
      boxed[i] = query[i];
    }
    List<List<Object>> keys = new ArrayList<>();
    try (PreparedStatement ps = ivf.getConnection()
      .prepareStatement("SELECT /*+ NO_INDEX */ " + select + " FROM "
        + SchemaUtil.getEscapedFullTableName(ivf.getDataTable().getName().getString()) + " WHERE "
        + expr + " IS NOT NULL ORDER BY " + function + "(" + expr + ", ?) LIMIT " + RECALL_K)) {
      ps.setArray(1, ivf.getConnection().createArrayOf("FLOAT", boxed));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          List<Object> key = new ArrayList<>();
          for (int i = 1; i <= columns.size(); i++) {
            key.add(rs.getObject(i));
          }
          keys.add(key);
        }
      }
    }
    return keys;
  }

  private static void bind(PreparedStatement ps, List<Object> values, int first)
    throws SQLException {
    for (int i = 0; i < values.size(); i++) {
      ps.setObject(first + i, values.get(i));
    }
  }

  static float[] parseVector(String text) {
    String body = text.trim();
    if (!body.startsWith("[") || !body.endsWith("]")) {
      throw new IllegalArgumentException("A vector is written [v1,v2,...]: " + text);
    }
    String[] parts = body.substring(1, body.length() - 1).split(",");
    float[] vector = new float[parts.length];
    for (int i = 0; i < parts.length; i++) {
      vector[i] = Float.parseFloat(parts[i].trim());
    }
    return vector;
  }

  /** Returns the zero-based position of the centroid ID column in the index row key. */
  private static int centroidPosition(PTable index) {
    List<PColumn> pk = index.getPKColumns();
    for (int i = 0; i < pk.size(); i++) {
      if (MetaDataUtil.VECTOR_CENTROID_ID_COLUMN_NAME.equals(pk.get(i).getName().getString())) {
        return i;
      }
    }
    throw new IllegalStateException(index.getName() + " has no centroid id column");
  }

  private static int centroidIdOf(RowKeySchema schema, int position, byte[] row) {
    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    int maxOffset = schema.iterator(row, ptr);
    for (int i = 0; i <= position; i++) {
      schema.next(ptr, i, maxOffset);
    }
    return (Integer) PInteger.INSTANCE.toObject(ptr, SortOrder.ASC);
  }

  /**
   * Returns the scans that cover the index rows of one centroid ID: one scan for each salt bucket,
   * or one scan for an unsalted index. If the index is not multi-tenant, a scan uses the centroid
   * ID, after the salt byte if there is one, as its row key prefix. A multi-tenant index has the
   * tenant ID before the centroid ID. Thus each scan of a multi-tenant index covers a full bucket,
   * or the full table if the index is not salted. The caller must filter the rows by centroid ID.
   */
  private List<Scan> postingsScans(PTable index, int centroidId) {
    List<byte[]> prefixes = new ArrayList<>();
    int buckets = index.getBucketNum() == null ? 0 : index.getBucketNum();
    if (buckets == 0) {
      prefixes.add(new byte[0]);
    } else {
      for (int b = 0; b < buckets; b++) {
        prefixes.add(new byte[] { (byte) b });
      }
    }
    boolean bounded = !index.isMultiTenant();
    List<Scan> scans = new ArrayList<>();
    for (byte[] prefix : prefixes) {
      Scan scan = new Scan();
      scan.setStartStopRowForPrefixScan(
        bounded ? Bytes.add(prefix, PInteger.INSTANCE.toBytes(centroidId)) : prefix);
      scans.add(scan);
    }
    return scans;
  }
}
