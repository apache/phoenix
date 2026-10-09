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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.phoenix.index.vector.CentroidManager;
import org.apache.phoenix.index.vector.GenerationSummary;
import org.apache.phoenix.index.vector.ScorecardRow;
import org.apache.phoenix.index.vector.VectorIndexScorecard;
import org.apache.phoenix.mapreduce.index.fsck.Finding;
import org.apache.phoenix.mapreduce.index.fsck.GlobalIndexFsckProvider;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckContext;
import org.apache.phoenix.mapreduce.index.fsck.IndexFsckTool;
import org.apache.phoenix.mapreduce.index.fsck.RepairAction;
import org.apache.phoenix.mapreduce.index.fsck.RepairPlan;
import org.apache.phoenix.mapreduce.index.fsck.Report;
import org.apache.phoenix.mapreduce.index.fsck.Retry;
import org.apache.phoenix.mapreduce.index.fsck.Severity;
import org.apache.phoenix.mapreduce.index.fsck.VerifyFindings;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.TaskType;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;

/**
 * The FSCK provider for Inverted File (IVF) vector indexes.
 * <p>
 * The provider adds IVF checks to the row verification of {@link GlobalIndexFsckProvider}. The IVF
 * checks have three scopes:
 * <ul>
 * <li>{@code CATALOG}: the index header, the training state, the rebuild claim, the migration
 * state, the reconcile task, and the vector state of dropped indexes.</li>
 * <li>{@code CENTROIDS}: the centroid model and the summary of each live generation in
 * {@code SYSTEM.VECTOR_CENTROID}, and the generations that are not live.</li>
 * <li>{@code POSTINGS}: the index rows under retired centroid IDs, and the scorecard against the
 * posting counts, its age, and drift.</li>
 * </ul>
 * A repair holds the rebuild claim of the index, so that no rebuild or reconciliation runs at the
 * same time.
 */
public class IvfIndexFsckProvider extends GlobalIndexFsckProvider {
  public static final String SCOPE_CATALOG = "CATALOG";
  public static final String SCOPE_CENTROIDS = "CENTROIDS";
  public static final String SCOPE_POSTINGS = "POSTINGS";

  public static final String HEADER_INVALID = "HEADER_INVALID";
  public static final String ACTIVE_UNTRAINED = "ACTIVE_UNTRAINED";
  public static final String UNTRAINED = "UNTRAINED";
  public static final String MIGRATION_IN_PROGRESS = "MIGRATION_IN_PROGRESS";
  public static final String MIGRATION_STALLED = "MIGRATION_STALLED";
  public static final String CLAIM_HELD = "CLAIM_HELD";
  public static final String CLAIM_EXPIRED = "CLAIM_EXPIRED";
  public static final String RECONCILE_TASK_MISSING = "RECONCILE_TASK_MISSING";
  public static final String ORPHAN_INDEX_STATE = "ORPHAN_INDEX_STATE";
  public static final String MODEL_MISSING = "MODEL_MISSING";
  public static final String MODEL_INVALID = "MODEL_INVALID";
  public static final String SUMMARY_INVALID = "SUMMARY_INVALID";
  public static final String GENERATION_INACTIVE = "GENERATION_INACTIVE";
  public static final String GENERATION_ID_OVERLAP = "GENERATION_ID_OVERLAP";
  public static final String CENTROID_DUPLICATE = "CENTROID_DUPLICATE";
  public static final String POSTINGS_RETIRED_IDS = "POSTINGS_RETIRED_IDS";
  public static final String SCORECARD_DIVERGED = "SCORECARD_DIVERGED";
  public static final String SCORECARD_STALE = "SCORECARD_STALE";
  public static final String DRIFT = "DRIFT";
  public static final String REPAIR_REFUSED = "REPAIR_REFUSED";

  public static final String PURGE_GENERATION = "PURGE_GENERATION";
  public static final String DELETE_ORPHAN_STATE = "DELETE_ORPHAN_STATE";
  public static final String ENQUEUE_RECONCILE_TASK = "ENQUEUE_RECONCILE_TASK";
  public static final String RECONCILE_SCORECARD = "RECONCILE_SCORECARD";

  @Override
  public Report fsck(IndexFsckContext context) throws Exception {
    Report report = super.fsck(context);
    try (IvfIndexContext ivf = new IvfIndexContext(context)) {
      report.addFindings(checkIvf(ivf));
    }
    return report;
  }

  @Override
  public Report inspect(IndexFsckContext context, String command, List<String> args)
    throws Exception {
    Report report = new Report(IndexFsckTool.CMD_INSPECT + " " + command, context);
    try (IvfIndexContext ivf = new IvfIndexContext(context)) {
      new IvfIndexInspector(ivf).inspect(command, args, report);
    }
    return report;
  }

  /**
   * Returns true if the rows of the IVF index can be verified. In addition to the checks of the
   * superclass, rows are not verified during a centroid migration or for an untrained index. Each
   * of these cases adds a finding to the report.
   */
  @Override
  protected boolean canVerifyRows(IndexFsckContext context, Report report) {
    if (!super.canVerifyRows(context, report)) {
      return false;
    }
    PTable index = context.getIndexTable();
    if (index.isVectorRebuildInProgress()) {
      report.addFinding(new Finding(Severity.ERROR, VerifyFindings.SCOPE, ROWS_NOT_VERIFIED,
        "Rows are not verified while the index migrates to generation "
          + index.getVectorBuildingGeneration() + "; run fsck to see whether the migration is "
          + "stalled"));
      return false;
    }
    if (index.getVectorCentroidGeneration() == null) {
      report.addFinding(new Finding(Severity.INFO, VerifyFindings.SCOPE, ROWS_NOT_VERIFIED,
        "The index is untrained and has no rows to verify"));
      return false;
    }
    return true;
  }

  @Override
  protected List<Finding> planRepair(IndexFsckContext context, RepairPlan plan) throws Exception {
    List<Finding> findings = super.planRepair(context, plan);
    try (IvfIndexContext ivf = new IvfIndexContext(context)) {
      List<Finding> ivfFindings = checkIvf(ivf);
      findings.addAll(ivfFindings);
      for (Finding finding : ivfFindings) {
        switch (finding.getRule()) {
          case CLAIM_HELD:
            plan.add("WAIT_FOR_CLAIM", "Wait for the rebuild claim held by "
              + finding.getDetails().get("token") + " to be released");
            break;
          case GENERATION_INACTIVE:
            if ((Long) finding.getDetails().get("generation") < ivf.getActiveGeneration()) {
              plan.add(PURGE_GENERATION,
                "Delete inactive generation " + finding.getDetails().get("generation")
                  + " once no index rows remain under its ids");
            }
            break;
          case ORPHAN_INDEX_STATE:
            plan.add(DELETE_ORPHAN_STATE,
              "Delete the vector state of dropped index " + finding.getDetails().get("index"));
            break;
          case RECONCILE_TASK_MISSING:
            plan.add(ENQUEUE_RECONCILE_TASK, "Enqueue the VECTOR_SCORECARD_RECONCILE task");
            break;
          case SCORECARD_DIVERGED:
            plan.add(RECONCILE_SCORECARD, "Reconcile the scorecard with the index rows");
            break;
          default:
        }
      }
    }
    return findings;
  }

  /**
   * Repairs the index while this tool holds the rebuild claim of the index, so that no rebuild or
   * reconciliation runs at the same time. A dry run does not take the claim. If another holder
   * keeps the claim through all retries, the repair changes nothing and reports REPAIR_REFUSED.
   */
  @Override
  public Report repair(IndexFsckContext context) throws Exception {
    if (!context.isConfirm()) {
      return super.repair(context);
    }
    String indexName = context.getIndexTableName();
    String token = "indexfsck-" + UUID.randomUUID();
    try (IvfIndexContext ivf = new IvfIndexContext(context)) {
      CentroidManager.RebuildClaim claim;
      try {
        claim = Retry.call("Claim " + indexName, () -> {
          CentroidManager.RebuildClaim held =
            CentroidManager.claim(ivf.getInternalConnection(), indexName, token);
          if (held == null) {
            throw new IllegalStateException("The rebuild claim of " + indexName + " is held");
          }
          return held;
        });
      } catch (IllegalStateException e) {
        Report report = new Report(IndexFsckTool.CMD_REPAIR, context);
        report.setRepairPlan(new RepairPlan(false));
        report.addFinding(new Finding(Severity.ERROR, SCOPE_CATALOG, REPAIR_REFUSED,
          "A rebuild or reconciliation holds the index's claim; repair again when it finishes"));
        return report;
      }
      boolean interrupted = false;
      try (CentroidManager.RebuildClaim held = claim) {
        try {
          return super.repair(context);
        } finally {
          // An interrupted repair must also release the claim, and the release needs a clear
          // interrupt status. The outer finally sets the status again.
          interrupted = Thread.interrupted();
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Runs one repair round. After the row repair of the superclass, the round deletes inactive
   * generations and the state of dropped indexes. It also enqueues a missing reconcile task and
   * reconciles a diverged scorecard. Returns the findings that remain after the round.
   */
  @Override
  protected List<Finding> repairRound(IndexFsckContext context, RepairPlan plan, int round)
    throws Exception {
    List<Finding> remaining = super.repairRound(context, plan, round);
    try (IvfIndexContext ivf = new IvfIndexContext(context)) {
      IvfIndexReader reader = new IvfIndexReader(ivf);
      // The actions below do not change index rows, so the round uses one count of the postings
      Map<Integer, Long> postings = countPostings(ivf, reader);
      purgeInactiveGenerations(ivf, reader, postings, plan, round);
      deleteOrphanState(reader, plan, round);
      if (reader.loadLiveTasks(TaskType.VECTOR_SCORECARD_RECONCILE).isEmpty()) {
        RepairAction enqueue =
          plan.add(ENQUEUE_RECONCILE_TASK, "Enqueue the VECTOR_SCORECARD_RECONCILE task");
        enqueue.putDetail("round", round);
        Retry.run("Enqueue reconcile task",
          () -> CentroidManager.enqueueTask(ivf.getInternalConnection(), ivf.getIndexTable(),
            TaskType.VECTOR_SCORECARD_RECONCILE, null));
        enqueue.markExecuted();
      }
      reconcileScorecard(ivf, reader, postings, plan, round);
      remaining.addAll(checkIvf(ivf, reader, postings));
    }
    return remaining;
  }

  /**
   * Deletes each inactive generation that is older than the active generation and has no index rows
   * under its IDs. This method deletes nothing from an untrained index, because its first
   * generation can be in training. It keeps a generation that is newer than the active generation.
   * Generations only increase, and a rebuild writes its generation before the catalog records it.
   */
  private void purgeInactiveGenerations(IvfIndexContext ivf, IvfIndexReader reader,
    Map<Integer, Long> postings, RepairPlan plan, int round) throws Exception {
    Long active = ivf.getActiveGeneration();
    if (active == null) {
      return;
    }
    List<Long> live = ivf.getLiveGenerations();
    for (long generation : reader.listGenerations()) {
      if (live.contains(generation)) {
        continue;
      }
      RepairAction purge = plan.add(PURGE_GENERATION, "Delete inactive generation " + generation);
      purge.putDetail("round", round);
      if (generation > active) {
        purge.markSkipped("it is newer than the active generation " + active);
        continue;
      }
      // An ID that a live generation also uses holds rows of that generation, so do not count it
      long rows = 0;
      for (IvfIndexReader.CentroidRow row : reader.loadCentroidRows(generation)) {
        if (ivf.generationOf(row.getId()) == null) {
          rows += postings.getOrDefault(row.getId(), 0L);
        }
      }
      if (rows > 0) {
        purge.markSkipped(rows + " index rows remain under its ids");
        continue;
      }
      purge.putDetail("removed", reader.exportGeneration(generation));
      purge.logExecuting();
      Retry.run("Delete generation " + generation, () -> CentroidManager
        .deleteGeneration(ivf.getInternalConnection(), ivf.getIndexName(), generation));
      purge.markExecuted();
    }
  }

  private void deleteOrphanState(IvfIndexReader reader, RepairPlan plan, int round)
    throws Exception {
    for (Map.Entry<String, String> orphan : reader.findOrphans().entrySet()) {
      String name = orphan.getKey();
      RepairAction delete =
        plan.add(DELETE_ORPHAN_STATE, "Delete the vector state of dropped index " + name);
      delete.putDetail("round", round);
      delete.putDetail("state", orphan.getValue());
      // Check again before the delete, because a user can create the index again with the same name
      if (reader.vectorIndexExists(name)) {
        delete.markSkipped(name + " exists");
        continue;
      }
      delete.logExecuting();
      Retry.run("Delete vector state of " + name, () -> {
        CentroidManager.deleteAllCentroids(reader.getConnection(), name);
        reader.deleteOrphanTasks(name);
      });
      delete.markExecuted();
    }
  }

  /**
   * Reconciles the scorecard of the active generation if its cluster sizes differ from the posting
   * counts. This method does nothing for an untrained index or during a centroid migration.
   */
  private void reconcileScorecard(IvfIndexContext ivf, IvfIndexReader reader,
    Map<Integer, Long> postings, RepairPlan plan, int round) throws Exception {
    Long active = ivf.getActiveGeneration();
    if (active == null || ivf.getBuildingGeneration() != null) {
      return;
    }
    boolean diverged = false;
    for (ScorecardRow row : reader.loadScorecard(active)) {
      diverged |= row.getClusterSize() != postings.getOrDefault(row.getCentroidId(), 0L);
    }
    if (!diverged) {
      return;
    }
    RepairAction reconcile =
      plan.add(RECONCILE_SCORECARD, "Reconcile the scorecard with the index rows");
    reconcile.putDetail("round", round);
    reconcile.logExecuting();
    Retry.run("Reconcile scorecard", () -> VectorIndexScorecard
      .reconcile(ivf.getInternalConnection(), ivf.getIndexTable(), active));
    reconcile.markExecuted();
  }

  /**
   * Runs the IVF consistency checks. The CATALOG checks always run. The CENTROIDS and POSTINGS
   * checks run only for a trained index.
   */
  protected List<Finding> checkIvf(IvfIndexContext ivf) throws Exception {
    IvfIndexReader reader = new IvfIndexReader(ivf);
    return checkIvf(ivf, reader, countPostings(ivf, reader));
  }

  private List<Finding> checkIvf(IvfIndexContext ivf, IvfIndexReader reader,
    Map<Integer, Long> postings) throws Exception {
    List<Finding> findings = new ArrayList<>();
    checkCatalog(ivf, reader, findings);
    if (ivf.getActiveGeneration() != null) {
      Map<Long, int[]> idRanges = checkCentroids(ivf, reader, findings);
      checkPostings(ivf, postings, reader, idRanges, findings);
    }
    return findings;
  }

  /**
   * Counts the index rows under each centroid ID with one grouped scan. Returns null for an
   * untrained index, which has no rows.
   */
  private static Map<Integer, Long> countPostings(IvfIndexContext ivf, IvfIndexReader reader)
    throws SQLException {
    return ivf.getActiveGeneration() == null ? null : reader.countPostings();
  }

  private void checkCatalog(IvfIndexContext ivf, IvfIndexReader reader, List<Finding> findings)
    throws Exception {
    PTable index = ivf.getIndexTable();
    Integer dimension = index.getVectorDimension();
    Integer declared = org.apache.phoenix.index.vector.VectorIndexTrainer
      .getIndexedVectorColumn(index).getMaxLength();
    List<String> problems = new ArrayList<>();
    if (!"IVF".equals(index.getVectorIndexAlgorithm())) {
      problems.add("algorithm " + index.getVectorIndexAlgorithm());
    }
    if (ivf.getMetric() == null) {
      problems.add("metric " + index.getVectorDistanceMetric());
    }
    if (dimension == null || dimension <= 0 || (declared != null && !declared.equals(dimension))) {
      problems.add("dimension " + dimension + " for a column of dimension " + declared);
    }
    if (index.getVectorIvfLists() == null || index.getVectorIvfLists() <= 0) {
      problems.add("list count " + index.getVectorIvfLists());
    }
    if (!problems.isEmpty()) {
      findings.add(new Finding(Severity.ERROR, SCOPE_CATALOG, HEADER_INVALID,
        "The catalog header records an invalid " + String.join(", ", problems)));
    }

    if (ivf.getActiveGeneration() == null) {
      if (index.getIndexState() == PIndexState.ACTIVE) {
        findings.add(new Finding(Severity.ERROR, SCOPE_CATALOG, ACTIVE_UNTRAINED,
          "The index is ACTIVE but has no centroid generation; rebuild it with IndexTool"));
      } else {
        long vectors = countVectors(ivf);
        boolean trainable =
          index.getVectorIvfLists() != null && vectors >= index.getVectorIvfLists();
        findings
          .add(new Finding(trainable ? Severity.WARN : Severity.INFO, SCOPE_CATALOG, UNTRAINED,
            trainable
              ? "The index is untrained though the table holds " + vectors
                + " vectors; build it with IndexTool"
              : "The index is untrained until the table holds " + index.getVectorIvfLists()
                + " vectors"));
      }
    }

    IvfIndexReader.Claim claim = reader.loadClaim();
    long now = EnvironmentEdgeManager.currentTimeMillis();
    boolean liveClaim = claim != null && now - claim.getTime() < CentroidManager.CLAIM_LEASE_MS;
    if (claim != null) {
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("token", claim.getToken());
      details.put("ageMs", now - claim.getTime());
      findings.add(liveClaim
        ? new Finding(Severity.INFO, SCOPE_CATALOG, CLAIM_HELD,
          "The index's rebuild claim is held by " + claim.getToken(), details)
        : new Finding(Severity.WARN, SCOPE_CATALOG, CLAIM_EXPIRED,
          "The index's claim has expired; the next rebuild, reconciliation, or repair takes it over",
          details));
    }

    if (ivf.getBuildingGeneration() != null) {
      boolean rebuildQueued = !reader.loadLiveTasks(TaskType.VECTOR_INDEX_REBUILD).isEmpty();
      findings.add(liveClaim || rebuildQueued
        ? new Finding(Severity.INFO, SCOPE_CATALOG, MIGRATION_IN_PROGRESS,
          "The index is migrating to generation " + ivf.getBuildingGeneration())
        : new Finding(Severity.WARN, SCOPE_CATALOG, MIGRATION_STALLED,
          "The migration to generation " + ivf.getBuildingGeneration()
            + " is stalled; the next reconciliation enqueues a rebuild to resume it"));
    }

    if (reader.loadLiveTasks(TaskType.VECTOR_SCORECARD_RECONCILE).isEmpty()) {
      findings.add(new Finding(Severity.ERROR, SCOPE_CATALOG, RECONCILE_TASK_MISSING,
        "The index has no VECTOR_SCORECARD_RECONCILE task"));
    }

    for (Map.Entry<String, String> orphan : reader.findOrphans().entrySet()) {
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("index", orphan.getKey());
      details.put("state", orphan.getValue());
      findings.add(new Finding(Severity.WARN, SCOPE_CATALOG, ORPHAN_INDEX_STATE,
        "Vector " + orphan.getValue() + " remain for dropped index " + orphan.getKey(), details));
    }
  }

  /**
   * Checks the centroid model and the summary of each live generation. Returns the first and last
   * centroid ID of each live generation that has centroids.
   */
  private Map<Long, int[]> checkCentroids(IvfIndexContext ivf, IvfIndexReader reader,
    List<Finding> findings) throws SQLException {
    PTable index = ivf.getIndexTable();
    Map<Long, int[]> idRanges = new LinkedHashMap<>();
    for (long generation : ivf.getLiveGenerations()) {
      boolean active = generation == ivf.getActiveGeneration();
      List<IvfIndexReader.CentroidRow> rows = reader.loadCentroidRows(generation);
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("generation", generation);
      if (rows.isEmpty()) {
        findings.add(
          new Finding(Severity.ERROR, SCOPE_CENTROIDS, MODEL_MISSING, "Generation " + generation
            + " has no centroids; rebuild the index with ALTER INDEX ... REBUILD", details));
        continue;
      }
      int firstId = rows.get(0).getId();
      idRanges.put(generation, new int[] { firstId, rows.get(rows.size() - 1).getId() });
      List<String> problems = new ArrayList<>();
      if (active && rows.size() != index.getVectorIvfLists()) {
        problems
          .add(rows.size() + " centroids where the catalog records " + index.getVectorIvfLists());
      }
      for (int i = 0; i < rows.size(); i++) {
        IvfIndexReader.CentroidRow row = rows.get(i);
        float[] v = row.getVector();
        if (row.getId() != firstId + i) {
          problems.add("centroid ids not consecutive at " + row.getId());
        } else if (v == null) {
          problems.add("centroid " + row.getId() + " has no vector");
        } else if (v.length != index.getVectorDimension()) {
          problems.add("centroid " + row.getId() + " has dimension " + v.length);
        } else {
          for (float x : v) {
            if (!Float.isFinite(x)) {
              problems.add("centroid " + row.getId() + " is not finite");
              break;
            }
          }
        }
      }
      if (!problems.isEmpty()) {
        details.put("problems", problems);
        findings
          .add(new Finding(Severity.ERROR, SCOPE_CENTROIDS, MODEL_INVALID,
            "Generation " + generation + " has an invalid model, against which index rows are "
              + "misplaced or not written; rebuild the index with ALTER INDEX ... REBUILD",
            details));
      } else {
        checkDuplicates(generation, rows, findings);
      }
      GenerationSummary summary = reader.loadSummary(generation);
      String expected = active ? GenerationSummary.ACTIVE : GenerationSummary.BUILDING;
      if (summary == null || !expected.equals(summary.getRebuildState())) {
        findings.add(new Finding(Severity.WARN, SCOPE_CENTROIDS, SUMMARY_INVALID,
          "Generation " + generation
            + (summary == null
              ? " has no summary"
              : " has summary state " + summary.getRebuildState() + " where " + expected
                + " is expected"),
          details));
      }
    }
    if (idRanges.size() == 2) {
      int[] a = idRanges.get(ivf.getActiveGeneration());
      int[] b = idRanges.get(ivf.getBuildingGeneration());
      if (Math.max(a[0], b[0]) <= Math.min(a[1], b[1])) {
        findings.add(new Finding(Severity.ERROR, SCOPE_CENTROIDS, GENERATION_ID_OVERLAP,
          "The centroid ids of the active generation, " + a[0] + " to " + a[1]
            + ", overlap those of the building generation, " + b[0] + " to " + b[1]));
      }
    }
    for (long generation : reader.listGenerations()) {
      if (!ivf.getLiveGenerations().contains(generation)) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("generation", generation);
        findings.add(new Finding(Severity.WARN, SCOPE_CENTROIDS, GENERATION_INACTIVE,
          "Generation " + generation + " is neither active nor building", details));
      }
    }
    return idRanges;
  }

  /**
   * Finds the pairs of centroids in a generation whose vectors are equal or differ only by float
   * rounding. A hash cannot find matches within a tolerance, so this check compares every pair. The
   * cost is O(k^2 d) for k centroids of dimension d.
   */
  private static void checkDuplicates(long generation, List<IvfIndexReader.CentroidRow> rows,
    List<Finding> findings) {
    List<String> pairs = new ArrayList<>();
    for (int i = 0; i < rows.size(); i++) {
      float[] a = rows.get(i).getVector();
      double norm = 0;
      for (float x : a) {
        norm += (double) x * x;
      }
      for (int j = i + 1; j < rows.size(); j++) {
        float[] b = rows.get(j).getVector();
        double distance = 0;
        for (int d = 0; d < a.length; d++) {
          double diff = a[d] - b[d];
          distance += diff * diff;
        }
        if (distance <= 1e-12 * Math.max(norm, 1.0)) {
          pairs.add(rows.get(i).getId() + "/" + rows.get(j).getId());
        }
      }
    }
    if (!pairs.isEmpty()) {
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("generation", generation);
      details.put("pairs", pairs);
      findings.add(new Finding(Severity.INFO, SCOPE_CENTROIDS, CENTROID_DUPLICATE,
        "Generation " + generation + " has " + pairs.size() + " pairs of duplicate centroids",
        details));
    }
  }

  private void checkPostings(IvfIndexContext ivf, Map<Integer, Long> postings,
    IvfIndexReader reader, Map<Long, int[]> idRanges, List<Finding> findings) throws SQLException {
    Map<Integer, Long> retired = new HashMap<>();
    for (Map.Entry<Integer, Long> posting : postings.entrySet()) {
      boolean live = false;
      for (int[] range : idRanges.values()) {
        live |= posting.getKey() >= range[0] && posting.getKey() <= range[1];
      }
      if (!live) {
        retired.put(posting.getKey(), posting.getValue());
      }
    }
    if (!retired.isEmpty()) {
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("rowsByCentroidId", retired);
      findings.add(new Finding(Severity.ERROR, SCOPE_POSTINGS, POSTINGS_RETIRED_IDS,
        retired.values().stream().mapToLong(Long::longValue).sum()
          + " index rows sit under centroid ids of no live generation; repair deletes them",
        details));
    }

    Long active = ivf.getActiveGeneration();
    if (!idRanges.containsKey(active)) {
      return;
    }
    List<ScorecardRow> scorecard = reader.loadScorecard(active);
    Map<Integer, Long> diverged = new LinkedHashMap<>();
    for (ScorecardRow row : scorecard) {
      long count = postings.getOrDefault(row.getCentroidId(), 0L);
      if (row.getClusterSize() != count) {
        diverged.put(row.getCentroidId(), row.getClusterSize() - count);
      }
    }
    ReadOnlyProps props = ivf.getConnection().getQueryServices().getProps();
    GenerationSummary summary = reader.loadSummary(active);
    Long lastUpdate = summary == null ? null : summary.getLastScorecardUpdate();
    long age = lastUpdate == null ? -1 : EnvironmentEdgeManager.currentTimeMillis() - lastUpdate;
    if (!diverged.isEmpty()) {
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("excessByCentroidId", diverged);
      details.put("lastReconciliationAgeMs", age);
      findings.add(new Finding(Severity.INFO, SCOPE_POSTINGS, SCORECARD_DIVERGED,
        "The scorecard differs from the index rows for " + diverged.size()
          + " centroids, as it may between reconciliations",
        details));
    }
    if (
      lastUpdate == null
        || age >= props.getLong(QueryServices.VECTOR_SCORECARD_RECONCILE_INTERVAL_MS_ATTRIB,
          QueryServicesOptions.DEFAULT_VECTOR_SCORECARD_RECONCILE_INTERVAL_MS)
    ) {
      findings.add(new Finding(Severity.INFO, SCOPE_POSTINGS, SCORECARD_STALE,
        lastUpdate == null
          ? "The scorecard has never been reconciled"
          : "The scorecard was last reconciled " + age + " ms ago"));
    }
    List<ScorecardRow> live = new ArrayList<>(scorecard.size());
    for (ScorecardRow row : scorecard) {
      live.add(new ScorecardRow(row.getCentroidId(), postings.getOrDefault(row.getCentroidId(), 0L),
        row.getReassignCount()));
    }
    VectorIndexScorecard.Assessment assessment = VectorIndexScorecard.assess(live, props);
    if (assessment.isDrifted()) {
      findings.add(new Finding(Severity.WARN, SCOPE_POSTINGS, DRIFT, "The index has drifted ("
        + assessment.getReason() + "); rebuild it with ALTER INDEX ... REBUILD"));
    }
  }

  private static long countVectors(IvfIndexContext ivf) throws SQLException {
    try (Statement stmt = ivf.getConnection().createStatement();
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM "
        + SchemaUtil.getEscapedFullTableName(ivf.getDataTable().getName().getString()) + " WHERE "
        + ivf.getVectorExpression() + " IS NOT NULL")) {
      return rs.next() ? rs.getLong(1) : 0;
    }
  }
}
