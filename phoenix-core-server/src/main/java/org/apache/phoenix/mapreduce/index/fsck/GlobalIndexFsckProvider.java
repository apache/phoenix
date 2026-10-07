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
package org.apache.phoenix.mapreduce.index.fsck;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexTool.IndexVerifyType;
import org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;

/**
 * FSCK provider for global secondary indexes.
 * <p>
 * Performs row level verification and repair using bidirectional {@link IndexTool} passes
 * (data-to-index and index-to-data). Validates table level invariants including index state and
 * required coprocessor configurations for the verified write protocol.
 */
public class GlobalIndexFsckProvider implements IndexFsckProvider {
  public static final String SCOPE_TABLE = "TABLE";

  public static final String INDEX_DISABLED = "INDEX_DISABLED";
  public static final String MISSING_COPROCESSOR = "MISSING_COPROCESSOR";
  public static final String ROWS_NOT_VERIFIED = "ROWS_NOT_VERIFIED";

  public static final String REBUILD_INDEX_ROWS = "REBUILD_INDEX_ROWS";
  public static final String DELETE_ORPHAN_ROWS = "DELETE_ORPHAN_ROWS";

  @Override
  public Report verify(IndexFsckContext context) throws Exception {
    Report report = new Report(IndexFsckTool.CMD_VERIFY, context);
    checkTable(context, report);
    if (canVerifyRows(context, report)) {
      report.addFindings(rowFindings(runIndexTool(context, IndexVerifyType.ONLY, false), false));
      report.addFindings(rowFindings(runIndexTool(context, IndexVerifyType.ONLY, true), true));
    }
    return report;
  }

  @Override
  public Report fsck(IndexFsckContext context) throws Exception {
    Report report = new Report(IndexFsckTool.CMD_FSCK, context);
    checkTable(context, report);
    Finding lastVerification =
      VerifyFindings.lastVerification(context.getConnection(), context.getIndexTable());
    if (lastVerification != null) {
      report.addFinding(lastVerification);
    }
    return report;
  }

  /**
   * Generates and optionally executes an iterative index repair plan.
   * <p>
   * In confirmed mode, repair executes in rounds until all inconsistencies are resolved or a
   * convergence plateau is reached (no decrease in error count). Returns remaining findings from
   * the final round.
   */
  @Override
  public Report repair(IndexFsckContext context) throws Exception {
    Report report = new Report(IndexFsckTool.CMD_REPAIR, context);
    RepairPlan plan = new RepairPlan(!context.isConfirm());
    report.setRepairPlan(plan);
    if (!context.isConfirm()) {
      report.addFindings(planRepair(context, plan));
      return report;
    }
    long previousErrors = Long.MAX_VALUE;
    for (int round = 1;; round++) {
      List<Finding> remaining = repairRound(context, plan, round);
      long errors = remaining.stream().filter(f -> f.getSeverity() == Severity.ERROR).count();
      if (errors == 0 || errors >= previousErrors) {
        report.addFindings(remaining);
        return report;
      }
      previousErrors = errors;
    }
  }

  /**
   * Identifies inconsistencies and builds an idempotent remediation plan without modifying data.
   */
  protected List<Finding> planRepair(IndexFsckContext context, RepairPlan plan) throws Exception {
    Report checks = new Report(IndexFsckTool.CMD_REPAIR, context);
    checkTable(context, checks);
    if (canVerifyRows(context, checks)) {
      List<Finding> fromData =
        rowFindings(runIndexTool(context, IndexVerifyType.ONLY, false), false);
      List<Finding> fromIndex =
        rowFindings(runIndexTool(context, IndexVerifyType.ONLY, true), true);
      checks.addFindings(fromData);
      checks.addFindings(fromIndex);
      if (fromData.stream().anyMatch(f -> f.getSeverity() == Severity.ERROR)) {
        plan.add(REBUILD_INDEX_ROWS, "Rebuild missing and invalid index rows from the data table");
      }
      if (fromIndex.stream().anyMatch(f -> f.getRule().equals(VerifyFindings.ORPHAN_VERIFIED))) {
        plan.add(DELETE_ORPHAN_ROWS, "Delete verified orphan index rows (IndexTool -fi -do)");
      }
    }
    return new ArrayList<>(checks.getFindings());
  }

  /** Executes a single repair iteration and returns post-repair findings. */
  protected List<Finding> repairRound(IndexFsckContext context, RepairPlan plan, int round)
    throws Exception {
    Report checks = new Report(IndexFsckTool.CMD_REPAIR, context);
    checkTable(context, checks);
    if (canVerifyRows(context, checks)) {
      checks.addFindings(repairRows(context, plan, round));
    }
    return new ArrayList<>(checks.getFindings());
  }

  /**
   * Rebuilds missing and invalid index rows from the data table, then purges verified orphan rows
   * from the index table.
   */
  protected List<Finding> repairRows(IndexFsckContext context, RepairPlan plan, int round)
    throws Exception {
    List<Finding> remaining = new ArrayList<>();
    RepairAction rebuild =
      plan.add(REBUILD_INDEX_ROWS, "Rebuild missing and invalid index rows from the data table");
    rebuild.putDetail("round", round);
    remaining.addAll(repairPass(context, false, rebuild));
    RepairAction deleteOrphans =
      plan.add(DELETE_ORPHAN_ROWS, "Delete verified orphan index rows (IndexTool -fi -do)");
    deleteOrphans.putDetail("round", round);
    remaining.addAll(repairPass(context, true, deleteOrphans));
    return remaining;
  }

  /**
   * Executes a single IndexTool repair pass. If the repair job encounters residual defects, re-runs
   * verification to collect remaining defect metrics for subsequent rounds.
   */
  private List<Finding> repairPass(IndexFsckContext context, boolean fromIndex, RepairAction action)
    throws Exception {
    Counters counters;
    try {
      counters = runIndexToolOnce(context, IndexVerifyType.AFTER, fromIndex);
    } catch (Exception e) {
      if (Retry.isUnrecoverable(e)) {
        throw e;
      }
      action.markSkipped("IndexTool failed or left defects: " + e.getMessage());
      return rowFindings(runIndexTool(context, IndexVerifyType.ONLY, fromIndex), fromIndex);
    }
    if (fromIndex) {
      action.putDetail("deletedOrphanRows", VerifyFindings.deletedOrphans(counters));
    } else {
      action.putDetail("rebuiltIndexRows",
        counters.findCounter(PhoenixIndexToolJobCounters.REBUILT_INDEX_ROW_COUNT).getValue());
    }
    action.markExecuted();
    return VerifyFindings.fromCounters(counters, fromIndex, true);
  }

  @Override
  public Report inspect(IndexFsckContext context, String command, List<String> args)
    throws Exception {
    throw new IllegalArgumentException(
      "There are no inspect commands for index type " + context.getIndexTable().getIndexType());
  }

  /**
   * Validates whether row verification can proceed safely. Verifying a disabled index is skipped to
   * avoid mutating index state to BUILDING.
   */
  protected boolean canVerifyRows(IndexFsckContext context, Report report) {
    if (context.getIndexTable().getIndexState().isDisabled()) {
      report.addFinding(new Finding(Severity.ERROR, VerifyFindings.SCOPE, ROWS_NOT_VERIFIED,
        "Rows of a disabled index are not verified; rebuild the index with IndexTool"));
      return false;
    }
    return true;
  }

  /**
   * Validates index state and coprocessor configurations required for the verified write protocol.
   */
  protected void checkTable(IndexFsckContext context, Report report) throws Exception {
    PTable index = context.getIndexTable();
    PIndexState state = index.getIndexState();
    if (state.isDisabled() || index.getIndexDisableTimestamp() > 0) {
      report.addFinding(new Finding(Severity.WARN, SCOPE_TABLE, INDEX_DISABLED,
        "The index is " + state + " with disable timestamp " + index.getIndexDisableTimestamp()
          + "; rebuild it with IndexTool"));
    }
    Admin admin =
      context.getConnection().unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
    try {
      TableDescriptor data =
        admin.getDescriptor(TableName.valueOf(context.getDataTable().getPhysicalName().getBytes()));
      TableDescriptor indexTable =
        admin.getDescriptor(TableName.valueOf(index.getPhysicalName().getBytes()));
      boolean dataObserver = data.hasCoprocessor(QueryConstants.INDEX_REGION_OBSERVER_CLASSNAME);
      if (!dataObserver && !data.hasCoprocessor(QueryConstants.INDEXER_CLASSNAME)) {
        report.addFinding(new Finding(Severity.ERROR, SCOPE_TABLE, MISSING_COPROCESSOR,
          "Data table " + data.getTableName() + " has no index maintenance coprocessor"));
      }
      if (
        dataObserver && !indexTable.hasCoprocessor(QueryConstants.GLOBAL_INDEX_CHECKER_CLASSNAME)
      ) {
        report.addFinding(new Finding(Severity.ERROR, SCOPE_TABLE, MISSING_COPROCESSOR,
          "Index table " + indexTable.getTableName() + " has no "
            + QueryConstants.GLOBAL_INDEX_CHECKER_CLASSNAME));
      }
    } finally {
      admin.close();
    }
  }

  protected static List<Finding> rowFindings(Counters counters, boolean fromIndex) {
    return VerifyFindings.fromCounters(counters, fromIndex, false);
  }

  /** Executes an IndexTool job in the foreground with retry logic and returns the job counters. */
  protected Counters runIndexTool(IndexFsckContext context, IndexVerifyType verifyType,
    boolean fromIndex) throws Exception {
    return Retry.call("IndexTool -v " + verifyType + (fromIndex ? " -fi" : ""),
      () -> runIndexToolOnce(context, verifyType, fromIndex));
  }

  private Counters runIndexToolOnce(IndexFsckContext context, IndexVerifyType verifyType,
    boolean fromIndex) throws Exception {
    PTable data = context.getDataTable();
    PTable index = context.getIndexTable();
    List<String> args = new ArrayList<>();
    if (data.getSchemaName().getString().length() > 0) {
      args.add("-s");
      args.add(data.getSchemaName().getString());
    }
    args.add("-dt");
    args.add(data.getTableName().getString());
    args.add("-it");
    args.add(index.getTableName().getString());
    if (context.getTenantId() != null) {
      args.add("-tenant");
      args.add(context.getTenantId());
    }
    if (context.getStartTime() != null) {
      args.add("-st");
      args.add(context.getStartTime().toString());
    }
    if (context.getEndTime() != null) {
      args.add("-et");
      args.add(context.getEndTime().toString());
    }
    args.add("-v");
    args.add(verifyType.getValue());
    if (fromIndex) {
      args.add("-fi");
      if (verifyType == IndexVerifyType.AFTER) {
        args.add("-do");
      }
    }
    args.add("-runfg");
    Configuration conf = HBaseConfiguration.create(context.getConfiguration());
    Path outputPath =
      new Path(FileSystem.get(conf).getHomeDirectory(), "phoenix-indexfsck-" + UUID.randomUUID());
    args.add("-op");
    args.add(outputPath.toString());
    IndexTool tool = new IndexTool();
    tool.setConf(conf);
    try {
      int status = tool.run(args.toArray(new String[0]));
      if (status != 0) {
        throw new IllegalStateException("IndexTool " + args + " failed with status " + status);
      }
      return tool.getJob().getCounters();
    } finally {
      outputPath.getFileSystem(conf).delete(outputPath, true);
    }
  }
}
