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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexTool.IndexVerifyType;
import org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The fsck provider for global secondary indexes.
 * <p>
 * The provider verifies and repairs rows with two {@link IndexTool} passes. One pass goes from the
 * data table to the index table, and one pass goes from the index table to the data table. The
 * provider also checks table invariants: the index state and the coprocessors that the verified
 * write protocol requires.
 */
public class GlobalIndexFsckProvider implements IndexFsckProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalIndexFsckProvider.class);

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
   * Makes an index repair plan and, if the context confirms the repair, executes it in rounds.
   * <p>
   * Without confirmation, the method only makes the plan and does not change data. With
   * confirmation, each round repairs the index and then counts the errors that remain. The rounds
   * stop when no errors remain or when a round does not decrease the error count. The report
   * contains the findings of the last round. If the repair fails, the method logs the report of the
   * actions that it took and throws the exception again.
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
    try {
      long previousErrors = Long.MAX_VALUE;
      for (int round = 1;; round++) {
        List<Finding> remaining = repairRound(context, plan, round);
        long errors = countErrors(remaining);
        if (errors == 0 || errors >= previousErrors) {
          report.addFindings(remaining);
          return report;
        }
        previousErrors = errors;
      }
    } catch (Exception e) {
      LOGGER.error("Repair of {} failed after these actions: {}", context.getIndexTableName(),
        report.toJson(), e);
      throw e;
    }
  }

  /**
   * Counts the errors that measure repair progress. An error finding with a numeric count detail
   * adds that count of rows. Each other error finding adds one.
   */
  static long countErrors(List<Finding> findings) {
    long errors = 0;
    for (Finding finding : findings) {
      if (finding.getSeverity() == Severity.ERROR) {
        Object count = finding.getDetails().get("count");
        errors += count instanceof Number ? ((Number) count).longValue() : 1;
      }
    }
    return errors;
  }

  /**
   * Finds the inconsistencies and adds the repair actions that they need to the plan. The actions
   * are idempotent. This method does not change data, and returns the findings of all checks.
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

  /**
   * Executes one repair round and returns the findings after the repair. The round first reads the
   * index metadata again, because a concurrent rebuild can change it. A round does not repair the
   * rows of a disabled index.
   */
  protected List<Finding> repairRound(IndexFsckContext context, RepairPlan plan, int round)
    throws Exception {
    context.refreshIndexTable();
    Report checks = new Report(IndexFsckTool.CMD_REPAIR, context);
    checkTable(context, checks);
    if (canVerifyRows(context, checks)) {
      checks.addFindings(repairRows(context, plan, round));
    }
    return new ArrayList<>(checks.getFindings());
  }

  /**
   * Rebuilds the missing and invalid index rows from the data table. Then this method deletes the
   * verified orphan rows from the index table. It returns the findings that remain after both
   * passes.
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
   * Executes one IndexTool repair pass and returns the findings that remain. The pass from the data
   * table uses {@code -v BOTH}, which rebuilds only the rows that fail verification. The pass from
   * the index table uses {@code -v AFTER -do}, the mode in which IndexTool deletes orphan rows. If
   * IndexTool fails or leaves defects, and the error is recoverable, the pass marks the action
   * skipped. Then it verifies again to find the defects that remain for the next round. An
   * interrupt or an unrecoverable error stops the repair.
   */
  private List<Finding> repairPass(IndexFsckContext context, boolean fromIndex, RepairAction action)
    throws Exception {
    Counters counters;
    action.logExecuting();
    try {
      counters = runIndexToolOnce(context, fromIndex ? IndexVerifyType.AFTER : IndexVerifyType.BOTH,
        fromIndex);
    } catch (Exception e) {
      if (Retry.isInterrupt(e)) {
        Thread.currentThread().interrupt();
        throw e;
      }
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
   * Returns true if row verification is safe. The method does not verify the rows of a disabled
   * index, because verification can change the state of that index to BUILDING. For a disabled
   * index, it adds an error finding and returns false.
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
   * Checks the index state and the coprocessors that the verified write protocol requires. An index
   * that is disabled or has a disable timestamp gives a warning finding. A missing coprocessor
   * gives an error finding.
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

  /**
   * Runs an IndexTool job in the foreground, retries recoverable failures, and returns the job
   * counters.
   */
  protected Counters runIndexTool(IndexFsckContext context, IndexVerifyType verifyType,
    boolean fromIndex) throws Exception {
    return Retry.call("IndexTool -v " + verifyType + (fromIndex ? " -fi" : ""),
      () -> runIndexToolOnce(context, verifyType, fromIndex));
  }

  protected Counters runIndexToolOnce(IndexFsckContext context, IndexVerifyType verifyType,
    boolean fromIndex) throws Exception {
    List<String> args = indexToolArgs(context, verifyType, fromIndex);
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
        throw indexToolFailure(context, tool.getJob(), args, status);
      }
      return tool.getJob().getCounters();
    } finally {
      outputPath.getFileSystem(conf).delete(outputPath, true);
    }
  }

  /**
   * Returns the exception for an IndexTool run that failed with the given status. IndexTool logs
   * its exceptions and does not throw them, so this method finds the cause where it can. If the job
   * did not start, the method reads the tables again. That read can throw the root cause, for
   * example for a dropped table or a denied access.
   * <p>
   * If the submitted job still runs, the wait for the job stopped early. An interrupt or an error
   * from a poll of the job status can cause this, and IndexTool does not show which one. The method
   * kills the job, so that it does not run concurrently with a retry or after the tool stops. In
   * this case, the method returns an exception that does not permit a retry, because a retry would
   * ignore an interrupt. In the other cases, the returned exception permits a retry.
   */
  static Exception indexToolFailure(IndexFsckContext context, Job job, List<String> args,
    int status) throws Exception {
    if (job == null) {
      // The run failed before the job started. Read the tables again, so that a dropped table or
      // a denied access shows as an unrecoverable root cause.
      PhoenixConnection pconn = context.getConnection().unwrap(PhoenixConnection.class);
      pconn.getTableNoCache(context.getDataTableName());
      pconn.getTableNoCache(context.getIndexTableName());
    } else if (job.getJobID() != null && !job.isComplete()) {
      job.killJob();
      return new DoNotRetryIOException(
        "IndexTool " + args + " stopped waiting for job " + job.getJobID() + ", which was killed");
    }
    return new IllegalStateException("IndexTool " + args + " failed with status " + status);
  }

  /**
   * Builds the IndexTool arguments for a pass. The method quotes the names, because IndexTool
   * normalizes the names that it gets, and the names in the catalog are already normalized.
   */
  static List<String> indexToolArgs(IndexFsckContext context, IndexVerifyType verifyType,
    boolean fromIndex) {
    PTable data = context.getDataTable();
    PTable index = context.getIndexTable();
    List<String> args = new ArrayList<>();
    if (data.getSchemaName().getString().length() > 0) {
      args.add("-s");
      args.add(SchemaUtil.getEscapedArgument(data.getSchemaName().getString()));
    }
    args.add("-dt");
    args.add(SchemaUtil.getEscapedArgument(data.getTableName().getString()));
    args.add("-it");
    args.add(SchemaUtil.getEscapedArgument(index.getTableName().getString()));
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
    return args;
  }
}
