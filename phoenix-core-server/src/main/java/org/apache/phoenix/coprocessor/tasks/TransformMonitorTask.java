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
package org.apache.phoenix.coprocessor.tasks;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.DEFAULT_TRANSFORM_RETRY_COUNT;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.TRANSFORM_RETRY_COUNT_VALUE;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtilHelper.DEFAULT_TRANSFORM_MONITOR_ENABLED;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtilHelper.TRANSFORM_MONITOR_ENABLED;

import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.transform.TransformTool;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.ServerTask;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Task runs periodically to monitor and orchestrate ongoing transforms in System.Transform table.
 */
public class TransformMonitorTask extends BaseTask {
  public static final String DEFAULT = "IndexName";

  public static final Logger LOGGER = LoggerFactory.getLogger(TransformMonitorTask.class);

  // After the cutover pointer swap, clients may still hold a cached pointer to the old physical
  // table until their update-cache-frequency window elapses. We wait for that window (with a small
  // safety margin) before running the partial pass, so late writes routed to the old table are not
  // stranded as unverified rows. The multiplier adds headroom over the raw cache frequency and the
  // floor guarantees a minimum wait even when the cache frequency is very small or zero.
  private static final double CACHE_FREQUENCY_SAFETY_MULTIPLIER = 1.10;
  // 30 minutes
  private static final long MIN_PARTIAL_PASS_WAIT_MS = 30L * 60L * 1000L;
  // 24 hours. Upper bound on the wait. A table configured to never refresh its cache resolves its
  // update-cache-frequency to Long.MAX_VALUE; scaling that and adding it to the current time would
  // saturate and overflow into a negative (past) deadline, which would defeat the wait entirely.
  // Clamping the wait to this ceiling keeps the persisted deadline a bounded, valid future
  // timestamp while still deferring the partial pass long enough for clients to refresh.
  private static final long MAX_PARTIAL_PASS_WAIT_MS = 24L * 60L * 60L * 1000L;

  private static boolean isDisabled = false;

  // Called from testing
  @VisibleForTesting
  public static void disableTransformMonitorTask(boolean disabled) {
    isDisabled = disabled;
  }

  /**
   * Resolves the running MapReduce job for a given job id. Extracted behind an overridable seam so
   * a test can inject a completed/failed job and exercise the PARTIAL_PASS_RUNNING branch's
   * retries-exhausted -&gt; FAILED transition deterministically, without submitting a real MR job
   * that fails. The default implementation looks the job up on the real cluster.
   */
  @VisibleForTesting
  public interface JobLookup {
    Job getJob(Configuration configuration, String jobId) throws Exception;
  }

  private static JobLookup defaultJobLookup() {
    return (configuration, jobId) -> {
      Cluster cluster = new Cluster(configuration);
      return cluster.getJob(JobID.forName(jobId));
    };
  }

  private static JobLookup jobLookup = defaultJobLookup();

  @VisibleForTesting
  public static void setJobLookupForTesting(JobLookup lookup) {
    jobLookup = lookup;
  }

  @VisibleForTesting
  public static void resetJobLookupForTesting() {
    jobLookup = defaultJobLookup();
  }

  @Override
  public TaskRegionObserver.TaskResult run(Task.TaskRecord taskRecord) {
    Configuration conf = HBaseConfiguration.create(env.getConfiguration());
    Configuration configuration = HBaseConfiguration.addHbaseResources(conf);
    boolean transformMonitorEnabled =
      configuration.getBoolean(TRANSFORM_MONITOR_ENABLED, DEFAULT_TRANSFORM_MONITOR_ENABLED);
    if (!transformMonitorEnabled || isDisabled) {
      return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
        "TransformMonitor is disabled");
    }

    try (PhoenixConnection conn =
      QueryUtil.getConnectionOnServer(conf).unwrap(PhoenixConnection.class)) {
      SystemTransformRecord systemTransformRecord =
        Transform.getTransformRecord(taskRecord.getSchemaName(), taskRecord.getTableName(), null,
          taskRecord.getTenantId(), conn);
      if (systemTransformRecord == null) {
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
          "No transform record is found");
      }
      // Forward-compatibility guard: if the persisted transform type was written by a newer binary
      // that introduced a new TransformType constant, this binary will see UNKNOWN. Skip the row
      // without retrying — the monitor task cannot make a safe decision about how to advance it.
      TaskRegionObserver.TaskResult unknownTypeResult =
        skipIfUnknownTransformType(systemTransformRecord);
      if (unknownTypeResult != null) {
        return unknownTypeResult;
      }
      String tableName = SchemaUtil.getTableName(systemTransformRecord.getSchemaName(),
        systemTransformRecord.getLogicalTableName());

      if (
        systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.CREATED.name())
      ) {
        LOGGER.info("Transform is created, starting the TransformTool {}", tableName);
        // Kick a TransformTool run, it will already update transform record status and job id
        TransformTool transformTool = TransformTool.runTransformTool(systemTransformRecord, conf,
          false, null, null, false, false);
        if (transformTool == null) {
          // This is not a map/reduce error. There must be some unexpected issue. So, retrying will
          // not solve the underlying issue.
          return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
            "TransformTool run failed. Check the parameters.");
        }
      } else if (
        systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.COMPLETED.name())
      ) {
        LOGGER.info("Transform is completed, TransformMonitor is done {}", tableName);
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS, "");
      } else if (
        systemTransformRecord.getTransformStatus()
          .equals(PTable.TransformStatus.PENDING_CUTOVER.name())
          && !PTable.TransformType.isPartialTransform(systemTransformRecord.getTransformType())
      ) {
        LOGGER.info("Transform is pending cutover {}", tableName);
        // Persist the cutover instant (repair-floor anchor) durably before doCutover. doCutover
        // commits the pointer swap durably, but this instant is otherwise only in the buffered
        // PENDING_PARTIAL_PASS upsert below, so a crash in that gap would lose it and a re-entry
        // would re-capture a later one, pushing the repair floor past the real cutover. A re-entry
        // instead reuses the persisted value (resolveCutoverTs).
        long cutoverTs = resolveCutoverTs(systemTransformRecord);
        if (systemTransformRecord.getCutoverTs() == null) {
          Transform.updateTransformRecord(conn, systemTransformRecord,
            PTable.TransformStatus.PENDING_CUTOVER,
            systemTransformRecord.getPendingPartialPassUntilTs(), cutoverTs);
          conn.commit();
        }
        Transform.doCutover(conn, systemTransformRecord);

        PTable.TransformType partialTransform =
          PTable.TransformType.getPartialTransform(systemTransformRecord.getTransformType());
        if (partialTransform != null) {
          // After the pointer swap, wait for clients to refresh their cached physical-table pointer
          // before running the partial pass. Persist the earliest time the wait may end and move to
          // PENDING_PARTIAL_PASS; the partial pass is NOT kicked here.
          long waitUntilTs = EnvironmentEdgeManager.currentTimeMillis()
            + computePartialPassWaitMs(conn, systemTransformRecord);
          // One INFO per transform recording the deferral deadline; the per-scan "still waiting"
          // line is logged at DEBUG (the wait spans 30 min to 24 h at ~60 s scans) so the wait
          // window is observable without flooding the log.
          LOGGER.info(
            "Cutover complete for {}; deferring the partial pass until ts {} so clients "
              + "can refresh their cached physical-table pointer before it runs",
            tableName, waitUntilTs);
          Transform.updateTransformRecord(conn, systemTransformRecord,
            PTable.TransformStatus.PENDING_PARTIAL_PASS, waitUntilTs, cutoverTs);
        } else {
          // No partial transform needed so, we update state of the transform
          LOGGER.warn("No partial type of the transform is found. Completing the transform {}",
            tableName);
          Transform.updateTransformRecord(conn, systemTransformRecord,
            PTable.TransformStatus.COMPLETED);
        }
      } else if (
        systemTransformRecord.getTransformStatus()
          .equals(PTable.TransformStatus.PENDING_PARTIAL_PASS.name())
      ) {
        Long waitUntilTs = systemTransformRecord.getPendingPartialPassUntilTs();
        if (waitUntilTs != null && EnvironmentEdgeManager.currentTimeMillis() < waitUntilTs) {
          // Still inside the client cache-refresh window; re-poll on the next scan. Logged at DEBUG
          // because the monitor scans every ~60s while this wait can span 30 min to 24 h, so an
          // INFO here would emit the same line hundreds of times per transform.
          LOGGER.debug(
            "Transform is pending partial pass, still waiting for cache refresh window {}",
            tableName);
        } else {
          LOGGER.info("Transform wait window elapsed, starting the partial pass {}", tableName);
          // Make the PARTIAL_PASS_RUNNING transition authoritative and committed BEFORE launching
          // the partial pass. Committing here (rather than relying on the monitor's ServerTask
          // commit at the end of run()) means a later monitor scan can observe
          // PARTIAL_PASS_RUNNING,
          // and the partial pass's own reducer-committed COMPLETED (written on a separate
          // connection when the async MR job ends) is never clobbered by a stale buffered upsert.
          //
          // Clear the inherited job id on this transition. The record still carries the full-pass
          // job id (the PENDING_PARTIAL_PASS record inherited it and neither the cutover transition
          // nor the builder copy ctor clears it). That old job id points at the already-completed,
          // successful full pass. If a monitor scan observes the record while still
          // PARTIAL_PASS_RUNNING before the kicked partial pass has registered its own job id, the
          // PARTIAL_PASS_RUNNING branch below would look up that stale job, see it successful, and
          // drive the record straight to COMPLETED -- skipping the partial pass that repairs
          // unverified rows. Nulling it here makes the `if (jobId != null)` guard correctly no-op
          // until the partial pass has registered its own job id.
          updateTransformRecordClearingJobId(conn, systemTransformRecord,
            PTable.TransformStatus.PARTIAL_PASS_RUNNING, waitUntilTs);
          conn.commit();
          // Re-read so the kick builds its upsert off the PARTIAL_PASS_RUNNING record rather than
          // the pre-kick PENDING_PARTIAL_PASS state.
          SystemTransformRecord runningRecord = Transform.getTransformRecord(
            systemTransformRecord.getSchemaName(), systemTransformRecord.getLogicalTableName(),
            null, systemTransformRecord.getTenantId(), conn);
          // First partial-pass kick (not a retry): the initial partial pass must not consume retry
          // budget.
          kickPartialPass(conn, conf, runningRecord, tableName, false);
        }
      } else if (
        systemTransformRecord.getTransformStatus()
          .equals(PTable.TransformStatus.PARTIAL_PASS_RUNNING.name())
      ) {
        LOGGER.info("Partial pass is running, we will monitor {}", tableName);
        // Monitor the partial-pass job to completion, then advance to COMPLETED.
        String jobId = systemTransformRecord.getTransformJobId();
        // Defense-in-depth alongside the job-id clearing on the PENDING_PARTIAL_PASS ->
        // PARTIAL_PASS_RUNNING transition: only a partial-type record may be driven to a terminal
        // state by this branch. A record still carrying the full transform type has not yet had its
        // partial pass registered (the kick flips the type to the partial variant before
        // launching),
        // so any job id it carries is the stale, already-successful full-pass job. Refusing to act
        // on it here prevents a premature COMPLETED that would skip the partial pass; such a record
        // is transient and is re-evaluated on the next scan once the kick has flipped the type.
        if (PTable.TransformType.isPartialTransform(systemTransformRecord.getTransformType())) {
          // A null job id here means no partial-pass job is currently registered. This is NOT a
          // benign no-op: the pointer swap already happened, so the record must still reach a
          // terminal state. It occurs when the initial partial-pass kick failed before
          // TransformTool's STARTED transition (e.g. connection acquisition, index-table creation,
          // or argument validation threw, so runTransformTool returned null without registering a
          // job id and without throwing), leaving a committed (PARTIAL_PASS_RUNNING, partial-type,
          // jobId=null) row. A job id that resolves to null (aged out of the job-history server,
          // resource-manager restart, etc.) is likewise unconfirmable. In every one of these cases
          // the pass cannot be confirmed successful, so it is routed through the same
          // retry-budgeted path as an outright failed job below -- never left to no-op forever.
          Job job = jobId != null ? jobLookup.getJob(configuration, jobId) : null;
          if (job != null && !job.isComplete()) {
            // Partial pass is still running; re-evaluate on the next monitor scan.
            LOGGER.info("Partial pass job is still running, we will keep monitoring {}", tableName);
          } else if (job != null && job.isSuccessful()) {
            Transform.updateTransformRecord(conn, systemTransformRecord,
              PTable.TransformStatus.COMPLETED);
          } else {
            // The partial pass could not be confirmed successful: no job id is registered (the
            // initial kick failed before its STARTED transition), the job completed unsuccessfully,
            // or the job id could not be resolved at all (aged out of the job-history server,
            // resource-manager restart, etc.). All are treated as a failed partial pass and routed
            // through the retry-budgeted path. Returning SKIPPED here instead would strand the
            // transform: a SKIPPED result leaves the self-healing task in the STARTED state, and
            // the monitor scan re-picks up only CREATED/RETRY tasks, so PARTIAL_PASS_RUNNING would
            // never be re-evaluated and the already-pointer-swapped table would never get its
            // repairing partial pass.
            if (jobId == null) {
              LOGGER.warn("No partial-pass job is registered for {}; the initial kick did not "
                + "register one. Treating as a failed partial pass and retrying.", tableName);
            } else if (job == null) {
              LOGGER.warn(String.format(
                "Transform job with Id=%s is not found; treating as a failed partial pass", jobId));
            }
            // Account for a pre-STARTED failure. TransformTool increments the retry count only on
            // its STARTED transition, so a kick that failed before STARTED (jobId == null) was
            // never counted. Left uncounted, a deterministically pre-STARTED-failing partial pass
            // (e.g. connection acquisition or index-table creation throws every attempt) would
            // resubmit forever and never reach the retries-exhausted -> FAILED transition below.
            // A jobId that merely resolved to null (job aged out of the history server) already
            // reached STARTED and was counted there, so it is deliberately not re-counted here.
            SystemTransformRecord failedRecord = systemTransformRecord;
            if (jobId == null) {
              SystemTransformRecord.SystemTransformBuilder builder =
                new SystemTransformRecord.SystemTransformBuilder(systemTransformRecord);
              builder.setTransformRetryCount(systemTransformRecord.getTransformRetryCount() + 1);
              failedRecord = builder.build();
            }
            int maxRetryCount =
              configuration.getInt(TRANSFORM_RETRY_COUNT_VALUE, DEFAULT_TRANSFORM_RETRY_COUNT);
            if (failedRecord.getTransformRetryCount() < maxRetryCount) {
              // Retry the partial pass. A kick that reaches STARTED has its count strictly
              // increased by TransformTool's STARTED transition; a pre-STARTED failure has it
              // increased by the block above. Either way the count strictly increases per failed
              // attempt, so the retries-exhausted -> FAILED transition below stays reachable and a
              // repeatedly-failing partial pass cannot resubmit forever.
              kickPartialPass(conn, conf, failedRecord, tableName, true);
            } else {
              // Retries are exhausted. Move to a terminal FAILED state so the record does not
              // re-enter PARTIAL_PASS_RUNNING forever on subsequent scans.
              LOGGER
                .error("Partial pass failed and retries are exhausted. Marking transform as failed "
                  + tableName);
              Transform.updateTransformRecord(conn, failedRecord, PTable.TransformStatus.FAILED);
            }
          }
        } else {
          // Defensive guard: no code path produces a full-type PARTIAL_PASS_RUNNING record, since
          // the transition to PARTIAL_PASS_RUNNING commits the partial type. Reachable only by an
          // externally seeded record; log without acting on the stale full-pass job id.
          LOGGER.info("Partial pass not yet registered as partial-type, will re-evaluate {}",
            tableName);
        }
      } else if (
        systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.STARTED.name())
          || (systemTransformRecord.getTransformStatus()
            .equals(PTable.TransformStatus.PENDING_CUTOVER.name())
            && PTable.TransformType.isPartialTransform(systemTransformRecord.getTransformType()))
      ) {
        LOGGER.info(
          systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.STARTED.name())
            ? "Transform is started, we will monitor "
            : "Partial transform is going on, we will monitor",
          tableName);
        // Monitor the job of transform tool and decide to retry
        String jobId = systemTransformRecord.getTransformJobId();
        if (jobId != null) {
          Job job = jobLookup.getJob(configuration, jobId);
          if (job == null) {
            LOGGER.warn(String.format("Transform job with Id=%s is not found", jobId));
            return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED,
              "The job cannot be found");
          }
          if (job != null && job.isComplete()) {
            if (job.isSuccessful()) {
              LOGGER.warn(
                "TransformTool job is successful. Transform should have been in a COMPLETED state "
                  + taskRecord.getTableName());
            } else {
              // Retry TransformTool run
              int maxRetryCount =
                configuration.getInt(TRANSFORM_RETRY_COUNT_VALUE, DEFAULT_TRANSFORM_RETRY_COUNT);
              if (systemTransformRecord.getTransformRetryCount() < maxRetryCount) {
                // Retry count will be incremented in TransformTool
                TransformTool.runTransformTool(systemTransformRecord, conf, false, null, null,
                  false, true);
              }
            }
          }
        }
      } else if (
        systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.FAILED.name())
      ) {
        String str =
          "Transform is marked as failed because either TransformTool is run on the foreground and failed "
            + "or it is run as async but there is something wrong with the TransformTool parameters";
        LOGGER.error(str);
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, str);
      } else if (
        systemTransformRecord.getTransformStatus().equals(PTable.TransformStatus.PAUSED.name())
      ) {
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SUCCESS,
          "Transform is paused. No need to monitor");
      } else {
        String str = "Transform status is not known " + systemTransformRecord.getString();
        LOGGER.error(str);
        return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL, str);
      }

      // Update task status to RETRY so that it is retried
      ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder().setConn(conn)
        .setTaskType(taskRecord.getTaskType()).setTenantId(taskRecord.getTenantId())
        .setSchemaName(taskRecord.getSchemaName()).setTableName(taskRecord.getTableName())
        .setTaskStatus(PTable.TaskStatus.RETRY.toString()).setData(taskRecord.getData())
        .setPriority(taskRecord.getPriority()).setStartTs(taskRecord.getTimeStamp()).setEndTs(null)
        .setAccessCheckEnabled(true).build());
      return null;
    } catch (Throwable t) {
      LOGGER.warn("Exception while running transform monitor task. "
        + "It will be retried in the next system task table scan : " + taskRecord.getSchemaName()
        + "." + taskRecord.getTableName() + " with tenant id "
        + (taskRecord.getTenantId() == null ? " IS NULL" : taskRecord.getTenantId()) + " and data "
        + taskRecord.getData(), t);
      return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.FAIL,
        t.toString());
    }
  }

  /**
   * Updates the transform record to the given status while clearing any inherited job id. Used on
   * the PENDING_PARTIAL_PASS -&gt; PARTIAL_PASS_RUNNING transition so the record does not carry the
   * completed full-pass job id into the PARTIAL_PASS_RUNNING monitoring branch. Mirrors
   * {@link Transform#updateTransformRecord} (bumps last-state-ts, preserves the wait timestamp) but
   * additionally sets the job id to null; the kicked partial pass registers its own job id when it
   * launches. It also flips the transform type to the partial variant so the committed
   * PARTIAL_PASS_RUNNING record is restart-safe.
   */
  private void updateTransformRecordClearingJobId(PhoenixConnection conn,
    SystemTransformRecord systemTransformRecord, PTable.TransformStatus newStatus,
    Long pendingPartialPassUntilTs) throws SQLException {
    SystemTransformRecord.SystemTransformBuilder builder =
      new SystemTransformRecord.SystemTransformBuilder(systemTransformRecord);
    builder.setTransformStatus(newStatus.name());
    builder.setTransformJobId(null);
    builder.setLastStateTs(new Timestamp(EnvironmentEdgeManager.currentTimeMillis()));
    builder.setPendingPartialPassUntilTs(pendingPartialPassUntilTs);
    // Flip to the partial transform type in this same committed transition so the record is
    // restart-safe: a crash after this commit but before the partial-pass kick leaves a
    // partial-type, null-job-id record, which the PARTIAL_PASS_RUNNING branch routes through the
    // retry path instead of the log-only branch that never re-kicks.
    PTable.TransformType partialTransform =
      PTable.TransformType.getPartialTransform(systemTransformRecord.getTransformType());
    if (partialTransform != null) {
      builder.setTransformType(partialTransform);
    }
    Transform.upsertTransform(builder.build(), conn);
  }

  /**
   * Computes how long to wait after cutover before running the partial pass. The wait is the
   * logical (parent) table's update-cache-frequency scaled by a safety margin, clamped to
   * [{@link #MIN_PARTIAL_PASS_WAIT_MS}, {@link #MAX_PARTIAL_PASS_WAIT_MS}] so a small or zero cache
   * frequency still yields a meaningful wait and a table that never refreshes its cache (whose
   * update-cache-frequency resolves to Long.MAX_VALUE) does not produce an unbounded wait that
   * would overflow the deadline arithmetic in the caller. The returned value is always a small,
   * positive number of milliseconds, so adding it to the current time cannot overflow.
   */
  private long computePartialPassWaitMs(PhoenixConnection conn,
    SystemTransformRecord systemTransformRecord) {
    long updateCacheFrequency = 0;
    try {
      String logicalTableName = SchemaUtil.getTableName(systemTransformRecord.getSchemaName(),
        systemTransformRecord.getLogicalTableName());
      PTable logicalTable = conn.getTable(systemTransformRecord.getTenantId(), logicalTableName);
      updateCacheFrequency = logicalTable.getUpdateCacheFrequency();
    } catch (Exception e) {
      LOGGER.warn("Could not resolve update cache frequency for the logical table; "
        + "falling back to the minimum partial-pass wait", e);
    }
    return boundedPartialPassWaitMs(updateCacheFrequency);
  }

  /**
   * Clamps and scales a raw update-cache-frequency into a bounded partial-pass wait. Extracted as a
   * pure function so the overflow-safety of the arithmetic can be unit-tested without a cluster.
   * The raw frequency is clamped to the ceiling BEFORE scaling so the multiplication cannot
   * saturate (a never-refreshed table reports Long.MAX_VALUE); the scaled result is then clamped to
   * the [{@link #MIN_PARTIAL_PASS_WAIT_MS}, {@link #MAX_PARTIAL_PASS_WAIT_MS}] window. The return
   * is always in that window -- small, positive, and safe to add to the current time without
   * overflow.
   */
  @VisibleForTesting
  static long boundedPartialPassWaitMs(long updateCacheFrequency) {
    long bounded = Math.min(updateCacheFrequency, MAX_PARTIAL_PASS_WAIT_MS);
    long scaled = (long) (bounded * CACHE_FREQUENCY_SAFETY_MULTIPLIER);
    return Math.min(Math.max(scaled, MIN_PARTIAL_PASS_WAIT_MS), MAX_PARTIAL_PASS_WAIT_MS);
  }

  /**
   * Resolves the cutover instant that anchors the partial-pass repair floor
   * ({@link #repairScanFloor}). A first run captures the current time -- taken before the pointer
   * swap, the most conservative floor -- while a run re-entering the PENDING_CUTOVER handling after
   * a crash reuses the instant the prior run already persisted, so the floor cannot drift past the
   * real cutover. The PENDING_CUTOVER branch of {@link #run} persists this instant durably before
   * the swap so it survives such a crash.
   */
  @VisibleForTesting
  static long resolveCutoverTs(SystemTransformRecord record) {
    return record.getCutoverTs() != null
      ? record.getCutoverTs()
      : EnvironmentEdgeManager.currentTimeMillis();
  }

  /**
   * Repair-scan lower bound for the partial pass. Derived from the cutover instant (minus one, so
   * the floor is inclusive of writes stamped exactly at cutover) so the pass re-verifies every row
   * written to the old pointer during {@code [cutover, cutover + waitWindow]}. A floor derived from
   * the post-wait {@code lastStateTs} would sit past that window and strand those rows. Falls back
   * to {@code lastStateTs} only for records that predate the CUTOVER_TS column, and to 0 (full
   * scan) when neither is set. The cutover instant is persisted durably before the swap and reused
   * on crash re-entry (see {@link #resolveCutoverTs}) so it never drifts later than the real
   * cutover.
   */
  @VisibleForTesting
  static long repairScanFloor(SystemTransformRecord record) {
    if (record.getCutoverTs() != null) {
      return record.getCutoverTs() - 1;
    }
    if (record.getTransformLastStateTs() != null) {
      return record.getTransformLastStateTs().getTime() - 1;
    }
    return 0;
  }

  /**
   * Kicks the partial-pass TransformTool run that fixes unverified rows on the new physical table.
   * This preserves the partial-pass invocation that previously ran inline during cutover. The
   * partial-type marker is committed before launching the tool; the tool is launched asynchronously
   * (TransformTool submits the MR job and returns without blocking).
   * <p>
   * The running partial pass must be observable in status PARTIAL_PASS_RUNNING so that the
   * PARTIAL_PASS_RUNNING branch of {@link #run} -- not the STARTED branch -- monitors it and can
   * drive it to a terminal FAILED state once retries are exhausted. TransformTool.runTransform
   * unconditionally moves the record to STARTED and registers the partial-pass job id under
   * STARTED; left as-is, a running partial pass would live in STARTED, whose retries-exhausted case
   * does nothing, stranding a permanently-failing partial pass. To prevent that, after the async
   * submit returns we re-assert PARTIAL_PASS_RUNNING while preserving the just-registered
   * partial-pass job id, so the PARTIAL_PASS_RUNNING branch owns the running partial pass. We only
   * re-assert when the record is still STARTED: if the (background) job already finished and its
   * reducer committed a terminal status, re-asserting is skipped so the reducer-committed COMPLETED
   * is never clobbered.
   * <p>
   * {@code isRetry} governs retry-count accounting. Launching the partial pass runs it through
   * TransformTool.runTransform, whose STARTED transition unconditionally increments the retry
   * count. For the FIRST partial-pass kick that increment is spurious -- the initial partial pass
   * is not a retry and must not consume retry budget -- so we compensate with a matching decrement
   * ({@code isRetry == false}). For a genuine RETRY after a failed partial pass ({@code isRetry ==
   * true}) we let the increment stand, so the retry count strictly increases and the
   * retries-exhausted -&gt; terminal FAILED transition in the PARTIAL_PASS_RUNNING branch is
   * reachable. Cancelling the increment on the retry path would pin the count and resubmit a
   * deterministically-failing partial pass on every monitor tick forever.
   */
  private void kickPartialPass(PhoenixConnection conn, Configuration conf,
    SystemTransformRecord systemTransformRecord, String tableName, boolean isRetry)
    throws Exception {
    PTable.TransformType partialTransform =
      PTable.TransformType.getPartialTransform(systemTransformRecord.getTransformType());
    if (partialTransform == null) {
      LOGGER.warn("No partial type of the transform is found. Completing the transform {}",
        tableName);
      Transform.updateTransformRecord(conn, systemTransformRecord,
        PTable.TransformStatus.COMPLETED);
      return;
    }
    // Update transform to be partial
    SystemTransformRecord.SystemTransformBuilder builder =
      new SystemTransformRecord.SystemTransformBuilder(systemTransformRecord);
    builder.setTransformType(partialTransform);
    if (!isRetry) {
      // First (non-retry) partial-pass kick: TransformTool's STARTED transition will increment the
      // retry count, but the initial partial pass must not consume retry budget, so pre-decrement
      // to net zero. On the retry path we deliberately skip this so the count strictly increases
      // and retries-exhausted -> FAILED remains reachable.
      builder.setTransformRetryCount(systemTransformRecord.getTransformRetryCount() - 1);
    }
    SystemTransformRecord partialRecord = builder.build();
    Transform.upsertTransform(partialRecord, conn);
    // Commit the partial-type marker before launching the async tool run so the tool (which runs on
    // a separate connection) sees committed state and the monitor holds no stale buffered upsert
    // that could later overwrite the reducer-committed COMPLETED.
    conn.commit();

    // Fix unverified rows. TransformTool moves the record to STARTED, submits the MR job
    // asynchronously, registers the partial-pass job id, and returns; the reducer advances the
    // record to COMPLETED when the job finishes successfully.
    // Derive the repair-scan lower bound from the cutover instant, not the post-wait lastStateTs
    // (see repairScanFloor), so writes to the old pointer during [cutover, cutover + waitWindow]
    // are re-verified rather than stranded.
    long startFromTs = repairScanFloor(partialRecord);
    TransformTool.runTransformTool(partialRecord, conf, true, startFromTs, null, true, false);

    // Re-assert PARTIAL_PASS_RUNNING so the running partial pass is monitored by the
    // PARTIAL_PASS_RUNNING branch (which handles job failure, retry, and retries-exhausted ->
    // FAILED) rather than the STARTED branch (which has no terminal transition for a
    // repeatedly-failing partial pass and would strand it forever). The partial-pass job id that
    // TransformTool just registered is preserved. We re-assert only if the record is still STARTED
    // to narrow the window in which a background job that already finished and had its reducer
    // commit a terminal status gets clobbered. This is a read-then-write with no lock, so a narrow
    // TOCTOU window remains: the reducer could commit COMPLETED between this read and the upsert
    // below. That is self-correcting -- the next monitor scan looks the (successful) job up and
    // re-drives the record to COMPLETED -- so the worst case is one extra scan, not a stranded
    // transform.
    SystemTransformRecord afterLaunch = Transform.getTransformRecord(partialRecord.getSchemaName(),
      partialRecord.getLogicalTableName(), partialRecord.getLogicalParentName(),
      partialRecord.getTenantId(), conn);
    if (
      afterLaunch != null
        && PTable.TransformStatus.STARTED.name().equals(afterLaunch.getTransformStatus())
    ) {
      Transform.updateTransformRecord(conn, afterLaunch,
        PTable.TransformStatus.PARTIAL_PASS_RUNNING);
      conn.commit();
    }
    // In the future, if we are changing the PK structure, we need to run indextools as well
  }

  @Override
  public TaskRegionObserver.TaskResult checkCurrentResult(Task.TaskRecord taskRecord)
    throws Exception {
    // We don't need to check MR job result here since the job itself changes task state.
    return null;
  }

  /**
   * If the supplied transform record carries an {@link PTable.TransformType#UNKNOWN} transform type
   * (typically because the row was written by a newer binary that introduced a new transform type
   * this binary does not understand), return a {@link TaskRegionObserver.TaskResultCode#SKIPPED}
   * result with an operator-readable message. Otherwise return {@code null} to signal that normal
   * processing should continue. Package-private to keep the gate independently testable without
   * standing up a full coprocessor environment.
   */
  static TaskRegionObserver.TaskResult
    skipIfUnknownTransformType(SystemTransformRecord systemTransformRecord) {
    if (systemTransformRecord.getTransformType() == PTable.TransformType.UNKNOWN) {
      String msg = "Skipping transform monitor for record with UNKNOWN transform type; "
        + "likely written by a newer binary. " + systemTransformRecord.getString();
      LOGGER.warn(msg);
      return new TaskRegionObserver.TaskResult(TaskRegionObserver.TaskResultCode.SKIPPED, msg);
    }
    return null;
  }
}
