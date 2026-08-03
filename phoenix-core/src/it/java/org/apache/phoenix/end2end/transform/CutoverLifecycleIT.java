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
package org.apache.phoenix.end2end.transform;

import static org.apache.phoenix.query.QueryConstants.UNVERIFIED_BYTES;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessor.tasks.TransformMonitorTask;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.task.ServerTask;
import org.apache.phoenix.schema.task.SystemTaskParams;
import org.apache.phoenix.schema.task.Task;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Integration tests for the cutover lifecycle: after the physical-table pointer swap the transform
 * monitor waits for clients to refresh their cached pointer (status PENDING_PARTIAL_PASS) before
 * running the partial pass (status PARTIAL_PASS_RUNNING) and finally completing. The wait uses an
 * injectable clock so the tests never sleep for the real wait window.
 */
@Category(ParallelStatsDisabledTest.class)
public class CutoverLifecycleIT extends ParallelStatsDisabledIT {

  private static RegionCoprocessorEnvironment taskRegionEnvironment;

  private final Properties testProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);

  public CutoverLifecycleIT() throws IOException, InterruptedException {
    testProps.put(QueryServices.DEFAULT_IMMUTABLE_STORAGE_SCHEME_ATTRIB, "ONE_CELL_PER_COLUMN");
    testProps.put(QueryServices.DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB, "0");

    taskRegionEnvironment = (RegionCoprocessorEnvironment) getUtility()
      .getRSForFirstRegionInTable(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
      .getRegions(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME).get(0).getCoprocessorHost()
      .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
  }

  @Before
  public void setupTest() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      conn.createStatement()
        .execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME);
      conn.createStatement().execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME);
    }
  }

  @After
  public void tearDownTest() {
    EnvironmentEdgeManager.reset();
    TransformMonitorTask.resetJobLookupForTesting();
  }

  /**
   * Advances the monitor clock so it is comfortably past the persisted wait deadline, letting the
   * PENDING_PARTIAL_PASS branch proceed without waiting for the real 30-minute floor.
   */
  private static class AdvancingClock extends EnvironmentEdge {
    private long value = System.currentTimeMillis();

    @Override
    public long currentTime() {
      return value;
    }

    void setValue(long millis) {
      value = millis;
    }
  }

  private void runMonitorOnce() {
    TaskRegionObserver.SelfHealingTask task = new TaskRegionObserver.SelfHealingTask(
      taskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
    task.run();
  }

  private SystemTransformRecord fetch(PhoenixConnection conn, String schemaName, String tableName,
    String parentName) throws SQLException {
    return Transform.getTransformRecord(schemaName, tableName, parentName, null, conn);
  }

  /**
   * Drives the monitor, advancing the injected clock as needed, until the transform reaches the
   * requested status or we run out of attempts.
   */
  private SystemTransformRecord driveMonitorToStatus(PhoenixConnection conn, String schemaName,
    String tableName, String parentName, PTable.TransformStatus target, AdvancingClock clock)
    throws Exception {
    for (int i = 0; i < 60; i++) {
      SystemTransformRecord record = fetch(conn, schemaName, tableName, parentName);
      if (record != null && target.name().equals(record.getTransformStatus())) {
        return record;
      }
      // If we are inside the wait window, jump the clock past the deadline so the monitor advances.
      if (
        record != null
          && PTable.TransformStatus.PENDING_PARTIAL_PASS.name().equals(record.getTransformStatus())
          && record.getPendingPartialPassUntilTs() != null
      ) {
        clock.setValue(record.getPendingPartialPassUntilTs() + 1);
      }
      runMonitorOnce();
      Thread.sleep(200);
    }
    SystemTransformRecord record = fetch(conn, schemaName, tableName, parentName);
    fail("Ran out of attempts waiting for transform status " + target + " but it was "
      + (record == null ? "<no record>" : record.getTransformStatus()));
    return null;
  }

  private long countLinkRows(Connection conn, String schemaName, String tableName)
    throws SQLException {
    String sql = "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + " WHERE "
      + PhoenixDatabaseMetaData.TABLE_SCHEM + " = ? AND " + PhoenixDatabaseMetaData.TABLE_NAME
      + " = ? AND " + PhoenixDatabaseMetaData.LINK_TYPE + " = ?";
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, schemaName);
      stmt.setString(2, tableName);
      stmt.setByte(3, PTable.LinkType.TRANSFORMING_NEW_TABLE.getSerializedValue());
      ResultSet rs = stmt.executeQuery();
      rs.next();
      return rs.getLong(1);
    }
  }

  private long countUnverified(Connection conn, String physicalTableFullName) throws Exception {
    return org.apache.phoenix.end2end.index.ImmutableIndexExtendedIT
      .getRowCountForEmptyColValue(conn, physicalTableFullName, UNVERIFIED_BYTES);
  }

  /**
   * Counts every TRANSFORMING_NEW_TABLE link row that points at the given new physical table,
   * regardless of which logical entity (base table or child view) owns the row. The link's
   * COLUMN_FAMILY holds the full new physical table name, so this catches base-table and view links
   * alike.
   */
  private long countLinksToNewPhysicalTable(Connection conn, String newPhysicalFullName)
    throws SQLException {
    String sql = "SELECT COUNT(*) FROM " + PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + " WHERE "
      + PhoenixDatabaseMetaData.COLUMN_FAMILY + " = ? AND " + PhoenixDatabaseMetaData.LINK_TYPE
      + " = ?";
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, newPhysicalFullName);
      stmt.setByte(2, PTable.LinkType.TRANSFORMING_NEW_TABLE.getSerializedValue());
      ResultSet rs = stmt.executeQuery();
      rs.next();
      return rs.getLong(1);
    }
  }

  private void runCutoverLifecycle(boolean createIndex, boolean isImmutable) throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
    String newTableName = dataTableName + "_1";
    String indexName = "IDX_" + generateUniqueName();
    String createIndexStmt = "CREATE INDEX %s ON " + dataTableFullName + " (NAME) INCLUDE (ZIP) ";

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      int numOfRows = 10;
      TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, numOfRows,
        isImmutable ? " IMMUTABLE_ROWS=true" : "");
      if (createIndex) {
        conn.createStatement().execute(String.format(createIndexStmt, indexName));
      }

      // Kick off the transform. The monitor task is registered as part of the ALTER.
      conn.createStatement().execute("ALTER TABLE " + dataTableFullName
        + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

      SystemTransformRecord record = fetch(conn, schemaName, dataTableName, null);
      assertNotNull(record);

      List<Task.TaskRecord> taskRecordList = Task.queryTaskTable(conn, null);
      assertEquals(1, taskRecordList.size());
      assertEquals(PTable.TaskType.TRANSFORM_MONITOR, taskRecordList.get(0).getTaskType());

      // Drive to PENDING_PARTIAL_PASS: the pointer swap has happened and the monitor is waiting.
      SystemTransformRecord pendingPartial = driveMonitorToStatus(conn, schemaName, dataTableName,
        null, PTable.TransformStatus.PENDING_PARTIAL_PASS, clock);

      // The wait deadline is set and lies in the future relative to when it was computed.
      Long untilTs = pendingPartial.getPendingPartialPassUntilTs();
      assertNotNull("PENDING_PARTIAL_PASS must persist a wait deadline", untilTs);

      // The pointer swap is already visible and the dual-write link row is gone in the same cache
      // generation (the DELETE was committed together with the pointer swap).
      PTable swappedTable = conn.getTableNoCache(dataTableFullName);
      assertEquals(newTableName, swappedTable.getPhysicalName(true).getString());
      assertEquals("TRANSFORMING_NEW_TABLE link must be deleted at cutover", 0,
        countLinkRows(conn, schemaName, dataTableName));

      // While the clock is still before the deadline, the monitor must NOT advance.
      clock.setValue(untilTs - 1);
      runMonitorOnce();
      Thread.sleep(200);
      SystemTransformRecord stillWaiting = fetch(conn, schemaName, dataTableName, null);
      assertEquals("Monitor must no-op while clock < deadline",
        PTable.TransformStatus.PENDING_PARTIAL_PASS.name(), stillWaiting.getTransformStatus());

      // Once the clock reaches the deadline the monitor advances through the partial pass to
      // COMPLETED.
      clock.setValue(untilTs + 1);
      SystemTransformRecord completed = driveMonitorToStatus(conn, schemaName, dataTableName, null,
        PTable.TransformStatus.COMPLETED, clock);
      assertNotNull(completed);

      // No stranded unverified rows on the new physical table after the partial pass.
      assertEquals("No unverified rows should remain after the partial pass", 0,
        countUnverified(conn, completed.getNewPhysicalTableName()));

      // Sanity: pointer still points at the new physical table.
      PTable finalTable = conn.getTableNoCache(dataTableFullName);
      assertEquals(newTableName, finalTable.getPhysicalName(true).getString());
    }
  }

  @Test
  public void testCutoverLifecycleMutableTableWithoutIndex() throws Exception {
    runCutoverLifecycle(false, false);
  }

  @Test
  public void testCutoverLifecycleImmutableTableWithoutIndex() throws Exception {
    runCutoverLifecycle(false, true);
  }

  @Test
  public void testCutoverLifecycleTableWithSecondaryIndex() throws Exception {
    runCutoverLifecycle(true, false);
  }

  /**
   * Cutover on a base table that HAS a child view must tear down the dual-write links installed on
   * both the base table AND the child view. Asserts that after cutover reaches PENDING_PARTIAL_PASS
   * (the point at which the pointer swap and link teardown are committed) no TRANSFORMING_NEW_TABLE
   * link rows pointing at the new physical table remain, for the base table or the view.
   */
  @Test
  public void testCutoverTearsDownViewDualWriteLinks() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
    String newTableName = dataTableName + "_1";
    String newTableFullName = SchemaUtil.getTableName(schemaName, newTableName);
    String viewName = "VW_" + generateUniqueName();
    String viewFullName = SchemaUtil.getTableName(schemaName, viewName);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, 10, "");
      // Create a child view over the base table before starting the transform, so the transform
      // installs a dual-write link on the view as well as the base table.
      conn.createStatement()
        .execute("CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName);

      conn.createStatement().execute("ALTER TABLE " + dataTableFullName
        + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

      SystemTransformRecord record = fetch(conn, schemaName, dataTableName, null);
      assertNotNull(record);

      // Before cutover, links exist on both the base table and the view (2 total).
      assertEquals("Base table and child view must each have a TRANSFORMING_NEW_TABLE link", 2,
        countLinksToNewPhysicalTable(conn, newTableFullName));

      // Drive to PENDING_PARTIAL_PASS: pointer swap and link teardown are committed at this point.
      SystemTransformRecord pendingPartial = driveMonitorToStatus(conn, schemaName, dataTableName,
        null, PTable.TransformStatus.PENDING_PARTIAL_PASS, clock);
      assertNotNull(pendingPartial);

      PTable swappedTable = conn.getTableNoCache(dataTableFullName);
      assertEquals(newTableName, swappedTable.getPhysicalName(true).getString());

      // No TRANSFORMING_NEW_TABLE links (base OR view) may survive cutover.
      assertEquals("All TRANSFORMING_NEW_TABLE links (base + view) must be deleted at cutover", 0,
        countLinksToNewPhysicalTable(conn, newTableFullName));
      assertEquals("Base-table TRANSFORMING_NEW_TABLE link must be deleted at cutover", 0,
        countLinkRows(conn, schemaName, dataTableName));
      assertEquals("View TRANSFORMING_NEW_TABLE link must be deleted at cutover", 0,
        countLinkRows(conn, schemaName, viewName));
    }
  }

  /**
   * Seeds a PENDING_PARTIAL_PASS record plus its monitor task, then drives the monitor to prove it
   * honors the persisted wait deadline: while the injected clock is before the deadline a monitor
   * run is a no-op (still PENDING_PARTIAL_PASS), and once the clock reaches the deadline a monitor
   * run advances the record to PARTIAL_PASS_RUNNING. The PARTIAL_PASS_RUNNING transition is
   * committed before the partial-pass tool is launched, so it is observable even though the tool
   * run for this seeded (backing-table-less) record does not itself finish. No real sleep is used.
   */
  @Test
  public void testMonitorHonorsWaitDeadline() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      String logicalTableName = generateUniqueName();
      long deadline = clock.currentTime() + (60L * 60L * 1000L);

      SystemTransformRecord.SystemTransformBuilder builder =
        new SystemTransformRecord.SystemTransformBuilder();
      builder.setLogicalTableName(logicalTableName);
      builder.setNewPhysicalTableName(logicalTableName + "_1");
      // METADATA_TRANSFORM has a defined partial-transform variant, so the monitor is able to move
      // the record out of PENDING_PARTIAL_PASS once the wait window elapses.
      builder.setTransformType(PTable.TransformType.METADATA_TRANSFORM);
      builder.setTransformStatus(PTable.TransformStatus.PENDING_PARTIAL_PASS.name());
      builder.setPendingPartialPassUntilTs(deadline);
      Transform.upsertTransform(builder.build(), conn);

      SystemTransformRecord readBack = fetch(conn, null, logicalTableName, null);
      assertNotNull(readBack);
      // The BIGINT column round-trips exactly.
      assertEquals(Long.valueOf(deadline), readBack.getPendingPartialPassUntilTs());

      // Register the monitor task so runMonitorOnce() dispatches to this transform record.
      Timestamp startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
      ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder().setConn(conn)
        .setTaskType(PTable.TaskType.TRANSFORM_MONITOR).setTenantId(null).setSchemaName(null)
        .setTableName(logicalTableName).setTaskStatus(PTable.TaskStatus.CREATED.toString())
        .setData(null).setPriority(null).setStartTs(startTs).setEndTs(null).build());

      // Before the deadline: a monitor run must NOT advance the record.
      clock.setValue(deadline - 1);
      runMonitorOnce();
      Thread.sleep(200);
      SystemTransformRecord stillWaiting = fetch(conn, null, logicalTableName, null);
      assertEquals("Monitor must no-op while clock < deadline",
        PTable.TransformStatus.PENDING_PARTIAL_PASS.name(), stillWaiting.getTransformStatus());

      // At/after the deadline: a monitor run advances the record to PARTIAL_PASS_RUNNING, which is
      // committed before the (asynchronous) partial pass is launched.
      clock.setValue(deadline + 1);
      SystemTransformRecord running = driveMonitorToStatus(conn, null, logicalTableName, null,
        PTable.TransformStatus.PARTIAL_PASS_RUNNING, clock);
      assertEquals(PTable.TransformStatus.PARTIAL_PASS_RUNNING.name(),
        running.getTransformStatus());
    }
  }

  /**
   * Drives a real cutover to PENDING_PARTIAL_PASS and asserts the record carries the completed
   * full-pass job id at that point, then advances into PARTIAL_PASS_RUNNING and asserts the stale
   * full-pass job id never leaks into that state. The PENDING_PARTIAL_PASS -&gt;
   * PARTIAL_PASS_RUNNING transition clears the inherited full-pass job id; the launched partial
   * pass then registers its OWN job id under PARTIAL_PASS_RUNNING. Either way the record must never
   * carry the already-successful full-pass job id while PARTIAL_PASS_RUNNING, which is what keeps
   * the PARTIAL_PASS_RUNNING monitoring branch from mistaking that job for the partial-pass job and
   * prematurely completing the transform.
   */
  @Test
  public void testPartialPassRunningTransitionClearsInheritedJobId() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, 10, "");
      conn.createStatement().execute("ALTER TABLE " + dataTableFullName
        + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

      // Drive the full pass through cutover to PENDING_PARTIAL_PASS. The full pass ran a
      // TransformTool job, so the record carries that (now completed) job id here.
      SystemTransformRecord pendingPartial = driveMonitorToStatus(conn, schemaName, dataTableName,
        null, PTable.TransformStatus.PENDING_PARTIAL_PASS, clock);
      String fullPassJobId = pendingPartial.getTransformJobId();
      assertNotNull("Full-pass job id must be present on the PENDING_PARTIAL_PASS record",
        fullPassJobId);

      // Advance past the wait deadline and into PARTIAL_PASS_RUNNING. The transition nulls out the
      // inherited full-pass job id, and the partial pass registers its own job id afterward. The
      // record's job id while PARTIAL_PASS_RUNNING is therefore either null (before the partial
      // pass
      // registers) or the partial-pass job id -- but never the stale full-pass job id.
      clock.setValue(pendingPartial.getPendingPartialPassUntilTs() + 1);
      SystemTransformRecord running = driveMonitorToStatus(conn, schemaName, dataTableName, null,
        PTable.TransformStatus.PARTIAL_PASS_RUNNING, clock);
      assertTrue("PARTIAL_PASS_RUNNING must not carry the stale full-pass job id",
        running.getTransformJobId() == null || !fullPassJobId.equals(running.getTransformJobId()));
    }
  }

  /**
   * Defense-in-depth guard for the PARTIAL_PASS_RUNNING monitoring branch: a record that is
   * PARTIAL_PASS_RUNNING but still carries the FULL transform type together with a stale
   * (already-successful) full-pass job id must NOT be driven to COMPLETED. Only a partial-type
   * record whose own partial pass has registered may complete via that branch. Seeds exactly that
   * hazardous state and asserts a monitor run leaves the record untouched.
   */
  @Test
  public void testMonitorDoesNotCompletePartialPassRunningWithStaleFullPassJob() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    // Inject a completed-and-successful job matching the seeded full-pass job id. This is what
    // makes
    // the test discriminating: without the type gate the PARTIAL_PASS_RUNNING branch would look up
    // this (successful) job and drive the record straight to COMPLETED, failing the assertion
    // below.
    // The gate keeps a full-type record from ever consulting the lookup, so the record stays put.
    // Reset via @After resetJobLookupForTesting().
    Job successfulJob = Mockito.mock(Job.class);
    Mockito.when(successfulJob.isComplete()).thenReturn(true);
    Mockito.when(successfulJob.isSuccessful()).thenReturn(true);
    TransformMonitorTask.setJobLookupForTesting((configuration, jobId) -> successfulJob);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      String logicalTableName = generateUniqueName();

      SystemTransformRecord.SystemTransformBuilder builder =
        new SystemTransformRecord.SystemTransformBuilder();
      builder.setLogicalTableName(logicalTableName);
      builder.setNewPhysicalTableName(logicalTableName + "_1");
      // FULL transform type + a stale full-pass job id is the pre-fix hazard state: without the
      // type gate the branch would look up that job, find it successful, and complete the
      // transform.
      builder.setTransformType(PTable.TransformType.METADATA_TRANSFORM);
      builder.setTransformStatus(PTable.TransformStatus.PARTIAL_PASS_RUNNING.name());
      builder.setTransformJobId("job_000000000000_0001");
      Transform.upsertTransform(builder.build(), conn);

      // Register the monitor task so runMonitorOnce() dispatches to this transform record.
      Timestamp startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
      ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder().setConn(conn)
        .setTaskType(PTable.TaskType.TRANSFORM_MONITOR).setTenantId(null).setSchemaName(null)
        .setTableName(logicalTableName).setTaskStatus(PTable.TaskStatus.CREATED.toString())
        .setData(null).setPriority(null).setStartTs(startTs).setEndTs(null).build());

      runMonitorOnce();
      Thread.sleep(200);

      SystemTransformRecord after = fetch(conn, null, logicalTableName, null);
      assertNotNull(after);
      assertEquals(
        "Full-type PARTIAL_PASS_RUNNING record with a stale full-pass job must not be completed",
        PTable.TransformStatus.PARTIAL_PASS_RUNNING.name(), after.getTransformStatus());
    }
  }

  /**
   * A running partial pass must be observable in status PARTIAL_PASS_RUNNING (partial transform
   * type plus a non-null partial-pass job id), not in STARTED. Only then does the
   * PARTIAL_PASS_RUNNING branch of the monitor -- which alone can drive a repeatedly-failing
   * partial pass to a terminal FAILED state -- own the running partial pass. If the running partial
   * pass were left in STARTED (the state TransformTool.runTransform sets), the STARTED branch,
   * which has no terminal transition for an exhausted partial pass, would monitor it and could
   * strand it forever. Drives a real cutover through the partial pass and asserts the record is
   * observed at least once in PARTIAL_PASS_RUNNING with a partial type and a registered job id
   * before completing.
   */
  @Test
  public void testRunningPartialPassIsObservableAsPartialPassRunning() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, 10, "");
      conn.createStatement().execute("ALTER TABLE " + dataTableFullName
        + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

      // Drive to PENDING_PARTIAL_PASS, then past the wait deadline so the next monitor step kicks
      // the partial pass and re-asserts PARTIAL_PASS_RUNNING with the partial-pass job id.
      SystemTransformRecord pendingPartial = driveMonitorToStatus(conn, schemaName, dataTableName,
        null, PTable.TransformStatus.PENDING_PARTIAL_PASS, clock);
      clock.setValue(pendingPartial.getPendingPartialPassUntilTs() + 1);

      // Reach the running partial pass deterministically. driveMonitorToStatus fails the test if
      // the record never reaches PARTIAL_PASS_RUNNING, so a regression that ran the partial pass in
      // STARTED is caught here rather than passing silently.
      SystemTransformRecord running = driveMonitorToStatus(conn, schemaName, dataTableName, null,
        PTable.TransformStatus.PARTIAL_PASS_RUNNING, clock);
      assertTrue("Running partial pass must carry the partial transform type",
        PTable.TransformType.isPartialTransform(running.getTransformType()));
      assertNotNull("Running partial pass must have a registered partial-pass job id",
        running.getTransformJobId());

      // The transform must ultimately complete.
      SystemTransformRecord completed = driveMonitorToStatus(conn, schemaName, dataTableName, null,
        PTable.TransformStatus.COMPLETED, clock);
      assertNotNull(completed);
    }
  }

  /**
   * A partial pass whose job repeatedly fails must not strand the transform. Once retries are
   * exhausted the PARTIAL_PASS_RUNNING branch must drive the record to a terminal FAILED state
   * rather than re-entering PARTIAL_PASS_RUNNING (or STARTED) forever. Seeds the exact observable
   * state a running partial pass now produces -- PARTIAL_PASS_RUNNING, partial transform type, a
   * registered job id, and a retry count already at/above the maximum -- and injects a
   * completed-and-failed job through the monitor's job-lookup seam. Asserts a single monitor run
   * moves the record to FAILED.
   */
  @Test
  public void testPartialPassRetriesExhaustedReachesFailed() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    // Inject a job that is complete and unsuccessful so the PARTIAL_PASS_RUNNING branch takes the
    // failure path. The retry count is seeded above the maximum so the branch treats retries as
    // exhausted and must transition to FAILED.
    Job failedJob = Mockito.mock(Job.class);
    Mockito.when(failedJob.isComplete()).thenReturn(true);
    Mockito.when(failedJob.isSuccessful()).thenReturn(false);
    TransformMonitorTask.setJobLookupForTesting((configuration, jobId) -> failedJob);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      String logicalTableName = generateUniqueName();

      SystemTransformRecord.SystemTransformBuilder builder =
        new SystemTransformRecord.SystemTransformBuilder();
      builder.setLogicalTableName(logicalTableName);
      builder.setNewPhysicalTableName(logicalTableName + "_1");
      // This is the real observable state of a running partial pass: partial transform type,
      // PARTIAL_PASS_RUNNING, and a registered partial-pass job id.
      builder.setTransformType(PTable.TransformType.METADATA_TRANSFORM_PARTIAL);
      builder.setTransformStatus(PTable.TransformStatus.PARTIAL_PASS_RUNNING.name());
      builder.setTransformJobId("job_000000000000_0002");
      // Well above the default maximum retry count so retries are exhausted.
      builder.setTransformRetryCount(1000);
      Transform.upsertTransform(builder.build(), conn);

      Timestamp startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
      ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder().setConn(conn)
        .setTaskType(PTable.TaskType.TRANSFORM_MONITOR).setTenantId(null).setSchemaName(null)
        .setTableName(logicalTableName).setTaskStatus(PTable.TaskStatus.CREATED.toString())
        .setData(null).setPriority(null).setStartTs(startTs).setEndTs(null).build());

      SystemTransformRecord failed = driveMonitorToStatus(conn, null, logicalTableName, null,
        PTable.TransformStatus.FAILED, clock);
      assertEquals(
        "A partial pass whose retries are exhausted must reach terminal FAILED, not strand",
        PTable.TransformStatus.FAILED.name(), failed.getTransformStatus());
    }
  }

  /**
   * A partial-pass job that cannot be resolved (aged out of the job-history server,
   * resource-manager restart, etc.) must be treated like a failed job and routed through the
   * retry-budgeted path, not left to strand. A job that cannot be found cannot be confirmed
   * successful, so the PARTIAL_PASS_RUNNING branch must NOT return SKIPPED for it: after the
   * pointer swap the task sits in the STARTED task state and only CREATED/RETRY tasks are re-picked
   * up, so a SKIPPED result would leave the already-cut-over table's repairing partial pass forever
   * unrun. Seeds an exhausted PARTIAL_PASS_RUNNING record and injects a null (not-found) job
   * through the lookup seam; asserts a monitor run drives the record to terminal FAILED rather than
   * stranding it in PARTIAL_PASS_RUNNING.
   */
  @Test
  public void testPartialPassRunningJobNotFoundDoesNotStrand() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    // Inject a not-found job (the lookup returns null). The retry count is seeded above the maximum
    // so retries are exhausted and the not-found job must be treated as a terminal failure without
    // needing a real TransformTool run.
    TransformMonitorTask.setJobLookupForTesting((configuration, jobId) -> null);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      String logicalTableName = generateUniqueName();

      SystemTransformRecord.SystemTransformBuilder builder =
        new SystemTransformRecord.SystemTransformBuilder();
      builder.setLogicalTableName(logicalTableName);
      builder.setNewPhysicalTableName(logicalTableName + "_1");
      // The real observable state of a running partial pass: partial transform type,
      // PARTIAL_PASS_RUNNING, and a registered partial-pass job id (which the lookup cannot
      // resolve).
      builder.setTransformType(PTable.TransformType.METADATA_TRANSFORM_PARTIAL);
      builder.setTransformStatus(PTable.TransformStatus.PARTIAL_PASS_RUNNING.name());
      builder.setTransformJobId("job_000000000000_0004");
      // Well above the default maximum retry count so retries are exhausted.
      builder.setTransformRetryCount(1000);
      Transform.upsertTransform(builder.build(), conn);

      Timestamp startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
      ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder().setConn(conn)
        .setTaskType(PTable.TaskType.TRANSFORM_MONITOR).setTenantId(null).setSchemaName(null)
        .setTableName(logicalTableName).setTaskStatus(PTable.TaskStatus.CREATED.toString())
        .setData(null).setPriority(null).setStartTs(startTs).setEndTs(null).build());

      SystemTransformRecord failed = driveMonitorToStatus(conn, null, logicalTableName, null,
        PTable.TransformStatus.FAILED, clock);
      assertEquals(
        "A PARTIAL_PASS_RUNNING record whose job cannot be found must reach terminal "
          + "FAILED, not strand in PARTIAL_PASS_RUNNING",
        PTable.TransformStatus.FAILED.name(), failed.getTransformStatus());
    }
  }

  /**
   * Regression test: a partial-pass record left in PARTIAL_PASS_RUNNING with NO registered job id
   * must still reach a terminal state, not no-op forever. This is the state a failed initial
   * partial-pass kick produces: the cutover transition commits (PARTIAL_PASS_RUNNING, partial type,
   * job id cleared) and then kicks the partial pass; if that first TransformTool run fails before
   * its STARTED transition (connection acquisition, index-table creation, or argument validation
   * throws, so the run returns without registering a job id and without propagating an exception),
   * the committed row stays partial-type/PARTIAL_PASS_RUNNING with a null job id. The pointer swap
   * has already happened, so the record must reach a terminal state so the unverified rows are
   * either repaired (via retry) or the transform is surfaced as FAILED. A monitoring branch that
   * only acted when a job id was present would silently no-op on every subsequent scan, stranding
   * the already-cut-over table with no repairing partial pass and no terminal status.
   * <p>
   * Seeds exactly that observable state -- PARTIAL_PASS_RUNNING, partial transform type, a null job
   * id, retry count above the maximum -- and installs a job-lookup seam that FAILS if invoked, to
   * prove the null-job-id path reaches FAILED without ever consulting the (irrelevant) job lookup.
   * A single monitor run must move the record to terminal FAILED.
   */
  @Test
  public void testPartialPassRunningNullJobIdDoesNotStrand() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    // The job lookup must never be consulted when no job id is registered: a null job id is handled
    // directly as an unconfirmable (failed) partial pass. Fail loudly if the lookup is invoked.
    TransformMonitorTask.setJobLookupForTesting((configuration, jobId) -> {
      throw new AssertionError(
        "Job lookup must not be called when no partial-pass job id is registered");
    });

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      String logicalTableName = generateUniqueName();

      SystemTransformRecord.SystemTransformBuilder builder =
        new SystemTransformRecord.SystemTransformBuilder();
      builder.setLogicalTableName(logicalTableName);
      builder.setNewPhysicalTableName(logicalTableName + "_1");
      // The observable state a failed initial partial-pass kick leaves behind: partial transform
      // type and PARTIAL_PASS_RUNNING, but no job id was ever registered.
      builder.setTransformType(PTable.TransformType.METADATA_TRANSFORM_PARTIAL);
      builder.setTransformStatus(PTable.TransformStatus.PARTIAL_PASS_RUNNING.name());
      builder.setTransformJobId(null);
      // Well above the default maximum retry count so retries are exhausted and the branch must
      // move
      // straight to terminal FAILED without launching a real TransformTool run.
      builder.setTransformRetryCount(1000);
      Transform.upsertTransform(builder.build(), conn);

      Timestamp startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
      ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder().setConn(conn)
        .setTaskType(PTable.TaskType.TRANSFORM_MONITOR).setTenantId(null).setSchemaName(null)
        .setTableName(logicalTableName).setTaskStatus(PTable.TaskStatus.CREATED.toString())
        .setData(null).setPriority(null).setStartTs(startTs).setEndTs(null).build());

      SystemTransformRecord failed = driveMonitorToStatus(conn, null, logicalTableName, null,
        PTable.TransformStatus.FAILED, clock);
      assertEquals(
        "A PARTIAL_PASS_RUNNING record with no registered job id must reach terminal FAILED, not "
          + "strand in PARTIAL_PASS_RUNNING",
        PTable.TransformStatus.FAILED.name(), failed.getTransformStatus());
    }
  }

  /**
   * Regression test for the partial-pass retry-count accounting. A genuine retry must strictly
   * ADVANCE the retry count, tick by tick, so it eventually reaches the maximum and the
   * retries-exhausted -&gt; terminal FAILED transition becomes reachable.
   * <p>
   * Mechanism under test: on the retry path {@code kickPartialPass} skips the compensating
   * pre-decrement it applies to the very first (non-retry) partial-pass kick. TransformTool's
   * STARTED transition unconditionally increments the retry count and auto-commits it on
   * TransformTool's own connection, so skipping the decrement lets the persisted count strictly
   * increase. A prior implementation cancelled that increment with a matching decrement on the
   * retry path too, pinning the count so the terminal FAILED transition was unreachable and a
   * deterministically-failing partial pass resubmitted forever; this test fails against that
   * implementation (the count nets back to the seed) and passes once the decrement is skipped on
   * the retry path.
   * <p>
   * The retry's kick runs a real TransformTool invocation whose pre-run validation resolves both
   * the logical table and the new physical table before the STARTED increment is reached. The test
   * therefore drives a real cutover to PENDING_PARTIAL_PASS, which creates both backing tables
   * through the production machinery, before forcing the retry -- so validation passes and the
   * increment actually fires. A backing-table-less seed would throw inside validation before the
   * increment and would false-fail against the correct fix.
   */
  @Test
  public void testPartialPassRetryAdvancesRetryCount() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      // Real backing tables via the production ALTER/cutover machinery, so the retry's
      // TransformTool
      // run passes pre-run validation and reaches the STARTED increment.
      TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, 10, "");
      conn.createStatement().execute("ALTER TABLE " + dataTableFullName
        + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

      // Drive the real transform through cutover to PENDING_PARTIAL_PASS using the REAL job-lookup
      // seam: this drive depends on the actual full-pass MR job completing successfully. Only after
      // reaching PENDING_PARTIAL_PASS do we swap in the failing-job seam below -- installing it
      // earlier would make the monitor see the real full-pass job as failed and never cut over.
      // At this point both the logical table and the new physical table exist, so the partial-pass
      // TransformTool run will pass validation.
      SystemTransformRecord pendingPartial = driveMonitorToStatus(conn, schemaName, dataTableName,
        null, PTable.TransformStatus.PENDING_PARTIAL_PASS, clock);

      // Now force the PARTIAL_PASS_RUNNING branch down the retry path: the injected job is complete
      // and unsuccessful, so the monitor retries the partial pass rather than completing it.
      Job failedJob = Mockito.mock(Job.class);
      Mockito.when(failedJob.isComplete()).thenReturn(true);
      Mockito.when(failedJob.isSuccessful()).thenReturn(false);
      TransformMonitorTask.setJobLookupForTesting((configuration, jobId) -> failedJob);

      // Seed the observable state of a running-but-failing partial pass, one retry below the
      // maximum so the failure path takes a genuine retry (not the already-exhausted
      // straight-to-FAILED path). Build from the driven record so schema/logical/new-physical names
      // and the last-state timestamp carry over and the seeded record stays consistent with the
      // real backing tables (the partial pass validates the last-transform time against that ts).
      int seededRetryCount = PhoenixConfigurationUtil.DEFAULT_TRANSFORM_RETRY_COUNT - 1;
      SystemTransformRecord.SystemTransformBuilder builder =
        new SystemTransformRecord.SystemTransformBuilder(pendingPartial);
      builder.setTransformType(PTable.TransformType.METADATA_TRANSFORM_PARTIAL);
      builder.setTransformStatus(PTable.TransformStatus.PARTIAL_PASS_RUNNING.name());
      builder.setTransformJobId("job_000000000000_0003");
      builder.setTransformRetryCount(seededRetryCount);
      Transform.upsertTransform(builder.build(), conn);

      // Replace the monitor task chain left by the drive with a single fresh CREATED task, so the
      // next runMonitorOnce() dispatches exactly once to the seeded record.
      conn.createStatement().execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME);
      Timestamp startTs = new Timestamp(EnvironmentEdgeManager.currentTimeMillis());
      ServerTask.addTask(new SystemTaskParams.SystemTaskParamsBuilder().setConn(conn)
        .setTaskType(PTable.TaskType.TRANSFORM_MONITOR).setTenantId(null).setSchemaName(schemaName)
        .setTableName(dataTableName).setTaskStatus(PTable.TaskStatus.CREATED.toString())
        .setData(null).setPriority(null).setStartTs(startTs).setEndTs(null).build());

      // A monitor run takes the PARTIAL_PASS_RUNNING failure path and retries the partial pass;
      // TransformTool's STARTED transition then increments the (auto-committed) retry count. Poll
      // for the persisted count to exceed the seed.
      int observedRetryCount = seededRetryCount;
      for (int i = 0; i < 60; i++) {
        SystemTransformRecord record = fetch(conn, schemaName, dataTableName, null);
        if (record != null && record.getTransformRetryCount() > seededRetryCount) {
          observedRetryCount = record.getTransformRetryCount();
          break;
        }
        runMonitorOnce();
        Thread.sleep(200);
      }
      assertTrue("A genuine partial-pass retry must strictly advance the retry count (seed "
        + seededRetryCount + ", observed " + observedRetryCount
        + ") so retries-exhausted -> FAILED is reachable", observedRetryCount > seededRetryCount);
    }
  }

  /**
   * Verifies the new PENDING_PARTIAL_PASS_UNTIL_TS column exists on a freshly created
   * SYSTEM.TRANSFORM and that a BIGINT value, including NULL, round-trips through upsert and
   * select.
   */
  @Test
  public void testSystemTransformNewColumnReadWrite() throws Exception {
    try (Connection conn = DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      String logicalTableName = generateUniqueName();
      String upsert = "UPSERT INTO " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " ("
        + PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + ", "
        + PhoenixDatabaseMetaData.NEW_PHYS_TABLE_NAME + ", "
        + PhoenixDatabaseMetaData.PENDING_PARTIAL_PASS_UNTIL_TS + ") VALUES (?, ?, ?)";
      try (PreparedStatement stmt = conn.prepareStatement(upsert)) {
        stmt.setString(1, logicalTableName);
        stmt.setString(2, logicalTableName + "_1");
        stmt.setLong(3, 1234567890123L);
        stmt.execute();
      }
      String select = "SELECT " + PhoenixDatabaseMetaData.PENDING_PARTIAL_PASS_UNTIL_TS + " FROM "
        + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " WHERE "
        + PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + " = ?";
      try (PreparedStatement stmt = conn.prepareStatement(select)) {
        stmt.setString(1, logicalTableName);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1234567890123L, rs.getLong(1));
      }
      // A NULL round-trips as NULL (wasNull path).
      String logicalTableName2 = generateUniqueName();
      try (PreparedStatement stmt =
        conn.prepareStatement("UPSERT INTO " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME + " ("
          + PhoenixDatabaseMetaData.LOGICAL_TABLE_NAME + ", "
          + PhoenixDatabaseMetaData.NEW_PHYS_TABLE_NAME + ") VALUES (?, ?)")) {
        stmt.setString(1, logicalTableName2);
        stmt.setString(2, logicalTableName2 + "_1");
        stmt.execute();
      }
      SystemTransformRecord record = Transform.getTransformRecord(null, logicalTableName2, null,
        null, ((PhoenixConnection) conn));
      assertNotNull(record);
      assertNull(record.getPendingPartialPassUntilTs());
    }
  }

  /**
   * A logical table configured to never refresh its cache resolves its update-cache-frequency to
   * Long.MAX_VALUE. The persisted wait deadline must still be a bounded timestamp strictly in the
   * future: scaling Long.MAX_VALUE and adding it to the current time would overflow into a negative
   * (past) deadline, which would make the monitor skip the wait entirely and run the partial pass
   * with no delay -- the opposite of the intended behavior for a table whose clients cache
   * indefinitely. This drives a real cutover on a NEVER-cache table and asserts the deadline lands
   * within a sane bounded window rather than overflowing.
   */
  @Test
  public void testNeverCachedTableYieldsBoundedWaitDeadline() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      long beforeCutover = clock.currentTime();
      // UPDATE_CACHE_FREQUENCY=NEVER makes the logical table's cache frequency Long.MAX_VALUE.
      TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, 10,
        " UPDATE_CACHE_FREQUENCY=NEVER");
      conn.createStatement().execute("ALTER TABLE " + dataTableFullName
        + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

      SystemTransformRecord record = fetch(conn, schemaName, dataTableName, null);
      assertNotNull(record);

      SystemTransformRecord pendingPartial = driveMonitorToStatus(conn, schemaName, dataTableName,
        null, PTable.TransformStatus.PENDING_PARTIAL_PASS, clock);
      Long untilTs = pendingPartial.getPendingPartialPassUntilTs();
      assertNotNull(
        "PENDING_PARTIAL_PASS must persist a wait deadline even for a NEVER-cache table", untilTs);
      // The deadline must be strictly in the future (the overflow bug produced a negative value).
      assertTrue("Wait deadline for a NEVER-cache table must be in the future, was " + untilTs
        + " vs cutover time " + beforeCutover, untilTs > beforeCutover);
      // And it must be bounded, not effectively infinite: the wait is capped, so the deadline is at
      // most the cutover time plus the ceiling (with slack for clock advances during the drive).
      long ceiling = beforeCutover + (24L * 60L * 60L * 1000L) + (60L * 60L * 1000L);
      assertTrue("Wait deadline for a NEVER-cache table must be bounded by the ceiling, was "
        + untilTs + " vs ceiling " + ceiling, untilTs <= ceiling);
    }
  }

  /**
   * Regression test for the partial-pass repair-scan floor. After cutover the monitor waits for
   * clients to refresh their cached physical-table pointer; a stale client can still write to the
   * old pointer during that window. The partial pass must re-verify those writes, so its scan lower
   * bound has to reach back to the cutover instant -- not to lastStateTs, which is re-stamped only
   * after the wait window elapses. A floor derived from lastStateTs would exclude every write in
   * (cutover, cutover + waitWindow], the exact rows the wait exists to protect, and silently drop
   * them.
   * <p>
   * Drives a real cutover to PENDING_PARTIAL_PASS (which captures the cutover instant), advances
   * the clock past the wait deadline, and drives into PARTIAL_PASS_RUNNING (whose transition
   * re-stamps lastStateTs to the post-wait clock). Asserts the captured cutover instant survives
   * every transition unchanged and lies strictly before the post-wait lastStateTs, so the
   * cutover-derived repair floor genuinely covers the wait window that a lastStateTs-derived floor
   * would strand.
   */
  @Test
  public void testPartialPassRepairFloorCoversPostCutoverWaitWindow() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, 10, "");
      conn.createStatement().execute("ALTER TABLE " + dataTableFullName
        + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

      // Drive to PENDING_PARTIAL_PASS: the pointer swap has happened and the cutover instant is
      // captured on the record.
      SystemTransformRecord pendingPartial = driveMonitorToStatus(conn, schemaName, dataTableName,
        null, PTable.TransformStatus.PENDING_PARTIAL_PASS, clock);
      Long cutoverTs = pendingPartial.getCutoverTs();
      assertNotNull("Cutover instant must be captured at PENDING_PARTIAL_PASS", cutoverTs);
      Long deadline = pendingPartial.getPendingPartialPassUntilTs();
      assertNotNull("PENDING_PARTIAL_PASS must persist a wait deadline", deadline);
      // The cutover instant strictly precedes the wait deadline it seeds (the bounded wait is
      // always positive), so there is a genuine [cutover, deadline] window to protect.
      assertTrue("Cutover instant (" + cutoverTs + ") must strictly precede the wait deadline ("
        + deadline + ")", cutoverTs < deadline);

      // Advance well past the wait deadline (simulating the full client cache-refresh wait) and
      // drive into PARTIAL_PASS_RUNNING, whose transition re-stamps lastStateTs to the post-wait
      // clock.
      clock.setValue(deadline + 1);
      SystemTransformRecord running = driveMonitorToStatus(conn, schemaName, dataTableName, null,
        PTable.TransformStatus.PARTIAL_PASS_RUNNING, clock);

      // The cutover instant survives the transition unchanged...
      assertEquals("Cutover instant must be preserved across transitions", cutoverTs,
        running.getCutoverTs());
      // ...and lies strictly before the post-wait lastStateTs. The interval (cutoverTs,
      // lastStateTs] is precisely the window a lastStateTs-derived repair floor would strand; the
      // cutover-derived floor used by kickPartialPass covers it.
      assertNotNull(running.getTransformLastStateTs());
      assertTrue(
        "Post-wait lastStateTs (" + running.getTransformLastStateTs().getTime()
          + ") must be strictly after the cutover instant (" + cutoverTs + "); the intervening"
          + " window is exactly what a lastStateTs-derived repair floor would drop",
        running.getTransformLastStateTs().getTime() > cutoverTs);
    }
  }

  /**
   * Regression test for the crash-gated form of the repair-floor bug. doCutover commits the pointer
   * swap durably, but the cutover instant that anchors the repair floor is persisted at the
   * PENDING_CUTOVER handling. A crash after the swap but before that persist would, on re-entry,
   * re-capture a later instant and push the repair floor past the real cutover -- silently dropping
   * the post-cutover-window writes the partial pass exists to re-verify. The monitor must instead
   * reuse the instant already persisted on the record rather than re-capturing the current time.
   * <p>
   * Simulates the re-entry by driving a real cutover to PENDING_PARTIAL_PASS (pointer swapped,
   * cutover instant persisted), then resetting the record back to PENDING_CUTOVER preserving that
   * instant and the still-full transform type, advancing the clock far past it, and running the
   * monitor again. Asserts the re-entry advances the record without moving the persisted cutover
   * instant to the later clock -- doCutover is an idempotent no-op on the already-swapped pointer.
   */
  @Test
  public void testCutoverReentryReusesPersistedCutoverTs() throws Exception {
    AdvancingClock clock = new AdvancingClock();
    EnvironmentEdgeManager.injectEdge(clock);

    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

    try (PhoenixConnection conn =
      (PhoenixConnection) DriverManager.getConnection(getUrl(), testProps)) {
      conn.setAutoCommit(true);
      TransformToolIT.createTableAndUpsertRows(conn, dataTableFullName, 10, "");
      conn.createStatement().execute("ALTER TABLE " + dataTableFullName
        + " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

      // Drive to PENDING_PARTIAL_PASS: the pointer swap has happened and the cutover instant is
      // persisted on the record.
      SystemTransformRecord pendingPartial = driveMonitorToStatus(conn, schemaName, dataTableName,
        null, PTable.TransformStatus.PENDING_PARTIAL_PASS, clock);
      Long cutoverTs = pendingPartial.getCutoverTs();
      assertNotNull("Cutover instant must be persisted at PENDING_PARTIAL_PASS", cutoverTs);
      // Precondition for the PENDING_CUTOVER re-entry guard: the transform type is still full at
      // PENDING_PARTIAL_PASS (it flips to the partial variant only at PARTIAL_PASS_RUNNING), so a
      // record reset to PENDING_CUTOVER re-enters the same branch.
      assertFalse("Transform type must still be full at PENDING_PARTIAL_PASS",
        PTable.TransformType.isPartialTransform(pendingPartial.getTransformType()));

      // Simulate a crash re-entry: reset the record to PENDING_CUTOVER preserving the persisted
      // cutover instant and the full transform type, exactly the on-disk state a crash between the
      // durable pre-commit and the PENDING_PARTIAL_PASS transition would leave behind.
      Transform.updateTransformRecord(conn, pendingPartial, PTable.TransformStatus.PENDING_CUTOVER,
        null, cutoverTs);

      // Move the clock far past the cutover instant so a re-capture (the bug) would produce a
      // visibly later instant, then re-run the monitor.
      clock.setValue(cutoverTs + (30L * 24L * 60L * 60L * 1000L));
      SystemTransformRecord reentered = driveMonitorToStatus(conn, schemaName, dataTableName, null,
        PTable.TransformStatus.PENDING_PARTIAL_PASS, clock);

      // The re-entry advanced the record but reused the persisted cutover instant verbatim rather
      // than the far-later clock -- so the repair floor still tracks the real cutover.
      assertEquals("Re-entry must reuse the persisted cutover instant, not re-capture a later one",
        cutoverTs, reentered.getCutoverTs());
      // Sanity: the pointer still points at the new physical table (doCutover was an idempotent
      // no-op on re-entry).
      PTable finalTable = conn.getTableNoCache(dataTableFullName);
      assertEquals(dataTableName + "_1", finalTable.getPhysicalName(true).getString());
    }
  }
}
