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
package org.apache.phoenix.jdbc;

import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_HA_STALE_CRR_DETECTED_COUNT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.phoenix.exception.MutationBlockedIOException;
import org.apache.phoenix.exception.ReplicationFailoverInProgressException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.exception.StaleClusterRoleRecordException;
import org.apache.phoenix.monitoring.HAGroupMetricsManager;
import org.junit.After;
import org.junit.Test;

/**
 * Unit tests for {@link HighAvailabilityUtil}. The throwable-walk helpers are tested on each
 * detection branch (null guard, direct match, RetriesExhaustedWithDetailsException causes,
 * recursive {@code getCause()}); {@link HighAvailabilityUtil#toFailoverInProgressIfStale} is tested
 * on each mapping outcome.
 */
public class HighAvailabilityUtilTest {

  @Test
  public void testNullThrowableReturnsFalse() {
    assertFalse("null Throwable must short-circuit to false",
      HighAvailabilityUtil.isMutationBlockedIOExceptionExistsInThrowable(null));
  }

  @Test
  public void testDirectMutationBlockedIOExceptionMatches() {
    Throwable e = new MutationBlockedIOException("blocked");
    assertTrue("direct MutationBlockedIOException must match",
      HighAvailabilityUtil.isMutationBlockedIOExceptionExistsInThrowable(e));
  }

  @Test
  public void testRetriesExhaustedCausesAreScanned() {
    MutationBlockedIOException blocked = new MutationBlockedIOException("blocked-in-batch");
    IOException unrelated = new IOException("unrelated");
    RetriesExhaustedWithDetailsException rewde =
      new RetriesExhaustedWithDetailsException(Arrays.<Throwable> asList(unrelated, blocked),
        Collections.<Row> emptyList(), Collections.<String> emptyList());
    assertTrue("RetriesExhaustedWithDetailsException causes must be scanned for MBIOE",
      HighAvailabilityUtil.isMutationBlockedIOExceptionExistsInThrowable(rewde));
  }

  @Test
  public void testRecursiveGetCauseFindsNestedMutationBlocked() {
    MutationBlockedIOException root = new MutationBlockedIOException("root-cause");
    Throwable mid = new RuntimeException("mid", root);
    Throwable outer = new RuntimeException("outer", mid);
    assertTrue("recursive getCause() walk must surface a deeply-nested MBIOE",
      HighAvailabilityUtil.isMutationBlockedIOExceptionExistsInThrowable(outer));
  }

  @Test
  public void testReplicationFailoverInProgressDetection() {
    assertFalse(
      HighAvailabilityUtil.isReplicationFailoverInProgressExceptionExistsInThrowable(null));
    ReplicationFailoverInProgressException failover =
      new ReplicationFailoverInProgressException("failover");
    assertTrue(HighAvailabilityUtil.isReplicationFailoverInProgressExceptionExistsInThrowable(
      new RuntimeException("outer", failover)));
    RetriesExhaustedWithDetailsException rewde = new RetriesExhaustedWithDetailsException(
      Arrays.<Throwable> asList(new IOException("unrelated"), failover),
      Collections.<Row> emptyList(), Collections.<String> emptyList());
    assertTrue("RetriesExhaustedWithDetailsException causes must be scanned",
      HighAvailabilityUtil.isReplicationFailoverInProgressExceptionExistsInThrowable(rewde));
    assertFalse("A blocked write is not a failover failure",
      HighAvailabilityUtil.isReplicationFailoverInProgressExceptionExistsInThrowable(
        new MutationBlockedIOException("blocked")));
  }

  private static final String HA_GROUP_NAME = "haUtilTestGroup";

  @After
  public void tearDown() {
    // The stale-CRR mapping registers a per-group metrics source; do not leak it to other tests.
    HAGroupMetricsManager.remove(HA_GROUP_NAME);
  }

  private static HighAvailabilityGroup mockHaGroup() {
    HighAvailabilityGroup haGroup = mock(HighAvailabilityGroup.class);
    when(haGroup.getName()).thenReturn(HA_GROUP_NAME);
    when(haGroup.getGroupInfo())
      .thenReturn(new HighAvailabilityGroup.HAGroupInfo(HA_GROUP_NAME, "zk1", "zk2"));
    return haGroup;
  }

  private static PhoenixConnection mockConnection(HighAvailabilityGroup haGroup) {
    PhoenixConnection conn = mock(PhoenixConnection.class);
    when(conn.getHAGroup()).thenReturn(haGroup);
    return conn;
  }

  /** A commit error whose cause chain holds a stale-CRR rejection below a non-SQL wrapper. */
  private static SQLException staleCommitError() {
    return new SQLException("commit failed",
      new RuntimeException("wrapper", new StaleClusterRoleRecordException("stale CRR")));
  }

  private static long staleCrrCount() {
    return GLOBAL_HA_STALE_CRR_DETECTED_COUNT.getMetric().getValue();
  }

  @Test
  public void testToFailoverInProgressIfStaleIgnoresNonHaConnection() throws SQLException {
    SQLException e = staleCommitError();
    assertSame(e, HighAvailabilityUtil.toFailoverInProgressIfStale(null, e));
    assertSame(e, HighAvailabilityUtil.toFailoverInProgressIfStale(mockConnection(null), e));
  }

  @Test
  public void testToFailoverInProgressIfStaleKeepsFailoverInProgress() throws SQLException {
    HighAvailabilityGroup haGroup = mockHaGroup();
    SQLException e = new SQLExceptionInfo.Builder(SQLExceptionCode.FAILOVER_IN_PROGRESS)
      .setRootCause(new StaleClusterRoleRecordException("stale CRR")).build().buildException();
    long before = staleCrrCount();
    assertSame(e, HighAvailabilityUtil.toFailoverInProgressIfStale(mockConnection(haGroup), e));
    verify(haGroup, never()).refreshClusterRoleRecord(anyBoolean());
    assertEquals(before, staleCrrCount());
  }

  @Test
  public void testToFailoverInProgressIfStaleKeepsNonStaleError() throws SQLException {
    HighAvailabilityGroup haGroup = mockHaGroup();
    SQLException e = new SQLException("unrelated", new IOException("io"));
    long before = staleCrrCount();
    assertSame(e, HighAvailabilityUtil.toFailoverInProgressIfStale(mockConnection(haGroup), e));
    verify(haGroup, never()).refreshClusterRoleRecord(anyBoolean());
    assertEquals(before, staleCrrCount());
  }

  @Test
  public void testToFailoverInProgressIfStaleMapsNestedStaleError() throws SQLException {
    HighAvailabilityGroup haGroup = mockHaGroup();
    when(haGroup.refreshClusterRoleRecord(true)).thenReturn(true);
    SQLException e = staleCommitError();
    long before = staleCrrCount();

    SQLException mapped =
      HighAvailabilityUtil.toFailoverInProgressIfStale(mockConnection(haGroup), e);

    assertEquals(SQLExceptionCode.FAILOVER_IN_PROGRESS.getErrorCode(), mapped.getErrorCode());
    assertSame(e, mapped.getCause());
    assertTrue("message must keep the original error: " + mapped.getMessage(),
      mapped.getMessage().contains("commit failed"));
    assertEquals(0, mapped.getSuppressed().length);
    verify(haGroup, times(1)).refreshClusterRoleRecord(true);
    assertEquals(before + 1, staleCrrCount());
  }

  @Test
  public void testToFailoverInProgressIfStaleWhenRefreshReturnsFalse() throws SQLException {
    HighAvailabilityGroup haGroup = mockHaGroup();
    when(haGroup.refreshClusterRoleRecord(true)).thenReturn(false);
    SQLException e = staleCommitError();
    long before = staleCrrCount();

    SQLException mapped =
      HighAvailabilityUtil.toFailoverInProgressIfStale(mockConnection(haGroup), e);

    assertEquals(SQLExceptionCode.FAILOVER_IN_PROGRESS.getErrorCode(), mapped.getErrorCode());
    assertSame(e, mapped.getCause());
    assertEquals(0, mapped.getSuppressed().length);
    verify(haGroup, times(1)).refreshClusterRoleRecord(true);
    assertEquals(before + 1, staleCrrCount());
  }

  @Test
  public void testToFailoverInProgressIfStaleWhenRefreshThrows() throws SQLException {
    HighAvailabilityGroup haGroup = mockHaGroup();
    SQLException refreshError = new SQLException("CRR fetch failed");
    when(haGroup.refreshClusterRoleRecord(true)).thenThrow(refreshError);
    SQLException e = staleCommitError();
    long before = staleCrrCount();

    SQLException mapped =
      HighAvailabilityUtil.toFailoverInProgressIfStale(mockConnection(haGroup), e);

    assertEquals(SQLExceptionCode.FAILOVER_IN_PROGRESS.getErrorCode(), mapped.getErrorCode());
    assertSame(e, mapped.getCause());
    assertArrayEquals(new Throwable[] { refreshError }, mapped.getSuppressed());
    assertNull("the caller's exception must not be changed", e.getNextException());
    assertEquals(0, e.getSuppressed().length);
    verify(haGroup, times(1)).refreshClusterRoleRecord(true);
    assertEquals(before + 1, staleCrrCount());
  }

  @Test
  public void testToFailoverInProgressIfStaleRethrowsTransitionNotAllowed() throws SQLException {
    HighAvailabilityGroup haGroup = mockHaGroup();
    SQLException notAllowed =
      new SQLExceptionInfo.Builder(SQLExceptionCode.HA_ROLE_TRANSITION_NOT_ALLOWED).build()
        .buildException();
    when(haGroup.refreshClusterRoleRecord(true)).thenThrow(notAllowed);
    SQLException e = staleCommitError();
    long before = staleCrrCount();

    SQLException mapped =
      HighAvailabilityUtil.toFailoverInProgressIfStale(mockConnection(haGroup), e);

    assertSame(notAllowed, mapped);
    assertArrayEquals(new Throwable[] { e }, mapped.getSuppressed());
    verify(haGroup, times(1)).refreshClusterRoleRecord(true);
    assertEquals(before + 1, staleCrrCount());
  }
}
