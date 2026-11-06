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

import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ABORT_TO_ACTIVE_IN_SYNC;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ABORT_TO_STANDBY;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ACTIVE_IN_SYNC_TO_STANDBY;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.ACTIVE_WITH_OFFLINE_PEER;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.OFFLINE;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.STANDBY;
import static org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState.UNKNOWN;
import static org.apache.phoenix.jdbc.PhoenixHAAdminTool.RET_ARGUMENT_ERROR;
import static org.apache.phoenix.jdbc.PhoenixHAAdminTool.RET_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixHAAdminTool.FailoverMarker;
import org.junit.Test;

public class PhoenixHAAdminToolTest {

  @Test
  public void testStableFailoverPairRejectsTransitionalRoles() {
    assertTrue(PhoenixHAAdminTool.isStableFailoverPair(STANDBY, ACTIVE_IN_SYNC));
    assertTrue(PhoenixHAAdminTool.isStableFailoverPair(STANDBY, ACTIVE_NOT_IN_SYNC));

    assertFalse(PhoenixHAAdminTool.isStableFailoverPair(ABORT_TO_STANDBY, ACTIVE_IN_SYNC));
    assertFalse(PhoenixHAAdminTool.isStableFailoverPair(STANDBY, ABORT_TO_ACTIVE_IN_SYNC));
    assertFalse(PhoenixHAAdminTool.isStableFailoverPair(ACTIVE_IN_SYNC_TO_STANDBY, STANDBY));
    assertFalse(PhoenixHAAdminTool.isStableFailoverPair(DEGRADED_STANDBY, ACTIVE_IN_SYNC));
    assertFalse(PhoenixHAAdminTool.isStableFailoverPair(STANDBY, DEGRADED_STANDBY));
    assertFalse(PhoenixHAAdminTool.isStableFailoverPair(STANDBY, OFFLINE));
    assertFalse(PhoenixHAAdminTool.isStableFailoverPair(STANDBY, UNKNOWN));
    assertFalse(PhoenixHAAdminTool.isStableFailoverPair(STANDBY, ACTIVE_WITH_OFFLINE_PEER));
    assertFalse(
      PhoenixHAAdminTool.isStableFailoverPair(STANDBY, ACTIVE_NOT_IN_SYNC_WITH_OFFLINE_PEER));
    assertFalse(PhoenixHAAdminTool.isStableFailoverPair(null, ACTIVE_IN_SYNC));
    assertFalse(PhoenixHAAdminTool.isStableFailoverPair(STANDBY, null));
  }

  @Test
  public void testFailoverMarkerFormat() {
    assertEquals(
      "PHOENIX_HA_FAILOVER_MARKER marker=FAILOVER_START group=g1 ts=1762387954123"
        + " ts_utc=2025-11-06T00:12:34.123Z",
      PhoenixHAAdminTool.formatFailoverMarker(FailoverMarker.FAILOVER_START, "g1", 1762387954123L));
    assertEquals(
      "PHOENIX_HA_FAILOVER_MARKER marker=ABORT_TIMEOUT group=g2 ts=0"
        + " ts_utc=1970-01-01T00:00:00.000Z",
      PhoenixHAAdminTool.formatFailoverMarker(FailoverMarker.ABORT_TIMEOUT, "g2", 0L));
  }

  @Test
  public void testEarlyExitPrintsFailedEndMarker() throws Exception {
    assertEarlyExitMarkers("initiate-failover", "FAILOVER_START", "FAILOVER_FAILED");
    assertEarlyExitMarkers("abort-failover", "ABORT_START", "ABORT_FAILED");
  }

  private void assertEarlyExitMarkers(String command, String startMarker, String endMarker)
    throws Exception {
    // No ZK quorum in the configuration, so the command stops after the start marker.
    String output = runWithCapturedOutput(command, "-g", "g1");
    int start = output.indexOf("marker=" + startMarker + " group=g1 ts=");
    int end = output.indexOf("marker=" + endMarker + " group=g1 ts=");
    assertTrue(output, start >= 0 && start < end);

    // A missing HA group fails before the start marker, so no marker is printed.
    output = runWithCapturedOutput(command);
    assertFalse(output, output.contains(PhoenixHAAdminTool.FAILOVER_MARKER_PREFIX));
  }

  private String runWithCapturedOutput(String... args) throws Exception {
    PrintStream originalOut = System.out;
    PrintStream originalErr = System.err;
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(output));
      System.setErr(new PrintStream(output));
      PhoenixHAAdminTool tool = new PhoenixHAAdminTool();
      tool.setConf(new Configuration(false));
      assertEquals(RET_ARGUMENT_ERROR, tool.run(args));
    } finally {
      System.setOut(originalOut);
      System.setErr(originalErr);
    }
    return output.toString();
  }

  @Test
  public void testTopLevelHelpReturnsSuccess() throws Exception {
    PhoenixHAAdminTool tool = new PhoenixHAAdminTool();
    assertEquals(RET_SUCCESS, tool.run(new String[] { "help" }));
    assertEquals(RET_SUCCESS, tool.run(new String[] { "-h" }));
    assertEquals(RET_SUCCESS, tool.run(new String[] { "--help" }));
    assertEquals(RET_ARGUMENT_ERROR, tool.run(new String[0]));
    assertEquals(RET_ARGUMENT_ERROR, tool.run(new String[] { "unknown" }));
  }

  @Test
  public void testVersionOnlyUpdateExplainsThatAConfigurationFieldIsRequired() throws Exception {
    PrintStream originalErr = System.err;
    ByteArrayOutputStream error = new ByteArrayOutputStream();
    try {
      System.setErr(new PrintStream(error));
      PhoenixHAAdminTool tool = new PhoenixHAAdminTool();
      assertEquals(RET_ARGUMENT_ERROR, tool.run(new String[] { "update", "-g", "g", "-v", "1" }));
    } finally {
      System.setErr(originalErr);
    }
    assertTrue(error.toString().contains("in addition to"));
  }
}
