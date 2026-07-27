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
