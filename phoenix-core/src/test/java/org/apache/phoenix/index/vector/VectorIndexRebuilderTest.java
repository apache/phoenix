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
package org.apache.phoenix.index.vector;

import static org.apache.phoenix.index.vector.VectorIndexRebuilder.isDue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.JacksonUtil;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class VectorIndexRebuilderTest {

  private static final long INTERVAL = 86400000L;
  private static final long NOW = 1_000_000_000_000L;
  private static final String INDEX = "S.IDX";

  @Test
  public void testNeverReconciledIsDue() {
    assertTrue(isDue(null, INTERVAL, NOW));
  }

  @Test
  public void testNotDueWithinInterval() {
    assertFalse(isDue(NOW, INTERVAL, NOW));
    assertFalse(isDue(NOW - INTERVAL + 1, INTERVAL, NOW));
  }

  @Test
  public void testDueAtInterval() {
    assertTrue(isDue(NOW - INTERVAL, INTERVAL, NOW));
    assertTrue(isDue(NOW - 10 * INTERVAL, INTERVAL, NOW));
  }

  @Test
  public void testZeroIntervalIsAlwaysDue() {
    assertTrue(isDue(NOW, 0, NOW));
  }

  /** Verifies that a future update time, for example from clock skew, does not make a task due. */
  @Test
  public void testFutureUpdateIsNotDue() {
    assertFalse(isDue(NOW + INTERVAL, INTERVAL, NOW));
    assertFalse(isDue(NOW + 1, 0, NOW));
  }

  @Test
  public void testRebuildTaskData() throws Exception {
    JsonNode data = JacksonUtil.getObjectReader()
      .readTree(VectorIndexRebuilder.rebuildTaskData(false, "SKEW_RATIO_EXCEEDED: \"x\""));
    assertFalse(data.path("manual").asBoolean(true));
    assertEquals("SKEW_RATIO_EXCEEDED: 'x'", data.path("reason").asText());
  }

  /** Verifies that an index disabled during the rebuild stays disabled with no state update. */
  @Test
  public void testActivateLeavesIndexDisabledDuringRebuild() throws Exception {
    PhoenixConnection conn = connection(PIndexState.DISABLE, MutationCode.TABLE_ALREADY_EXISTS);
    VectorIndexRebuilder.activate(conn, INDEX);
    verify(conn.getQueryServices(), never()).updateIndexState(any(), any());
  }

  /**
   * Verifies that activation does not fail if the endpoint refuses the state change. The endpoint
   * refuses the change after a concurrent disable, and the rebuild result stays.
   */
  @Test
  public void testActivateToleratesRefusedTransition() throws Exception {
    PhoenixConnection conn =
      connection(PIndexState.BUILDING, MutationCode.UNALLOWED_TABLE_MUTATION);
    VectorIndexRebuilder.activate(conn, INDEX);
    verify(conn.getQueryServices()).updateIndexState(any(), isNull());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testActivatePromotesBuildingIndex() throws Exception {
    PhoenixConnection conn = connection(PIndexState.BUILDING, MutationCode.TABLE_ALREADY_EXISTS);
    VectorIndexRebuilder.activate(conn, INDEX);
    ArgumentCaptor<List<Mutation>> metadata = ArgumentCaptor.forClass(List.class);
    verify(conn.getQueryServices()).updateIndexState(metadata.capture(), isNull());
    Cell state = ((Put) metadata.getValue().get(0))
      .get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.INDEX_STATE_BYTES)
      .get(0);
    assertEquals(PIndexState.ACTIVE,
      PIndexState.fromSerializedValue(CellUtil.cloneValue(state)[0]));
  }

  /**
   * Returns a mock connection that reads the index in {@code state} and returns {@code code} for
   * each index state update.
   */
  private static PhoenixConnection connection(PIndexState state, MutationCode code)
    throws Exception {
    PTable index = mock(PTable.class);
    when(index.getIndexState()).thenReturn(state);
    ConnectionQueryServices services = mock(ConnectionQueryServices.class);
    when(services.updateIndexState(any(), any()))
      .thenReturn(new MetaDataMutationResult(code, 0L, null));
    PhoenixConnection conn = mock(PhoenixConnection.class);
    when(conn.getTableNoCache(INDEX)).thenReturn(index);
    when(conn.getQueryServices()).thenReturn(services);
    return conn;
  }
}
