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
package org.apache.phoenix.schema.transform;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

/**
 * Unit test for the forward-compatibility guard in
 * {@link Transform#finishForceCutover(PhoenixConnection, SystemTransformRecord)}, the completion
 * step of {@code doForceCutover}. When the record carries {@link PTable.TransformType#UNKNOWN} the
 * preceding cutover short-circuits, so this step must NOT mark the record COMPLETED and must NOT
 * commit; otherwise a transform this binary could not run would be falsely reported as finished. A
 * recognized transform type must still be marked COMPLETED and committed.
 */
public class TransformForceCutoverUnknownTypeTest {

  @Test
  public void finishForceCutoverDoesNotCompleteOrCommitWhenTypeIsUnknown() throws Exception {
    PhoenixConnection conn = mock(PhoenixConnection.class);
    SystemTransformRecord record = mock(SystemTransformRecord.class);
    when(record.getTransformType()).thenReturn(PTable.TransformType.UNKNOWN);
    when(record.getSchemaName()).thenReturn("S");
    when(record.getLogicalTableName()).thenReturn("T");
    when(record.getNewPhysicalTableName()).thenReturn("S.T_2");
    when(record.getTenantId()).thenReturn(null);

    Transform.finishForceCutover(conn, record);

    // No COMPLETED upsert and no commit may reach the connection: any interaction would mean the
    // record was being marked COMPLETED despite the cutover having been skipped.
    verifyNoInteractions(conn);
  }

  @Test
  public void finishForceCutoverCompletesAndCommitsForKnownType() throws Exception {
    PhoenixConnection conn = mock(PhoenixConnection.class);
    PreparedStatement stmt = mock(PreparedStatement.class);
    when(conn.prepareStatement(anyString())).thenReturn(stmt);

    SystemTransformRecord record = mock(SystemTransformRecord.class);
    when(record.getTransformType()).thenReturn(PTable.TransformType.METADATA_TRANSFORM);
    when(record.getSchemaName()).thenReturn("S");
    when(record.getLogicalTableName()).thenReturn("T");
    when(record.getNewPhysicalTableName()).thenReturn("S.T_2");
    when(record.getTenantId()).thenReturn(null);

    Transform.finishForceCutover(conn, record);

    // The guard must not interfere with the normal path: the record is upserted (COMPLETED) and
    // the transaction is committed.
    verify(conn).prepareStatement(anyString());
    verify(conn, times(1)).commit();
  }
}
