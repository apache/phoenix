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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

/**
 * Unit test for
 * {@link TransformClient#enforceKnownTransformType(SystemTransformRecord, String, String)} — the
 * forward-compatibility gate used by {@link TransformClient#checkIsTransformNeeded}. An existing
 * SYSTEM.TRANSFORM row with an unrecognised transform type must defensively block a new ALTER on
 * the same table, regardless of the row's status, since this binary cannot reason about whether the
 * on-disk schema is in a state we understand.
 */
public class TransformClientUnknownTypeTest {

  @Test
  public void enforceKnownTransformTypeBlocksWhenExistingRecordIsUnknown() {
    SystemTransformRecord record = mock(SystemTransformRecord.class);
    when(record.getTransformType()).thenReturn(PTable.TransformType.UNKNOWN);

    try {
      TransformClient.enforceKnownTransformType(record, "S", "T");
      fail("Expected SQLException for UNKNOWN transform type on existing record");
    } catch (SQLException e) {
      assertEquals(SQLExceptionCode.CANNOT_TRANSFORM_ALREADY_TRANSFORMING_TABLE.getErrorCode(),
        e.getErrorCode());
    }
  }

  @Test
  public void enforceKnownTransformTypeAllowsKnownType() throws SQLException {
    SystemTransformRecord record = mock(SystemTransformRecord.class);
    when(record.getTransformType()).thenReturn(PTable.TransformType.METADATA_TRANSFORM);

    // Must not throw — a known transform type lets the rest of checkIsTransformNeeded run its
    // normal isActive() check.
    TransformClient.enforceKnownTransformType(record, "S", "T");
  }

  @Test
  public void enforceKnownTransformTypeAllowsNullExistingRecord() throws SQLException {
    // Null means there is no existing transform; the guard must be a no-op so the caller can
    // proceed to create a new transform.
    TransformClient.enforceKnownTransformType(null, "S", "T");
  }
}
