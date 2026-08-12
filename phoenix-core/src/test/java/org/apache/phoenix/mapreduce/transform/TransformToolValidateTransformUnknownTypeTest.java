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
package org.apache.phoenix.mapreduce.transform;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.junit.Test;

/**
 * Unit test for the forward-compatibility fail-fast guard in
 * {@link TransformTool#validateTransform}: when the SYSTEM.TRANSFORM record carries an unrecognised
 * transform type, the tool must refuse to run with an operator-readable
 * {@link IllegalStateException} rather than attempt a transform it cannot reason about.
 */
public class TransformToolValidateTransformUnknownTypeTest {

  @Test
  public void validateTransformFailsFastForUnknownTransformType() throws Exception {
    PTable dataTable = mock(PTable.class);
    when(dataTable.getType()).thenReturn(PTableType.TABLE);
    when(dataTable.isTransactional()).thenReturn(false);

    SystemTransformRecord record = mock(SystemTransformRecord.class);
    when(record.getTransformType()).thenReturn(PTable.TransformType.UNKNOWN);
    when(record.getSchemaName()).thenReturn("S");
    when(record.getLogicalTableName()).thenReturn("T");
    when(record.getNewPhysicalTableName()).thenReturn("S.T_2");
    when(record.getTenantId()).thenReturn(null);

    TransformTool tool = new TransformTool();
    try {
      tool.validateTransform(dataTable, null, record);
      fail("Expected IllegalStateException for UNKNOWN transform type");
    } catch (IllegalStateException e) {
      String msg = e.getMessage();
      assertTrue("Expected operator-readable message; got: " + msg,
        msg != null && msg.contains("unrecognized transform type"));
      assertTrue("Expected message to mention the schema; got: " + msg, msg.contains("S"));
      assertTrue("Expected message to mention the logical table; got: " + msg, msg.contains("T"));
    }
  }

  @Test
  public void validateTransformAllowsKnownTransformType() throws Exception {
    PTable dataTable = mock(PTable.class);
    when(dataTable.getType()).thenReturn(PTableType.TABLE);
    when(dataTable.isTransactional()).thenReturn(false);

    SystemTransformRecord record = mock(SystemTransformRecord.class);
    when(record.getTransformType()).thenReturn(PTable.TransformType.METADATA_TRANSFORM);

    TransformTool tool = new TransformTool();
    // No exception — a known transform type passes the forward-compat guard. The remaining
    // index-table-shape checks inside validateTransform are skipped because argIndexTable is null.
    tool.validateTransform(dataTable, null, record);
  }
}
