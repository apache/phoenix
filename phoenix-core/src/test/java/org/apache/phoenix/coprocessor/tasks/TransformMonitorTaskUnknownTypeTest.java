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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.junit.Test;

/**
 * Unit test for the forward-compatibility gate in
 * {@link TransformMonitorTask#skipIfUnknownTransformType(SystemTransformRecord)}: when the
 * persisted transform type is unrecognised, the monitor task must short-circuit with a SKIPPED
 * result rather than treat the row as a known transform and either crash or push it into a wrong
 * state branch.
 */
public class TransformMonitorTaskUnknownTypeTest {

  @Test
  public void skipIfUnknownTransformTypeReturnsSkippedForUnknownType() {
    SystemTransformRecord record = mock(SystemTransformRecord.class);
    when(record.getTransformType()).thenReturn(PTable.TransformType.UNKNOWN);
    when(record.getString()).thenReturn("schema=S logicalTable=T newPhysical=S.T_2");

    TaskRegionObserver.TaskResult result = TransformMonitorTask.skipIfUnknownTransformType(record);

    assertNotNull("UNKNOWN type must produce a non-null TaskResult", result);
    assertEquals(TaskRegionObserver.TaskResultCode.SKIPPED, result.getResultCode());
  }

  @Test
  public void skipIfUnknownTransformTypeReturnsNullForKnownType() {
    SystemTransformRecord record = mock(SystemTransformRecord.class);
    when(record.getTransformType()).thenReturn(PTable.TransformType.METADATA_TRANSFORM);

    TaskRegionObserver.TaskResult result = TransformMonitorTask.skipIfUnknownTransformType(record);

    assertNull("Known transform types must let normal processing proceed", result);
  }

  @Test
  public void skipIfUnknownTransformTypeReturnsNullForKnownPartialType() {
    SystemTransformRecord record = mock(SystemTransformRecord.class);
    when(record.getTransformType()).thenReturn(PTable.TransformType.METADATA_TRANSFORM_PARTIAL);

    TaskRegionObserver.TaskResult result = TransformMonitorTask.skipIfUnknownTransformType(record);

    assertNull("Partial transform type must let normal processing proceed", result);
  }
}
