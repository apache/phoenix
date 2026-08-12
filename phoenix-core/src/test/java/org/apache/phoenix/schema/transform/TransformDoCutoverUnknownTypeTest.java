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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.junit.Test;

/**
 * Unit test for the forward-compatibility guard at the front of
 * {@link Transform#doCutover(PhoenixConnection, SystemTransformRecord)}: when the record carries
 * {@link PTable.TransformType#UNKNOWN}, the cutover must short-circuit with a warn log and perform
 * no further work against the connection.
 */
public class TransformDoCutoverUnknownTypeTest {

  @Test
  public void doCutoverSkipsAndDoesNotTouchConnectionWhenTypeIsUnknown() throws Exception {
    PhoenixConnection conn = mock(PhoenixConnection.class);
    SystemTransformRecord record = mock(SystemTransformRecord.class);
    when(record.getTransformType()).thenReturn(PTable.TransformType.UNKNOWN);
    when(record.getSchemaName()).thenReturn("S");
    when(record.getLogicalTableName()).thenReturn("T");
    when(record.getNewPhysicalTableName()).thenReturn("S.T_2");
    when(record.getTenantId()).thenReturn(null);

    Transform.doCutover(conn, record);

    // The early return must not have triggered any SQL or DDL via the connection. Any interaction
    // here would mean the guard let the cutover proceed against a record we cannot reason about.
    verifyNoInteractions(conn);
  }
}
