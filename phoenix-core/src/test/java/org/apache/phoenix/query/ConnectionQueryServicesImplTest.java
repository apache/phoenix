/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.query;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Test;

public class ConnectionQueryServicesImplTest {
    private static final PhoenixIOException PHOENIX_IO_EXCEPTION = new PhoenixIOException(new Exception("Test exception"));

    @SuppressWarnings("unchecked")
    @Test
    public void testExceptionHandlingOnSystemNamespaceCreation() throws Exception {
        ConnectionQueryServicesImpl cqs = mock(ConnectionQueryServicesImpl.class);
        // Invoke the real methods for these two calls
        when(cqs.createSchema(any(List.class), anyString())).thenCallRealMethod();
        doCallRealMethod().when(cqs).ensureSystemTablesUpgraded(any(ReadOnlyProps.class));

        // Spoof out this call so that ensureSystemTablesUpgrade() will return-fast.
        when(cqs.getSystemTableNames(any(HBaseAdmin.class))).thenReturn(Collections.<TableName> emptyList());

        // Throw a special exception to check on later
        doThrow(PHOENIX_IO_EXCEPTION).when(cqs).ensureNamespaceCreated(anyString());

        // Make sure that ensureSystemTablesUpgraded will try to migrate the system tables.
        Map<String,String> props = new HashMap<>();
        props.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        cqs.ensureSystemTablesUpgraded(new ReadOnlyProps(props));

        // Should be called after upgradeSystemTables()
        // Proves that execution proceeded
        verify(cqs).getSystemTableNames(any(HBaseAdmin.class));

        try {
            // Verifies that the exception is propagated back to the caller
            cqs.createSchema(Collections.<Mutation> emptyList(), "");
        } catch (PhoenixIOException e) {
            assertEquals(PHOENIX_IO_EXCEPTION, e);
        }
    }
}
