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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_FOR_MUTEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ConnectionQueryServicesImplTest {
    private static final PhoenixIOException PHOENIX_IO_EXCEPTION =
            new PhoenixIOException(new Exception("Test exception"));
    private HTableDescriptor sysMutexTableDescCorrectTTL =
            new HTableDescriptor(TableName.valueOf(SYSTEM_MUTEX_NAME))
                    .addFamily(new HColumnDescriptor(SYSTEM_MUTEX_FAMILY_NAME_BYTES)
                            .setTimeToLive(TTL_FOR_MUTEX));

    @Mock
    private ConnectionQueryServicesImpl mockCqs;
    @Mock
    private HBaseAdmin mockAdmin;

    @Before
    public void reset() throws IOException {
        MockitoAnnotations.initMocks(this);
        when(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin))
                .thenCallRealMethod();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExceptionHandlingOnSystemNamespaceCreation() throws Exception {
        // Invoke the real methods for these two calls
        when(mockCqs.createSchema(any(List.class), anyString())).thenCallRealMethod();
        doCallRealMethod().when(mockCqs).ensureSystemTablesMigratedToSystemNamespace();
        // Do nothing for this method, just check that it was invoked later
        doNothing().when(mockCqs).createSysMutexTableIfNotExists(any(HBaseAdmin.class));

        // Spoof out this call so that ensureSystemTablesUpgrade() will return-fast.
        when(mockCqs.getSystemTableNamesInDefaultNamespace(any(HBaseAdmin.class)))
                .thenReturn(Collections.<TableName> emptyList());

        // Throw a special exception to check on later
        doThrow(PHOENIX_IO_EXCEPTION).when(mockCqs).ensureNamespaceCreated(anyString());

        // Make sure that ensureSystemTablesMigratedToSystemNamespace will try to migrate
        // the system tables.
        Map<String,String> props = new HashMap<>();
        props.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        when(mockCqs.getProps()).thenReturn(new ReadOnlyProps(props));
        mockCqs.ensureSystemTablesMigratedToSystemNamespace();

        // Should be called after upgradeSystemTables()
        // Proves that execution proceeded
        verify(mockCqs).getSystemTableNamesInDefaultNamespace(any(HBaseAdmin.class));

        try {
            // Verifies that the exception is propagated back to the caller
            mockCqs.createSchema(Collections.<Mutation> emptyList(), "");
        } catch (PhoenixIOException e) {
            assertEquals(PHOENIX_IO_EXCEPTION, e);
        }
    }

    @Test
    public void testSysMutexCheckReturnsFalseWhenTableAbsent() throws Exception {
        // Override the getTableDescriptor() call to throw instead
        doThrow(new TableNotFoundException())
                .when(mockAdmin)
                .getTableDescriptor(Bytes.toBytes(SYSTEM_MUTEX_NAME));
        doThrow(new TableNotFoundException())
                .when(mockAdmin)
                .getTableDescriptor(TableName.valueOf(SYSTEM_SCHEMA_NAME,
                        SYSTEM_MUTEX_TABLE_NAME));
        assertFalse(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin));
    }

    @Test
    public void testSysMutexCheckModifiesTTLWhenWrong() throws Exception {
        // Set the wrong TTL
        HTableDescriptor sysMutexTableDesc =
                new HTableDescriptor(TableName.valueOf(SYSTEM_MUTEX_NAME))
                        .addFamily(new HColumnDescriptor(SYSTEM_MUTEX_FAMILY_NAME_BYTES)
                                .setTimeToLive(HConstants.FOREVER));
        when(mockAdmin.getTableDescriptor(Bytes.toBytes(SYSTEM_MUTEX_NAME)))
                .thenReturn(sysMutexTableDesc);

        assertTrue(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin));
        verify(mockAdmin, Mockito.times(1)).modifyTable(
                sysMutexTableDescCorrectTTL.getTableName(), sysMutexTableDescCorrectTTL);
    }

    @Test
    public void testSysMutexCheckDoesNotModifyTableDescWhenTTLCorrect() throws Exception {
        when(mockAdmin.getTableDescriptor(Bytes.toBytes(SYSTEM_MUTEX_NAME)))
                .thenReturn(sysMutexTableDescCorrectTTL);

        assertTrue(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin));
        verify(mockAdmin, Mockito.times(0)).modifyTable(
                any(TableName.class), any(HTableDescriptor.class));
    }
}
