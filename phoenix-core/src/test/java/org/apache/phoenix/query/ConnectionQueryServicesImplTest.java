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
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ConnectionQueryServicesImplTest {
    private static final PhoenixIOException PHOENIX_IO_EXCEPTION =
            new PhoenixIOException(new Exception("Test exception"));
    private TableDescriptor sysMutexTableDescCorrectTTL = TableDescriptorBuilder
            .newBuilder(TableName.valueOf(SYSTEM_MUTEX_NAME))
            .setColumnFamily(ColumnFamilyDescriptorBuilder
                    .newBuilder(SYSTEM_MUTEX_FAMILY_NAME_BYTES)
                    .setTimeToLive(TTL_FOR_MUTEX)
                    .build())
            .build();

    @Mock
    private ConnectionQueryServicesImpl mockCqs;

    @Mock
    private Admin mockAdmin;

    @Mock
    private ReadOnlyProps readOnlyProps;

    public static final TableDescriptorBuilder SYS_TASK_TDB = TableDescriptorBuilder
        .newBuilder(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME));
    public static final TableDescriptorBuilder SYS_TASK_TDB_SP = TableDescriptorBuilder
        .newBuilder(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME))
        .setRegionSplitPolicyClassName("abc");


    @Before
    public void setup() throws IOException, NoSuchFieldException,
            IllegalAccessException, SQLException {
        MockitoAnnotations.initMocks(this);
        Field props = ConnectionQueryServicesImpl.class
            .getDeclaredField("props");
        props.setAccessible(true);
        props.set(mockCqs, readOnlyProps);
        when(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin))
            .thenCallRealMethod();
        when(mockCqs.updateAndConfirmSplitPolicyForTask(SYS_TASK_TDB))
            .thenCallRealMethod();
        when(mockCqs.updateAndConfirmSplitPolicyForTask(SYS_TASK_TDB_SP))
            .thenCallRealMethod();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExceptionHandlingOnSystemNamespaceCreation() throws Exception {
        // Invoke the real methods for these two calls
        when(mockCqs.createSchema(any(List.class), anyString())).thenCallRealMethod();
        doCallRealMethod().when(mockCqs).ensureSystemTablesMigratedToSystemNamespace();
        // Do nothing for this method, just check that it was invoked later
        doNothing().when(mockCqs).createSysMutexTableIfNotExists(any(Admin.class));

        // Spoof out this call so that ensureSystemTablesUpgrade() will return-fast.
        when(mockCqs.getSystemTableNamesInDefaultNamespace(any(Admin.class)))
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
        verify(mockCqs).getSystemTableNamesInDefaultNamespace(any(Admin.class));

        try {
            // Verifies that the exception is propagated back to the caller
            mockCqs.createSchema(Collections.<Mutation> emptyList(), "");
        } catch (PhoenixIOException e) {
            assertEquals(PHOENIX_IO_EXCEPTION, e);
        }
    }

    @Test
    public void testSysMutexCheckReturnsFalseWhenTableAbsent() throws Exception {
        // Override the getDescriptor() call to throw instead
        doThrow(new TableNotFoundException())
                .when(mockAdmin)
                .getDescriptor(TableName.valueOf(SYSTEM_MUTEX_NAME));
        doThrow(new TableNotFoundException())
                .when(mockAdmin)
                .getDescriptor(TableName.valueOf(SYSTEM_SCHEMA_NAME, SYSTEM_MUTEX_TABLE_NAME));
        assertFalse(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin));
    }

    @Test
    public void testSysMutexCheckModifiesTTLWhenWrong() throws Exception {
        // Set the wrong TTL
        TableDescriptor sysMutexTableDescWrongTTL = TableDescriptorBuilder
                .newBuilder(TableName.valueOf(SYSTEM_MUTEX_NAME))
                .setColumnFamily(ColumnFamilyDescriptorBuilder
                        .newBuilder(SYSTEM_MUTEX_FAMILY_NAME_BYTES)
                        .setTimeToLive(HConstants.FOREVER)
                        .build())
                .build();
        when(mockAdmin.getDescriptor(TableName.valueOf(SYSTEM_MUTEX_NAME)))
                .thenReturn(sysMutexTableDescWrongTTL);

        assertTrue(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin));
        verify(mockAdmin, Mockito.times(1)).modifyTable(sysMutexTableDescCorrectTTL);
    }

    @Test
    public void testSysMutexCheckDoesNotModifyTableDescWhenTTLCorrect() throws Exception {
        when(mockAdmin.getDescriptor(TableName.valueOf(SYSTEM_MUTEX_NAME)))
                .thenReturn(sysMutexTableDescCorrectTTL);

        assertTrue(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin));
        verify(mockAdmin, Mockito.times(0)).modifyTable(any(TableDescriptor.class));
    }

    @Test
    public void testSysTaskSplitPolicy() throws Exception {
        assertTrue(mockCqs.updateAndConfirmSplitPolicyForTask(SYS_TASK_TDB));
        assertFalse(mockCqs.updateAndConfirmSplitPolicyForTask(SYS_TASK_TDB));
    }

    @Test
    public void testSysTaskSplitPolicyWithError() {
        try {
            mockCqs.updateAndConfirmSplitPolicyForTask(SYS_TASK_TDB_SP);
            fail("Split policy for SYSTEM.TASK cannot be updated");
        } catch (SQLException e) {
            assertEquals("ERROR 908 (43M19): REGION SPLIT POLICY is incorrect."
                + " Region split policy for table TASK is expected to be "
                + "among: [null, org.apache.phoenix.schema.SystemTaskSplitPolicy]"
                + " , actual split policy: abc tableName=SYSTEM.TASK",
                e.getMessage());
        }
    }
}
