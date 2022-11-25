/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LAST_DDL_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID;
import static org.apache.phoenix.query.QueryConstants.SYSTEM_SCHEMA_NAME;
import static org.apache.phoenix.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.exception.UpgradeInProgressException;
import org.apache.phoenix.exception.UpgradeRequiredException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.DelegateConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.LinkType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SequenceAllocation;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.phoenix.util.UpgradeUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class UpgradeIT extends ParallelStatsDisabledIT {
        
    @Test
    public void testUpgradeRequiredPreventsSQL() throws SQLException {
        String tableName = generateUniqueName();
        try (Connection conn = getConnection(false, null)) {
            conn.createStatement().execute(
                    "CREATE TABLE " + tableName
                            + " (PK1 VARCHAR NOT NULL, PK2 VARCHAR, KV1 VARCHAR, KV2 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2))");
            final ConnectionQueryServices delegate = conn.unwrap(PhoenixConnection.class).getQueryServices();
            ConnectionQueryServices servicesWithUpgrade = new DelegateConnectionQueryServices(delegate) {
                @Override
                public boolean isUpgradeRequired() {
                    return true;
                }
            };
            try (PhoenixConnection phxConn = new PhoenixConnection(servicesWithUpgrade, getUrl(), PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES))) {
                try {
                    phxConn.createStatement().execute(
                            "CREATE TABLE " + generateUniqueName()
                                    + " (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT PK PRIMARY KEY(K1,K2))");
                    fail("CREATE TABLE should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
                try {
                    phxConn.createStatement().execute("SELECT * FROM " + tableName);
                    fail("SELECT should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
                try {
                    phxConn.createStatement().execute("DELETE FROM " + tableName);
                    fail("DELETE should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
                try {
                    phxConn.createStatement().execute(
                            "CREATE INDEX " + tableName + "_IDX ON " + tableName + " (KV1) INCLUDE (KV2)" );
                    fail("CREATE INDEX should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
                try {
                    phxConn.createStatement().execute(
                            "UPSERT INTO " + tableName + " VALUES ('PK1', 'PK2', 'KV1', 'KV2')" );
                    fail("UPSERT VALUES should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
            }
        }
    }
    
    @Test
    public void testUpgradingConnectionBypassesUpgradeRequiredCheck() throws Exception {
        String tableName = generateUniqueName();
        try (Connection conn = getConnection(false, null)) {
            conn.createStatement()
                    .execute(
                            "CREATE TABLE "
                                    + tableName
                                    + " (PK1 VARCHAR NOT NULL, PK2 VARCHAR, KV1 VARCHAR, KV2 VARCHAR CONSTRAINT PK PRIMARY KEY(PK1, PK2))");
            final ConnectionQueryServices delegate = conn.unwrap(PhoenixConnection.class).getQueryServices();
            ConnectionQueryServices servicesWithUpgrade = new DelegateConnectionQueryServices(delegate) {
                @Override
                public boolean isUpgradeRequired() {
                    return true;
                }
            };
            try (PhoenixConnection phxConn = new PhoenixConnection(conn.unwrap(PhoenixConnection.class),
                    servicesWithUpgrade, conn.getClientInfo())) {
                // Because upgrade is required, this SQL should fail.
                try {
                    phxConn.createStatement().executeQuery("SELECT * FROM " + tableName);
                    fail("SELECT should have failed with UpgradeRequiredException");
                } catch (UpgradeRequiredException expected) {

                }
                // Marking connection as the one running upgrade should let SQL execute fine.
                phxConn.setRunningUpgrade(true);
                phxConn.createStatement().execute(
                        "UPSERT INTO " + tableName + " VALUES ('PK1', 'PK2', 'KV1', 'KV2')" );
                phxConn.commit();
                try (ResultSet rs = phxConn.createStatement().executeQuery("SELECT * FROM " + tableName)) {
                    assertTrue(rs.next());
                    assertFalse(rs.next());
                }
            }
        }
    }
    
    @Test
    public void testAcquiringAndReleasingUpgradeMutex() throws Exception {
        ConnectionQueryServices services = null;
        try (Connection conn = getConnection(false, null)) {
            services = conn.unwrap(PhoenixConnection.class).getQueryServices();
            assertTrue(((ConnectionQueryServicesImpl)services)
                    .acquireUpgradeMutex(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0));
            try {
                ((ConnectionQueryServicesImpl)services)
                        .acquireUpgradeMutex(MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0);
                fail();
            } catch (UpgradeInProgressException expected) {

            }
            ((ConnectionQueryServicesImpl)services).releaseUpgradeMutex();
        }
    }
    
    @Test
    public void testConcurrentUpgradeThrowsUpgradeInProgressException() throws Exception {
        final AtomicBoolean mutexStatus1 = new AtomicBoolean(false);
        final AtomicBoolean mutexStatus2 = new AtomicBoolean(false);
        final AtomicBoolean mutexStatus3 = new AtomicBoolean(true);
        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicInteger numExceptions = new AtomicInteger(0);
        try (Connection conn = getConnection(false, null)) {
            ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class)
                .getQueryServices();
            Callable<Void> task1 = new AcquireMutexRunnable(
                mutexStatus1, services, latch, numExceptions);
            Callable<Void> task2 = new AcquireMutexRunnable(
                mutexStatus2, services, latch, numExceptions);
            Callable<Void> task3 = new AcquireMutexRunnable(
                mutexStatus3, services, latch, numExceptions);
            ExecutorService executorService = Executors.newFixedThreadPool(2,
                new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("mutex-acquire-%d").build());
            Future<?> futureTask1 = executorService.submit(task1);
            Future<?> futureTask2 = executorService.submit(task2);
            latch.await();
            // make sure tasks didn't fail by calling get()
            futureTask1.get();
            futureTask2.get();
            executorService.submit(task3).get();
            assertTrue("One of the threads should have acquired the mutex",
                mutexStatus1.get() || mutexStatus2.get() || mutexStatus3.get());
            assertNotEquals("One and only one thread should have acquired the mutex ",
                mutexStatus1.get(), mutexStatus2.get());
            assertFalse("mutexStatus3 should never be true ",
                mutexStatus3.get());
            assertEquals("One and only one thread should have caught UpgradeRequiredException ",
                2, numExceptions.get());
            // release mutex only after all threads are done executing
            // so as to avoid case where one thread releases lock and only
            // after that another thread acquires lock (due to slow thread
            // execution)
            ((ConnectionQueryServicesImpl) services).releaseUpgradeMutex();
        }
    }
    
    private static class AcquireMutexRunnable implements Callable<Void> {
        
        private final AtomicBoolean acquireStatus;
        private final ConnectionQueryServices services;
        private final CountDownLatch latch;
        private final AtomicInteger numExceptions;

        private AcquireMutexRunnable(AtomicBoolean acquireStatus,
                ConnectionQueryServices services, CountDownLatch latch, AtomicInteger numExceptions) {
            this.acquireStatus = acquireStatus;
            this.services = services;
            this.latch = latch;
            this.numExceptions = numExceptions;
        }

        @Override
        public Void call() throws Exception {
            try {
                ((ConnectionQueryServicesImpl)services).acquireUpgradeMutex(
                        MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_7_0);
                acquireStatus.set(true);
            } catch (UpgradeInProgressException e) {
                acquireStatus.set(false);
                numExceptions.incrementAndGet();
            } finally {
                latch.countDown();
            }
            return null;
        }
        
    }

    private Connection createTenantConnection(String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), props);
    }

    private Connection getConnection(boolean tenantSpecific, String tenantId, boolean isNamespaceMappingEnabled)
        throws SQLException {
        if (tenantSpecific) {
            checkNotNull(tenantId);
            return createTenantConnection(tenantId);
        }
        Properties props = new Properties();
        if (isNamespaceMappingEnabled){
            props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        }
        return DriverManager.getConnection(getUrl(), props);
    }
    private Connection getConnection(boolean tenantSpecific, String tenantId) throws SQLException {
        return getConnection(tenantSpecific, tenantId, false);
    }
    
    @Test
    public void testMoveParentChildLinks() throws Exception {
        String schema = "S_" + generateUniqueName();
        String table1 = "T_" + generateUniqueName();
        String table2 = "T_" + generateUniqueName();
        String tableName = SchemaUtil.getTableName(schema, table1);
        String multiTenantTableName = SchemaUtil.getTableName(schema, table2);
        String viewName1 = "VIEW_" + generateUniqueName();
        String viewIndexName1 = "VIDX_" + generateUniqueName();
        String viewName2 = "VIEW_" + generateUniqueName();
        String viewIndexName2 = "VIDX_" + generateUniqueName();
        try (Connection conn = getConnection(false, null);
                Connection tenantConn = getConnection(true, "tenant1");
                Connection metaConn = getConnection(false, null)) {
            // create a non multi-tenant and multi-tenant table
            conn.createStatement()
                    .execute("CREATE TABLE IF NOT EXISTS " + tableName + " ("
                            + " TENANT_ID CHAR(15) NOT NULL, " + " PK1 integer NOT NULL, "
                            + "PK2 bigint NOT NULL, " + "V1 VARCHAR, " + "V2 VARCHAR "
                            + " CONSTRAINT NAME_PK PRIMARY KEY (TENANT_ID, PK1, PK2))");
            conn.createStatement()
                    .execute("CREATE TABLE IF NOT EXISTS " + multiTenantTableName + " ("
                            + " TENANT_ID CHAR(15) NOT NULL, " + " PK1 integer NOT NULL, "
                            + "PK2 bigint NOT NULL, " + "V1 VARCHAR, " + "V2 VARCHAR "
                            + " CONSTRAINT NAME_PK PRIMARY KEY (TENANT_ID, PK1, PK2)"
                            + " ) MULTI_TENANT= true");
            // create tenant and global view
            conn.createStatement().execute(
                "CREATE VIEW " + viewName1 + " (col VARCHAR) AS SELECT * FROM " + tableName);
            tenantConn.createStatement().execute("CREATE VIEW " + viewName2
                    + "(col VARCHAR) AS SELECT * FROM " + multiTenantTableName);
            // create index on the above views
            conn.createStatement()
                    .execute("create index " + viewIndexName1 + "  on " + viewName1 + "(col)");
            tenantConn.createStatement()
                    .execute("create index " + viewIndexName2 + " on " + viewName2 + "(col)");

            // query all parent -> child links
            Set<String> expectedChildLinkSet = getChildLinks(conn);

            // delete all the child links
            conn.createStatement().execute("DELETE FROM SYSTEM.CHILD_LINK WHERE LINK_TYPE = "
                    + LinkType.CHILD_TABLE.getSerializedValue());

            // re-create them by running the upgrade code
            PhoenixConnection phxMetaConn = metaConn.unwrap(PhoenixConnection.class);
            phxMetaConn.setRunningUpgrade(true);
            // create the parent-> child links in SYSTEM.CATALOG
            UpgradeUtil.addParentToChildLinks(phxMetaConn);
            // move the parent->child links to SYSTEM.CHILD_LINK
            UpgradeUtil.moveChildLinks(phxMetaConn);
            Set<String> actualChildLinkSet = getChildLinks(conn);

            assertEquals("Unexpected child links", expectedChildLinkSet, actualChildLinkSet);
        }
    }

    @Test
    public void testRemoveScnFromTaskAndLog() throws Exception {
        PhoenixConnection conn = getConnection(false, null).unwrap(PhoenixConnection.class);
        ConnectionQueryServicesImpl cqs = (ConnectionQueryServicesImpl)(conn.getQueryServices());
        //Set the SCN related properties on SYSTEM.STATS and SYSTEM.LOG
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("ALTER TABLE " +
                    PhoenixDatabaseMetaData.SYSTEM_LOG_NAME + " SET " +
                    KEEP_DELETED_CELLS + "='" + KeepDeletedCells.TRUE + "',\n" +
                    HConstants.VERSIONS + "='1000'\n");
            stmt.executeUpdate("ALTER TABLE " +
                    PhoenixDatabaseMetaData.SYSTEM_STATS_NAME + " SET " +
                    KEEP_DELETED_CELLS + "='" + KeepDeletedCells.TRUE + "',\n" +
                    HConstants.VERSIONS + "='1000'\n");
        }
        //Check that the HBase tables reflect the change
        PTable sysLogTable = PhoenixRuntime.getTable(conn, PhoenixDatabaseMetaData.SYSTEM_LOG_NAME);
        TableDescriptor sysLogDesc = utility.getAdmin().getDescriptor(SchemaUtil.getPhysicalTableName(
            PhoenixDatabaseMetaData.SYSTEM_LOG_NAME, cqs.getProps()));
        assertEquals(KeepDeletedCells.TRUE, sysLogDesc.getColumnFamily(
            SchemaUtil.getEmptyColumnFamily(sysLogTable)).getKeepDeletedCells());
        assertEquals(1000, sysLogDesc.getColumnFamily(
                SchemaUtil.getEmptyColumnFamily(sysLogTable)).getMaxVersions());

        PTable sysStatsTable = PhoenixRuntime.getTable(conn, PhoenixDatabaseMetaData.SYSTEM_STATS_NAME);
        TableDescriptor sysStatsDesc = utility.getAdmin().getDescriptor(SchemaUtil.getPhysicalTableName(
            PhoenixDatabaseMetaData.SYSTEM_STATS_NAME, cqs.getProps()));
        assertEquals(KeepDeletedCells.TRUE, sysStatsDesc.getColumnFamily(
            SchemaUtil.getEmptyColumnFamily(sysStatsTable)).getKeepDeletedCells());
        assertEquals(1000, sysStatsDesc.getColumnFamily(
            SchemaUtil.getEmptyColumnFamily(sysStatsTable)).getMaxVersions());

        //now call the upgrade code
        cqs.upgradeSystemLog(conn, new HashMap<String, String>());
        cqs.upgradeSystemStats(conn, new HashMap<String, String>());

        //Check that HBase tables are fixed
        sysLogDesc = utility.getAdmin().getDescriptor(SchemaUtil.getPhysicalTableName(
            PhoenixDatabaseMetaData.SYSTEM_LOG_NAME, cqs.getProps()));
        assertEquals(KeepDeletedCells.FALSE, sysLogDesc.getColumnFamily(
            SchemaUtil.getEmptyColumnFamily(sysLogTable)).getKeepDeletedCells());
        assertEquals(1, sysLogDesc.getColumnFamily(
                SchemaUtil.getEmptyColumnFamily(sysLogTable)).getMaxVersions());

        sysStatsDesc = utility.getAdmin().getDescriptor(SchemaUtil.getPhysicalTableName(
            PhoenixDatabaseMetaData.SYSTEM_STATS_NAME, cqs.getProps()));
        assertEquals(KeepDeletedCells.FALSE, sysStatsDesc.getColumnFamily(
            SchemaUtil.getEmptyColumnFamily(sysStatsTable)).getKeepDeletedCells());
        assertEquals(1, sysStatsDesc.getColumnFamily(
            SchemaUtil.getEmptyColumnFamily(sysStatsTable)).getMaxVersions());
    }

    @Test
    public void testCacheOnWritePropsOnSystemSequence() throws Exception {
        PhoenixConnection conn = getConnection(false, null).
            unwrap(PhoenixConnection.class);
        ConnectionQueryServicesImpl cqs = (ConnectionQueryServicesImpl)(conn.getQueryServices());

        TableDescriptor initialTD = utility.getAdmin().getDescriptor(
            SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME,
                cqs.getProps()));
        ColumnFamilyDescriptor initialCFD = initialTD.getColumnFamily(
            QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);

        // Confirm that the Cache-On-Write related properties are set
        // on SYSTEM.SEQUENCE during creation.
        assertEquals(Boolean.TRUE, initialCFD.isCacheBloomsOnWrite());
        assertEquals(Boolean.TRUE, initialCFD.isCacheDataOnWrite());
        assertEquals(Boolean.TRUE, initialCFD.isCacheIndexesOnWrite());

        // Check to see whether the Cache-On-Write related properties are set on
        // pre-existing tables too via the upgrade path. We do the below to test it :
        // 1. Explicitly disable the Cache-On-Write related properties on the table.
        // 2. Call the Upgrade Path on the table.
        // 3. Verify that the property is set after the upgrades too.
        ColumnFamilyDescriptorBuilder newCFBuilder =
            ColumnFamilyDescriptorBuilder.newBuilder(initialCFD);
        newCFBuilder.setCacheBloomsOnWrite(false);
        newCFBuilder.setCacheDataOnWrite(false);
        newCFBuilder.setCacheIndexesOnWrite(false);
        TableDescriptorBuilder newTD = TableDescriptorBuilder.newBuilder(initialTD);
        newTD.modifyColumnFamily(newCFBuilder.build());
        utility.getAdmin().modifyTable(newTD.build());

        // Check that the Cache-On-Write related properties are now disabled.
        TableDescriptor updatedTD = utility.getAdmin().getDescriptor(
            SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME,
                cqs.getProps()));
        ColumnFamilyDescriptor updatedCFD = updatedTD.getColumnFamily(
            QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
        assertEquals(Boolean.FALSE, updatedCFD.isCacheBloomsOnWrite());
        assertEquals(Boolean.FALSE, updatedCFD.isCacheDataOnWrite());
        assertEquals(Boolean.FALSE, updatedCFD.isCacheIndexesOnWrite());

        // Let's try upgrading the existing table - and see if the property is set on
        // during upgrades.
        cqs.upgradeSystemSequence(conn, new HashMap<String, String>());

        updatedTD = utility.getAdmin().getDescriptor(SchemaUtil.getPhysicalTableName(
            PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME, cqs.getProps()));
        updatedCFD = updatedTD.getColumnFamily(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
        assertEquals(Boolean.TRUE, updatedCFD.isCacheBloomsOnWrite());
        assertEquals(Boolean.TRUE, updatedCFD.isCacheDataOnWrite());
        assertEquals(Boolean.TRUE, updatedCFD.isCacheIndexesOnWrite());
    }

    private Set<String> getChildLinks(Connection conn) throws SQLException {
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT TENANT_ID, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, COLUMN_FAMILY FROM SYSTEM.CHILD_LINK WHERE LINK_TYPE = "
                            + LinkType.CHILD_TABLE.getSerializedValue());
        Set<String> childLinkSet = Sets.newHashSet();
        while (rs.next()) {
            String key =
                    rs.getString("TENANT_ID") + " " + rs.getString("TABLE_SCHEM") + " "
                            + rs.getString("TABLE_NAME") + " " + rs.getString("COLUMN_NAME") + " "
                            + rs.getString("COLUMN_FAMILY");
            childLinkSet.add(key);
        }
        return childLinkSet;
    }

    @Test
    public void testMergeViewIndexSequences() throws Exception {
        testMergeViewIndexSequencesHelper(false);
    }

    @Test
    public void testMergeViewIndexSequencesWithNamespaces() throws Exception {
        testMergeViewIndexSequencesHelper(true);
    }

    private void testMergeViewIndexSequencesHelper(boolean isNamespaceMappingEnabled) throws Exception {
        PhoenixConnection conn = getConnection(false, null, isNamespaceMappingEnabled).unwrap(PhoenixConnection.class);
        ConnectionQueryServices cqs = conn.getQueryServices();
        //First delete any sequences that may exist from previous tests
        conn.createStatement().execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE);
        conn.commit();
        cqs.clearCache();
        //Now make sure that running the merge logic doesn't cause a problem when there are no
        //sequences
        try (PhoenixConnection mockUpgradeScnTsConn = new PhoenixConnection(
                conn, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_15_0)) {
            UpgradeUtil.mergeViewIndexIdSequences(mockUpgradeScnTsConn);
        }
        PName tenantOne = PNameFactory.newName("TENANT_ONE");
        PName tenantTwo = PNameFactory.newName("TENANT_TWO");
        String tableName =
            SchemaUtil.getPhysicalHBaseTableName("TEST",
                "T_" + generateUniqueName(), isNamespaceMappingEnabled).getString();
        PName viewIndexTable = PNameFactory.newName(MetaDataUtil.getViewIndexPhysicalName(tableName));
        SequenceKey sequenceOne =
            createViewIndexSequenceWithOldName(cqs, tenantOne, viewIndexTable, isNamespaceMappingEnabled);
        SequenceKey sequenceTwo =
            createViewIndexSequenceWithOldName(cqs, tenantTwo, viewIndexTable, isNamespaceMappingEnabled);
        SequenceKey sequenceGlobal =
            createViewIndexSequenceWithOldName(cqs, null, viewIndexTable, isNamespaceMappingEnabled);

        List<SequenceAllocation> allocations = Lists.newArrayList();
        long val1 = 10;
        long val2 = 100;
        long val3 = 1000;
        allocations.add(new SequenceAllocation(sequenceOne, val1));
        allocations.add(new SequenceAllocation(sequenceGlobal, val2));
        allocations.add(new SequenceAllocation(sequenceTwo, val3));


        long[] incrementedValues = new long[3];
        SQLException[] exceptions = new SQLException[3];
        //simulate incrementing the view indexes
        cqs.incrementSequences(allocations, EnvironmentEdgeManager.currentTimeMillis(), incrementedValues,
            exceptions);
        for (SQLException e : exceptions) {
            assertNull(e);
        }

        try (PhoenixConnection mockUpgradeScnTsConn = new PhoenixConnection(
                conn, MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_15_0)) {
            UpgradeUtil.mergeViewIndexIdSequences(mockUpgradeScnTsConn);
        }
        //now check that there exists a sequence using the new naming convention, whose value is the
        //max of all the previous sequences for this table.

        List<SequenceAllocation> afterUpgradeAllocations = Lists.newArrayList();
        SequenceKey sequenceUpgrade = MetaDataUtil.getViewIndexSequenceKey(null, viewIndexTable, 0, isNamespaceMappingEnabled);
        afterUpgradeAllocations.add(new SequenceAllocation(sequenceUpgrade, 1));
        long[] afterUpgradeValues = new long[1];
        SQLException[] afterUpgradeExceptions = new SQLException[1];
        cqs.incrementSequences(afterUpgradeAllocations, EnvironmentEdgeManager.currentTimeMillis(), afterUpgradeValues, afterUpgradeExceptions);

        assertNull(afterUpgradeExceptions[0]);
        int safetyIncrement = 100;
        if (isNamespaceMappingEnabled){
            //since one sequence (the global one) will be reused as the "new" sequence,
            // it's already in cache and will reflect the final increment immediately
            assertEquals(Long.MIN_VALUE + val3 + safetyIncrement + 1, afterUpgradeValues[0]);
        } else {
            assertEquals(Long.MIN_VALUE + val3 + safetyIncrement, afterUpgradeValues[0]);
        }
    }

    private SequenceKey createViewIndexSequenceWithOldName(ConnectionQueryServices cqs, PName tenant, PName viewIndexTable, boolean isNamespaceMapped) throws SQLException {
        String tenantId = tenant == null ? null : tenant.getString();
        SequenceKey key = MetaDataUtil.getOldViewIndexSequenceKey(tenantId, viewIndexTable, 0, isNamespaceMapped);
        //Sequences are owned globally even if they contain a tenantId in the name
        String sequenceTenantId = isNamespaceMapped ? tenantId : null;
        cqs.createSequence(sequenceTenantId, key.getSchemaName(), key.getSequenceName(),
            Long.MIN_VALUE, 1, 1, Long.MIN_VALUE, Long.MAX_VALUE, false, EnvironmentEdgeManager.currentTimeMillis());
        return key;
    }

    @Test
    public void testUpgradeViewIndexIdDataType() throws Exception {
        byte[] rowKey = SchemaUtil.getColumnKey(null,
                SYSTEM_SCHEMA_NAME, SYSTEM_CATALOG_TABLE, VIEW_INDEX_ID,
                PhoenixDatabaseMetaData.TABLE_FAMILY);
        byte[] syscatBytes = PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME.getBytes();
        byte[] viewIndexIdTypeCellValueIn414 = PInteger.INSTANCE.toBytes(Types.SMALLINT);
        byte[] viewIndexIdTypeCellValueIn416 = PInteger.INSTANCE.toBytes(Types.BIGINT);

        try (PhoenixConnection conn = getConnection(false, null).
                unwrap(PhoenixConnection.class)) {
            // update the VIEW_INDEX_ID 0:DATAT_TYPE cell value to SMALLINT
            // (4.14 and prior version is a SMALLINT column)
            updateViewIndexIdColumnValue(rowKey, syscatBytes, viewIndexIdTypeCellValueIn414);
            assertTrue(UpgradeUtil.isUpdateViewIndexIdColumnDataTypeFromShortToLongNeeded(
                    conn, rowKey, syscatBytes));
            verifyExpectedCellValue(rowKey, syscatBytes, viewIndexIdTypeCellValueIn414);
            // calling UpgradeUtil to mock the upgrade VIEW_INDEX_ID data type to BIGINT
            UpgradeUtil.updateViewIndexIdColumnDataTypeFromShortToLong(conn, rowKey, syscatBytes);
            verifyExpectedCellValue(rowKey, syscatBytes, viewIndexIdTypeCellValueIn416);
            assertFalse(UpgradeUtil.isUpdateViewIndexIdColumnDataTypeFromShortToLongNeeded(
                    conn, rowKey, syscatBytes));
        } finally {
            updateViewIndexIdColumnValue(rowKey, syscatBytes, viewIndexIdTypeCellValueIn416);
        }
    }

    private void updateViewIndexIdColumnValue(byte[] rowKey, byte[] syscatBytes,
                                              byte[] newColumnValue) throws Exception {

        try (PhoenixConnection conn =
                     DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class);
            Table sysTable = conn.getQueryServices().getTable(syscatBytes)) {
            KeyValue viewIndexIdKV = new KeyValue(rowKey,
                    PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.DATA_TYPE_BYTES,
                    MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP,
                    newColumnValue);
            Put viewIndexIdPut = new Put(rowKey);
            viewIndexIdPut.add(viewIndexIdKV);
            sysTable.put(viewIndexIdPut);
        }
    }

    private void verifyExpectedCellValue(byte[] rowKey, byte[] syscatBytes,
                                         byte[] expectedDateTypeBytes) throws Exception {
        try(PhoenixConnection conn = getConnection(false, null).
                unwrap(PhoenixConnection.class);
            Table sysTable = conn.getQueryServices().getTable(syscatBytes)) {
            Scan s = new Scan();
            s.setRowPrefixFilter(rowKey);
            s.addColumn(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.DATA_TYPE_BYTES);
            ResultScanner scanner = sysTable.getScanner(s);
            Result result= scanner.next();
            Cell cell = result.getColumnLatestCell(
                    PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES,
                    PhoenixDatabaseMetaData.DATA_TYPE_BYTES);
            assertArrayEquals(expectedDateTypeBytes, CellUtil.cloneValue(cell));
        }
    }

    @Test
    public void testLastDDLTimestampBootstrap() throws Exception {
        //Create a table, view, and index
        String schemaName = "S_" + generateUniqueName();
        String tableName = "T_" + generateUniqueName();
        String viewName = "V_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        try (Connection conn = getConnection(false, null)) {
            conn.createStatement().execute(
                "CREATE TABLE " + fullTableName
                    + " (PK1 VARCHAR NOT NULL, PK2 VARCHAR, KV1 VARCHAR, KV2 VARCHAR CONSTRAINT " +
                    "PK PRIMARY KEY(PK1, PK2)) ");
            conn.createStatement().execute(
                "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName);

            //Now we null out any existing last ddl timestamps
            nullDDLTimestamps(conn);

            //now get the row timestamps for each header row
            long tableTS = getRowTimestampForMetadata(conn, schemaName, tableName,
                PTableType.TABLE);
            long viewTS = getRowTimestampForMetadata(conn, schemaName, viewName, PTableType.VIEW);

            UpgradeUtil.bootstrapLastDDLTimestamp(conn.unwrap(PhoenixConnection.class));
            long actualTableTS = getLastTimestampForMetadata(conn, schemaName, tableName,
                PTableType.TABLE);
            long actualViewTS = getLastTimestampForMetadata(conn, schemaName, viewName,
                PTableType.VIEW);
            assertEquals(tableTS, actualTableTS);
            assertEquals(viewTS, actualViewTS);

        }
    }

    private void nullDDLTimestamps(Connection conn) throws SQLException {
        String pkCols = TENANT_ID + ", " + TABLE_SCHEM +
            ", " + TABLE_NAME + ", " + COLUMN_NAME + ", " + COLUMN_FAMILY;
        String upsertSql =
            "UPSERT INTO " + SYSTEM_CATALOG_NAME + " (" + pkCols + ", " +
                LAST_DDL_TIMESTAMP + ")" + " " +
                "SELECT " + pkCols + ", NULL FROM " + SYSTEM_CATALOG_NAME + " " +
                "WHERE " + TABLE_TYPE + " IS NOT NULL";
        conn.createStatement().execute(upsertSql);
        conn.commit();
    }

    private long getRowTimestampForMetadata(Connection conn, String schemaName, String objectName,
                                            PTableType type) throws SQLException {
        String sql = "SELECT PHOENIX_ROW_TIMESTAMP() FROM " + SYSTEM_CATALOG_NAME + " WHERE " +
            " TENANT_ID IS NULL AND TABLE_SCHEM = ? AND TABLE_NAME = ? and TABLE_TYPE = ?";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, schemaName);
        stmt.setString(2, objectName);
        stmt.setString(3, type.getSerializedValue());

        ResultSet rs = stmt.executeQuery();
        assertNotNull(rs);
        assertTrue("Result set was empty!", rs.next());
        return rs.getLong(1);
    }

    private long getLastTimestampForMetadata(Connection conn, String schemaName, String objectName,
                                            PTableType type) throws SQLException {
        String sql = "SELECT LAST_DDL_TIMESTAMP FROM " + SYSTEM_CATALOG_NAME + " WHERE " +
            " TENANT_ID IS NULL AND TABLE_SCHEM = ? AND TABLE_NAME = ? and TABLE_TYPE = ?";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, schemaName);
        stmt.setString(2, objectName);
        stmt.setString(3, type.getSerializedValue());

        ResultSet rs = stmt.executeQuery();
        assertNotNull(rs);
        assertTrue("Result set was empty!", rs.next());
        return rs.getLong(1);
    }

}
