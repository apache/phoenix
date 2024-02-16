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

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost
        .PHOENIX_META_DATA_COPROCESSOR_CONF_KEY;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_MUTATE_TABLE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.phoenix.coprocessor.BaseMetaDataEndpointObserver;
import org.apache.phoenix.coprocessor.MetaDataEndpointObserver;
import org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost
        .PhoenixMetaDataControllerEnvironment;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ConcurrentTableMutationException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for views dealing with other ongoing concurrent operations and
 * failure scenarios
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ViewConcurrencyAndFailureIT extends SplitSystemCatalogIT {

    protected String tableDDLOptions;
    protected String transactionProvider;
    protected boolean columnEncoded;

    private static final String FAILED_VIEWNAME = SchemaUtil.getTableName(
            SCHEMA2, "FAILED_VIEW_" + generateUniqueName());
    private static final String SLOW_VIEWNAME_PREFIX =
            SchemaUtil.getTableName(SCHEMA2, "SLOW_VIEW");

    private static volatile CountDownLatch latch1 = null;
    private static volatile CountDownLatch latch2 = null;
    private static volatile boolean throwExceptionInChildLinkPreHook = false;
    private static volatile boolean slowDownAddingChildLink = false;

    public ViewConcurrencyAndFailureIT(String transactionProvider,
            boolean columnEncoded) {
        StringBuilder optionBuilder = new StringBuilder();
        this.transactionProvider = transactionProvider;
        this.columnEncoded = columnEncoded;
        if (transactionProvider != null) {
            optionBuilder.append(" TRANSACTION_PROVIDER='")
                    .append(transactionProvider)
                    .append("'");
        }
        if (!columnEncoded) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    // name is used by failsafe as file name in reports
    @Parameters(name="ViewConcurrencyAndFailureIT_transactionProvider={0}, "
            + "columnEncoded={1}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            // OMID does not support column encoding
            { "OMID", false },
            { null, false }, { null, true }});
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        NUM_SLAVES_BASE = 6;
        boolean splitSystemCatalog = (driver == null);
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.PHOENIX_ACLS_ENABLED, "true");
        serverProps.put(PHOENIX_META_DATA_COPROCESSOR_CONF_KEY,
                TestMetaDataRegionObserver.class.getName());
        serverProps.put("hbase.coprocessor.abortonerror", "false");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                ReadOnlyProps.EMPTY_PROPS);
        // Split SYSTEM.CATALOG once after the mini-cluster is started
        if (splitSystemCatalog) {
            // splitSystemCatalog is incompatible with the balancer chore
            getUtility().getHBaseCluster().getMaster().balanceSwitch(false);
            splitSystemCatalog();
        }
    }

    @After
    public void cleanup() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        latch1 = null;
        latch2 = null;
        throwExceptionInChildLinkPreHook = false;
        slowDownAddingChildLink = false;
        assertFalse("refCount leaked", refCountLeaked);
    }

    @Test
    public void testChildViewCreationFails() throws Exception {
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {

            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String failingViewName = FAILED_VIEWNAME;
            String succeedingViewName = SchemaUtil.getTableName(SCHEMA3,
                    generateUniqueName());

            String createTableDdl = "CREATE TABLE " + fullTableName
                    + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)"
                    + tableDDLOptions;
            stmt.execute(createTableDdl);

            String createViewDdl = "CREATE VIEW " + failingViewName
                    + " (v2 VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE k > 5";
            try {
                stmt.execute(createViewDdl);
                fail();
            } catch (PhoenixIOException ignored) {
            }
            createViewDdl = "CREATE VIEW " + succeedingViewName
                    + "(v2 VARCHAR) AS SELECT * FROM " + fullTableName
                    + " WHERE k > 10";
            stmt.execute(createViewDdl);

            // the first child view should not exist
            try {
                conn.getTableNoCache(failingViewName);
                fail();
            } catch (TableNotFoundException ignored) {
            }

            // we should be able to load the table
            conn.getTableNoCache(fullTableName);
            // we should be able to load the second view
            conn.getTableNoCache(succeedingViewName);
        }
    }

    @Test
    public void testConcurrentViewCreationAndTableDrop() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String fullViewName1 = SLOW_VIEWNAME_PREFIX + "_"
                    + generateUniqueName();
            latch1 = new CountDownLatch(1);
            latch2 = new CountDownLatch(1);
            String tableDdl = "CREATE TABLE " + fullTableName +
                    "  (k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER, v2 INTEGER)"
                    + tableDDLOptions;
            stmt.execute(tableDdl);

            ExecutorService executorService = newSingleThreadExecutor(
                    new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    t.setPriority(Thread.MIN_PRIORITY);
                    return t;
                }
            });

            // When dropping a table, we check the parent->child links in the
            // SYSTEM.CHILD_LINK table and check that cascade is set, if it
            // isn't, we throw an exception (see ViewUtil.hasChildViews).
            // After PHOENIX-4810, we first send a client-server RPC to add
            // parent->child links to SYSTEM.CHILD_LINK and then add metadata
            // for the view in SYSTEM.CATALOG, so we must delay link creation
            // so that the drop table does not fail
            slowDownAddingChildLink = true;
            // create the view in a separate thread (which will take some time
            // to complete)
            Future<Exception> future = executorService.submit(
                    new CreateViewRunnable(fullTableName, fullViewName1));
            // wait till the thread makes the rpc to create the view
            latch1.await();
            tableDdl = "DROP TABLE " + fullTableName;

            // Revert this flag since we don't want to wait in preDropTable
            slowDownAddingChildLink = false;
            // drop table goes through first and so the view creation
            // should fail
            stmt.execute(tableDdl);
            latch2.countDown();

            Exception e = future.get();
            assertTrue("Expected TableNotFoundException since drop table"
                            + " goes through first",
                    e instanceof TableNotFoundException &&
                            fullTableName.equals(((TableNotFoundException) e)
                                    .getTableName()));

        }
    }

    @Test
    public void testChildLinkCreationFailThrowsException() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String fullViewName1 = SchemaUtil.getTableName(SCHEMA3,
                    generateUniqueName());
            // create base table
            String tableDdl = "CREATE TABLE " + fullTableName
                    + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)"
                    + tableDDLOptions;
            stmt.execute(tableDdl);

            // Throw an exception in ChildLinkMetaDataEndpoint while adding
            // parent->child links to simulate a failure
            throwExceptionInChildLinkPreHook = true;
            // create a view
            String ddl = "CREATE VIEW " + fullViewName1
                    + " (v2 VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE k = 6";
            try {
                stmt.execute(ddl);
                fail("Should have thrown an exception");
            } catch(SQLException sqlE) {
                assertEquals("Expected a different Error code",
                        SQLExceptionCode.UNABLE_TO_CREATE_CHILD_LINK
                                .getErrorCode(), sqlE.getErrorCode());
            }
        }
    }

    @Test
    public void testConcurrentAddSameColumnDifferentType() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String fullViewName1 = SLOW_VIEWNAME_PREFIX + "_"
                    + generateUniqueName();
            String fullViewName2 = SchemaUtil.getTableName(SCHEMA3,
                    generateUniqueName());
            // create base table
            String tableDdl = "CREATE TABLE " + fullTableName
                    + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)"
                    + tableDDLOptions;
            stmt.execute(tableDdl);
            // create a view
            String ddl = "CREATE VIEW " + fullViewName1
                    + " (v2 VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE k = 6";
            stmt.execute(ddl);

            latch1 = new CountDownLatch(1);
            latch2 = new CountDownLatch(1);
            ExecutorService executorService = newSingleThreadExecutor(
                    new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    t.setPriority(Thread.MIN_PRIORITY);
                    return t;
                }
            });

            // add a column with the same name and different type to the view
            // in a separate thread (which will take some time to complete)
            Future<Exception> future = executorService.submit(
                    new AddColumnRunnable(fullViewName1, null));
            // wait till the thread makes the rpc to add the column
            boolean result = latch1.await(2, TimeUnit.MINUTES);
            if (!result) {
                fail("The create view rpc look too long");
            }
            tableDdl = "ALTER TABLE " + fullTableName + " ADD v3 INTEGER";
            try {
                // add the same column to the base table with a different type
                stmt.execute(tableDdl);
                fail("Adding a column to a base table should fail when "
                        + "the same column of a different type is being added "
                        + "to a child view");
            } catch (ConcurrentTableMutationException ignored) {
            }
            latch2.countDown();

            Exception e = future.get();
            assertNull(e);

            // add the same column to the another view  to ensure that the cell
            // used to prevent concurrent modifications was removed
            ddl = "CREATE VIEW " + fullViewName2
                    + " (v2 VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE k = 6";
            stmt.execute(ddl);
            tableDdl = "ALTER VIEW " + fullViewName2 + " ADD v3 INTEGER";
            stmt.execute(tableDdl);
        }
    }

    // Test that we do a checkAndPut even in case of tenant-specific connections
    // (see PHOENIX-6075)
    @Test
    public void testConcurrentAddSameColumnDifferentTypeTenantView()
            throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName1 = SLOW_VIEWNAME_PREFIX + "_"
                + generateUniqueName();
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3,
                generateUniqueName());
        String tenantId = "t001";
        String tableDdl = "CREATE TABLE " + fullTableName
                + " (TENANT_ID VARCHAR NOT NULL, k INTEGER NOT NULL, v1 DATE "
                + "CONSTRAINT PK"
                + " PRIMARY KEY (TENANT_ID, k)) MULTI_TENANT=true"
                + (!tableDDLOptions.isEmpty() ? ", " : "") + tableDDLOptions;
        String viewDdl = "CREATE VIEW " + fullViewName1
                + " (v2 VARCHAR) AS SELECT * FROM "
                + fullTableName + " WHERE k = 6";

        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            // create a multi-tenant base table
            stmt.execute(tableDdl);

            Properties props = new Properties();
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            try (Connection tenantConn = DriverManager.getConnection(getUrl(),
                    props);
                    Statement tenantStmt = tenantConn.createStatement()) {
                // create a tenant-specific view
                tenantStmt.execute(viewDdl);
            }

            latch1 = new CountDownLatch(1);
            latch2 = new CountDownLatch(1);
            ExecutorService executorService = newSingleThreadExecutor(
                    new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    t.setPriority(Thread.MIN_PRIORITY);
                    return t;
                }
            });

            // add a column with the same name and different type to the
            // tenant-specific view in a
            // separate thread (which will take some time to complete)
            Future<Exception> future = executorService.submit(
                    new AddColumnRunnable(fullViewName1, tenantId));
            // wait till the thread makes the rpc to add the column
            boolean result = latch1.await(2, TimeUnit.MINUTES);
            if (!result) {
                fail("The tenant-specific view creation rpc look too long");
            }
            tableDdl = "ALTER TABLE " + fullTableName + " ADD v3 INTEGER";
            try {
                // add the same column to the base table with a different type
                stmt.execute(tableDdl);
                fail("Adding a column to a base table should fail when "
                        + "the same column of a different type is being added"
                        + " to a child view");
            } catch (ConcurrentTableMutationException ignored) {
            }
            latch2.countDown();

            Exception e = future.get();
            assertNull(e);

            // add the same column to the another view  to ensure that the cell
            // used to prevent concurrent modifications was removed
            viewDdl = "CREATE VIEW " + fullViewName2
                    + " (v2 VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE k = 6";
            stmt.execute(viewDdl);
            tableDdl = "ALTER VIEW " + fullViewName2 + " ADD v3 INTEGER";
            stmt.execute(tableDdl);
        }
    }

    @Test
    public void testConcurrentAddDifferentColumnParentHasColEncoding()
            throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String fullViewName1 = SLOW_VIEWNAME_PREFIX + "_"
                    + generateUniqueName();
            String fullViewName2 = SchemaUtil.getTableName(SCHEMA3,
                    generateUniqueName());
            String fullViewName3 = SchemaUtil.getTableName(SCHEMA4,
                    generateUniqueName());
            // create base table
            String tableDdl = "CREATE TABLE " + fullTableName
                    + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)"
                    + tableDDLOptions;
            stmt.execute(tableDdl);
            // create two views
            String ddl = "CREATE VIEW " + fullViewName1
                    + " (v2 VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE k = 6";
            stmt.execute(ddl);
            ddl = "CREATE VIEW " + fullViewName3
                    + " (v2 VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE k = 7";
            stmt.execute(ddl);

            latch1 = new CountDownLatch(1);
            latch2 = new CountDownLatch(1);
            ExecutorService executorService = newSingleThreadExecutor(
                    new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    t.setPriority(Thread.MIN_PRIORITY);
                    return t;
                }
            });

            // add a column to a view in a separate thread (we slow this
            // operation down)
            Future<Exception> future = executorService.submit(
                    new AddColumnRunnable(fullViewName1, null));
            // wait till the thread makes the rpc to add the column
            boolean result = latch1.await(2, TimeUnit.MINUTES);
            if (!result) {
                fail("The alter view rpc look too long");
            }
            tableDdl = "ALTER VIEW " + fullViewName3 + " ADD v4 INTEGER";
            try {
                // add a column to another view
                stmt.execute(tableDdl);
                if (columnEncoded) {
                    // this should fail as the previous add column is still
                    // not complete
                    fail("Adding columns to two different views concurrently"
                            + " where the base table"
                            + " uses encoded columns should fail");
                }
            } catch (ConcurrentTableMutationException e) {
                if (!columnEncoded) {
                    // this should not fail as we don't need to update the
                    // parent table for non column encoded tables
                    fail("Adding columns to two different views concurrently"
                            + " where the base table does not use encoded"
                            + " columns should succeed");
                }
            }
            latch2.countDown();

            Exception e = future.get();
            // if the base table uses column encoding then the add column
            // operation for fullViewName1 fails
            assertNull(e);

            // add the same column to the another view  to ensure that the cell
            // used to prevent concurrent modifications was removed
            ddl = "CREATE VIEW " + fullViewName2
                    + " (v2 VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE k = 6";
            stmt.execute(ddl);
            tableDdl = "ALTER VIEW " + fullViewName2 + " ADD v3 INTEGER";
            stmt.execute(tableDdl);
        }
    }

    /**
     * Concurrently create a view with a WHERE condition and also try to drop
     * the parent column on which the WHERE condition depends
     */
    @Test
    public void testConcurrentViewCreationParentColDropViewCondition()
            throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String fullViewName1 = SLOW_VIEWNAME_PREFIX + "_"
                    + generateUniqueName();

            // create base table
            String tableDdl = "CREATE TABLE " + fullTableName
                    + "  (k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER)"
                    + tableDDLOptions;
            stmt.execute(tableDdl);

            latch1 = new CountDownLatch(1);
            latch2 = new CountDownLatch(1);
            ExecutorService executorService = newSingleThreadExecutor(
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = Executors.defaultThreadFactory()
                                    .newThread(r);
                            t.setDaemon(true);
                            t.setPriority(Thread.MIN_PRIORITY);
                            return t;
                        }
                    });

            Future<Exception> future = executorService.submit(
                    new CreateViewRunnable(fullTableName, fullViewName1));
            // wait till the thread makes the rpc to add the column
            boolean result = latch1.await(2, TimeUnit.MINUTES);
            if (!result) {
                fail("The create view rpc look too long");
            }
            tableDdl = "ALTER TABLE " + fullTableName + " DROP COLUMN v1";
            try {
                // drop the view WHERE condition column from the parent
                stmt.execute(tableDdl);
                fail("Dropping a column from a base table should fail when a"
                        + " child view is concurrently being created whose"
                        + " view WHERE condition depends on this column");
            } catch (ConcurrentTableMutationException ignored) {
            }
            latch2.countDown();

            Exception e = future.get();
            assertNull(e);

            // Now doing the same DROP COLUMN from the parent should fail with
            // a different exception, but not a ConcurrentTableMutationException
            // since the cell used to prevent concurrent modifications
            // should be removed
            try {
                stmt.execute(tableDdl);
                fail("Dropping a column from a parent that a child view depends"
                        + " on should fail");
            } catch (SQLException sqlE) {
                assertEquals("Expected a different SQLException",
                        CANNOT_MUTATE_TABLE.getErrorCode(),sqlE.getErrorCode());
            }
        }
    }

    /**
     * Concurrently create a view which has its own new column and also try to
     * add the same column to its parent.
     * See PHOENIX-6191
     */
    @Test
    public void testConcurrentViewCreationWithNewColParentColAddition()
    throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String fullViewName1 = SLOW_VIEWNAME_PREFIX + "_"
                    + generateUniqueName();

            // create base table
            String tableDdl = "CREATE TABLE " + fullTableName
                    + "  (k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER)"
                    + tableDDLOptions;
            stmt.execute(tableDdl);

            latch1 = new CountDownLatch(1);
            latch2 = new CountDownLatch(1);
            ExecutorService executorService = newSingleThreadExecutor(
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = Executors.defaultThreadFactory()
                                    .newThread(r);
                            t.setDaemon(true);
                            t.setPriority(Thread.MIN_PRIORITY);
                            return t;
                        }
                    });

            Future<Exception> future = executorService.submit(
                    new CreateViewRunnable(fullTableName, fullViewName1));
            // wait till the thread makes the rpc to add the column
            boolean result = latch1.await(2, TimeUnit.MINUTES);
            if (!result) {
                fail("The create view rpc look too long");
            }
            tableDdl = "ALTER TABLE " + fullTableName + " ADD new_col INTEGER";
            try {
                // add the same column to the parent which is being introduced
                // as a new column during view creation
                stmt.execute(tableDdl);
                fail("Adding a column to a base table should fail when a"
                        + " child view is concurrently being created that"
                        + " has that new column");
            } catch (ConcurrentTableMutationException ignored) {
            }
            latch2.countDown();

            Exception e = future.get();
            assertNull(e);

            // Now doing the same ADD COLUMN to the parent should fail with
            // a different exception, but not a ConcurrentTableMutationException
            // since the cell used to prevent concurrent modifications
            // should be removed and since the column type is different
            try {
                stmt.execute(tableDdl);
                fail("Adding a column to a parent that its child view already "
                        + "contains should fail");
            } catch (SQLException sqlE) {
                assertEquals("Expected a different SQLException",
                        CANNOT_MUTATE_TABLE.getErrorCode(),sqlE.getErrorCode());
            }
        }
    }

    public static class TestMetaDataRegionObserver
            extends BaseMetaDataEndpointObserver {

        // Note that we need this in master branch since it is invoked via
        // PhoenixObserverOperation#callObserver(). Without this, the pre-hooks
        // of this RegionObserver will not be hit.
        @Override
        public Optional<MetaDataEndpointObserver> getPhoenixObserver() {
            return Optional.of(this);
        }

        @Override
        public void preAlterTable(
                final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx,
                final String tenantId, final String tableName,
                final TableName physicalTableName,
                final TableName parentPhysicalTableName, final PTableType type)
                throws IOException {
            processTable(tableName);
        }

        @Override
        public void preCreateTable(
                final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx,
                final String tenantId, final String tableName,
                final TableName physicalTableName,
                final TableName parentPhysicalTableName,
                final PTableType tableType,
                final Set<byte[]> familySet, final Set<TableName> indexes)
                throws IOException {
            processTable(tableName);
        }

        @Override
        public void preDropTable(
                final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx,
                final String tenantId, final String tableName,
                final TableName physicalTableName,
                final TableName parentPhysicalTableName,
                final PTableType tableType,
                final List<PTable> indexes) throws IOException {
            processTable(tableName);
        }

        @Override
        public void preCreateViewAddChildLink(
                final ObserverContext<PhoenixMetaDataControllerEnvironment> ctx,
                final String tableName) throws IOException {
            if (throwExceptionInChildLinkPreHook) {
                throw new IOException();
            }
            processTable(tableName);
        }

        private void processTable(String tableName)
                throws DoNotRetryIOException {
            if (tableName.equals(FAILED_VIEWNAME)) {
                // throwing anything other than instances of IOException result
                // in this coprocessor being unloaded
                // DoNotRetryIOException tells HBase not to retry this mutation
                // multiple times
                throw new DoNotRetryIOException();
            } else if (tableName.startsWith(SLOW_VIEWNAME_PREFIX) ||
                    slowDownAddingChildLink) {
                // simulate a slow write to SYSTEM.CATALOG or SYSTEM.CHILD_LINK
                if (latch1 != null) {
                    latch1.countDown();
                }
                if (latch2 != null) {
                    try {
                        // wait till the second task is complete before
                        // completing the first task
                        boolean result = latch2.await(2, TimeUnit.MINUTES);
                        if (!result) {
                            throw new RuntimeException("Second task took too "
                                    + "long to complete");
                        }
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    }

    private static class CreateViewRunnable implements Callable<Exception> {
        private final String fullTableName;
        private final String fullViewName;

        public CreateViewRunnable(String fullTableName, String fullViewName) {
            this.fullTableName = fullTableName;
            this.fullViewName = fullViewName;
        }

        @Override
        public Exception call() throws Exception {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            try (Connection conn = DriverManager.getConnection(getUrl(), props);
                    Statement stmt = conn.createStatement()) {
                String ddl = "CREATE VIEW " + fullViewName
                        + " (new_pk VARCHAR PRIMARY KEY, new_col VARCHAR)"
                        + " AS SELECT * FROM " + fullTableName
                        + " WHERE v1 = 5";
                stmt.execute(ddl);
            } catch (SQLException e) {
                return e;
            }
            return null;
        }
    }

    private static class AddColumnRunnable implements Callable<Exception> {
        private final String fullViewName;
        private final String tenantId;

        public AddColumnRunnable(String fullViewName, String tenantId) {
            this.fullViewName = fullViewName;
            this.tenantId = tenantId;
        }

        @Override
        public Exception call() throws Exception {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            if (this.tenantId != null) {
                props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB,
                        this.tenantId);
            }
            try (Connection conn = DriverManager.getConnection(getUrl(), props);
                    Statement stmt = conn.createStatement()) {
                String ddl = "ALTER VIEW " + fullViewName + " ADD v3 CHAR(15)";
                stmt.execute(ddl);
            } catch (SQLException e) {
                return e;
            }
            return null;
        }
    }
}
