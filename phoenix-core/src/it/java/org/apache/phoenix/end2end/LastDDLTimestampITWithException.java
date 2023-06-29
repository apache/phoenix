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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.exception.StaleMetadataCacheException;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.LAST_DDL_TIMESTAMP_MAINTAINERS;
import static org.apache.phoenix.query.QueryServices.PHOENIX_VERIFY_LAST_DDL_TIMESTAMP;
import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_PHOENIX_VERIFY_LAST_DDL_TIMESTAMP;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({NeedsOwnMiniClusterTest.class })
public class LastDDLTimestampITWithException extends BaseTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(LastDDLTimestampITWithException.class);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(BaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(60*60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        props.put(PHOENIX_VERIFY_LAST_DDL_TIMESTAMP, "true");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void resetConnectionCache() {
        getDriver().clearConnectionCache();
    }

    /**
     * Make sure that DDLTimestamp maintainer is generated for select queries on base table.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampInScanAttributeForQueryBaseTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            assertEquals(Boolean.valueOf(true),
                    pConn.getQueryServices().getConfiguration().getBoolean(
                            PHOENIX_VERIFY_LAST_DDL_TIMESTAMP,
                            DEFAULT_PHOENIX_VERIFY_LAST_DDL_TIMESTAMP));

            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, tableNameStr,
                    DDLTimestampMaintainerRegionObserverWithExceptions.class);
            // Get DDLTimestampMaintainerRegionObserver from table's region.
            DDLTimestampMaintainerRegionObserverWithExceptions regionObserver =
                    getObserver(tableNameStr);
            // Get a PTable object to compare last ddl timestamp from maintainer object.
            PTable pTable = PhoenixRuntime.getTable(conn, tableNameStr);
            // Upsert couple of rows so that execute query will return results.
            String dml = "UPSERT INTO " + tableNameStr + " VALUES(?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.execute();
            stmt.setString(1, "a");
            stmt.execute();
            conn.commit();

            String query = "SELECT * FROM " + tableNameStr;
            // Execute query
            try (ResultSet rs = conn.createStatement().executeQuery(query)) {
                try {
                    rs.next();
                    fail("rs.next should throw exception");
                } catch (SQLException e) {
                    assertTrue(e instanceof StaleMetadataCacheException);
                }
            }
            PTableKey key = new PTableKey(null, tableNameStr);
            try {
                pConn.getQueryServices().getMetaDataCache().getTableRef(key);
                Assert.fail("Should not reach here.");
            } catch (TableNotFoundException tnfe) {
                // Expected exception.
            }
            stmt.close();
        }
    }

    /**
     * Make sure that DDLTimestamp maintainer is generated for upsert queries on base table.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampForUpsertOnBaseTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            assertEquals(Boolean.valueOf(true),
                    pConn.getQueryServices().getConfiguration().getBoolean(
                            PHOENIX_VERIFY_LAST_DDL_TIMESTAMP,
                            DEFAULT_PHOENIX_VERIFY_LAST_DDL_TIMESTAMP));
            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, tableNameStr,
                    DDLTimestampMaintainerRegionObserverForMutationsWithExceptions.class);
            // Upsert couple of rows so that execute query will return results.
            String dml = "UPSERT INTO " + tableNameStr + " VALUES(?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.execute();
            stmt.setString(1, "a");
            stmt.execute();
            try {
                conn.commit();
                Assert.fail("should have thrown exception");
            } catch (SQLException se) {
                assertTrue(se instanceof CommitException);
                Throwable cause = se.getCause();
                assertNotNull(cause);
                assertTrue(cause instanceof StaleMetadataCacheException);
            }
            PTableKey key = new PTableKey(null, tableNameStr);
            try {
                pConn.getQueryServices().getMetaDataCache().getTableRef(key);
                Assert.fail("Should not reach here.");
            } catch (TableNotFoundException tnfe) {
                // Expected exception.
            }
            stmt.close();
        }
    }

    /**
     * Test that we invalidate the client side cache when UpsertSelect queries encounters
     * StaleMetadataCacheException.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampForUpsertSelectMutations() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            // Set autocommit to true to make sure that upsert select statements will create
            // ServerUpsertSelectMutationPlan
            conn.setAutoCommit(true);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, tableNameStr,
                    DDLTimestampMaintainerRegionObserverForMutationsWithExceptions.class);

            String upsertSelectStmt =
                    "UPSERT INTO " + tableNameStr + "(A_STRING, COL1)" +
                            " SELECT A_STRING, 2000 FROM " + tableNameStr + " WHERE COL1 = 1000";
            PreparedStatement stmt = conn.prepareStatement(upsertSelectStmt);
            try {
                stmt.execute();
                Assert.fail("should have thrown exception");
            } catch (SQLException se) {
                assertTrue(se instanceof StaleMetadataCacheException);
            }
            PTableKey key = new PTableKey(null, tableNameStr);
            try {
                pConn.getQueryServices().getMetaDataCache().getTableRef(key);
                Assert.fail("Should not reach here.");
            } catch (TableNotFoundException tnfe) {
                // Expected exception.
            }
        }
    }

    /**
     * Test that we invalidate the client side cache when delete mutations with where clause queries
     * encounters StaleMetadataCacheException.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampForDeleteMutations() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            // Set autocommit to true to make sure that deletes happen on the server side.
            conn.setAutoCommit(true);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserverForMutations coproc.
            TestUtil.addCoprocessor(conn, tableNameStr,
                    DDLTimestampMaintainerRegionObserverForMutationsWithExceptions.class);

            String deleteStmt =
                    "DELETE FROM " + tableNameStr + " WHERE COL1 = 1000";
            PreparedStatement stmt = conn.prepareStatement(deleteStmt);
            try {
                stmt.execute();
                Assert.fail("should have thrown exception");
            } catch (SQLException se) {
                assertTrue(se instanceof StaleMetadataCacheException);
            }
            PTableKey key = new PTableKey(null, tableNameStr);
            try {
                pConn.getQueryServices().getMetaDataCache().getTableRef(key);
                Assert.fail("Should not reach here.");
            } catch (TableNotFoundException tnfe) {
                // Expected exception.
            }
        }
    }

    /**
     * Test that we invalidate the client side cache when point lookup deletes
     * encounters StaleMetadataCacheException.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampForPointLookupDeletes() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            // Set autocommit to true to make sure that deletes happen on the server side.
            conn.setAutoCommit(true);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);

            // Upsert couple of rows so that execute query will return results.
            String dml ="UPSERT INTO " + tableNameStr + "(A_STRING, COL1) VALUES (?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 1000);
            stmt.execute();

            // Add DDLTimestampMaintainerRegionObserverForMutations coproc.
            TestUtil.addCoprocessor(conn, tableNameStr,
                    DDLTimestampMaintainerRegionObserverForMutationsWithExceptions.class);

            String deleteStmt =
                    "DELETE FROM " + tableNameStr + " WHERE A_STRING = 'b'";
            stmt = conn.prepareStatement(deleteStmt);
            try {
                stmt.execute();
                Assert.fail("should have thrown exception");
            } catch (SQLException se) {
                assertTrue(se instanceof CommitException);
                Throwable cause = se.getCause();
                assertNotNull(cause);
                assertTrue(cause instanceof StaleMetadataCacheException);
            }
            PTableKey key = new PTableKey(null, tableNameStr);
            try {
                pConn.getQueryServices().getMetaDataCache().getTableRef(key);
                Assert.fail("Should not reach here.");
            } catch (TableNotFoundException tnfe) {
                // Expected exception.
            }
        }
    }

    /**
     * RegionObserver to intercept preScannerOpen calls and extract DDLTimestampMaintainers object.
     */
    public static class DDLTimestampMaintainerRegionObserverWithExceptions
            extends BaseScannerRegionObserver implements RegionCoprocessor {
        @Override
        public void preScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext
                <RegionCoprocessorEnvironment> c, Scan scan) throws IOException {
            byte[] maintainersBytes;
            maintainersBytes = scan.getAttribute(LAST_DDL_TIMESTAMP_MAINTAINERS);
            if (maintainersBytes != null) {
                ServerUtil.throwIOException("Stale metadata cache exception",
                        new StaleMetadataCacheException("Stale MetadataCache Exception"));
            }
        }

        @Override
        public Optional<RegionObserver> getRegionObserver() {
            return Optional.of(this);
        }

        @Override
        protected boolean isRegionObserverFor(Scan scan) {
            return true;
        }

        @Override
        protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
                                                  Scan scan, RegionScanner s) {
            return s;
        }
    }

    /**
     * RegionObserver to intercept preScannerOpen and preBatchMutate calls
     * and throw StaleMetadataException.
     */
    public static class DDLTimestampMaintainerRegionObserverForMutationsWithExceptions
            implements RegionCoprocessor, RegionObserver {
        @Override
        public void preScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext<
                RegionCoprocessorEnvironment> c, Scan scan) throws IOException {
            byte[] maintainersBytes;
            maintainersBytes = scan.getAttribute(LAST_DDL_TIMESTAMP_MAINTAINERS);
            if (maintainersBytes != null) {
                ServerUtil.throwIOException("Stale metadata cache exception in" +
                                " preScannerOpen method",
                        new StaleMetadataCacheException("Stale MetadataCache Exception in" +
                                " preScannerOpen method"));
            }
        }

        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                                   MiniBatchOperationInProgress<Mutation> miniBatchOp)
                throws IOException {
            byte[] maintainersBytes = miniBatchOp.getOperation(0).getAttribute(
                    LAST_DDL_TIMESTAMP_MAINTAINERS);
            if (maintainersBytes != null) {
                ServerUtil.throwIOException("Stale metadata cache exception in" +
                                " preBatchMutate method",
                        new StaleMetadataCacheException("Stale MetadataCache Exception in" +
                                " preBatchMutate method"));
            }
        }

        @Override
        public Optional<RegionObserver> getRegionObserver() {
            return Optional.of(this);
        }
    }


    /*
    Get DDLTimestampMaintainerRegionObserverForMutations for the given table.
 */
    private DDLTimestampMaintainerRegionObserverWithExceptions
            getObserverForMutationsWithExceptions(String tableNameStr) {
        TableName tableName = TableName.valueOf(tableNameStr);
        List<HRegion> regions = getUtility().getMiniHBaseCluster().getRegions(tableName);
        HRegion region = regions.get(0);
        // Get DDLTimestampMaintainerRegionObserverWithExceptions from table's region.
        DDLTimestampMaintainerRegionObserverWithExceptions regionObserver =
                region.getCoprocessorHost().findCoprocessor(
                        DDLTimestampMaintainerRegionObserverWithExceptions.class);
        return regionObserver;
    }

    /*
    Get DDLTimestampMaintainerRegionObserver for the given table.
 */
    private DDLTimestampMaintainerRegionObserverWithExceptions getObserver(String tableNameStr) {
        TableName tableName = TableName.valueOf(tableNameStr);
        List<HRegion> regions = getUtility().getMiniHBaseCluster().getRegions(tableName);
        HRegion region = regions.get(0);
        // Get DDLTimestampMaintainerRegionObserver from table's region.
        DDLTimestampMaintainerRegionObserverWithExceptions regionObserver =
                region.getCoprocessorHost().findCoprocessor(
                        DDLTimestampMaintainerRegionObserverWithExceptions.class);
        return regionObserver;
    }

    private String getCreateTableStmt(String tableName) {
        return  "CREATE TABLE " + tableName +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) ";
    }
}