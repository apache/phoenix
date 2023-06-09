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
import org.apache.phoenix.coprocessor.generated.DDLTimestampMaintainersProtos;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.LastDDLTimestampMaintainerUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.LAST_DDL_TIMESTAMP_MAINTAINERS;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LastDDLTimestampIT extends ParallelStatsDisabledIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(LastDDLTimestampIT.class);

    /**
     * Make sure that DDLTimestamp maintainer is generated for select queries on base table.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampInScanAttributeForQueryBaseTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, tableNameStr, DDLTimestampMaintainerRegionObserver.class);
            // Get DDLTimestampMaintainerRegionObserver from table's region.
            DDLTimestampMaintainerRegionObserver regionObserver = getObserver(tableNameStr);
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
                // Reset maintainers before query
                regionObserver.resetMaintainers();
                assertTrue(rs.next());
            }

            // There will be only 1 table in the maintainer.
            assertEquals(1, regionObserver.getMaintainers().getDDLTimestampMaintainersCount());
            DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer =
                    regionObserver.getMaintainers().getDDLTimestampMaintainersList().get(0);
            LOGGER.info("Maintainer within test tenantID: {}, table name: {}, last ddl timestamp: {}",
                    maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                    maintainer.getLastDDLTimestamp());
            assertNotNull("DDLTimestampMaintainer should not be null", maintainer);
            assertEquals(0, maintainer.getTenantID().size());
            assertEquals(tableNameStr, maintainer.getTableName().toStringUtf8());
            assertEquals(pTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());
        }
    }

    /**
     * Make sure that DDLTimestamp maintainer is generated for select queries on index table.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampInScanAttributeForQueryIndexTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, tableNameStr, DDLTimestampMaintainerRegionObserver.class);

            String indexNameStr = generateUniqueName();
            String indexDDLStmt = getCreateIndexStmt(indexNameStr, tableNameStr);
            conn.createStatement().execute(indexDDLStmt);

            // Get a PTable object of index to compare last ddl timestamp from maintainer object.
            PTable indexTable = PhoenixRuntime.getTable(conn, indexNameStr);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, indexNameStr, DDLTimestampMaintainerRegionObserver.class);

            DDLTimestampMaintainerRegionObserver tableRegionObserver = getObserver(tableNameStr);
            DDLTimestampMaintainerRegionObserver indexRegionObserver = getObserver(indexNameStr);
            // Upsert couple of rows so that execute query will return results.
            String dml = "UPSERT INTO " + tableNameStr + " VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 1000);
            stmt.execute();
            conn.commit();

            String query = "SELECT * FROM " + tableNameStr + " WHERE COL1 = 1000";
            // Execute query
            try (ResultSet rs = conn.createStatement().executeQuery(query)) {
                // Reset maintainers for both base table and index table.
                // The request should have gone directly to index table and NOT to base table.
                tableRegionObserver.resetMaintainers();
                indexRegionObserver.resetMaintainers();
                assertTrue(rs.next());
            }
            // Request shouldn't have gone to base table.
            assertNull(tableRegionObserver.getMaintainers());
            assertNotNull(indexRegionObserver.getMaintainers());
            // There will be only 1 table in the maintainer.
            assertEquals(1, indexRegionObserver.getMaintainers().getDDLTimestampMaintainersCount());
            DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer =
                    indexRegionObserver.getMaintainers().getDDLTimestampMaintainersList().get(0);
            LOGGER.info("Maintainer within test tenantID: {}, table name: {}, last ddl timestamp: {}",
                    maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                    maintainer.getLastDDLTimestamp());
            assertNotNull("DDLTimestampMaintainer should not be null", maintainer);
            assertEquals(0, maintainer.getTenantID().size());
            assertEquals(indexNameStr, maintainer.getTableName().toStringUtf8());
            assertEquals(indexTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());
        }
    }

    /**
     * Make sure that DDLTimestamp maintainer is generated for select queries on view.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampInScanAttributeForQueryonView() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, tableNameStr, DDLTimestampMaintainerRegionObserver.class);

            // Get DDLTimestampMaintainerRegionObserver from table's region.
            DDLTimestampMaintainerRegionObserver regionObserver = getObserver(tableNameStr);
            // Get a PTable object to compare last ddl timestamp from maintainer object.
            PTable pTable = PhoenixRuntime.getTable(conn, tableNameStr);
            // Upsert couple of rows so that execute query will return results.
            String dml = "UPSERT INTO " + tableNameStr + " VALUES(?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 1000);
            stmt.execute();
            conn.commit();

            // Create view on table.
            String whereClause = " WHERE COL1 = 1000";
            String viewName = generateUniqueName();
            conn.createStatement().execute(getCreateViewStmt(viewName, tableNameStr, whereClause));
            PTable viewTable = PhoenixRuntime.getTable(conn, viewName);

            String queryView = "SELECT * FROM " + viewName;
            // Execute query
            try (ResultSet rs = conn.createStatement().executeQuery(queryView)) {
                regionObserver.resetMaintainers();
                assertTrue(rs.next());
            }

            // There will be 2 tables in the maintainer, one for the view and other base table.
            assertEquals(2, regionObserver.getMaintainers().getDDLTimestampMaintainersCount());
            boolean baseTableFound = false, viewFound = false;

            for (DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer:
                    regionObserver.getMaintainers().getDDLTimestampMaintainersList()) {
                LOGGER.info("Maintainer within test tenantID: {}, table name: {}, last ddl timestamp: {}",
                        maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                        maintainer.getLastDDLTimestamp());
                assertNotNull("DDLTimestampMaintainer should not be null", maintainer);
                if (maintainer.getTableName().toStringUtf8().equals(tableNameStr)) {
                    baseTableFound = true;
                    assertEquals(0, maintainer.getTenantID().size());
                    assertEquals(pTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());

                } else if (maintainer.getTableName().toStringUtf8().equals(viewName)) {
                    viewFound = true;
                    assertEquals(0, maintainer.getTenantID().size());
                    assertEquals(viewTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());
                }
            }
            assertTrue("There should be base table in the maintainers list", baseTableFound);
            assertTrue("There should logical view table in the maintainers list", viewFound);
        }
    }

    /*
        Get DDLTimestampMaintainerRegionObserver for the given table.
     */
    private DDLTimestampMaintainerRegionObserver getObserver(String tableNameStr) {
        TableName tableName = TableName.valueOf(tableNameStr);
        List<HRegion> regions = getUtility().getMiniHBaseCluster().getRegions(tableName);
        HRegion region = regions.get(0);
        // Get DDLTimestampMaintainerRegionObserver from table's region.
        DDLTimestampMaintainerRegionObserver regionObserver =
                region.getCoprocessorHost().findCoprocessor(DDLTimestampMaintainerRegionObserver.class);
        return regionObserver;
    }

    /**
     * RegionObserver to intercept preScannerOpen calls and extract DDLTimestampMaintainers object.
     */
    public static class DDLTimestampMaintainerRegionObserver extends BaseScannerRegionObserver
            implements RegionCoprocessor {
        private DDLTimestampMaintainersProtos.DDLTimestampMaintainers maintainers;
        @Override
        public void preScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
                                   Scan scan) {
            byte[] maintainersBytes;
            maintainersBytes = scan.getAttribute(LAST_DDL_TIMESTAMP_MAINTAINERS);
            if (maintainersBytes != null) {
                maintainers = LastDDLTimestampMaintainerUtil.deserialize(maintainersBytes);
                for (DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer:
                        maintainers.getDDLTimestampMaintainersList()) {
                    LOGGER.info("Maintainer within corpoc tenantID: {}, table name: {}, last ddl timestamp: {}",
                            maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                            maintainer.getLastDDLTimestamp());
                }
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
        protected RegionScanner doPostScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
                                                  RegionScanner s) {
            return s;
        }

        public DDLTimestampMaintainersProtos.DDLTimestampMaintainers getMaintainers() {
            return maintainers;
        }

        public void resetMaintainers() {
            this.maintainers = null;
        }
    }

    private String getCreateTableStmt(String tableName) {
        return   "CREATE TABLE " + tableName +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) ";
    }

    private String getCreateIndexStmt(String indexName, String fullTableName) {
        return "CREATE INDEX " + indexName + " ON " + fullTableName + "(COL1)";
    }

    private String getCreateViewStmt(String viewName, String fullTableName, String whereClause) {
        String viewStmt =  "CREATE VIEW " + viewName + " AS SELECT * FROM "+ fullTableName + whereClause;
        return  viewStmt;
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
            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, tableNameStr, DDLTimestampMaintainerRegionObserverForMutations.class);
            // Get DDLTimestampMaintainerRegionObserverForMutations from table's region.
            DDLTimestampMaintainerRegionObserverForMutations regionObserver = getObserverForMutations(tableNameStr);
            // Get a PTable object to compare last ddl timestamp from maintainer object.
            PTable pTable = PhoenixRuntime.getTable(conn, tableNameStr);
            // Reset the maintainers.
            regionObserver.resetMaintainers();
            // Upsert couple of rows so that execute query will return results.
            String dml = "UPSERT INTO " + tableNameStr + " VALUES(?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.execute();
            stmt.setString(1, "a");
            stmt.execute();
            conn.commit();

            // There will be only 1 table in the maintainer.
            assertEquals(1, regionObserver.getMaintainersFromMutate().getDDLTimestampMaintainersCount());
            DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer =
                    regionObserver.getMaintainersFromMutate().getDDLTimestampMaintainersList().get(0);
            LOGGER.info("Maintainer within test tenantID: {}, table name: {}, last ddl timestamp: {}",
                    maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                    maintainer.getLastDDLTimestamp());
            assertNotNull("DDLTimestampMaintainer should not be null", maintainer);
            assertEquals(0, maintainer.getTenantID().size());
            assertEquals(tableNameStr, maintainer.getTableName().toStringUtf8());
            assertEquals(pTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());
        }
    }

    /**
     * Make sure that DDLTimestamp maintainer is generated for upsert queries on base table with index.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampForUpsertOnBaseTableWithIndex() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr =  generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, tableNameStr, DDLTimestampMaintainerRegionObserverForMutations.class);

            String indexNameStr = generateUniqueName();
            String indexDDLStmt = getCreateIndexStmt(indexNameStr, tableNameStr);
            conn.createStatement().execute(indexDDLStmt);

            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, indexNameStr, DDLTimestampMaintainerRegionObserverForMutations.class);

            // Get a PTable object of base table and index table to compare last ddl timestamp from maintainer object.
            PTable baseTable = PhoenixRuntime.getTable(conn, tableNameStr);
            PTable indexTable = PhoenixRuntime.getTable(conn, indexNameStr);

            DDLTimestampMaintainerRegionObserverForMutations tableRegionObserver =
                    getObserverForMutations(tableNameStr);
            DDLTimestampMaintainerRegionObserverForMutations indexRegionObserver =
                    getObserverForMutations(indexNameStr);
            // Reset maintainers for both tables.
            tableRegionObserver.resetMaintainers();
            indexRegionObserver.resetMaintainers();
            // Upsert couple of rows so that execute query will return results.
            String dml = "UPSERT INTO " + tableNameStr + " VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.setInt(2, 1000);
            stmt.execute();
            conn.commit();

            // Request shouldn't have gone to base table.
            assertNotNull(tableRegionObserver.getMaintainersFromMutate());
            assertNull(indexRegionObserver.getMaintainersFromMutate());
            // There will be only 1 table in the maintainer.
            assertEquals(2, tableRegionObserver.getMaintainersFromMutate().getDDLTimestampMaintainersCount());
            boolean baseTableFound = false, indexTableFound = false;
            for (DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer:
                    tableRegionObserver.getMaintainersFromMutate().getDDLTimestampMaintainersList()) {
                LOGGER.info("Maintainer within test tenantID: {}, table name: {}, last ddl timestamp: {}",
                        maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                        maintainer.getLastDDLTimestamp());
                assertNotNull("DDLTimestampMaintainer should not be null", maintainer);
                if (maintainer.getTableName().toStringUtf8().equals(tableNameStr)) {
                    baseTableFound = true;
                    assertEquals(0, maintainer.getTenantID().size());
                    assertEquals(baseTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());
                } else if (maintainer.getTableName().toStringUtf8().equals(indexNameStr)) {
                    indexTableFound = true;
                    assertEquals(0, maintainer.getTenantID().size());
                    assertEquals(indexTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());
                }
            }
            assertTrue("There should be base table in the maintainers list", baseTableFound);
            assertTrue("There should index table in the maintainers list", indexTableFound);
        }
    }

    /**
     * Make sure that DDLTimestamp maintainer is generated for upsert queries on base table with view.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampForUpsertOnBaseTableWithView() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, tableNameStr, DDLTimestampMaintainerRegionObserverForMutations.class);

            // Get DDLTimestampMaintainerRegionObserver from table's region.
            DDLTimestampMaintainerRegionObserverForMutations regionObserver = getObserverForMutations(tableNameStr);
            // Get a PTable object to compare last ddl timestamp from maintainer object.
            PTable baseTable = PhoenixRuntime.getTable(conn, tableNameStr);

            // Create view on table.
            String whereClause = " WHERE COL1 = 1000";
            String viewName = generateUniqueName();
            conn.createStatement().execute(getCreateViewStmt(viewName, tableNameStr, whereClause));
            PTable viewTable = PhoenixRuntime.getTable(conn, viewName);

            // Reset maintainers for both base table.
            regionObserver.resetMaintainers();
            // Upsert some data
            String dml = "UPSERT INTO " + viewName + " VALUES(?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "a");
            stmt.execute();
            conn.commit();

            // There will be 2 tables in the maintainer, one for the view and other base table.
            assertEquals(2, regionObserver.getMaintainersFromMutate().getDDLTimestampMaintainersCount());
            boolean baseTableFound = false, viewFound = false;

            for (DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer:
                    regionObserver.getMaintainersFromMutate().getDDLTimestampMaintainersList()) {
                LOGGER.info("Maintainer within test tenantID: {}, table name: {}, last ddl timestamp: {}",
                        maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                        maintainer.getLastDDLTimestamp());
                assertNotNull("DDLTimestampMaintainer should not be null", maintainer);
                if (maintainer.getTableName().toStringUtf8().equals(tableNameStr)) {
                    baseTableFound = true;
                    assertEquals(0, maintainer.getTenantID().size());
                    assertEquals(baseTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());
                } else if (maintainer.getTableName().toStringUtf8().equals(viewName)) {
                    viewFound = true;
                    assertEquals(0, maintainer.getTenantID().size());
                    assertEquals(viewTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());
                }
            }
            assertTrue("There should be base table in the maintainers list", baseTableFound);
            assertTrue("There should logical view table in the maintainers list", viewFound);
        }
    }

    /**
     * Make sure that DDLTimestamp maintainer is generated for upsert select queries on base table.
     * For upsert select queries, we generate put mutations on the server side via scan attributes.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampForUpsertSelectMutations() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            // Set autocommit to true to make sure that upsert select statements will create
            // ServerUpsertSelectMutationPlan
            conn.setAutoCommit(true);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserver coproc.
            TestUtil.addCoprocessor(conn, tableNameStr, DDLTimestampMaintainerRegionObserverForMutations.class);

            // Get DDLTimestampMaintainerRegionObserver from table's region.
            DDLTimestampMaintainerRegionObserverForMutations regionObserverForMutations
                    = getObserverForMutations(tableNameStr);
            // Get a PTable object to compare last ddl timestamp from maintainer object.
            PTable baseTable = PhoenixRuntime.getTable(conn, tableNameStr);

            // Reset the maintainers.
            regionObserverForMutations.resetMaintainers();
            // Upsert data
            String dml = "UPSERT INTO " + tableNameStr + " VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 1000);
            stmt.execute();
            conn.commit();

            // Reset maintainers for the base table.
            regionObserverForMutations.resetMaintainers();
            String upsertSelectStmt =
                    "UPSERT INTO " + tableNameStr + "(A_STRING, COL1) SELECT A_STRING, 2000 FROM " + tableNameStr + " WHERE COL1 = 1000";
            // Reset maintainers for both base table.
            regionObserverForMutations.resetMaintainers();
            stmt = conn.prepareStatement(upsertSelectStmt);
            stmt.execute();
            conn.commit();

            // For upsert select statements, since the auto commit is ON then upsert select happens on server side
            // So there will LAST_DDL_TIMESTAMP maintainers only for the scanner request.
            assertNull(regionObserverForMutations.getMaintainersFromMutate());
            assertNotNull(regionObserverForMutations.getMaintainersFromScanner());

            // There will be 1 table in the scanner maintainer
            assertEquals(1, regionObserverForMutations.getMaintainersFromScanner().getDDLTimestampMaintainersCount());

            for (DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer:
                    regionObserverForMutations.getMaintainersFromScanner().getDDLTimestampMaintainersList()) {
                LOGGER.info("Maintainer within test tenantID: {}, table name: {}, last ddl timestamp: {}",
                        maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                        maintainer.getLastDDLTimestamp());
                assertNotNull("DDLTimestampMaintainer should not be null", maintainer);
                assertEquals(0, maintainer.getTenantID().size());
                assertEquals(baseTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());
            }
        }
    }

    /**
     * Make sure that DDLTimestamp maintainer is generated for delete mutations on base table.
     * If the auto commit is ON, we perform the deletes on the server side.
     * @throws Exception
     */
    @Test
    public void testLastDDLTimestampForDeleteMutations() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableNameStr = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            // Set autocommit to true to make sure that deletes happen on the server side.
            conn.setAutoCommit(true);
            String ddl = getCreateTableStmt(tableNameStr);
            // Create a test table.
            conn.createStatement().execute(ddl);
            // Add DDLTimestampMaintainerRegionObserverForMutations coproc.
            TestUtil.addCoprocessor(conn, tableNameStr, DDLTimestampMaintainerRegionObserverForMutations.class);

            // Get DDLTimestampMaintainerRegionObserver from table's region.
            DDLTimestampMaintainerRegionObserverForMutations regionObserverForMutations
                    = getObserverForMutations(tableNameStr);
            // Get a PTable object to compare last ddl timestamp from maintainer object.
            PTable baseTable = PhoenixRuntime.getTable(conn, tableNameStr);

            // Reset the maintainers.
            regionObserverForMutations.resetMaintainers();
            // Upsert data
            String dml = "UPSERT INTO " + tableNameStr + " VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "b");
            stmt.setInt(2, 1000);
            stmt.execute();
            conn.commit();

            // Reset maintainers for both base table.
            regionObserverForMutations.resetMaintainers();
            String deleteStmt =
                    "DELETE FROM " + tableNameStr + " WHERE COL1 = 1000";
            // Reset maintainers for base table.
            regionObserverForMutations.resetMaintainers();
            stmt = conn.prepareStatement(deleteStmt);
            stmt.execute();
            conn.commit();

            // For upsert select statements, since the auto commit is ON then upsert select happens on server side
            // So there will LAST_DDL_TIMESTAMP maintainers only for the scanner request.
            assertNull(regionObserverForMutations.getMaintainersFromMutate());
            assertNotNull(regionObserverForMutations.getMaintainersFromScanner());

            // There will be 1 table in the scanner maintainer
            assertEquals(1, regionObserverForMutations.getMaintainersFromScanner().getDDLTimestampMaintainersCount());

            for (DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer:
                    regionObserverForMutations.getMaintainersFromScanner().getDDLTimestampMaintainersList()) {
                LOGGER.info("Maintainer within test tenantID: {}, table name: {}, last ddl timestamp: {}",
                        maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                        maintainer.getLastDDLTimestamp());
                assertNotNull("DDLTimestampMaintainer should not be null", maintainer);
                assertEquals(0, maintainer.getTenantID().size());
                assertEquals(baseTable.getLastDDLTimestamp().longValue(), maintainer.getLastDDLTimestamp());
            }
        }
    }


    /**
     * RegionObserver to intercept preScannerOpen and preBatchMutate calls and extract DDLTimestampMaintainers object.
     */
    public static class DDLTimestampMaintainerRegionObserverForMutations implements RegionCoprocessor, RegionObserver {
        private DDLTimestampMaintainersProtos.DDLTimestampMaintainers maintainersFromScanner;
        private DDLTimestampMaintainersProtos.DDLTimestampMaintainers maintainersFromMutate;
        @Override
        public void preScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
                                   Scan scan) {
            byte[] maintainersBytes = scan.getAttribute(LAST_DDL_TIMESTAMP_MAINTAINERS);
            LOGGER.info("within preScannerOpen method, table name: {}",
                    c.getEnvironment().getRegion().getTableDescriptor().getTableName().getNameAsString());
            populateDDLTimestampMaintainers(maintainersBytes, true);
        }

        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                                   MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            byte[] maintainersBytes = miniBatchOp.getOperation(0).getAttribute(LAST_DDL_TIMESTAMP_MAINTAINERS);
            LOGGER.info("Within preBatchMutate method: {}",
                    c.getEnvironment().getRegion().getTableDescriptor().getTableName().getNameAsString());
            populateDDLTimestampMaintainers(maintainersBytes, false);
        }

        private void populateDDLTimestampMaintainers(byte[] maintainersBytes, boolean fromScanner) {
            if (maintainersBytes == null) {
                return;
            }
            if (fromScanner) {
                maintainersFromScanner = LastDDLTimestampMaintainerUtil.deserialize(maintainersBytes);
                for (DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer:
                        maintainersFromScanner.getDDLTimestampMaintainersList()) {
                    LOGGER.info("Maintainer from preScannerOpen tenantID: {}, table name: {}, last ddl timestamp: {}",
                            maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                            maintainer.getLastDDLTimestamp());
                }
            } else {
                maintainersFromMutate = LastDDLTimestampMaintainerUtil.deserialize(maintainersBytes);
                for (DDLTimestampMaintainersProtos.DDLTimestampMaintainer maintainer:
                        maintainersFromMutate.getDDLTimestampMaintainersList()) {
                    LOGGER.info("Maintainer from preBatchMutate tenantID: {}, table name: {}, last ddl timestamp: {}",
                            maintainer.getTenantID().toStringUtf8(), maintainer.getTableName().toStringUtf8(),
                            maintainer.getLastDDLTimestamp());
                }
            }
        }

        @Override
        public Optional<RegionObserver> getRegionObserver() {
            return Optional.of(this);
        }

        public DDLTimestampMaintainersProtos.DDLTimestampMaintainers getMaintainersFromScanner() {
            return maintainersFromScanner;
        }

        public DDLTimestampMaintainersProtos.DDLTimestampMaintainers getMaintainersFromMutate() {
            return maintainersFromMutate;
        }

        public void resetMaintainers() {
            this.maintainersFromScanner = null;
            this.maintainersFromMutate = null;
        }
    }

    /*
        Get DDLTimestampMaintainerRegionObserverForMutations for the given table.
     */
    private DDLTimestampMaintainerRegionObserverForMutations getObserverForMutations(String tableNameStr) {
        TableName tableName = TableName.valueOf(tableNameStr);
        List<HRegion> regions = getUtility().getMiniHBaseCluster().getRegions(tableName);
        HRegion region = regions.get(0);
        // Get DDLTimestampMaintainerRegionObserver from table's region.
        DDLTimestampMaintainerRegionObserverForMutations regionObserver =
                region.getCoprocessorHost().findCoprocessor(DDLTimestampMaintainerRegionObserverForMutations.class);
        return regionObserver;
    }
}