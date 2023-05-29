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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

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
}
