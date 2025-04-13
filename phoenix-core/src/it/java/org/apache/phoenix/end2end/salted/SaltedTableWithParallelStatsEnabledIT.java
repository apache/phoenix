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
package org.apache.phoenix.end2end.salted;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.junit.Test;
import org.junit.Assert;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;


@Category(ParallelStatsEnabledIT.class)
public class SaltedTableWithParallelStatsEnabledIT extends ParallelStatsEnabledIT {

    private void testPhoenix7580(boolean withStatsForParallelization, boolean withFullTableScan,
                                 boolean withPointLookups)
            throws Exception {
        String tableName = generateUniqueName();
        int saltBucketCount = 5;
        int rowsToInsert = saltBucketCount * 10;
        String primaryKeyPrefix = "pk1_1";
        // First 3 values are from salt bucket 0, next 3 are from salt bucket 1 and so on
        // till the last salt bucket. These values have been specifically selected so that
        // we have values from all the 5 salt buckets.
        int[] pk2ValuesForPointLookups = new int[]
                { 4, 9, 13, 0, 5, 14, 1, 6, 10, 2, 7, 11, 3, 8, 12 };
        int pointLookupsPerSaltBkt = pk2ValuesForPointLookups.length / saltBucketCount;

        String connProfile = "testRangeScanForPhoenix7580" + withStatsForParallelization;
        Properties props = new Properties();
        props.setProperty(QueryServices.USE_STATS_FOR_PARALLELIZATION,
                withStatsForParallelization ? Boolean.TRUE.toString() : Boolean.FALSE.toString());
        try (Connection conn = DriverManager.getConnection(getUrl(connProfile), props)) {
            createTable(conn, tableName, saltBucketCount);
            addRows(conn, tableName, primaryKeyPrefix, rowsToInsert);

            // Run COUNT(*) range query on Phoenix with row key prefix as {@code primaryKeyPrefix}.
            // Assert that count of rows reported by Phoenix is same as count of rows in HBase.
            if (withFullTableScan) {
                assertFullScanRowCntFromHBaseAndPhoenix(conn, rowsToInsert, tableName);
            }
            else if (withPointLookups) {
                assertPointLookupsRowCntFromHBaseAndPhoenix(conn, pk2ValuesForPointLookups.length,
                        tableName, saltBucketCount, primaryKeyPrefix, pk2ValuesForPointLookups,
                        pointLookupsPerSaltBkt);
            }
            else {
                assertRangeScanRowCntFromHBaseAndPhoenix(conn, rowsToInsert, tableName,
                        saltBucketCount, primaryKeyPrefix);
            }

            // ********** Create conditions to trigger PHOENIX-7580 ***********

            // Insert 3 rows with row key prefix greater than the row key prefix of rows
            // earlier inserted.
            String primaryKeyPrefixForNewRows = "pk1_2";
            // These values have been carefully selected such that newly inserted rows go to
            // second last salt bucket when salt bucket count = 5.
            int[] pk2ValuesForNewRows = new int[] { 1, 6, 10 };
            triggerPhoenix7580(conn, tableName, saltBucketCount, primaryKeyPrefixForNewRows,
                    pk2ValuesForNewRows);

            // **** Conditions to trigger PHOENIX-7580 have been satisfied. Test the fix now. ****

            if (withFullTableScan) {
                assertFullScanRowCntFromHBaseAndPhoenix(conn,
                        rowsToInsert + pk2ValuesForNewRows.length, tableName);
            }
            else if (withPointLookups) {
                assertPointLookupsRowCntFromHBaseAndPhoenix(conn, pk2ValuesForPointLookups.length,
                        tableName, saltBucketCount, primaryKeyPrefix, pk2ValuesForPointLookups,
                        pointLookupsPerSaltBkt);
            }
            else {
                assertRangeScanRowCntFromHBaseAndPhoenix(conn, rowsToInsert, tableName,
                        saltBucketCount, primaryKeyPrefix);
            }
        }
    }

    private void assertRangeScanRowCntFromHBaseAndPhoenix(Connection conn, int expectedRowCount,
                                                          String tableName, int saltBucketCount,
                                                          String primaryKeyPrefix)
            throws Exception {
        Table hTable = conn.unwrap(PhoenixConnection.class)
                .getQueryServices().getTable(tableName.getBytes());
        int rowCountFromHBase = 0;
        byte[] rowKeyPrefix = new byte[primaryKeyPrefix.length() + 1];
        System.arraycopy(Bytes.toBytes(primaryKeyPrefix), 0, rowKeyPrefix, 1,
                rowKeyPrefix.length - 1);
        for (int i = 0; i< saltBucketCount; i++) {
            rowKeyPrefix[0] = (byte) i;
            Scan scan = new Scan();
            scan.setStartStopRowForPrefixScan(rowKeyPrefix);
            try (ResultScanner scanner = hTable.getScanner(scan)) {
                while(scanner.next() != null) {
                    rowCountFromHBase++;
                }
            }
        }
        // Assert all the rows are visible on running prefix scan from HBase
        Assert.assertEquals(expectedRowCount, rowCountFromHBase);
        String rangeScanDql = "SELECT COUNT(*) FROM " + tableName + " WHERE PK1=?";
        try (PreparedStatement stmt = conn.prepareStatement(rangeScanDql)) {
            stmt.setString(1, primaryKeyPrefix);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            int rowsVisible = rs.getInt(1);
            rs.close();
            // Assert all the rows are visible on running range query from Phoenix
            Assert.assertEquals(expectedRowCount, rowsVisible);
        }
    }

    private void assertFullScanRowCntFromHBaseAndPhoenix(Connection conn, int expectedRowCount,
                                                         String tableName) throws Exception {
        Table hTable = conn.unwrap(PhoenixConnection.class)
                .getQueryServices().getTable(tableName.getBytes());
        int rowCountFromHBase = 0;
        Scan scan = new Scan();
        try (ResultScanner scanner = hTable.getScanner(scan)) {
            while(scanner.next() != null) {
                rowCountFromHBase++;
            }
        }
        // Assert all the rows are visible on full table scan from HBase
        Assert.assertEquals(expectedRowCount, rowCountFromHBase);
        String fullScanDql = "SELECT COUNT(*) FROM " + tableName;
        try (PreparedStatement stmt = conn.prepareStatement(fullScanDql)) {
            ResultSet rs = stmt.executeQuery();
            rs.next();
            int rowsVisible = rs.getInt(1);
            rs.close();
            // Assert all the rows are visible on full table scan from Phoenix
            Assert.assertEquals(expectedRowCount, rowsVisible);
        }
    }

    private void assertPointLookupsRowCntFromHBaseAndPhoenix(Connection conn, int expectedRowCount,
                                                             String tableName, int saltBucketCount,
                                                             String firstPrimaryKey,
                                                             int[] pk2Values, int rowsPerSaltBkt)
            throws Exception {
        String secondPrimaryKeyPrefix = "pk2_";
        String primaryKeyPrefix = firstPrimaryKey + secondPrimaryKeyPrefix;
        Table hTable = conn.unwrap(PhoenixConnection.class)
                .getQueryServices().getTable(tableName.getBytes());
        int rowCountFromHBase = 0;
        byte[] rowKey = new byte[primaryKeyPrefix.length() + 3];
        System.arraycopy(Bytes.toBytes(primaryKeyPrefix), 0, rowKey, 1,
                rowKey.length - 3);
        for (int i = 0; i < saltBucketCount; i++) {
            for (int j = i * rowsPerSaltBkt; j < (i + 1) * rowsPerSaltBkt; j++) {
                rowKey[0] = (byte) i;
                byte[] rowKeySuffix = Bytes.toBytes(String.format("%02d", pk2Values[j]));
                rowKey[rowKey.length - 2] = rowKeySuffix[0];
                rowKey[rowKey.length - 1] = rowKeySuffix[1];
                Get get = new Get(rowKey);
                if (!hTable.get(get).isEmpty()) {
                    rowCountFromHBase++;
                }
            }
        }
        // Assert all point lookups are visible from HBase
        Assert.assertEquals(expectedRowCount, rowCountFromHBase);
        StringBuilder pointLookupDql = new StringBuilder("SELECT COUNT(*) FROM ");
        pointLookupDql.append(tableName);
        pointLookupDql.append(" WHERE PK1=? AND PK2 IN (?");
        for (int i = 1; i < pk2Values.length; i++) {
            pointLookupDql.append(",?");
        }
        pointLookupDql.append(")");
        try (PreparedStatement stmt = conn.prepareStatement(pointLookupDql.toString())) {
            stmt.setString(1, firstPrimaryKey);
            for (int i = 0; i < pk2Values.length; i++) {
                stmt.setString(i + 2,
                        String.format(secondPrimaryKeyPrefix + "%02d", i));
            }
            ResultSet rs = stmt.executeQuery();
            rs.next();
            int rowsVisible = rs.getInt(1);
            rs.close();
            // Assert all point lookups are visible from Phoenix
            Assert.assertEquals(expectedRowCount, rowsVisible);
        }
    }

    private void triggerPhoenix7580(Connection conn, String tableName, int saltBucketCount,
                                    String primaryKeyPrefixForNewRows, int[] pk2ValuesForNewRows)
            throws Exception {
        String upsertDml = "UPSERT INTO " + tableName + " VALUES (?,?,?)";
        try (PreparedStatement stmt = conn.prepareStatement(upsertDml)) {
            for (int i: pk2ValuesForNewRows) {
                stmt.setString(1, primaryKeyPrefixForNewRows);
                stmt.setString(2, String.format("pk2_%02d", i));
                stmt.setString(3, "col1_" + i);
                stmt.executeUpdate();
            }
            conn.commit();
        }

        byte[] expectedEndKeyPrefixAfterSplit;
        // Compute split key for splitting region corresponding to the second last salt bucket.
        byte[] splitKey = null;
        byte[] rowKeyPrefix = new byte[primaryKeyPrefixForNewRows.length() + 1];
        System.arraycopy(Bytes.toBytes(primaryKeyPrefixForNewRows), 0,
                rowKeyPrefix, 1, rowKeyPrefix.length - 1);
        // Doing minus 2 from salt bucket count to get second last bucket.
        // Salt buckets are 0 indexed.
        rowKeyPrefix[0] = (byte) (saltBucketCount - 2);
        expectedEndKeyPrefixAfterSplit = new byte[rowKeyPrefix.length];
        // Save this and will be used to verify that conditions to trigger PHOENIX-7580 are
        // being met at the end of this method call.
        System.arraycopy(rowKeyPrefix, 0,
                expectedEndKeyPrefixAfterSplit, 0, rowKeyPrefix.length);
        Table hTable = conn.unwrap(PhoenixConnection.class)
                .getQueryServices().getTable(tableName.getBytes());
        Scan scan = new Scan();
        scan.setStartStopRowForPrefixScan(rowKeyPrefix);
        boolean pastFirstRow = false;
        try (ResultScanner scanner = hTable.getScanner(scan)) {
            Result r;
            while((r = scanner.next()) != null) {
                if (pastFirstRow) {
                    // Use row key of 2nd row out of 3 newly inserted rows as split key
                    // later for splitting the region corresponding to the second last
                    // salt bucket.
                    splitKey = r.getRow();
                    break;
                }
                pastFirstRow = true;
            }
        }

        // Identify region corresponding to the second last salt bucket for splitting
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        List<RegionInfo> regions = admin.getRegions(TableName.valueOf(tableName));
        RegionInfo secondLastSaltBucketRegion = null;
        for (RegionInfo regionInfo : regions) {
            byte[] startKey = regionInfo.getStartKey();
            byte[] endKey = regionInfo.getEndKey();
            if (startKey.length > 0 && startKey[0] == saltBucketCount - 2
                    && endKey.length > 0 && endKey[0] == saltBucketCount - 1) {
                secondLastSaltBucketRegion = regionInfo;
                break;
            }
        }
        Assert.assertNotNull("Not able to determine region of second last salt bucket",
                secondLastSaltBucketRegion);

        // Split region corresponding to the second last salt bucket
        admin.splitRegionAsync(secondLastSaltBucketRegion.getEncodedNameAsBytes(),
                splitKey).get();
        regions = admin.getRegions(TableName.valueOf(tableName));
        // Verify that after split the conditions to reproduce PHOENIX-7580 are being met
        for (RegionInfo regionInfo : regions) {
            byte[] startKey = regionInfo.getStartKey();
            byte[] endKey = regionInfo.getEndKey();
            if (startKey.length > 0 && startKey[0] == saltBucketCount - 2) {
                Assert.assertTrue(
                        Bytes.compareTo(expectedEndKeyPrefixAfterSplit, endKey) < 0);
                break;
            }
        }
    }

    private void createTable(Connection conn, String tableName, int saltBucketCount)
            throws Exception {
        String createTableDdl = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" +
                "    PK1 CHAR(5) NOT NULL,\n" +
                "    PK2 CHAR(6) NOT NULL,\n" +
                "    COL1 VARCHAR,\n" +
                "    CONSTRAINT PK PRIMARY KEY (\n" +
                "        PK1,\n" +
                "        PK2 \n" +
                "    )\n" +
                ") SALT_BUCKETS=" + saltBucketCount;
        // Create table
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableDdl);
        }
    }

    private void addRows(Connection conn, String tableName, String primaryKeyPrefix,
                         int rowsToInsert) throws Exception {
        String upsertDml = "UPSERT INTO " + tableName + " VALUES (?,?,?)";
        // Insert rows in the table with row key prefix at HBase level being
        // {@code primaryKeyPrefix}.
        try (PreparedStatement upsertStmt = conn.prepareStatement(upsertDml)) {
            for (int i = 0; i < rowsToInsert; i++) {
                upsertStmt.setString(1, primaryKeyPrefix);
                upsertStmt.setString(2, String.format("pk2_%02d", i));
                upsertStmt.setString(3, "col1_" + i);
                upsertStmt.executeUpdate();
            }
            conn.commit();
        }

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE STATISTICS " + tableName);
        }
    }

    @Test
    public void testRangeScanForPhoenix7580WithStatsForParallelization() throws Exception {
        testPhoenix7580(true, false, false);
    }

    @Test
    public void testRangeScanForPhoenix7580WithoutStatsForParallelization() throws Exception {
        testPhoenix7580(false, false, false);
    }

    @Test
    public void testFullTableScanWithStatsForParallelization() throws Exception {
        testPhoenix7580(true, true, false);
    }

    @Test
    public void testFullTableScanWithoutStatsForParallelization() throws Exception {
        testPhoenix7580(false, true, false);
    }

    @Test
    public void testPointLookupsWithStatsForParallelization() throws Exception {
        testPhoenix7580(true, false, true);
    }

    @Test
    public void testPointLookupsWithoutStatsForParallelization() throws Exception {
        testPhoenix7580(false, false, true);
    }

    // Added this test to assert that no additional testing is needed for local indexes for
    // PHOENIX-7580 as local indexes cannot be salted.
    @Test
    public void testLocalIndexesCannotBeSalted() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        int saltBucketCount = 5;
        String createLocalIndexDdl = "CREATE LOCAL INDEX IF NOT EXISTS " + indexName + " ON "
                + tableName + " (COL1)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            createTable(conn, tableName, saltBucketCount);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createLocalIndexDdl + " SALT_BUCKETS=" + saltBucketCount);
                Assert.fail("Local index should not be salted");
            }
            catch (SQLException e) {
                Assert.assertEquals(1110, e.getErrorCode());
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createLocalIndexDdl);
            }
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            Assert.assertNull(pconn.getTableNoCache(indexName).getBucketNum());
        }
    }
}
