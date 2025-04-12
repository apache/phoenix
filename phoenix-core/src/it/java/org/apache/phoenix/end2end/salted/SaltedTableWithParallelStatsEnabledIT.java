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
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.junit.Test;
import org.junit.Assert;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;


@Category(ParallelStatsEnabledIT.class)
public class SaltedTableWithParallelStatsEnabledIT extends ParallelStatsEnabledIT {

    public void testRangeScanForPhoenix7580(boolean withStatsForParallelization) throws Exception {
        String tableName = generateUniqueName();
        int saltBucketCount = 5;
        String createTableDdl = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" +
                "    PK1 CHAR(5) NOT NULL,\n" +
                "    PK2 CHAR(6) NOT NULL,\n" +
                "    COL1 VARCHAR,\n" +
                "    CONSTRAINT PK PRIMARY KEY (\n" +
                "        PK1,\n" +
                "        PK2 \n" +
                "    )\n" +
                ") SALT_BUCKETS=" + saltBucketCount;
        int rowsToInsert = saltBucketCount * 10;
        String upsertDml = "UPSERT INTO " + tableName + " VALUES (?,?,?)";
        String primaryKeyPrefix = "pk1_1";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create table
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableDdl);
            }

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

            // Run COUNT(*) range query on Phoenix with row key prefix as {@code primaryKeyPrefix}.
            // Assert that count of rows reported by Phoenix is same as count of rows in HBase.
            assertRowCountFromHBaseAndPhoenix(conn, rowsToInsert, tableName, saltBucketCount,
                    primaryKeyPrefix);

            // ********** Create conditions to trigger PHOENIX-7580 ***********

            // Insert 3 rows with row key prefix greater than the row key prefix of rows
            // earlier inserted.
            String primaryKeyPrefixForNewRows = "pk1_2";
            // These values have been carefully selected such that newly inserted rows go to
            // second last salt bucket when salt bucket count = 5.
            int[] pk2ValuesForNewRows = new int[] { 1, 6, 10 };
            triggerPhoenix7580(conn, tableName, upsertDml, saltBucketCount,
                    primaryKeyPrefixForNewRows, pk2ValuesForNewRows);

            // **** Conditions to trigger PHOENIX-7580 have been satisfied. Test the fix now. ****

            assertRowCountFromHBaseAndPhoenix(conn, rowsToInsert, tableName, saltBucketCount,
                    primaryKeyPrefix);
        }
    }

    private void assertRowCountFromHBaseAndPhoenix(Connection conn, int expectedRowCount,
                                                   String tableName, int saltBucketCount,
                                                   String primaryKeyPrefix) throws Exception {
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
        String rangeQueryDql = "SELECT COUNT(*) FROM " + tableName + " WHERE PK1=?";
        try (PreparedStatement stmt = conn.prepareStatement(rangeQueryDql)) {
            stmt.setString(1, primaryKeyPrefix);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            int rowsVisible = rs.getInt(1);
            rs.close();
            // Assert all the rows are visible on running range query from Phoenix
            Assert.assertEquals(expectedRowCount, rowsVisible);
        }
    }

    private void triggerPhoenix7580(Connection conn, String tableName, String upsertDml,
                                    int saltBucketCount, String primaryKeyPrefixForNewRows,
                                    int[] pk2ValuesForNewRows) throws Exception {
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
}
