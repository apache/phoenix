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
package org.apache.phoenix.end2end.index;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ImmutableIndexExtendedIT extends BaseUniqueNamesOwnClusterIT {

    private final String tableDDLOptions;

    public ImmutableIndexExtendedIT() {
        StringBuilder optionBuilder = new StringBuilder("IMMUTABLE_ROWS=true");
        this.tableDDLOptions = optionBuilder.toString();
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
        setUpTestDriver(new ReadOnlyProps(props));
    }

    public static class PreMutationFailingRegionObserver extends SimpleRegionObserver {

        @Override public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            System.out.println("preBatchMutate my coproc " + this);
            throw new IOException();
        }
    }

    public static class PostMutationFailingRegionObserver extends SimpleRegionObserver {
        
        @Override public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            System.out.println("postBatchMutate my coproc " + this);
            throw new IOException();
        }
    }

    public static class FailOnceMutationRegionObserver extends SimpleRegionObserver {

        private boolean failOnce = true;

        @Override public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            System.out.println("Fail once my coproc " + this);
            if (failOnce) {
                // next attempt don't raise
                failOnce = false;
                throw new IOException();
            }
        }
    }

    private void createAndPopulateTable(Connection conn, String tableName, int rowCount)
            throws Exception {
        String ddl = "CREATE TABLE " + tableName +
                " (id integer not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))"
                + tableDDLOptions;

        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + tableName + " (id, val1, val2, val3) VALUES (?, ?, ?, ?)";
        PreparedStatement stmt = conn.prepareStatement(dml);

        for (int id = 1; id <= rowCount; ++id) {
            stmt.setInt(1, id);
            stmt.setString(2, "a" + id);
            stmt.setString(3, "ab" + id);
            stmt.setString(4, "abc" + id);
            stmt.executeUpdate();
        }
        conn.commit();
    }

    private void createIndex(Connection conn, String dataTable, String indexTable)
            throws Exception {
        conn.createStatement().execute("CREATE INDEX " + indexTable + " on " + dataTable
                + " (val1) include (val2, val3)");
        conn.commit();
        TestUtil.waitForIndexState(conn, indexTable, PIndexState.ACTIVE);
    }
    
    private static int getRowCountForEmptyColValue(Connection conn, String tableName, byte[] valueBytes)
            throws IOException, SQLException {
        PTable table = PhoenixRuntime.getTable(conn, tableName);
        byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(table);
        byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(table).getFirst();
        HTable htable = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(table.getPhysicalName().getBytes());
        Scan scan = new Scan();
        scan.addColumn(emptyCF, emptyCQ);
        ResultScanner resultScanner = htable.getScanner(scan);
        int count = 0;

        for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
            if (Bytes.compareTo(result.getValue(emptyCF, emptyCQ), 0, valueBytes.length,
                    valueBytes, 0, valueBytes.length) == 0) {
                ++count;
            }
        }
        return count;
    }

    private static void verifyRowCountForEmptyCol(Connection conn, String indexTable,
            int expectedVerifiedCount, int expectedUnverifiedCount) throws Exception {

        assertEquals(expectedVerifiedCount,
                getRowCountForEmptyColValue(conn, indexTable, IndexRegionObserver.VERIFIED_BYTES));
        assertEquals(expectedUnverifiedCount,
                getRowCountForEmptyColValue(conn, indexTable, IndexRegionObserver.UNVERIFIED_BYTES));
    }
    
    @Test
    public void testGlobalImmutableIndexUpsertPreMutationFailure() throws Exception {
        String dataTable = "TBL_" + generateUniqueName();
        String indexTable = "IND_" + generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final int initialRowCount = 2;
            createAndPopulateTable(conn, dataTable, initialRowCount);
            createIndex(conn, dataTable, indexTable);
            TestUtil.addCoprocessor(conn, indexTable, PreMutationFailingRegionObserver.class);

            boolean upsertfailed = false;
            try {
                conn.createStatement().execute("UPSERT INTO " + dataTable +
                        " VALUES (3, 'a3', 'ab3', 'abc3')");
                System.out.println("upsert initiated");
                conn.commit();
            } catch (Exception ex)
            {
                upsertfailed = true;
            }
            assertEquals(true, upsertfailed);

            // verify that the row was not inserted into the data table
            String dql = "SELECT * FROM " + dataTable + " WHERE id = 3";
            ResultSet rs = conn.createStatement().executeQuery(dql);
            assertFalse(rs.next());

            verifyRowCountForEmptyCol(conn, indexTable, initialRowCount, 0);

            TestUtil.removeCoprocessor(conn, indexTable, PreMutationFailingRegionObserver.class);
        }
    }

    @Test
    public void testGlobalImmutableIndexUpsertPostMutationFailure() throws Exception {
        String dataTable = "TBL_" + generateUniqueName();
        String indexTable = "IND_" + generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final int initialRowCount = 2;
            createAndPopulateTable(conn, dataTable, initialRowCount);
            createIndex(conn, dataTable, indexTable);
            TestUtil.addCoprocessor(conn, indexTable, PostMutationFailingRegionObserver.class);

            boolean upsertfailed = false;
            try {
                conn.createStatement().execute("UPSERT INTO " + dataTable +
                        " VALUES (3, 'a3', 'ab3', 'abc3')");
                System.out.println("upsert initiated");
                conn.commit();
            } catch (Exception ex)
            {
                upsertfailed = true;
            }
            assertEquals(true, upsertfailed);

            // verify that the row was not inserted into the data table
            String dql = "SELECT * FROM " + dataTable + " WHERE id = 3";
            ResultSet rs = conn.createStatement().executeQuery(dql);
            assertFalse(rs.next());

            verifyRowCountForEmptyCol(conn, indexTable, initialRowCount, 1);

            TestUtil.removeCoprocessor(conn, indexTable, PostMutationFailingRegionObserver.class);
        }
    }

    @Test
    public void testGlobalImmutableIndexUpsertRetry() throws Exception {
        String dataTable = "TBL_" + generateUniqueName();
        String indexTable = "IND_" + generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final int initialRowCount = 2;
            createAndPopulateTable(conn, dataTable, initialRowCount);
            createIndex(conn, dataTable, indexTable);
            TestUtil.addCoprocessor(conn, indexTable, FailOnceMutationRegionObserver.class);

            conn.createStatement()
                    .execute("UPSERT INTO " + dataTable + " VALUES (3, 'a3', 'ab3', 'abc3')");
            conn.commit();

            // verify that the row was inserted into the data table
            String dql = "SELECT * FROM " + dataTable + " WHERE id = 3";
            ResultSet rs = conn.createStatement().executeQuery(dql);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            verifyRowCountForEmptyCol(conn, indexTable, initialRowCount + 1, 0);

            TestUtil.removeCoprocessor(conn, indexTable, FailOnceMutationRegionObserver.class);
        }
    }
}
