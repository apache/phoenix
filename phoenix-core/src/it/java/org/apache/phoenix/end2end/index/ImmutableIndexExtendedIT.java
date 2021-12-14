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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
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
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class ImmutableIndexExtendedIT extends ParallelStatsDisabledIT {

    private final String tableDDLOptions;
    private final FailingRegionObserver coproc;
    private final Boolean useView;

    public ImmutableIndexExtendedIT(FailingRegionObserver coproc, Boolean useView) {
        this.coproc = coproc;
        this.useView = useView;
        StringBuilder optionBuilder = new StringBuilder("IMMUTABLE_ROWS=true");
        this.tableDDLOptions = optionBuilder.toString();
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "5");
        setUpTestDriver(new ReadOnlyProps(props));
    }

    private enum FailStep {
        NONE,
        PRE_INDEX_TABLE_UPDATE,
        POST_INDEX_TABLE_UPDATE
    }

    private boolean getExpectedStatus(FailStep step) {
        boolean status;
        switch (step) {
        case NONE:
            status = true;
            break;
        case PRE_INDEX_TABLE_UPDATE:
        case POST_INDEX_TABLE_UPDATE:
        default:
            status = false;
        }
        return status;
    }

    private int getExpectedUnverifiedRowCount(FailStep step) {
        int unverifiedRowCount;
        switch (step) {
        case POST_INDEX_TABLE_UPDATE:
            unverifiedRowCount = 1;
            break;
        case NONE:
        case PRE_INDEX_TABLE_UPDATE:
        default:
            unverifiedRowCount = 0;
        }
        return unverifiedRowCount;
    }

    interface FailingRegionObserver {
        FailStep getFailStep();
    }

    public static class PreMutationFailingRegionObserver extends SimpleRegionObserver
            implements FailingRegionObserver {

        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            throw new IOException();
        }

        @Override
        public FailStep getFailStep() {
            return FailStep.PRE_INDEX_TABLE_UPDATE;
        }
    }

    public static class PostMutationFailingRegionObserver extends SimpleRegionObserver
            implements FailingRegionObserver{

        @Override
        public void postBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            throw new IOException();
        }

        @Override
        public FailStep getFailStep() {
            return FailStep.POST_INDEX_TABLE_UPDATE;
        }
    }

    public static class FailOnceMutationRegionObserver extends SimpleRegionObserver
            implements FailingRegionObserver{

        private boolean failOnce = true;

        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            if (failOnce) {
                // next attempt don't raise
                failOnce = false;
                throw new IOException();
            }
        }

        @Override
        public FailStep getFailStep() { return FailStep.NONE; }
    }

    @Parameterized.Parameters(name ="coproc = {0}, useView = {1}")
    public static Collection<Object[]> data() {
        List<Object[]> params = Lists.newArrayListWithExpectedSize(6);
        boolean[] Booleans = new boolean[] { false, true };
        for (boolean useView : Booleans) {
            params.add(new Object[]{new PreMutationFailingRegionObserver(), useView});
            params.add(new Object[]{new PostMutationFailingRegionObserver(), useView});
            params.add(new Object[]{new FailOnceMutationRegionObserver(), useView});
        }
        return params;
    }

    private void createAndPopulateTable(Connection conn, String tableName, int rowCount)
            throws Exception {
        String ddl = "CREATE TABLE " + tableName
                + " (id integer not null primary key, val1 varchar, val2 varchar, val3 varchar)"
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

    private void createView(Connection conn, String dataTable, String viewTable)
        throws Exception {
        String ddl = "CREATE VIEW " + viewTable + " AS SELECT * FROM " + dataTable;
        conn.createStatement().execute(ddl);
    }

    private void createIndex(Connection conn, String dataTable, String indexTable)
            throws Exception {
        String ddl = "CREATE INDEX " + indexTable + " on " + dataTable
                + " (val1) include (val2, val3)";
        conn.createStatement().execute(ddl);
        conn.commit();
        TestUtil.waitForIndexState(conn, indexTable, PIndexState.ACTIVE);
    }
    
    private static int getRowCountForEmptyColValue(Connection conn, String tableName,
            byte[] valueBytes)  throws IOException, SQLException {

        PTable table = PhoenixRuntime.getTable(conn, tableName);
        byte[] emptyCF = SchemaUtil.getEmptyColumnFamily(table);
        byte[] emptyCQ = EncodedColumnsUtil.getEmptyKeyValueInfo(table).getFirst();
        ConnectionQueryServices queryServices =
                conn.unwrap(PhoenixConnection.class).getQueryServices();
        HTable htable = (HTable) queryServices.getTable(table.getPhysicalName().getBytes());
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

        assertEquals(expectedVerifiedCount, getRowCountForEmptyColValue(conn, indexTable,
                QueryConstants.VERIFIED_BYTES));
        assertEquals(expectedUnverifiedCount, getRowCountForEmptyColValue(conn, indexTable,
                QueryConstants.UNVERIFIED_BYTES));
    }

    @Test
    public void testFailingUpsertMutations()  throws Exception {
        String dataTable = "TBL_" + generateUniqueName();
        String indexTable = "IND_" + generateUniqueName();
        String viewTable = "VIEW_" + generateUniqueName();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final int initialRowCount = 2;
            createAndPopulateTable(conn, dataTable, initialRowCount);
            createView(conn, dataTable, viewTable);
            String baseTable = useView ? viewTable : dataTable;
            createIndex(conn, baseTable, indexTable);
            String index = PhoenixRuntime.getTable(conn, indexTable).getPhysicalName().getString();
            TestUtil.addCoprocessor(conn, index, coproc.getClass());
            boolean upsertStatus = true;
            try {
                String dml = "UPSERT INTO " + baseTable + " VALUES (3, 'a3', 'ab3', 'abc3')";
                conn.createStatement().execute(dml);
                conn.commit();
            } catch (Exception ex) {
                upsertStatus = false;
            }
            boolean expectedStatus = getExpectedStatus(coproc.getFailStep());
            assertEquals(expectedStatus, upsertStatus);

            String dql = "SELECT * FROM " + baseTable + " WHERE id = 3";
            ResultSet rs = conn.createStatement().executeQuery(dql);
            if (!upsertStatus) {
                // verify that the row was not inserted into the data table
                assertFalse(rs.next());
                verifyRowCountForEmptyCol(conn, indexTable, initialRowCount,
                        getExpectedUnverifiedRowCount(coproc.getFailStep()));
            } else {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                verifyRowCountForEmptyCol(conn, indexTable, initialRowCount + 1,
                        getExpectedUnverifiedRowCount(coproc.getFailStep()));
            }
            TestUtil.removeCoprocessor(conn, index, coproc.getClass());
        }
    }
}
