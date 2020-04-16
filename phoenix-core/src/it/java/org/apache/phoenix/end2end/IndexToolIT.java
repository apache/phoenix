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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.PhoenixIndexImportDirectMapper;
import org.apache.phoenix.mapreduce.index.PhoenixServerBuildIndexMapper;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.IndexScrutiny;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.phoenix.mapreduce.PhoenixJobCounters.INPUT_RECORDS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.AFTER_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.REBUILT_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.SCANNED_DATA_ROW_COUNT;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class IndexToolIT extends BaseUniqueNamesOwnClusterIT {
    private final boolean localIndex;
    private final boolean mutable;
    private final boolean transactional;
    private final boolean directApi;
    private final String tableDDLOptions;
    private final boolean useSnapshot;
    private final boolean useTenantId;

    public IndexToolIT(String transactionProvider, boolean mutable, boolean localIndex,
            boolean directApi, boolean useSnapshot, boolean useTenantId) {
        this.localIndex = localIndex;
        this.mutable = mutable;
        this.transactional = transactionProvider != null;
        this.directApi = directApi;
        this.useSnapshot = useSnapshot;
        this.useTenantId = useTenantId;
        StringBuilder optionBuilder = new StringBuilder();
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true ");
        }
        if (transactional) {
            if (!(optionBuilder.length() == 0)) {
                optionBuilder.append(",");
            }
            optionBuilder.append(" TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + transactionProvider + "'");
        }
        optionBuilder.append(" SPLIT ON(1,2)");
        this.tableDDLOptions = optionBuilder.toString();
    }

    @BeforeClass
    public static synchronized void setup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        serverProps.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB, Long.toString(5));
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS, Long.toString(8));
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        clientProps.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
            new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @Parameters(
            name = "transactionProvider={0},mutable={1},localIndex={2},directApi={3},useSnapshot={4}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayListWithExpectedSize(48);
        boolean[] Booleans = new boolean[] { false, true };
        for (String transactionProvider : new String[] {"TEPHRA", "OMID", null}) {
            for (boolean mutable : Booleans) {
                for (boolean localIndex : Booleans) {
                    if (!localIndex 
                            || transactionProvider == null 
                            || !TransactionFactory.getTransactionProvider(
                                    TransactionFactory.Provider.valueOf(transactionProvider))
                                .isUnsupported(Feature.ALLOW_LOCAL_INDEX)) {
                        if (localIndex) {
                            for (boolean directApi : Booleans) {
                                list.add(new Object[]{transactionProvider, mutable, localIndex,
                                        directApi, false, false});
                            }
                        }
                        else {
                            // Due to PHOENIX-5375 and PHOENIX-5376, the snapshot and bulk load options are ignored for global indexes
                            list.add(new Object[]{transactionProvider, mutable, localIndex,
                                    true, false, false});
                        }
                    }
                }
            }
        }
        // Add the usetenantId
        list.add(new Object[] { null, false, false, true, false, true});
        return TestUtil.filterTxParamData(list,0);
    }

    private void setEveryNthRowWithNull(int nrows, int nthRowNull, PreparedStatement stmt) throws Exception {
        for (int i = 1; i <= nrows; i++) {
            stmt.setInt(1, i);
            stmt.setInt(2, i + 1);
            if (i % nthRowNull != 0) {
                stmt.setInt(3, i * i);
            } else {
                stmt.setNull(3, Types.INTEGER);
            }
            stmt.execute();
        }
    }

    @Test
    public void testWithSetNull() throws Exception {
        // This test is for building non-transactional mutable global indexes with direct api
        if (localIndex || transactional || !directApi || useSnapshot || useTenantId || !mutable) {
            return;
        }
        // This tests the cases where a column having a null value is overwritten with a not null value and vice versa;
        // and after that the index table is still rebuilt correctly
        final int NROWS = 2 * 3 * 5 * 7;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) "
                    + tableDDLOptions);
            String upsertStmt = "UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(upsertStmt);
            setEveryNthRowWithNull(NROWS, 2, stmt);
            conn.commit();
            setEveryNthRowWithNull(NROWS, 3, stmt);
            conn.commit();
            conn.createStatement().execute(String.format(
                    "CREATE %s INDEX %s ON %s (VAL1) INCLUDE (VAL2) ASYNC ",
                    (localIndex ? "LOCAL" : ""), indexTableName, dataTableFullName));
            // Run the index MR job and verify that the index table is built correctly
            IndexTool indexTool = runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0, new String[0]);
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
            actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
            setEveryNthRowWithNull(NROWS, 5, stmt);
            conn.commit();
            actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
            setEveryNthRowWithNull(NROWS, 7, stmt);
            conn.commit();
            actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
            actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
            indexTool = runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null,
                    0, IndexTool.IndexVerifyType.ONLY, new String[0]);
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS).getValue());
            dropIndexToolTables(conn);
        }
    }

    @Test
    public void testSecondaryIndex() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String stmString1 =
                    "CREATE TABLE " + dataTableFullName
                            + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                            + tableDDLOptions;
            conn.createStatement().execute(stmString1);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);

            // insert two rows
            upsertRow(stmt1, 1);
            upsertRow(stmt1, 2);
            conn.commit();

            if (transactional) {
                // insert two rows in another connection without committing so that they are not
                // visible to other transactions
                try (Connection conn2 = DriverManager.getConnection(getUrl(), props)) {
                    conn2.setAutoCommit(false);
                    PreparedStatement stmt2 = conn2.prepareStatement(upsertQuery);
                    upsertRow(stmt2, 5);
                    upsertRow(stmt2, 6);
                    ResultSet rs =
                            conn.createStatement()
                                    .executeQuery("SELECT count(*) from " + dataTableFullName);
                    assertTrue(rs.next());
                    assertEquals("Unexpected row count ", 2, rs.getInt(1));
                    assertFalse(rs.next());
                    rs =
                            conn2.createStatement()
                                    .executeQuery("SELECT count(*) from " + dataTableFullName);
                    assertTrue(rs.next());
                    assertEquals("Unexpected row count ", 4, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }

            String stmtString2 =
                    String.format(
                        "CREATE %s INDEX %s ON %s  (LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz') ASYNC ",
                        (localIndex ? "LOCAL" : ""), indexTableName, dataTableFullName);
            conn.createStatement().execute(stmtString2);

            // verify rows are fetched from data table.
            String selectSql =
                    String.format(
                        "SELECT ID FROM %s WHERE LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz' = 'xxUNAME2_xyz'",
                        dataTableFullName);
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);

            // assert we are pulling from data table.
            assertEquals(String.format(
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER %s\n"
                        + "    SERVER FILTER BY (LPAD(UPPER(NAME, 'en_US'), 8, 'x') || '_xyz') = 'xxUNAME2_xyz'",
                dataTableFullName), actualExplainPlan);

            rs = stmt1.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            conn.commit();

            // run the index MR job.
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName);

            // insert two more rows
            upsertRow(stmt1, 3);
            upsertRow(stmt1, 4);
            conn.commit();

            // assert we are pulling from index table.
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertExplainPlan(localIndex, actualExplainPlan, dataTableFullName, indexTableFullName);

            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            if (localIndex || transactional || useTenantId || useSnapshot) {
                return;
            }
            // Run the index MR job and verify that the global index table is built correctly
            IndexTool indexTool = runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0, IndexTool.IndexVerifyType.BEFORE, new String[0]);
            assertEquals(4, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(4, indexTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(4, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS).getValue());
        } finally {
            conn.close();
        }
    }

    private void dropIndexToolTables(Connection conn) throws Exception {
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        TableName indexToolOutputTable = TableName.valueOf(IndexTool.OUTPUT_TABLE_NAME_BYTES);
        admin.disableTable(indexToolOutputTable);
        admin.deleteTable(indexToolOutputTable);
        TableName indexToolResultTable = TableName.valueOf(IndexTool.RESULT_TABLE_NAME_BYTES);
        admin.disableTable(indexToolResultTable);
        admin.deleteTable(indexToolResultTable);
    }

    @Test
    public void testBuildSecondaryIndexAndScrutinize() throws Exception {
        // This test is for building non-transactional global indexes with direct api
        if (localIndex || transactional || !directApi || useSnapshot || useTenantId) {
            return;
        }
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String stmString1 =
                    "CREATE TABLE " + dataTableFullName
                            + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                            + tableDDLOptions;
            conn.createStatement().execute(stmString1);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);

            // Insert NROWS rows
            final int NROWS = 1000;
            for (int i = 0; i < NROWS; i++) {
                upsertRow(stmt1, i);
            }
            conn.commit();
            String stmtString2 =
                    String.format(
                            "CREATE %s INDEX %s ON %s (NAME) INCLUDE (ZIP) ASYNC ",
                            (localIndex ? "LOCAL" : ""), indexTableName, dataTableFullName);
            conn.createStatement().execute(stmtString2);

            // Run the index MR job and verify that the index table is built correctly
            IndexTool indexTool = runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0, IndexTool.IndexVerifyType.BEFORE, new String[0]);
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS).getValue());
            assertEquals(NROWS, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            long actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);

            // Add more rows and make sure that these rows will be visible to IndexTool
            for (int i = NROWS; i < 2 * NROWS; i++) {
                upsertRow(stmt1, i);
            }
            conn.commit();
            indexTool = runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0, IndexTool.IndexVerifyType.BOTH, new String[0]);
            assertEquals(2 * NROWS, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(2 * NROWS, indexTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(2 * NROWS, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(AFTER_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(AFTER_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(AFTER_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(AFTER_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    AFTER_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS).getValue());
            actualRowCount = IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(2 * NROWS, actualRowCount);
            dropIndexToolTables(conn);
        }
    }

    public static class MutationCountingRegionObserver extends SimpleRegionObserver {
        public static AtomicInteger mutationCount = new AtomicInteger(0);

        public static void setMutationCount(int value) {
            mutationCount.set(0);
        }

        public static int getMutationCount() {
            return mutationCount.get();
        }

        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
                                   MiniBatchOperationInProgress<Mutation> miniBatchOp) throws HBaseIOException {
            mutationCount.addAndGet(miniBatchOp.size());
        }
    }

    private void verifyIndexTableRowKey(byte[] rowKey, String indexTableFullName) {
        // The row key for the output table : timestamp | index table name | data row key
        // The row key for the result table : timestamp | index table name | datable table region name |
        //                                    scan start row | scan stop row

        // This method verifies the common prefix, i.e., "timestamp | index table name | ", since the rest of the
        // fields may include the separator key
        int offset = Bytes.indexOf(rowKey, IndexRebuildRegionScanner.ROW_KEY_SEPARATOR_BYTE);
        offset++;
        byte[] indexTableFullNameBytes = Bytes.toBytes(indexTableFullName);
        assertEquals(Bytes.compareTo(rowKey, offset, indexTableFullNameBytes.length, indexTableFullNameBytes, 0,
                indexTableFullNameBytes.length), 0);
        assertEquals(rowKey[offset + indexTableFullNameBytes.length], IndexRebuildRegionScanner.ROW_KEY_SEPARATOR_BYTE[0]);
    }

    private Cell getErrorMessageFromIndexToolOutputTable(Connection conn, String dataTableFullName, String indexTableFullName)
            throws Exception {
        byte[] indexTableFullNameBytes = Bytes.toBytes(indexTableFullName);
        byte[] dataTableFullNameBytes = Bytes.toBytes(dataTableFullName);
        Table hIndexTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getTable(IndexTool.OUTPUT_TABLE_NAME_BYTES);
        Scan scan = new Scan();
        ResultScanner scanner = hIndexTable.getScanner(scan);
        boolean dataTableNameCheck = false;
        boolean indexTableNameCheck = false;
        Cell errorMessageCell = null;
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            for (Cell cell : result.rawCells()) {
                assertTrue(Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                        IndexTool.OUTPUT_TABLE_COLUMN_FAMILY, 0,
                        IndexTool.OUTPUT_TABLE_COLUMN_FAMILY.length) == 0);
                if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        IndexTool.DATA_TABLE_NAME_BYTES, 0, IndexTool.DATA_TABLE_NAME_BYTES.length) == 0) {
                    dataTableNameCheck = true;
                    assertTrue(Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                            dataTableFullNameBytes, 0, dataTableFullNameBytes.length) == 0);
                } else if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        IndexTool.INDEX_TABLE_NAME_BYTES, 0, IndexTool.INDEX_TABLE_NAME_BYTES.length) == 0) {
                    indexTableNameCheck = true;
                    assertTrue(Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                            indexTableFullNameBytes, 0, indexTableFullNameBytes.length) == 0);
                } else if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        IndexTool.ERROR_MESSAGE_BYTES, 0, IndexTool.ERROR_MESSAGE_BYTES.length) == 0) {
                    errorMessageCell = cell;
                }
            }
        }
        assertTrue(dataTableNameCheck && indexTableNameCheck && errorMessageCell != null);
        verifyIndexTableRowKey(CellUtil.cloneRow(errorMessageCell), indexTableFullName);
        hIndexTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getTable(IndexTool.RESULT_TABLE_NAME_BYTES);
        scan = new Scan();
        scanner = hIndexTable.getScanner(scan);
        Result result = scanner.next();
        assert(result != null);
        verifyIndexTableRowKey(CellUtil.cloneRow(result.rawCells()[0]), indexTableFullName);
        return errorMessageCell;
    }

    @Test
    public void testIndexToolVerifyBeforeAndBothOptions() throws Exception {
        // This test is for building non-transactional global indexes with direct api
        if (localIndex || transactional || !directApi || useSnapshot || useTenantId) {
            return;
        }
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String schemaName = generateUniqueName();
            String dataTableName = generateUniqueName();
            String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
            String indexTableName = generateUniqueName();
            String viewName = generateUniqueName();
            String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                    + tableDDLOptions);
            conn.commit();
            conn.createStatement().execute("CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName);
            conn.commit();
            // Insert a row
            conn.createStatement().execute("upsert into " + viewFullName + " values (1, 'Phoenix', 12345)");
            conn.commit();
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP) ASYNC", indexTableName, viewFullName));
            TestUtil.addCoprocessor(conn, "_IDX_" + dataTableFullName, MutationCountingRegionObserver.class);
            // Run the index MR job and verify that the index table rebuild succeeds
            runIndexTool(directApi, useSnapshot, schemaName, viewName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.AFTER);
            assertEquals(1, MutationCountingRegionObserver.getMutationCount());
            MutationCountingRegionObserver.setMutationCount(0);
            // Since all the rows are in the index table, running the index tool with the "-v BEFORE" option should not
            // write any index rows
            runIndexTool(directApi, useSnapshot, schemaName, viewName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.BEFORE);
            assertEquals(0, MutationCountingRegionObserver.getMutationCount());
            // The "-v BOTH" option should not write any index rows either
            runIndexTool(directApi, useSnapshot, schemaName, viewName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.BOTH);
            assertEquals(0, MutationCountingRegionObserver.getMutationCount());
            dropIndexToolTables(conn);
        }
    }

    @Test
    public void testIndexToolVerifyAfterOption() throws Exception {
        // This test is for building non-transactional global indexes with direct api
        if (localIndex || transactional || !directApi || useSnapshot || useTenantId) {
            return;
        }
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String schemaName = generateUniqueName();
            String dataTableName = generateUniqueName();
            String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
            String indexTableName = generateUniqueName();
            String viewName = generateUniqueName();
            String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                    + tableDDLOptions);
            conn.commit();
            conn.createStatement().execute("CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName);
            conn.commit();
            // Insert a row
            conn.createStatement().execute("upsert into " + viewFullName + " values (1, 'Phoenix', 12345)");
            conn.commit();
            // Configure IndexRegionObserver to fail the first write phase. This should not
            // lead to any change on index and thus the index verify during index rebuild should fail
            IndexRegionObserver.setIgnoreIndexRebuildForTesting(true);
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (NAME) INCLUDE (ZIP) ASYNC", indexTableName, viewFullName));
            // Run the index MR job and verify that the index table rebuild fails
            runIndexTool(directApi, useSnapshot, schemaName, viewName, indexTableName,
                    null, -1, IndexTool.IndexVerifyType.AFTER);
            // The index tool output table should report that there is a missing index row
            Cell cell = getErrorMessageFromIndexToolOutputTable(conn, dataTableFullName, "_IDX_" + dataTableFullName);
            byte[] expectedValueBytes = Bytes.toBytes("Missing index row");
            assertTrue(Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                    expectedValueBytes, 0, expectedValueBytes.length) == 0);
            IndexRegionObserver.setIgnoreIndexRebuildForTesting(false);
            dropIndexToolTables(conn);
        }
    }

    @Test
    public void testIndexToolOnlyVerifyOption() throws Exception {
        // This test is for building non-transactional global indexes with direct api
        if (localIndex || transactional || !directApi || useSnapshot || useTenantId) {
            return;
        }
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, CODE VARCHAR) COLUMN_ENCODED_BYTES=0");
            // Insert a row
            conn.createStatement().execute("upsert into " + dataTableFullName + " values (1, 'Phoenix', 'A')");
            conn.commit();
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (NAME) INCLUDE (CODE) ASYNC", indexTableName, dataTableFullName));
            // Run the index MR job to only verify that each data table row has a corresponding index row
            // IndexTool will go through each data table row and record the mismatches in the output table
            // called PHOENIX_INDEX_TOOL
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);
            Cell cell = getErrorMessageFromIndexToolOutputTable(conn, dataTableFullName, indexTableFullName);
            byte[] expectedValueBytes = Bytes.toBytes("Missing index row");
            assertTrue(Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                    expectedValueBytes, 0, expectedValueBytes.length) == 0);
            // Delete the output table for the next test
            dropIndexToolTables(conn);
            // Run the index tool to populate the index while verifying rows
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.AFTER);
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, 0, IndexTool.IndexVerifyType.ONLY);
            dropIndexToolTables(conn);
        }
    }

    @Test
    public void testIndexToolVerifyWithExpiredIndexRows() throws Exception {
        // This test is for building non-transactional global indexes with direct api
        if (localIndex || transactional || !directApi || useSnapshot || useTenantId) {
            return;
        }
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTableFullName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, CODE VARCHAR) COLUMN_ENCODED_BYTES=0");
            // Insert a row
            conn.createStatement()
                    .execute("upsert into " + dataTableFullName + " values (1, 'Phoenix', 'A')");
            conn.commit();
            conn.createStatement()
                    .execute(String.format("CREATE INDEX %s ON %s (NAME) INCLUDE (CODE) ASYNC",
                        indexTableName, dataTableFullName));
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0,
                IndexTool.IndexVerifyType.ONLY);
            Cell cell =
                    getErrorMessageFromIndexToolOutputTable(conn, dataTableFullName,
                        indexTableFullName);
            byte[] expectedValueBytes = Bytes.toBytes("Missing index row");
            assertTrue(Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(),
                cell.getValueLength(), expectedValueBytes, 0, expectedValueBytes.length) == 0);

            // Run the index tool to populate the index while verifying rows
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0,
                IndexTool.IndexVerifyType.AFTER);

            // Set ttl of index table ridiculously low so that all data is expired
            Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            TableName indexTable = TableName.valueOf(indexTableFullName);
            ColumnFamilyDescriptor desc = admin.getDescriptor(indexTable).getColumnFamilies()[0];
            ColumnFamilyDescriptorBuilder builder =
                    ColumnFamilyDescriptorBuilder.newBuilder(desc).setTimeToLive(1);
            Future<Void> future = admin.modifyColumnFamilyAsync(indexTable, builder.build());
            Thread.sleep(1000);
            future.get(40, TimeUnit.SECONDS);

            TableName indexToolOutputTable = TableName.valueOf(IndexTool.OUTPUT_TABLE_NAME_BYTES);
            admin.disableTable(indexToolOutputTable);
            admin.deleteTable(indexToolOutputTable);
            // Run the index tool using the only-verify option, verify it gives no mismatch
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0,
                IndexTool.IndexVerifyType.ONLY);
            Scan scan = new Scan();
            Table hIndexToolTable =
                    conn.unwrap(PhoenixConnection.class).getQueryServices()
                            .getTable(indexToolOutputTable.getName());
            Result r = hIndexToolTable.getScanner(scan).next();
            assertTrue(r == null);
            dropIndexToolTables(conn);
        }
    }

    @Test
    public void testIndexToolWithTenantId() throws Exception {
        if (!useTenantId) { return;}
        String tenantId = generateUniqueName();
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String viewTenantName = generateUniqueName();
        String indexNameGlobal = generateUniqueName();
        String indexNameTenant = generateUniqueName();
        String viewIndexTableName = "_IDX_" + dataTableName;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection connGlobal = DriverManager.getConnection(getUrl(), props);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        Connection connTenant = DriverManager.getConnection(getUrl(), props);
        String createTblStr = "CREATE TABLE %s (TENANT_ID VARCHAR(15) NOT NULL,ID INTEGER NOT NULL"
                + ", NAME VARCHAR, CONSTRAINT PK_1 PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";
        String createViewStr = "CREATE VIEW %s AS SELECT * FROM %s";

        String upsertQueryStr = "UPSERT INTO %s (TENANT_ID, ID, NAME) VALUES('%s' , %d, '%s')";
        String createIndexStr = "CREATE INDEX %s ON %s (NAME) ";

        try {
            String tableStmtGlobal = String.format(createTblStr, dataTableName);
            connGlobal.createStatement().execute(tableStmtGlobal);

            String viewStmtTenant = String.format(createViewStr, viewTenantName, dataTableName);
            connTenant.createStatement().execute(viewStmtTenant);

            String idxStmtTenant = String.format(createIndexStr, indexNameTenant, viewTenantName);
            connTenant.createStatement().execute(idxStmtTenant);

            connTenant.createStatement()
                    .execute(String.format(upsertQueryStr, viewTenantName, tenantId, 1, "x"));
            connTenant.commit();

            runIndexTool(true, false, "", viewTenantName, indexNameTenant,
                    tenantId, 0, new String[0]);

            String selectSql = String.format("SELECT ID FROM %s WHERE NAME='x'", viewTenantName);
            ResultSet rs = connTenant.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertExplainPlan(false, actualExplainPlan, "", viewIndexTableName);
            rs = connTenant.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            // Remove from tenant view index and build.
            ConnectionQueryServices queryServices = connGlobal.unwrap(PhoenixConnection.class).getQueryServices();
            Admin admin = queryServices.getAdmin();
            TableName tableName = TableName.valueOf(viewIndexTableName);
            admin.disableTable(tableName);
            admin.truncateTable(tableName, false);

            runIndexTool(true, false, "", viewTenantName, indexNameTenant,
                    tenantId, 0, new String[0]);

            Table htable= queryServices.getTable(Bytes.toBytes(viewIndexTableName));
            int count = getUtility().countRows(htable);
            // Confirm index has rows
            assertTrue(count == 1);

            selectSql = String.format("SELECT /*+ INDEX(%s) */ COUNT(*) FROM %s",
                    indexNameTenant, viewTenantName);
            rs = connTenant.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            String idxStmtGlobal =
                    String.format(createIndexStr, indexNameGlobal, dataTableName);
            connGlobal.createStatement().execute(idxStmtGlobal);

            // run the index MR job this time with tenant id.
            // We expect it to return -1 because indexTable is not correct for this tenant.
            runIndexTool(true, false, schemaName, dataTableName, indexNameGlobal,
                    tenantId, -1, new String[0]);

        } finally {
            connGlobal.close();
            connTenant.close();
        }
    }

    @Test
    public void testSecondaryGlobalIndexFailure() throws Exception {
        // This test is for building non-transactional global indexes with direct api
        if (localIndex || transactional || !directApi || useSnapshot || useTenantId) {
            return;
        }
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String stmString1 =
                    "CREATE TABLE " + dataTableFullName
                            + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                            + tableDDLOptions;
            conn.createStatement().execute(stmString1);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);

            // Insert two rows
            upsertRow(stmt1, 1);
            upsertRow(stmt1, 2);
            conn.commit();

            String stmtString2 =
                    String.format(
                            "CREATE %s INDEX %s ON %s  (LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz') ASYNC ",
                            (localIndex ? "LOCAL" : ""), indexTableName, dataTableFullName);
            conn.createStatement().execute(stmtString2);

            // Run the index MR job.
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName);

            String qIndexTableName = SchemaUtil.getQualifiedTableName(schemaName, indexTableName);

            // Verify that the index table is in the ACTIVE state
            assertEquals(PIndexState.ACTIVE, TestUtil.getIndexState(conn, qIndexTableName));

            ConnectionQueryServices queryServices = conn.unwrap(PhoenixConnection.class).getQueryServices();
            Admin admin = queryServices.getAdmin();
            TableName tableName = TableName.valueOf(qIndexTableName);
            admin.disableTable(tableName);

            // Run the index MR job and it should fail (return -1)
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,
                    null, -1, new String[0]);

            // Verify that the index table should be still in the ACTIVE state
            assertEquals(PIndexState.ACTIVE, TestUtil.getIndexState(conn, qIndexTableName));
        }
    }
    
    @Test
    public void testSaltedVariableLengthPK() throws Exception {
        if (!mutable) return;
        if (transactional) return;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        try (Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))) {
            String dataDDL =
                    "CREATE TABLE " + dataTableFullName + "(\n"
                            + "ID VARCHAR NOT NULL PRIMARY KEY,\n"
                            + "\"info\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"info\".CAP_DATE VARCHAR NULL,\n" + "\"info\".ORG_ID BIGINT NULL,\n"
                            + "\"info\".ORG_NAME VARCHAR(255) NULL\n" + ") SALT_BUCKETS=3";
            conn.createStatement().execute(dataDDL);

            String upsert =
                    "UPSERT INTO " + dataTableFullName
                            + "(ID,CAR_NUM,CAP_DATE,ORG_ID,ORG_NAME) VALUES('1','car1','2016-01-01 00:00:00',11,'orgname1')";
            conn.createStatement().execute(upsert);
            conn.commit();

            String indexDDL =
                    String.format(
                        "CREATE %s INDEX %s on %s (\"info\".CAR_NUM,\"info\".CAP_DATE) ASYNC",
                        (localIndex ? "LOCAL" : ""), indexTableName, dataTableFullName);
            conn.createStatement().execute(indexDDL);

            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName);

            ResultSet rs =
                    conn.createStatement().executeQuery(
                        "SELECT ORG_ID,CAP_DATE,CAR_NUM,ORG_NAME FROM " + dataTableFullName
                                + " WHERE CAR_NUM='car1' AND CAP_DATE>='2016-01-01' AND CAP_DATE<='2016-05-02' LIMIT 10");
            assertTrue(rs.next());
            int orgId = rs.getInt(1);
            assertEquals(11, orgId);
        }
    }

    /**
     * Test presplitting an index table
     */
    @Test
    public void testSplitIndex() throws Exception {
        if (localIndex) return; // can't split local indexes
        if (!mutable) return;
        if (transactional) return;
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        final TableName dataTN = TableName.valueOf(dataTableFullName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        TableName indexTN = TableName.valueOf(indexTableFullName);
        try (Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
                Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            String dataDDL =
                    "CREATE TABLE " + dataTableFullName + "(\n"
                            + "ID VARCHAR NOT NULL PRIMARY KEY,\n"
                            + "\"info\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"test\".CAR_NUM VARCHAR(18) NULL,\n"
                            + "\"info\".CAP_DATE VARCHAR NULL,\n" + "\"info\".ORG_ID BIGINT NULL,\n"
                            + "\"info\".ORG_NAME VARCHAR(255) NULL\n" + ") COLUMN_ENCODED_BYTES = 0";
            conn.createStatement().execute(dataDDL);

            String[] carNumPrefixes = new String[] {"a", "b", "c", "d"};

            // split the data table, as the tool splits the index table to have the same # of regions
            // doesn't really matter what the split points are, we just want a target # of regions
            int numSplits = carNumPrefixes.length;
            int targetNumRegions = numSplits + 1;
            byte[][] splitPoints = new byte[numSplits][];
            for (String prefix : carNumPrefixes) {
                splitPoints[--numSplits] = Bytes.toBytes(prefix);
            }
            HTableDescriptor dataTD = admin.getTableDescriptor(dataTN);
            admin.disableTable(dataTN);
            admin.deleteTable(dataTN);
            admin.createTable(dataTD, splitPoints);
            assertEquals(targetNumRegions, admin.getTableRegions(dataTN).size());

            // insert data where index column values start with a, b, c, d
            int idCounter = 1;
            try (PreparedStatement ps = conn.prepareStatement("UPSERT INTO " + dataTableFullName
                + "(ID,\"info\".CAR_NUM,\"test\".CAR_NUM,CAP_DATE,ORG_ID,ORG_NAME) VALUES(?,?,?,'2016-01-01 00:00:00',11,'orgname1')")){
                for (String carNum : carNumPrefixes) {
                    for (int i = 0; i < 100; i++) {
                        ps.setString(1, idCounter++ + "");
                        ps.setString(2, carNum + "_" + i);
                        ps.setString(3, "test-" + carNum + "_ " + i);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
                conn.commit();
            }

            String indexDDL =
                    String.format(
                        "CREATE INDEX %s on %s (\"info\".CAR_NUM,\"test\".CAR_NUM,\"info\".CAP_DATE) ASYNC",
                        indexTableName, dataTableFullName);
            conn.createStatement().execute(indexDDL);

            // run with 50% sampling rate, split if data table more than 3 regions
            runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName,"-sp", "50", "-spa", "3");

            assertEquals(targetNumRegions, admin.getTableRegions(indexTN).size());
            List<Cell> values = new ArrayList<>();
            // every index region should have been written to, if the index table was properly split uniformly
            for (HRegion region : getUtility().getHBaseCluster().getRegions(indexTN)) {
                values.clear();
                RegionScanner scanner = region.getScanner(new Scan());
                scanner.next(values);
                if (values.isEmpty()) {
                    fail("Region did not have any results: " + region.getRegionInfo());
                }
            }
        }
    }

    public static void assertExplainPlan(boolean localIndex, String actualExplainPlan,
            String dataTableFullName, String indexTableFullName) {
        String expectedExplainPlan;
        if (localIndex) {
            expectedExplainPlan = String.format(" RANGE SCAN OVER %s [1,", dataTableFullName);
        } else {
            expectedExplainPlan = String.format(" RANGE SCAN OVER %s", indexTableFullName);
        }
        assertTrue(actualExplainPlan + "\n expected to contain \n" + expectedExplainPlan,
            actualExplainPlan.contains(expectedExplainPlan));
    }

    private static List<String> getArgList (boolean directApi, boolean useSnapshot, String schemaName,
                            String dataTable, String indxTable, String tenantId,
                            IndexTool.IndexVerifyType verifyType, Long startTime, Long endTime) {
        List<String> args = Lists.newArrayList();
        if (schemaName != null) {
            args.add("-s");
            args.add(schemaName);
        }
        args.add("-dt");
        args.add(dataTable);
        args.add("-it");
        args.add(indxTable);
        if (directApi) {
            args.add("-direct");
        }
        args.add("-v" + verifyType.getValue()); // verify index rows inline

        // Need to run this job in foreground for the test to be deterministic
        args.add("-runfg");
        if (useSnapshot) {
            args.add("-snap");
        }

        if (tenantId != null) {
            args.add("-tenant");
            args.add(tenantId);
        }
        if(startTime != null) {
            args.add("-st");
            args.add(String.valueOf(startTime));
        }
        if(endTime != null) {
            args.add("-et");
            args.add(String.valueOf(endTime));
        }
        args.add("-op");
        args.add("/tmp/" + UUID.randomUUID().toString());
        return args;
    }

    public static String[] getArgValues(boolean directApi, boolean useSnapshot, String schemaName,
                                        String dataTable, String indexTable, String tenantId, IndexTool.IndexVerifyType verifyType) {
        List<String> args = getArgList(directApi, useSnapshot, schemaName, dataTable, indexTable,
                tenantId, verifyType, null, null);
        return args.toArray(new String[0]);
    }

    public static String [] getArgValues(boolean directApi, boolean useSnapshot, String schemaName,
            String dataTable, String indexTable, String tenantId,
            IndexTool.IndexVerifyType verifyType, Long startTime, Long endTime) {
        List<String> args = getArgList(directApi, useSnapshot, schemaName, dataTable, indexTable,
                tenantId, verifyType, startTime, endTime);
        return args.toArray(new String[0]);
    }

    public static void upsertRow(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setInt(1, i);
        stmt.setString(2, "uname" + String.valueOf(i));
        stmt.setInt(3, 95050 + i);
        stmt.executeUpdate();
    }

    public static void runIndexTool(boolean directApi, boolean useSnapshot, String schemaName,
            String dataTableName, String indexTableName) throws Exception {
        runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, new String[0]);
    }

    private static void verifyMapper(Job job, boolean directApi, boolean useSnapshot, String schemaName,
                                  String dataTableName, String indexTableName, String tenantId) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        if (tenantId != null) {
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        try (Connection conn =
                     DriverManager.getConnection(getUrl(), props)) {
            PTable dataTable = PhoenixRuntime.getTable(conn, SchemaUtil.getTableName(schemaName, dataTableName));
            PTable indexTable = PhoenixRuntime.getTable(conn, SchemaUtil.getTableName(schemaName, indexTableName));
            boolean transactional = dataTable.isTransactional();
            boolean localIndex = PTable.IndexType.LOCAL.equals(indexTable.getIndexType());

            if ((localIndex || !transactional) && !useSnapshot) {
                assertEquals(job.getMapperClass(), PhoenixServerBuildIndexMapper.class);
            } else {
                assertEquals(job.getMapperClass(), PhoenixIndexImportDirectMapper.class);
            }
        }
    }

    public static void runIndexTool(boolean directApi, boolean useSnapshot, String schemaName,
                                    String dataTableName, String indexTableName, String... additionalArgs) throws Exception {
        runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, null, 0, additionalArgs);
    }

    public static IndexTool runIndexTool(boolean directApi, boolean useSnapshot, String schemaName,
                                         String dataTableName, String indexTableName, String tenantId, int expectedStatus,
                                         String... additionalArgs) throws Exception {
        return runIndexTool(directApi, useSnapshot, schemaName, dataTableName, indexTableName, tenantId, expectedStatus,
                IndexTool.IndexVerifyType.NONE, additionalArgs);
    }

    public static IndexTool runIndexTool(boolean directApi, boolean useSnapshot, String schemaName,
                                         String dataTableName, String indexTableName, String tenantId,
                                         int expectedStatus, IndexTool.IndexVerifyType verifyType,
                                         String... additionalArgs) throws Exception {
        IndexTool indexingTool = new IndexTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        conf.set(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        indexingTool.setConf(conf);
        final String[] cmdArgs = getArgValues(directApi, useSnapshot, schemaName, dataTableName,
                indexTableName, tenantId, verifyType);
        List<String> cmdArgList = new ArrayList<>(Arrays.asList(cmdArgs));
        cmdArgList.addAll(Arrays.asList(additionalArgs));
        int status = indexingTool.run(cmdArgList.toArray(new String[cmdArgList.size()]));

        if (expectedStatus == 0) {
            verifyMapper(indexingTool.getJob(), directApi, useSnapshot, schemaName, dataTableName, indexTableName, tenantId);
        }
        assertEquals(expectedStatus, status);
        return indexingTool;
    }
}
