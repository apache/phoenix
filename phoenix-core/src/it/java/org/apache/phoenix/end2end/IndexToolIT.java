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

import static org.apache.phoenix.mapreduce.PhoenixJobCounters.INPUT_RECORDS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.REBUILT_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.SCANNED_DATA_ROW_COUNT;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.end2end.index.IndexTestUtil;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.mapreduce.index.PhoenixIndexImportDirectMapper;
import org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters;
import org.apache.phoenix.mapreduce.index.PhoenixServerBuildIndexMapper;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.BeforeParam;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class IndexToolIT extends BaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexToolIT.class);

    private static String tmpDir;

    private final boolean localIndex;
    private final boolean mutable;
    private final boolean transactional;
    private final String tableDDLOptions;
    private final String indexDDLOptions;
    private final boolean useSnapshot;
    private final boolean useTenantId;
    private final boolean namespaceMapped;

    public IndexToolIT(String transactionProvider, boolean mutable, boolean localIndex,
            boolean useSnapshot, boolean useTenantId, boolean namespaceMapped) {
        this.localIndex = localIndex;
        this.mutable = mutable;
        this.transactional = transactionProvider != null;
        this.useSnapshot = useSnapshot;
        this.useTenantId = useTenantId;
        this.namespaceMapped = namespaceMapped;
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
        StringBuilder indexOptionBuilder = new StringBuilder();
        if (!localIndex && transactionProvider == null) {
            if (!(optionBuilder.length() == 0)) {
                optionBuilder.append(",");
            }
            optionBuilder.append(" COLUMN_ENCODED_BYTES=0");
            indexOptionBuilder.append(" IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS,COLUMN_ENCODED_BYTES=2");
        }
        this.indexDDLOptions = indexOptionBuilder.toString();
    }

    @BeforeParam
    public static synchronized void setup(
            String transactionProvider, boolean mutable, boolean localIndex,
            boolean useSnapshot, boolean useTenantId, boolean namespaceMapped)
                    throws Exception {
        if (clusterInitialized && Boolean.valueOf(namespaceMapped).equals(utility.getConfiguration()
                .getBoolean(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, true))) {
            //Perf optimization: no need to re-initialize the minicluster
            return;
        } else {
            //This has been known to cause flakeyness
            //Maybe we're gonna have to refactor the namespace
            if(clusterInitialized) {
                tearDownMiniCluster(0);
                System.setProperty("java.io.tmpdir", tmpDir);
            } else {
                tmpDir = System.getProperty("java.io.tmpdir");
            }
            Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
            serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
            serverProps.put(QueryServices.MAX_SERVER_METADATA_CACHE_TIME_TO_LIVE_MS_ATTRIB, Long.toString(5));
            serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
                QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
            serverProps.put(QueryServices.INDEX_REBUILD_PAGE_SIZE_IN_ROWS, Long.toString(8));
            serverProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
            serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
                Boolean.toString(namespaceMapped));
            Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
            clientProps.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
            clientProps.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
            clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
            clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
            clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
                Boolean.toString(namespaceMapped));
            setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
        }
    }

    @Parameters(
            name = "transactionProvider={0},mutable={1},localIndex={2}," +
                "useSnapshot={3},useTenant={4},namespaceMapped={5}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayListWithExpectedSize(48);
        boolean[] Booleans = new boolean[] { false, true };
        for (boolean namespaceMapped : Booleans) {
            for (boolean useSnapshot : Booleans) {
                for (String transactionProvider : new String[] { "OMID", null }) {
                    for (boolean mutable : Booleans) {
                        for (boolean localIndex : Booleans) {
                            if(localIndex && useSnapshot) {
                                continue;
                            }
                            if (!localIndex
                                    || transactionProvider == null
                                    || !TransactionFactory.getTransactionProvider(
                                            TransactionFactory.Provider.valueOf(transactionProvider))
                                        .isUnsupported(Feature.ALLOW_LOCAL_INDEX)) {
                                list.add(new Object[]{transactionProvider, mutable, localIndex,
                                        useSnapshot, false, namespaceMapped});
                            }
                        }
                    }
                }
            }
        }
        // Add the usetenantId
        list.add(new Object[] { null, false, true, false, true, false});
        return list;
    }

    protected static void setEveryNthRowWithNull(int nrows, int nthRowNull, PreparedStatement stmt) throws Exception {
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
    public void testSecondaryIndex() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String indexTableName = generateUniqueName();
        String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
        try {
            if(namespaceMapped) {
                conn.createStatement().execute("CREATE SCHEMA " + schemaName);
            }
            String stmString1 =
                    "CREATE TABLE " + dataTableFullName
                            + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                            + tableDDLOptions;
            conn.createStatement().execute(stmString1);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", dataTableFullName);
            PreparedStatement upsertStmt = conn.prepareStatement(upsertQuery);

            // insert two rows
            upsertRow(upsertStmt, 1);
            upsertRow(upsertStmt, 2);
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
                        "CREATE %s INDEX %s ON %s  (LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz') ASYNC " + this.indexDDLOptions,
                        (localIndex ? "LOCAL" : ""), indexTableName, dataTableFullName);
            conn.createStatement().execute(stmtString2);

            // verify rows are fetched from data table.
            String selectSql =
                    String.format(
                        "SELECT ID FROM %s WHERE LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz' = 'xxUNAME2_xyz'",
                        dataTableFullName);

            // assert we are pulling from data table.
            ExplainPlan plan = conn.prepareStatement(selectSql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(dataTableFullName,
                explainPlanAttributes.getTableName().replaceAll(":", "."));
            assertEquals("SERVER FILTER BY (LPAD(UPPER(NAME, 'en_US'), 8, 'x') || '_xyz') = 'xxUNAME2_xyz'",
                explainPlanAttributes.getServerWhereFilter().replaceAll(":", "."));

            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            conn.commit();

            // run the index MR job.
            runIndexTool(useSnapshot, schemaName, dataTableName, indexTableName);

            if (!localIndex) {
                IndexTestUtil.assertRowsForEmptyColValue(conn, indexTableFullName,
                    QueryConstants.VERIFIED_BYTES);
            }
            // insert two more rows
            upsertRow(upsertStmt, 3);
            upsertRow(upsertStmt, 4);
            conn.commit();

            if (!localIndex) {
                IndexTestUtil.assertRowsForEmptyColValue(conn, indexTableFullName,
                    QueryConstants.VERIFIED_BYTES);
            }
            // assert we are pulling from index table.
            plan = conn.prepareStatement(selectSql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            final String expectedTableName;
            if (localIndex) {
                expectedTableName = indexTableFullName + "(" + dataTableFullName + ")";
            } else {
                expectedTableName = indexTableFullName;
            }
            assertEquals(expectedTableName,
                explainPlanAttributes.getTableName().replaceAll(":", "."));

            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            if (localIndex || transactional || useTenantId || useSnapshot) {
                return;
            }
            // Run the index MR job and verify that the global index table is built correctly
            IndexTool indexTool = runIndexTool(useSnapshot, schemaName, dataTableName, indexTableName, null, 0, IndexTool.IndexVerifyType.BEFORE, new String[0]);
            assertEquals(4, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(4, indexTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(4, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_EXTRA_CELLS).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(
                    BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT_COZ_MISSING_CELLS).getValue());
        } finally {
            conn.close();
        }
    }

    protected static void dropIndexToolTables(Connection conn) throws Exception {
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        TableName indexToolOutputTable =
            TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME_BYTES);
        admin.disableTable(indexToolOutputTable);
        admin.deleteTable(indexToolOutputTable);
        TableName indexToolResultTable = TableName.valueOf(IndexVerificationResultRepository.RESULT_TABLE_NAME_BYTES);
        admin.disableTable(indexToolResultTable);
        admin.deleteTable(indexToolResultTable);
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

    private static void verifyIndexTableRowKey(byte[] rowKey, String indexTableFullName) {
        // The row key for the output table : timestamp | index table name | data row key
        // The row key for the result table : timestamp | index table name | datable table region name |
        //                                    scan start row | scan stop row

        // This method verifies the common prefix, i.e., "timestamp | index table name | ", since the rest of the
        // fields may include the separator key
        int offset = Bytes.indexOf(rowKey, IndexVerificationResultRepository.ROW_KEY_SEPARATOR_BYTE);
        offset++;
        byte[] indexTableFullNameBytes = Bytes.toBytes(indexTableFullName);
        assertEquals(Bytes.compareTo(rowKey, offset, indexTableFullNameBytes.length, indexTableFullNameBytes, 0,
                indexTableFullNameBytes.length), 0);
        assertEquals(rowKey[offset + indexTableFullNameBytes.length],
            IndexVerificationResultRepository.ROW_KEY_SEPARATOR_BYTE[0]);
    }

    public static Cell getErrorMessageFromIndexToolOutputTable(Connection conn, String dataTableFullName, String indexTableFullName)
            throws Exception {
        byte[] indexTableFullNameBytes = Bytes.toBytes(indexTableFullName);
        byte[] dataTableFullNameBytes = Bytes.toBytes(dataTableFullName);
        Table hIndexTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getTable(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME_BYTES);
        Scan scan = new Scan();
        ResultScanner scanner = hIndexTable.getScanner(scan);
        boolean dataTableNameCheck = false;
        boolean indexTableNameCheck = false;
        Cell errorMessageCell = null;
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            for (Cell cell : result.rawCells()) {
                assertTrue(Bytes.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                    IndexVerificationOutputRepository.OUTPUT_TABLE_COLUMN_FAMILY, 0,
                    IndexVerificationOutputRepository.OUTPUT_TABLE_COLUMN_FAMILY.length) == 0);
                if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                    IndexVerificationOutputRepository.DATA_TABLE_NAME_BYTES, 0, IndexVerificationOutputRepository.DATA_TABLE_NAME_BYTES.length) == 0) {
                    dataTableNameCheck = true;
                    assertTrue("Value was different! Expected: " + Bytes.toString(dataTableFullNameBytes)
                            + " Actual: " + Bytes.toString(cell.getValueArray(),
                        cell.getValueOffset(), cell.getValueLength()),
                        Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                            dataTableFullNameBytes, 0, dataTableFullNameBytes.length) == 0);
                } else if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                    IndexVerificationOutputRepository.INDEX_TABLE_NAME_BYTES, 0, IndexVerificationOutputRepository.INDEX_TABLE_NAME_BYTES.length) == 0) {
                    indexTableNameCheck = true;
                    assertTrue(Bytes.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                            indexTableFullNameBytes, 0, indexTableFullNameBytes.length) == 0);
                } else if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                    IndexVerificationOutputRepository.ERROR_MESSAGE_BYTES, 0, IndexVerificationOutputRepository.ERROR_MESSAGE_BYTES.length) == 0) {
                    errorMessageCell = cell;
                }
            }
        }
        assertTrue( "DataTableNameCheck was false", dataTableNameCheck);
        assertTrue("IndexTableNameCheck was false", indexTableNameCheck);
        assertTrue("Error message cell was null", errorMessageCell != null);
        verifyIndexTableRowKey(CellUtil.cloneRow(errorMessageCell), indexTableFullName);
        hIndexTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getTable(IndexVerificationResultRepository.RESULT_TABLE_NAME_BYTES);
        scan = new Scan();
        scanner = hIndexTable.getScanner(scan);
        Result result = scanner.next();
        if (result != null) {
            verifyIndexTableRowKey(CellUtil.cloneRow(result.rawCells()[0]), indexTableFullName);
        }

        return errorMessageCell;
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
        String createIndexStr = "CREATE INDEX %s ON %s (NAME) " + this.indexDDLOptions;

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

            runIndexTool(false, "", viewTenantName, indexNameTenant,
                    tenantId, 0, new String[0]);

            String selectSql = String.format("SELECT ID FROM %s WHERE NAME='x'", viewTenantName);
            ExplainPlan plan = connTenant.prepareStatement(selectSql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(viewIndexTableName,
                explainPlanAttributes.getTableName());

            ResultSet rs = connTenant.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            // Remove from tenant view index and build.
            ConnectionQueryServices queryServices = connGlobal.unwrap(PhoenixConnection.class).getQueryServices();
            Admin admin = queryServices.getAdmin();
            TableName tableName = TableName.valueOf(viewIndexTableName);
            admin.disableTable(tableName);
            admin.truncateTable(tableName, false);

            runIndexTool(false, "", viewTenantName, indexNameTenant,
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
            runIndexTool(false, schemaName, dataTableName, indexNameGlobal,
                    tenantId, -1, new String[0]);

        } finally {
            connGlobal.close();
            connTenant.close();
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
            if(namespaceMapped) {
                conn.createStatement().execute("CREATE SCHEMA " + schemaName);
            }
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
                        "CREATE %s INDEX %s on %s (\"info\".CAR_NUM,\"info\".CAP_DATE) ASYNC " + this.indexDDLOptions,
                        (localIndex ? "LOCAL" : ""), indexTableName, dataTableFullName);
            conn.createStatement().execute(indexDDL);

            runIndexTool(useSnapshot, schemaName, dataTableName, indexTableName);

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
        String indexTableName = generateUniqueName();
        try (Connection conn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
                Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            if(namespaceMapped) {
                conn.createStatement().execute("CREATE SCHEMA " + schemaName);
            }
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
            TableName hDataName = TableName.valueOf(
                    SchemaUtil.getPhysicalHBaseTableName(schemaName, dataTableName, namespaceMapped)
                    .getBytes());
            TableDescriptor dataTD = admin.getDescriptor(hDataName);
            admin.disableTable(hDataName);
            admin.deleteTable(hDataName);
            admin.createTable(dataTD, splitPoints);
            assertEquals(targetNumRegions, admin.getRegions(hDataName).size());

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
                        "CREATE INDEX %s on %s (\"info\".CAR_NUM,\"test\".CAR_NUM,\"info\".CAP_DATE) ASYNC " + this.indexDDLOptions,
                        indexTableName, dataTableFullName);
            conn.createStatement().execute(indexDDL);

            // run with 50% sampling rate, split if data table more than 3 regions
            runIndexTool(useSnapshot, schemaName, dataTableName, indexTableName, "-sp", "50", "-spa", "3");

            TableName hIndexName = TableName.valueOf(
                SchemaUtil.getPhysicalHBaseTableName(schemaName, indexTableName, namespaceMapped)
                .getBytes());
            assertEquals(targetNumRegions, admin.getRegions(hIndexName).size());
            List<Cell> values = new ArrayList<>();
            // every index region should have been written to, if the index table was properly split uniformly
            for (HRegion region : getUtility().getHBaseCluster().getRegions(hIndexName)) {
                values.clear();
                RegionScanner scanner = region.getScanner(new Scan());
                scanner.next(values);
                if (values.isEmpty()) {
                    fail("Region did not have any results: " + region.getRegionInfo());
                }
            }
        }
    }

    @Test
    public void testCaseSensitiveNames() throws Exception {
        // We have three references to the "same" table name. e.g. for `my_table`
        // The original name, unquoted: `my_table`
        // The quoted name to prevent Phoenix from auto-capitalizing it (s): `"my_table"`
        // The qualified named with schema (q): `"my_schema":"my_table"`
        String schemaName = generateUniqueName().toLowerCase();
        String dataTableName = generateUniqueName().toLowerCase();
        String sSchemaName = SchemaUtil.getCaseSensitiveColumnDisplayName(null, schemaName);
        String sDataTableName = SchemaUtil.getCaseSensitiveColumnDisplayName(null, dataTableName);
        String qDataTableName = SchemaUtil.getCaseSensitiveColumnDisplayName(schemaName, dataTableName);
        // The explain plan doesn't re-quote the qualified table name: `my_schema.my_table`
        String dataTableNameForExplain = SchemaUtil.getTableName(schemaName, dataTableName);

        String indexTableName = generateUniqueName().toLowerCase();
        String sIndexTableName = SchemaUtil.getCaseSensitiveColumnDisplayName(null, indexTableName);
        String indexTableNameForExplain = SchemaUtil.getTableName(schemaName, indexTableName);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            if(namespaceMapped) {
                conn.createStatement().execute("CREATE SCHEMA " + sSchemaName);
            }
            String stmString1 =
                    "CREATE TABLE " + qDataTableName
                         + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER) "
                         + tableDDLOptions;
            conn.createStatement().execute(stmString1);
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", qDataTableName);
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
                                    .executeQuery("SELECT count(*) from " + qDataTableName);
                    assertTrue(rs.next());
                    assertEquals("Unexpected row count ", 2, rs.getInt(1));
                    assertFalse(rs.next());
                    rs =
                            conn2.createStatement()
                                    .executeQuery("SELECT count(*) from " + qDataTableName);
                    assertTrue(rs.next());
                    assertEquals("Unexpected row count ", 4, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }

            String stmtString2 =
                    String.format(
                        "CREATE %s INDEX %s ON %s  (LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz') ASYNC ",
                        (localIndex ? "LOCAL" : ""), sIndexTableName, qDataTableName);
            conn.createStatement().execute(stmtString2);

            // verify rows are fetched from data table.
            String selectSql =
                    String.format(
                        "SELECT ID FROM %s WHERE LPAD(UPPER(NAME, 'en_US'),8,'x')||'_xyz' = 'xxUNAME2_xyz'",
                        qDataTableName);

            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);

            // assert we are pulling from data table.
            assertEquals(String.format(
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER %s\n"
                     + "    SERVER FILTER BY (LPAD(UPPER(NAME, 'en_US'), 8, 'x') || '_xyz') = 'xxUNAME2_xyz'",
                        dataTableNameForExplain), actualExplainPlan.replaceAll(":", "."));

            rs = stmt1.executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
            conn.commit();

            // run the index MR job.
            runIndexTool(useSnapshot, sSchemaName, sDataTableName, sIndexTableName);

            // insert two more rows
            upsertRow(stmt1, 3);
            upsertRow(stmt1, 4);
            conn.commit();

            // assert we are pulling from index table.
            rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
            actualExplainPlan = QueryUtil.getExplainPlan(rs);
            // Because the explain plan doesn't include double-quotes around case-sensitive table names,
            // we need to tell assertExplainPlan to not normalize our table names.
            assertExplainPlan(localIndex, actualExplainPlan, dataTableNameForExplain, indexTableNameForExplain, false);

            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    public static void assertExplainPlan(boolean localIndex, String actualExplainPlan,
            String dataTableFullName, String indexTableFullName) {
        assertExplainPlan(localIndex, actualExplainPlan, dataTableFullName, indexTableFullName, true);
    }

    public static void assertExplainPlan(boolean localIndex, String actualExplainPlan,
            String dataTableFullName, String indexTableFullName, boolean normalizeTableNames) {
        String expectedExplainPlan;
        if (localIndex) {
            expectedExplainPlan = String.format(" RANGE SCAN OVER %s [1,",
                    normalizeTableNames ?
                            SchemaUtil.normalizeIdentifier(indexTableFullName) + "(" +
                                    SchemaUtil.normalizeIdentifier(dataTableFullName) + ")":
                            indexTableFullName + "(" + dataTableFullName + ")");
        } else {
            expectedExplainPlan = String.format(" RANGE SCAN OVER %s",
                normalizeTableNames ? SchemaUtil.normalizeIdentifier(indexTableFullName)
                        : indexTableFullName);
        }
        assertTrue(actualExplainPlan + "\n expected to contain \n" + expectedExplainPlan,
            actualExplainPlan.replaceAll(":", ".").contains(expectedExplainPlan));
    }

    public static CounterGroup getMRJobCounters(IndexTool indexTool) throws IOException {
        return indexTool.getJob().getCounters().getGroup(PhoenixIndexToolJobCounters.class.getName());
    }

    private static List<String> getArgList (boolean useSnapshot, String schemaName,
                                            String dataTable, String indxTable, String tenantId,
                                            IndexTool.IndexVerifyType verifyType, Long startTime,
                                            Long endTime, Long incrementalVerify) {
        return getArgList(useSnapshot, schemaName, dataTable, indxTable, tenantId,
            verifyType, startTime, endTime, IndexTool.IndexDisableLoggingType.NONE, incrementalVerify, false);
    }

    private static List<String> getArgList (boolean useSnapshot, String schemaName,
                            String dataTable, String indxTable, String tenantId,
                            IndexTool.IndexVerifyType verifyType, Long startTime, Long endTime,
                                            IndexTool.IndexDisableLoggingType disableLoggingType,
                                            Long incrementalVerify, boolean useIndexTableAsSource) {
        List<String> args = Lists.newArrayList();
        if (schemaName != null) {
            args.add("--schema=" + schemaName);
        }
        // Work around CLI-254. The long-form arg parsing doesn't strip off double-quotes
        args.add("--data-table=" + dataTable);
        args.add("--index-table=" + indxTable);
        args.add("-v");
        args.add(verifyType.getValue()); // verify index rows inline

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

        if (verifyType != IndexTool.IndexVerifyType.NONE && disableLoggingType != null) {
            args.add("-dl");
            args.add(disableLoggingType.getValue());
        }

        if(incrementalVerify!=null) {
            args.add("-rv");
            args.add(String.valueOf(incrementalVerify));
        }

        if (useIndexTableAsSource) {
            args.add("-fi");
        }

        args.add("-op");
        args.add("/tmp/" + UUID.randomUUID().toString());
        return args;
    }

    public static String[] getArgValues( boolean useSnapshot, String schemaName,
            String dataTable, String indexTable, String tenantId, IndexTool.IndexVerifyType verifyType) {
        List<String> args = getArgList(useSnapshot, schemaName, dataTable, indexTable,
                tenantId, verifyType, null, null, null);
        return args.toArray(new String[0]);
    }

    public static String[] getArgValues(boolean useSnapshot, String schemaName,
                                        String dataTable, String indexTable, String tenantId, IndexTool.IndexVerifyType verifyType,
                                        IndexTool.IndexDisableLoggingType disableLoggingType) {
        List<String> args = getArgList(useSnapshot, schemaName, dataTable, indexTable,
                tenantId, verifyType, null, null, disableLoggingType, null, false);
        return args.toArray(new String[0]);
    }

    public static String [] getArgValues(boolean useSnapshot, String schemaName,
            String dataTable, String indexTable, String tenantId,
            IndexTool.IndexVerifyType verifyType, Long startTime, Long endTime) {
        List<String> args = getArgList(useSnapshot, schemaName, dataTable, indexTable,
                tenantId, verifyType, startTime, endTime, null);
        return args.toArray(new String[0]);
    }

    public static String [] getArgValues(boolean useSnapshot, String schemaName,
                                         String dataTable, String indexTable, String tenantId,
                                         IndexTool.IndexVerifyType verifyType, Long startTime,
                                         Long endTime,
                                         IndexTool.IndexDisableLoggingType disableLoggingType,
                                         Long incrementalVerify ) {
        List<String> args = getArgList(useSnapshot, schemaName, dataTable, indexTable,
            tenantId, verifyType, startTime, endTime, disableLoggingType, incrementalVerify, false);
        return args.toArray(new String[0]);
    }

    public static String [] getArgValues(boolean directApi, boolean useSnapshot, String schemaName,
        String dataTable, String indexTable, String tenantId,
        IndexTool.IndexVerifyType verifyType, Long startTime,
        Long endTime,
        IndexTool.IndexDisableLoggingType disableLoggingType,
        Long incrementalVerify,
        boolean useIndexTableAsSource) {
        List<String> args = getArgList(useSnapshot, schemaName, dataTable, indexTable,
            tenantId, verifyType, startTime, endTime, disableLoggingType, incrementalVerify, useIndexTableAsSource);
        return args.toArray(new String[0]);
    }

    public static void upsertRow(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setInt(1, i);
        stmt.setString(2, "uname" + String.valueOf(i));
        stmt.setInt(3, 95050 + i);
        stmt.executeUpdate();
    }

    public static void runIndexTool(boolean useSnapshot, String schemaName,
            String dataTableName, String indexTableName) throws Exception {
        runIndexTool(useSnapshot, schemaName, dataTableName, indexTableName, new String[0]);
    }

    private static void verifyMapper(Job job, boolean useSnapshot, String schemaName,
                                  String dataTableName, String indexTableName, String tenantId) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        if (tenantId != null) {
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }

        try (PhoenixConnection conn =
                (PhoenixConnection) DriverManager.getConnection(getUrl(), props)) {
            PTable indexTable = conn.getTableNoCache(SchemaUtil
                    .normalizeFullTableName(SchemaUtil.getTableName(schemaName, indexTableName)));
            PTable dataTable = conn.getTableNoCache(SchemaUtil
                    .normalizeFullTableName(SchemaUtil.getTableName(schemaName, dataTableName)));
            boolean transactional = dataTable.isTransactional();
            boolean localIndex = PTable.IndexType.LOCAL.equals(indexTable.getIndexType());
            if ((localIndex || !transactional) && !useSnapshot) {
                assertEquals(job.getMapperClass(), PhoenixServerBuildIndexMapper.class);
            } else {
                assertEquals(job.getMapperClass(), PhoenixIndexImportDirectMapper.class);
            }
        }
    }

    public static void runIndexTool(boolean useSnapshot, String schemaName,
            String dataTableName, String indexTableName, String... additionalArgs) throws Exception {
        runIndexTool(useSnapshot, schemaName, dataTableName, indexTableName, null, 0, additionalArgs);
    }

    public static IndexTool runIndexTool(boolean useSnapshot, String schemaName,
            String dataTableName, String indexTableName, String tenantId, int expectedStatus,
            String... additionalArgs) throws Exception {
        return runIndexTool(useSnapshot, schemaName, dataTableName, indexTableName, tenantId, expectedStatus,
                IndexTool.IndexVerifyType.NONE, additionalArgs);
    }

    public static IndexTool runIndexTool( boolean useSnapshot, String schemaName,
                                         String dataTableName, String indexTableName, String tenantId,
                                         int expectedStatus, IndexTool.IndexVerifyType verifyType,
                                         String... additionalArgs) throws Exception {
        Configuration conf = new Configuration(getUtility().getConfiguration());
        return runIndexTool(conf, useSnapshot, schemaName, dataTableName, indexTableName,
            tenantId, expectedStatus, verifyType, IndexTool.IndexDisableLoggingType.NONE, additionalArgs);
    }


    public static IndexTool runIndexTool(Configuration conf, boolean useSnapshot, String schemaName,
            String dataTableName, String indexTableName, String tenantId,
            int expectedStatus, IndexTool.IndexVerifyType verifyType, IndexTool.IndexDisableLoggingType disableLoggingType,
            String... additionalArgs) throws Exception {
        IndexTool indexingTool = new IndexTool();
        conf.set(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        indexingTool.setConf(conf);
        final String[] cmdArgs = getArgValues(useSnapshot, schemaName, dataTableName,
            indexTableName, tenantId, verifyType, disableLoggingType);
        List<String> cmdArgList = new ArrayList<>(Arrays.asList(cmdArgs));
        cmdArgList.addAll(Arrays.asList(additionalArgs));
        LOGGER.info("Running IndexTool with {}", Arrays.toString(cmdArgList.toArray()), new Exception("Stack Trace"));
        int status = indexingTool.run(cmdArgList.toArray(new String[cmdArgList.size()]));

        if (expectedStatus == 0) {
            verifyMapper(indexingTool.getJob(), useSnapshot, schemaName, dataTableName, indexTableName, tenantId);
        }
        assertEquals(expectedStatus, status);
        return indexingTool;
    }
}
