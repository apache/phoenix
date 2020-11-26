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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.mapreduce.index.IndexVerificationResultRepository;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexScrutiny;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import static org.apache.phoenix.hbase.index.IndexRegionObserver.VERIFIED_BYTES;
import static org.apache.phoenix.schema.types.PDataType.TRUE_BYTES;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class IndexRepairRegionScannerIT extends ParallelStatsDisabledIT {

    private final String tableDDLOptions;
    private final String indexDDLOptions;
    private boolean mutable;
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    public IndexRepairRegionScannerIT(boolean mutable, boolean singleCellIndex) {
        StringBuilder optionBuilder = new StringBuilder();
        StringBuilder indexOptionBuilder = new StringBuilder();
        this.mutable = mutable;
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true ");
        }
        if (singleCellIndex) {
            if (!(optionBuilder.length() == 0)) {
                optionBuilder.append(",");
            }
            optionBuilder.append(" IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0 ");
            indexOptionBuilder.append(" IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS,COLUMN_ENCODED_BYTES=2");
        }
        optionBuilder.append(" SPLIT ON(1,2)");
        this.indexDDLOptions = indexOptionBuilder.toString();
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Parameterized.Parameters(name = "mutable={0}, singleCellIndex={1}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {true, true},
                {true, false},
                {false, true},
                {false, false}});
    }

    @Before
    public void createIndexToolTables() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            IndexTool.createIndexToolTables(conn);
        }
    }

    @After
    public void cleanup() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            deleteAllRows(conn,
                TableName.valueOf(IndexVerificationOutputRepository.OUTPUT_TABLE_NAME_BYTES));
            deleteAllRows(conn,
                TableName.valueOf(IndexVerificationResultRepository.RESULT_TABLE_NAME));
        }
        EnvironmentEdgeManager.reset();
    }

    private void repairIndex(Connection conn, String schemaName, String dataTableFullName, String indexTableName, IndexTool.IndexVerifyType verifyType) throws Exception {
        PTable pDataTable = PhoenixRuntime.getTable(conn, dataTableFullName);
        PTable pIndexTable = PhoenixRuntime.getTable(conn, SchemaUtil.getTableName(schemaName, indexTableName));
        Table hTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getTable(pIndexTable.getPhysicalName().getBytes());
        Scan scan = new Scan();
        scan.setRaw(true);
        scan.setMaxVersions();
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
        IndexMaintainer.serialize(pDataTable, ptr, Collections.singletonList(pIndexTable), phoenixConnection);
        scan.setAttribute(BaseScannerRegionObserver.INDEX_REBUILD_VERIFY_TYPE, verifyType.toBytes());
        scan.setAttribute(BaseScannerRegionObserver.PHYSICAL_DATA_TABLE_NAME, pDataTable.getPhysicalName().getBytes());
        scan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, ByteUtil.copyKeyBytesIfNecessary(ptr));
        scan.setAttribute(BaseScannerRegionObserver.UNGROUPED_AGG, TRUE_BYTES);
        scan.setAttribute(PhoenixIndexCodec.INDEX_PROTO_MD, scan.getAttribute(PhoenixIndexCodec.INDEX_PROTO_MD));
        scan.setAttribute(BaseScannerRegionObserver.REBUILD_INDEXES, TRUE_BYTES);
        scan.setAttribute(BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
        }
    }

    private void setIndexRowStatusesToVerified(Connection conn, String dataTableFullName, String indexTableFullName) throws Exception {
        PTable pDataTable = PhoenixRuntime.getTable(conn, dataTableFullName);
        PTable pIndexTable = PhoenixRuntime.getTable(conn, indexTableFullName);
        Table hTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                .getTable(pIndexTable.getPhysicalName().getBytes());
        Scan scan = new Scan();
        PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
        IndexMaintainer indexMaintainer = pIndexTable.getIndexMaintainer(pDataTable, phoenixConnection);
        scan.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(), indexMaintainer.getEmptyKeyValueQualifier());
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            Put put = new Put(result.getRow());
            put.addColumn(indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                    indexMaintainer.getEmptyKeyValueQualifier(), result.rawCells()[0].getTimestamp(), VERIFIED_BYTES);
            hTable.put(put);
        }
    }

    @Test
    public void testRepairExtraIndexRows() throws Exception {
        final int NROWS = 20;
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
            PreparedStatement dataPreparedStatement =
                    conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
            for (int i = 1; i <= NROWS; i++) {
                dataPreparedStatement.setInt(1, i);
                dataPreparedStatement.setInt(2, i + 1);
                dataPreparedStatement.setInt(3, i * 2);
                dataPreparedStatement.execute();
            }
            conn.commit();
            conn.createStatement().execute(String.format(
                    "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName, dataTableFullName));
            // Add extra index rows
            PreparedStatement indexPreparedStatement =
                    conn.prepareStatement("UPSERT INTO " + indexTableFullName + " VALUES(?,?,?)");

            for (int i = NROWS + 1; i <= 2 * NROWS; i++) {
                indexPreparedStatement.setInt(1, i + 1); // the indexed column
                indexPreparedStatement.setInt(2, i); // the data pk column
                indexPreparedStatement.setInt(3, i * 2); // the included column
                indexPreparedStatement.execute();
            }
            conn.commit();
            // Set all index row statuses to verified so that read verify will not fix them. We want them to be fixed
            // by IndexRepairRegionScanner
            setIndexRowStatusesToVerified(conn, dataTableFullName, indexTableFullName);
            boolean failed;
            try {
                IndexScrutiny.scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
                failed = false;
            } catch (AssertionError e) {
                failed = true;
            }
            assertTrue(failed);
            // Repair the index
            repairIndex(conn, schemaName, dataTableFullName, indexTableName, IndexTool.IndexVerifyType.BEFORE);
            long actualRowCount = IndexScrutiny
                    .scrutinizeIndex(conn, dataTableFullName, indexTableFullName);
            assertEquals(NROWS, actualRowCount);
        }
    }

    public void deleteAllRows(Connection conn, TableName tableName) throws SQLException,
            IOException, InterruptedException {
        Scan scan = new Scan();
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().
                getAdmin();
        org.apache.hadoop.hbase.client.Connection hbaseConn = admin.getConnection();
        Table table = hbaseConn.getTable(tableName);
        boolean deletedRows = false;
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result r : scanner) {
                Delete del = new Delete(r.getRow());
                table.delete(del);
                deletedRows = true;
            }
        } catch (Exception e) {
            //if the table doesn't exist, we have no rows to delete. Easier to catch
            //than to pre-check for existence
        }
        //don't flush/compact if we didn't write anything, because we'll hang forever
        if (deletedRows) {
            getUtility().getAdmin().flush(tableName);
            TestUtil.majorCompact(getUtility(), tableName);
        }
    }

}
