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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class NoLookbackMutableIndexExtendedIT extends BaseTest {
    protected final boolean localIndex;
    protected final String tableDDLOptions;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    public NoLookbackMutableIndexExtendedIT(Boolean localIndex, String txProvider, Boolean columnEncoded) {
        this.localIndex = localIndex;
        StringBuilder optionBuilder = new StringBuilder();
        if (txProvider != null) {
            optionBuilder
                    .append("TRANSACTIONAL=true," + PhoenixDatabaseMetaData.TRANSACTION_PROVIDER
                            + "='" + txProvider + "'");
        }
        if (!columnEncoded) {
            if (optionBuilder.length() != 0) optionBuilder.append(",");
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    private static Connection getConnection(Properties props) throws SQLException {
        props.setProperty(QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB,
                Integer.toString(1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        return conn;
    }

    protected static Connection getConnection() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        return getConnection(props);
    }

    @Parameterized.Parameters(name = "NoLookbackMutableIndexExtendedIT_localIndex={0},transactionProvider={1},columnEncoded={2}")
    // name is used by failsafe as file name in reports
    public static Collection<Object[]> data() {
        return TestUtil.filterTxParamData(Arrays.asList(
                new Object[][] { { false, null, false }, { false, null, true },
                        { false, "TEPHRA", false }, { false, "TEPHRA", true },
                        { false, "OMID", false }, { true, null, false }, { true, null, true },
                        { true, "TEPHRA", false }, { true, "TEPHRA", true }, }), 1);
    }

    // Tests that if major compaction is run on a table with a disabled index,
    // deleted cells are kept
    @Test
    public void testCompactDisabledIndex() throws Exception {
        if (localIndex || tableDDLOptions.contains("TRANSACTIONAL=true")) {
            return;
        }

        try (Connection conn = getConnection()) {
            String schemaName = generateUniqueName();
            String dataTableName = generateUniqueName() + "_DATA";
            String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
            String indexTableName = generateUniqueName() + "_IDX";
            String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
            conn.createStatement().execute(
                    String.format(PartialScannerResultsDisabledIT.TEST_TABLE_DDL,
                            dataTableFullName));
            conn.createStatement().execute(
                    String.format(PartialScannerResultsDisabledIT.INDEX_1_DDL, indexTableName,
                            dataTableFullName));

            //insert a row, and delete it
            PartialScannerResultsDisabledIT.writeSingleBatch(conn, 1, 1, dataTableFullName);
            List<HRegion>
                    regions =
                    getUtility().getHBaseCluster().getRegions(TableName.valueOf(dataTableFullName));
            HRegion hRegion = regions.get(0);
            hRegion.flush(
                    true); // need to flush here, or else nothing will get written to disk due to the delete
            conn.createStatement().execute("DELETE FROM " + dataTableFullName);
            conn.commit();

            // disable the index, simulating an index write failure
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            IndexUtil.updateIndexState(pConn, indexTableFullName, PIndexState.DISABLE,
                    EnvironmentEdgeManager.currentTimeMillis());

            // major compaction should not remove the deleted row
            hRegion.flush(true);
            hRegion.compact(true);
            Table dataTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                            .getTable(Bytes.toBytes(dataTableFullName));
            assertEquals(1, TestUtil.getRawRowCount(dataTable));

            // reenable the index
            IndexUtil.updateIndexState(pConn, indexTableFullName, PIndexState.INACTIVE,
                    EnvironmentEdgeManager.currentTimeMillis());
            IndexUtil.updateIndexState(pConn, indexTableFullName, PIndexState.ACTIVE, 0L);

            // now major compaction should remove the deleted row
            hRegion.compact(true);
            dataTable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                            .getTable(Bytes.toBytes(dataTableFullName));
            assertEquals(0, TestUtil.getRawRowCount(dataTable));
        }
    }
}
