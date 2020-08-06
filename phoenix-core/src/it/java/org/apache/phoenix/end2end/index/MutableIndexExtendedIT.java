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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.PartialScannerResultsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.*;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class MutableIndexExtendedIT extends ParallelStatsDisabledIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(MutableIndexIT.class);

    protected final boolean localIndex;
    protected final String tableDDLOptions;

    public MutableIndexExtendedIT(Boolean localIndex, String txProvider, Boolean columnEncoded) {
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

    @Parameterized.Parameters(name = "MutableIndexExtendedIT_localIndex={0},transactionProvider={1},columnEncoded={2}")
    // name is used by failsafe as file name in reports
    public static Collection<Object[]> data() {
        return TestUtil.filterTxParamData(Arrays.asList(
                new Object[][] { { false, null, false }, { false, null, true },
                        { false, "TEPHRA", false }, { false, "TEPHRA", true },
                        { false, "OMID", false }, { true, null, false }, { true, null, true },
                        { true, "TEPHRA", false }, { true, "TEPHRA", true }, }), 1);
    }

    @Test
    public void testIndexHalfStoreFileReader() throws Exception {
        if (!localIndex)
            return;

        Connection conn1 = getConnection();
        ConnectionQueryServices connectionQueryServices = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES);
        HBaseAdmin admin = connectionQueryServices.getAdmin();
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        createBaseTable(conn1, tableName, "('e')");
        conn1.createStatement().execute("CREATE "+(localIndex?"LOCAL":"")+" INDEX " + indexName
                + " ON " + tableName + "(v1)" + (localIndex?"":" SPLIT ON ('e')"));
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('b',1,2,4,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('f',1,2,3,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('j',2,4,2,'a')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('q',3,1,1,'c')");
        conn1.commit();


        String query = "SELECT count(*) FROM " + tableName +" where v1<='z'";
        ResultSet rs = conn1.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));

        TableName indexTable = TableName.valueOf(localIndex?tableName: indexName);
        admin.flush(indexTable);
        boolean merged = false;
        HTableInterface table = connectionQueryServices.getTable(indexTable.getName());
        // merge regions until 1 left
        long numRegions = 0;
        while (true) {
            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1)); //TODO this returns 5 sometimes instead of 4, duplicate results?
            try {
                List<HRegionInfo> indexRegions = admin.getTableRegions(indexTable);
                numRegions = indexRegions.size();
                if (numRegions==1) {
                    break;
                }
                if(!merged) {
                    List<HRegionInfo> regions =
                            admin.getTableRegions(indexTable);
                    LOGGER.info("Merging: " + regions.size());
                    admin.mergeRegions(regions.get(0).getEncodedNameAsBytes(),
                            regions.get(1).getEncodedNameAsBytes(), false);
                    merged = true;
                    Threads.sleep(10000);
                }
            } catch (Exception ex) {
                LOGGER.info(ex.getMessage());
            }
            long waitStartTime = System.currentTimeMillis();
            // wait until merge happened
            while (System.currentTimeMillis() - waitStartTime < 10000) {
                List<HRegionInfo> regions = admin.getTableRegions(indexTable);
                LOGGER.info("Waiting:" + regions.size());
                if (regions.size() < numRegions) {
                    break;
                }
                Threads.sleep(1000);
            }
            SnapshotTestingUtils.waitForTableToBeOnline(BaseTest.getUtility(), indexTable);
            assertTrue("Index table should be online ", admin.isTableAvailable(indexTable));
        }
    }

    protected void createBaseTable(Connection conn, String tableName, String splits)
            throws SQLException {
        String ddl =
                "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n"
                        + "k1 INTEGER NOT NULL,\n" + "k2 INTEGER NOT NULL,\n" + "k3 INTEGER,\n"
                        + "v1 VARCHAR,\n" + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n" + (
                        tableDDLOptions != null ?
                                tableDDLOptions :
                                "") + (splits != null ? (" split on " + splits) : "");
        conn.createStatement().execute(ddl);
    }
}
