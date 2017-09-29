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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class LocalIndexSplitMergeIT extends BaseTest {

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
            new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    private Connection getConnectionForLocalIndexTest() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.TRUE.toString());
        return DriverManager.getConnection(getUrl(), props);
    }

    private void createBaseTable(String tableName, String splits) throws SQLException {
        Connection conn = getConnectionForLocalIndexTest();
        String ddl =
                "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n"
                        + "k1 INTEGER NOT NULL,\n" + "k2 INTEGER NOT NULL,\n" + "k3 INTEGER,\n"
                        + "v1 VARCHAR,\n" + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n"
                        + (splits != null ? (" split on " + splits) : "");
        conn.createStatement().execute(ddl);
        conn.close();
    }

    // Moved from LocalIndexIT because it was causing parallel runs to hang
    @Test
    public void testLocalIndexScanAfterRegionSplit() throws Exception {
        String schemaName = generateUniqueName();
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), false);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, "('e','j','o')");
        Connection conn1 = getConnectionForLocalIndexTest();
        try {
            String[] strings =
                    { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",
                            "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
            for (int i = 0; i < 26; i++) {
                conn1.createStatement()
                        .execute("UPSERT INTO " + tableName + " values('" + strings[i] + "'," + i
                                + "," + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
            }
            conn1.commit();
            conn1.createStatement()
                    .execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement()
                    .execute("CREATE LOCAL INDEX " + indexName + "_2 ON " + tableName + "(k3)");

            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());

            HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            for (int i = 1; i < 5; i++) {
                admin.split(physicalTableName, ByteUtil.concat(Bytes.toBytes(strings[3 * i])));
                List<HRegionInfo> regionsOfUserTable =
                        MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                            admin.getConnection(), physicalTableName, false);

                while (regionsOfUserTable.size() != (4 + i)) {
                    Thread.sleep(100);
                    regionsOfUserTable =
                            MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                                admin.getConnection(), physicalTableName, false);
                }
                assertEquals(4 + i, regionsOfUserTable.size());
                String[] tIdColumnValues = new String[26];
                String[] v1ColumnValues = new String[26];
                int[] k1ColumnValue = new int[26];
                String query = "SELECT t_id,k1,v1 FROM " + tableName;
                rs = conn1.createStatement().executeQuery(query);
                Thread.sleep(1000);
                for (int j = 0; j < 26; j++) {
                    assertTrue("No row found at " + j, rs.next());
                    tIdColumnValues[j] = rs.getString("t_id");
                    k1ColumnValue[j] = rs.getInt("k1");
                    v1ColumnValues[j] = rs.getString("V1");
                }
                Arrays.sort(tIdColumnValues);
                Arrays.sort(v1ColumnValues);
                Arrays.sort(k1ColumnValue);
                assertTrue(Arrays.equals(strings, tIdColumnValues));
                assertTrue(Arrays.equals(strings, v1ColumnValues));
                for (int m = 0; m < 26; m++) {
                    assertEquals(m, k1ColumnValue[m]);
                }

                rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
                assertEquals("CLIENT PARALLEL " + (4 + i) + "-WAY RANGE SCAN OVER "
                        + indexPhysicalTableName + " [1]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n" + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));

                query = "SELECT t_id,k1,k3 FROM " + tableName;
                rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
                assertEquals(
                    "CLIENT PARALLEL "
                            + ((strings[3 * i].compareTo("j") < 0) ? (4 + i) : (4 + i - 1))
                            + "-WAY RANGE SCAN OVER " + indexPhysicalTableName + " [2]\n"
                            + "    SERVER FILTER BY FIRST KEY ONLY\n" + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
                rs = conn1.createStatement().executeQuery(query);
                Thread.sleep(1000);
                int[] k3ColumnValue = new int[26];
                for (int j = 0; j < 26; j++) {
                    assertTrue(rs.next());
                    tIdColumnValues[j] = rs.getString("t_id");
                    k1ColumnValue[j] = rs.getInt("k1");
                    k3ColumnValue[j] = rs.getInt("k3");
                }
                Arrays.sort(tIdColumnValues);
                Arrays.sort(k1ColumnValue);
                Arrays.sort(k3ColumnValue);
                assertTrue(Arrays.equals(strings, tIdColumnValues));
                for (int m = 0; m < 26; m++) {
                    assertEquals(m, k1ColumnValue[m]);
                    assertEquals(m + 2, k3ColumnValue[m]);
                }
            }
        } finally {
            conn1.close();
        }
    }

    // Moved from LocalIndexIT because it was causing parallel runs to hang
    @Test
    public void testLocalIndexScanAfterRegionsMerge() throws Exception {
        String schemaName = generateUniqueName();
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), false);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, "('e','j','o')");
        Connection conn1 = getConnectionForLocalIndexTest();
        try {
            String[] strings =
                    { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",
                            "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
            for (int i = 0; i < 26; i++) {
                conn1.createStatement()
                        .execute("UPSERT INTO " + tableName + " values('" + strings[i] + "'," + i
                                + "," + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
            }
            conn1.commit();
            conn1.createStatement()
                    .execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement()
                    .execute("CREATE LOCAL INDEX " + indexName + "_2 ON " + tableName + "(k3)");

            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());

            HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
            List<HRegionInfo> regionsOfUserTable =
                    MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                        admin.getConnection(), physicalTableName, false);
            admin.mergeRegions(regionsOfUserTable.get(0).getEncodedNameAsBytes(),
                regionsOfUserTable.get(1).getEncodedNameAsBytes(), false);
            regionsOfUserTable =
                    MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                        admin.getConnection(), physicalTableName, false);

            while (regionsOfUserTable.size() != 3) {
                Thread.sleep(100);
                regionsOfUserTable =
                        MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                            admin.getConnection(), physicalTableName, false);
            }
            String query = "SELECT t_id,k1,v1 FROM " + tableName;
            rs = conn1.createStatement().executeQuery(query);
            Thread.sleep(1000);
            for (int j = 0; j < 26; j++) {
                assertTrue(rs.next());
                assertEquals(strings[25 - j], rs.getString("t_id"));
                assertEquals(25 - j, rs.getInt("k1"));
                assertEquals(strings[j], rs.getString("V1"));
            }
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(
                "CLIENT PARALLEL " + 3 + "-WAY RANGE SCAN OVER " + indexPhysicalTableName + " [1]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n" + "CLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));

            query = "SELECT t_id,k1,k3 FROM " + tableName;
            rs = conn1.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(
                "CLIENT PARALLEL " + 3 + "-WAY RANGE SCAN OVER " + indexPhysicalTableName + " [2]\n"
                        + "    SERVER FILTER BY FIRST KEY ONLY\n" + "CLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));

            rs = conn1.createStatement().executeQuery(query);
            Thread.sleep(1000);
            for (int j = 0; j < 26; j++) {
                assertTrue(rs.next());
                assertEquals(strings[j], rs.getString("t_id"));
                assertEquals(j, rs.getInt("k1"));
                assertEquals(j + 2, rs.getInt("k3"));
            }
        } finally {
            conn1.close();
        }
    }

}
