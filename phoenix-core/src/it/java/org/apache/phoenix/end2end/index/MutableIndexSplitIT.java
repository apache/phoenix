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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class MutableIndexSplitIT extends ParallelStatsDisabledIT {
    
    protected final boolean localIndex;
    protected final boolean multiTenant;
	
    public MutableIndexSplitIT(boolean localIndex,boolean multiTenant) {
		this.localIndex = localIndex;
		this.multiTenant = multiTenant;
	}
    
    private static Connection getConnection(Properties props) throws SQLException {
        props.setProperty(QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, Integer.toString(1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        return conn;
    }
    
	@Parameters(name="MutableIndexSplitIT_localIndex={0},multiTenant={1}") // name is used by failsafe as file name in reports
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] { 
                { false, false },{ false, true },{true, false}, { true, true } });
    }
    
    protected void testSplitDuringIndexScan(boolean isReverse) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.SCAN_CACHE_SIZE_ATTRIB, Integer.toString(2));
        props.setProperty(QueryServices.FORCE_ROW_KEY_ORDER_ATTRIB, Boolean.toString(false));
        Connection conn1 = getConnection(props);
		String tableName = "TBL_" + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
		HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        try{
            String[] strings = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
            createTableAndLoadData(conn1, tableName, indexName, strings, isReverse);

            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            splitDuringScan(conn1, tableName, indexName, strings, admin, isReverse);
       } finally {
           if(conn1 != null) conn1.close();
           if(admin != null) admin.close();
       }
    }

    private void createTableAndLoadData(Connection conn1, String tableName, String indexName, String[] strings, boolean isReverse) throws SQLException {
        createBaseTable(conn1, tableName, null);
        for (int i = 0; i < 26; i++) {
            conn1.createStatement().execute(
                "UPSERT INTO " + tableName + " values('"+strings[i]+"'," + i + ","
                        + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
        }
        conn1.commit();
        conn1.createStatement().execute(
            "CREATE " + (localIndex ? "LOCAL" : "")+" INDEX " + indexName + " ON " + tableName + "(v1"+(isReverse?" DESC":"")+") include (k3)");
    }

    private List<HRegionInfo> splitDuringScan(Connection conn1, String tableName, String indexName, String[] strings, HBaseAdmin admin, boolean isReverse)
            throws SQLException, IOException, InterruptedException {
        ResultSet rs;

        String query = "SELECT t_id,k1,v1 FROM " + tableName;
        rs = conn1.createStatement().executeQuery(query);
        String[] tIdColumnValues = new String[26]; 
        String[] v1ColumnValues = new String[26];
        int[] k1ColumnValue = new int[26];
        for (int j = 0; j < 5; j++) {
            assertTrue(rs.next());
            tIdColumnValues[j] = rs.getString("t_id");
            k1ColumnValue[j] = rs.getInt("k1");
            v1ColumnValues[j] = rs.getString("V1");
        }

        String[] splitKeys = new String[2];
        splitKeys[0] = strings[4];
        splitKeys[1] = strings[12];

        int[] splitInts = new int[2];
        splitInts[0] = 22;
        splitInts[1] = 4;
        List<HRegionInfo> regionsOfUserTable = null;
        for(int i = 0; i <=1; i++) {
            Threads.sleep(10000);
            if(localIndex) {
                admin.split(Bytes.toBytes(tableName),
                    ByteUtil.concat(Bytes.toBytes(splitKeys[i])));
            } else {
                admin.split(Bytes.toBytes(indexName), ByteUtil.concat(Bytes.toBytes(splitInts[i])));
            }
            Thread.sleep(100);
            regionsOfUserTable =
                    MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                        admin.getConnection(), TableName.valueOf(localIndex?tableName:indexName),
                        false);

            while (regionsOfUserTable.size() != (i+2)) {
                Thread.sleep(100);
                regionsOfUserTable =
                        MetaTableAccessor.getTableRegions(getUtility().getZooKeeperWatcher(),
                            admin.getConnection(),
                            TableName.valueOf(localIndex?tableName:indexName), false);
            }
            assertEquals(i+2, regionsOfUserTable.size());
        }
        for (int j = 5; j < 26; j++) {
            assertTrue(rs.next());
            tIdColumnValues[j] = rs.getString("t_id");
            k1ColumnValue[j] = rs.getInt("k1");
            v1ColumnValues[j] = rs.getString("V1");
        }
        Arrays.sort(tIdColumnValues);
        Arrays.sort(v1ColumnValues);
        Arrays.sort(k1ColumnValue);
        assertTrue(Arrays.equals(strings, tIdColumnValues));
        assertTrue(Arrays.equals(strings, v1ColumnValues));
        for(int i=0;i<26;i++) {
            assertEquals(i, k1ColumnValue[i]);
        }
        assertFalse(rs.next());
        return regionsOfUserTable;
    }

    private void createBaseTable(Connection conn, String tableName, String splits) throws SQLException {
        String ddl = "CREATE TABLE " + tableName + " (t_id VARCHAR NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "k3 INTEGER,\n" +
                "v1 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))" + (multiTenant ? " MULTI_TENANT=true ":"") +"\n"
                    + (splits != null ? (" split on " + splits) : "");
        conn.createStatement().execute(ddl);
    }
    
}
