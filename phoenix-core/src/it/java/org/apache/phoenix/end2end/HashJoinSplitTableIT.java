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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class HashJoinSplitTableIT extends BaseOwnClusterHBaseManagedTimeIT  {
    
    @BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Forces server cache to be used
        props.put(QueryServices.INDEX_MUTATE_BATCH_SIZE_THRESHOLD_ATTRIB, Integer.toString(2));
        // Must update config before starting server
        NUM_SLAVES_BASE = 2;
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @After
    public void assertNoUnfreedMemory() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        try {
            long unfreedBytes = conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
            assertEquals(0,unfreedBytes);
        } finally {
            conn.close();
        }
    }
    
    /**
     * only run in DistributedClusterMode to reproduce phoenix 2900
     */
    @Test
    public void testJoinOverSplitSaltedTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);

        try {
            // 1.create LHS SALT_TEST table
            conn.createStatement().execute("drop table if exists SALT_TEST2900");

            conn.createStatement().execute(
                "create table SALT_TEST2900" + "(" + "id UNSIGNED_INT not null primary key,"
                        + "appId VARCHAR" + ")SALT_BUCKETS=2");

            conn.createStatement().execute("upsert into SALT_TEST2900(id,appId) values(1,'app1')");
            conn.createStatement().execute("upsert into SALT_TEST2900(id,appId) values(2,'app2')");
            conn.createStatement().execute("upsert into SALT_TEST2900(id,appId) values(3,'app3')");
            conn.createStatement().execute("upsert into SALT_TEST2900(id,appId) values(4,'app4')");
            conn.createStatement().execute("upsert into SALT_TEST2900(id,appId) values(5,'app5')");
            conn.createStatement().execute("upsert into SALT_TEST2900(id,appId) values(6,'app6')");

            // 2.split SALT_TEST at rowkey3,i.e.,split the first region
            byte[] id3 = Bytes.toBytes(3);
            byte[] rowKey3 = new byte[1 + 4];
            System.arraycopy(id3, 0, rowKey3, 1, 4);
            byte salt3 = SaltingUtil.getSaltingByte(rowKey3, 1, rowKey3.length - 1, 2);
            rowKey3[0] = salt3;
            HBaseAdmin hbaseAdmin = getUtility().getHBaseAdmin();
            hbaseAdmin.split(Bytes.toBytes("SALT_TEST2900"), rowKey3);

            // 3.wait the SALT_TEST split complele
            while (hbaseAdmin.getTableRegions(Bytes.toBytes("SALT_TEST2900")).size() < 3) {
                Thread.sleep(1000);
            }
            // 4.we should make sure region0 and region1 is not on same region server
            if(NUM_SLAVES_BASE > 1)
            {
                HRegionInfo regionInfo0 =
                        hbaseAdmin.getTableRegions(Bytes.toBytes("SALT_TEST2900")).get(0);
                HRegionInfo regionInfo1 =
                        hbaseAdmin.getTableRegions(Bytes.toBytes("SALT_TEST2900")).get(1);
                ServerName serverName0 =
                        hbaseAdmin.getConnection().locateRegion(regionInfo0.getRegionName())
                        .getServerName();
                ServerName serverName1 =
                        hbaseAdmin.getConnection().locateRegion(regionInfo1.getRegionName())
                        .getServerName();
                if (serverName0.equals(serverName1)) {
                    String regionEncodedName1 = regionInfo1.getEncodedName();
                    hbaseAdmin.move(Bytes.toBytes(regionEncodedName1), null);
                    while (hbaseAdmin.getConnection().locateRegion(regionInfo1.getRegionName())
                            .getServerName().equals(serverName1)) {
                        Thread.sleep(1000);
                    }
                }
            }

            // 5.create RHS RIGHT_TEST table
            conn.createStatement().execute("drop table if exists RIGHT_TEST2900 ");
            conn.createStatement().execute(
                "create table RIGHT_TEST2900" + "(" + "appId VARCHAR not null primary key,"
                        + "createTime VARCHAR" + ")");

            conn.createStatement().execute(
                "upsert into RIGHT_TEST2900(appId,createTime) values('app2','201601')");
            conn.createStatement().execute(
                "upsert into RIGHT_TEST2900(appId,createTime) values('app3','201602')");
            conn.createStatement().execute(
                "upsert into RIGHT_TEST2900(appId,createTime) values('app4','201603')");
            conn.createStatement().execute(
                "upsert into RIGHT_TEST2900(appId,createTime) values('app5','201604')");

            // 6.clear SALT_TEST's TableRegionCache,let join know SALT_TEST's newest 3 region
            ((PhoenixConnection) conn).getQueryServices().clearTableRegionCache(
                Bytes.toBytes("SALT_TEST2900"));

            // 7.do join,throw exception
            String sql =
                    "select * from SALT_TEST2900 a inner join RIGHT_TEST2900 b on a.appId=b.appId where a.id>=3 and a.id<=5";
            ResultSet rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertTrue(rs.next());

        } finally {
            conn.close();
        }

    }

}
