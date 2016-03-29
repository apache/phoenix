/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.balancer;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.TestSplitTransactionOnCluster.MockedRegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexTestingUtils;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.master.IndexMasterObserver;
import org.apache.phoenix.util.ConfigUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class IndexLoadBalancerIT {

    private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

    private static HBaseAdmin admin = null;

    @BeforeClass
    public static void setupCluster() throws Exception {
        final int NUM_RS = 4;
        Configuration conf = UTIL.getConfiguration();
        conf.setBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO, true);
        conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, IndexMasterObserver.class.getName());
        conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, IndexLoadBalancer.class,
            LoadBalancer.class);
        IndexTestingUtils.setupConfig(conf);
        // disable version checking, so we can test against whatever version of HBase happens to be
        // installed (right now, its generally going to be SNAPSHOT versions).
        conf.setBoolean(Indexer.CHECK_VERSION_CONF_KEY, false);
        // set replication required parameter
        ConfigUtil.setReplicationConfigIfAbsent(conf);
        UTIL.startMiniCluster(NUM_RS);
        admin = UTIL.getHBaseAdmin();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            if (admin != null) {
                admin.disableTables(".*");
                admin.deleteTables(".*");
                admin.close();
            }
        } finally {
            UTIL.shutdownMiniCluster();
        }
    }

    @Test(timeout = 180000)
    public void testRoundRobinAssignmentDuringIndexTableCreation() throws Exception {
        MiniHBaseCluster cluster = UTIL.getHBaseCluster();
        HMaster master = cluster.getMaster();
        TableName tableName = TableName.valueOf("testRoundRobinAssignmentDuringIndexTableCreation");
        TableName indexTableName =
                TableName.valueOf("testRoundRobinAssignmentDuringIndexTableCreation_index");
        createUserAndIndexTable(tableName, indexTableName);
        boolean isRegionColocated =
                checkForColocation(master, tableName.getNameAsString(), indexTableName
                        .getNameAsString());
        assertTrue("User regions and index regions should colocate.", isRegionColocated);
    }

    @Test(timeout = 180000)
    public void testColocationAfterSplit() throws Exception {
        MiniHBaseCluster cluster = UTIL.getHBaseCluster();
        HMaster master = cluster.getMaster();
        // Table names to make use of the
        TableName tableName = TableName.valueOf("testSplitHooksBeforeAndAfterPONR_1");
        TableName indexTableName = TableName.valueOf("testSplitHooksBeforeAndAfterPONR_2");
        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addCoprocessor(MockedRegionObserver.class.getName());
        htd.addFamily(new HColumnDescriptor("cf"));
        char c = 'A';
        byte[][] split = new byte[20][];
        for (int i = 0; i < 20; i++) {
            byte[] b = { (byte) c };
            split[i] = b;
            c++;
        }
        admin.createTable(htd, split);
        HTableDescriptor iHtd = new HTableDescriptor(indexTableName);
        iHtd.addFamily(new HColumnDescriptor("cf"));
        iHtd.setValue(IndexLoadBalancer.PARENT_TABLE_KEY, tableName.toBytes());
        admin.createTable(iHtd, split);

        // test put with the indexed column

        insertData(tableName);
        insertData(indexTableName);

        admin.split(tableName.getNameAsString(), "c");
        List<HRegionInfo> regionsOfUserTable =
                master.getAssignmentManager().getRegionStates().getRegionsOfTable(tableName);

        while (regionsOfUserTable.size() != 22) {
            Thread.sleep(100);
            regionsOfUserTable =
                    master.getAssignmentManager().getRegionStates().getRegionsOfTable(tableName);
        }

        List<HRegionInfo> regionsOfIndexTable =
                master.getAssignmentManager().getRegionStates().getRegionsOfTable(indexTableName);

        while (regionsOfIndexTable.size() != 22) {
            Thread.sleep(100);
            regionsOfIndexTable =
                    master.getAssignmentManager().getRegionStates().getRegionsOfTable(
                        indexTableName);
        }
        boolean isRegionColocated =
                checkForColocation(master, tableName.getNameAsString(), indexTableName
                        .getNameAsString());
        assertTrue("User regions and index regions should colocate.", isRegionColocated);
    }

    @Test(timeout = 180000)
    public void testColocationAfterRegionsMerge() throws Exception {
        MiniHBaseCluster cluster = UTIL.getHBaseCluster();
        HMaster master = cluster.getMaster();
        RegionStates regionStates = master.getAssignmentManager().getRegionStates();
        // Table names to make use of the
        TableName tableName = TableName.valueOf("testColocationAfterRegionsMerge");
        TableName indexTableName = TableName.valueOf("testColocationAfterRegionsMerge_index");
        createUserAndIndexTable(tableName, indexTableName);
        ServerName server = cluster.getRegionServer(0).getServerName();
        List<HRegionInfo> regionsOfUserTable = regionStates.getRegionsOfTable(tableName);
        Pair<HRegionInfo, HRegionInfo> regionsToMerge = new Pair<HRegionInfo, HRegionInfo>();
        byte[] startKey1 = { (byte) 'C' };
        byte[] startKey2 = { (byte) 'D' };
        for (HRegionInfo region : regionsOfUserTable) {
            if (Bytes.compareTo(startKey1, region.getStartKey()) == 0) {
                regionsToMerge.setFirst(region);
            } else if (Bytes.compareTo(startKey2, region.getStartKey()) == 0) {
                regionsToMerge.setSecond(region);
            }
        }
        admin.move(regionsToMerge.getFirst().getEncodedNameAsBytes(), Bytes.toBytes(server
                .toString()));
        admin.move(regionsToMerge.getSecond().getEncodedNameAsBytes(), Bytes.toBytes(server
                .toString()));

        List<HRegionInfo> regionsOfIndexTable = regionStates.getRegionsOfTable(indexTableName);
        Pair<HRegionInfo, HRegionInfo> indexRegionsToMerge = new Pair<HRegionInfo, HRegionInfo>();
        for (HRegionInfo region : regionsOfIndexTable) {
            if (Bytes.compareTo(startKey1, region.getStartKey()) == 0) {
                indexRegionsToMerge.setFirst(region);
            } else if (Bytes.compareTo(startKey2, region.getStartKey()) == 0) {
                indexRegionsToMerge.setSecond(region);
            }
        }
        admin.move(indexRegionsToMerge.getFirst().getEncodedNameAsBytes(), Bytes.toBytes(server
                .toString()));
        admin.move(indexRegionsToMerge.getSecond().getEncodedNameAsBytes(), Bytes.toBytes(server
                .toString()));
        while (!regionStates.getRegionServerOfRegion(regionsToMerge.getFirst()).equals(server)
                || !regionStates.getRegionServerOfRegion(regionsToMerge.getSecond()).equals(server)
                || !regionStates.getRegionServerOfRegion(indexRegionsToMerge.getFirst()).equals(
                    server)
                || !regionStates.getRegionServerOfRegion(indexRegionsToMerge.getSecond()).equals(
                    server)) {
            Threads.sleep(1000);
        }
        admin.mergeRegions(regionsToMerge.getFirst().getEncodedNameAsBytes(), regionsToMerge
                .getSecond().getEncodedNameAsBytes(), true);
        admin.mergeRegions(indexRegionsToMerge.getFirst().getEncodedNameAsBytes(),
            indexRegionsToMerge.getSecond().getEncodedNameAsBytes(), true);

        while (regionsOfUserTable.size() != 20 || regionsOfIndexTable.size() != 20) {
            Thread.sleep(100);
            regionsOfUserTable = regionStates.getRegionsOfTable(tableName);
            regionsOfIndexTable = regionStates.getRegionsOfTable(indexTableName);
        }
        boolean isRegionColocated =
                checkForColocation(master, tableName.getNameAsString(), indexTableName
                        .getNameAsString());
        assertTrue("User regions and index regions should colocate.", isRegionColocated);
    }

    private void insertData(TableName tableName) throws IOException, InterruptedException {
        HTable table = new HTable(admin.getConfiguration(), tableName);
        Put p = new Put("a".getBytes());
        p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
        p.add("cf".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
        table.put(p);

        Put p1 = new Put("b".getBytes());
        p1.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
        p1.add("cf".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
        table.put(p1);

        Put p2 = new Put("c".getBytes());
        p2.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
        p2.add("cf".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
        table.put(p2);

        Put p3 = new Put("c1".getBytes());
        p3.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
        p3.add("cf".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
        table.put(p3);

        Put p4 = new Put("d".getBytes());
        p4.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("Val"));
        p4.add("cf".getBytes(), "q2".getBytes(), Bytes.toBytes("ValForCF2"));
        table.put(p4);
        admin.flush(tableName.getNameAsString());
    }

    @Test(timeout = 180000)
    public void testRandomAssignmentDuringIndexTableEnable() throws Exception {
        MiniHBaseCluster cluster = UTIL.getHBaseCluster();
        HMaster master = cluster.getMaster();
        master.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", false);
        TableName tableName = TableName.valueOf("testRandomAssignmentDuringIndexTableEnable");
        TableName indexTableName =
                TableName.valueOf("testRandomAssignmentDuringIndexTableEnable_index");
        createUserAndIndexTable(tableName, indexTableName);
        admin.disableTable(tableName);
        admin.disableTable(indexTableName);
        admin.enableTable(tableName);
        admin.enableTable(indexTableName);
        boolean isRegionColocated =
                checkForColocation(master, tableName.getNameAsString(), indexTableName
                        .getNameAsString());
        assertTrue("User regions and index regions should colocate.", isRegionColocated);

    }

    @Test(timeout = 180000)
    public void testBalanceCluster() throws Exception {
        MiniHBaseCluster cluster = UTIL.getHBaseCluster();
        HMaster master = cluster.getMaster();
        master.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", false);
        master.getConfiguration().setBoolean("hbase.master.startup.retainassign", false);
        master.getConfiguration().setBoolean("hbase.master.loadbalance.bytable", false);

        TableName tableName = TableName.valueOf("testBalanceCluster");
        TableName indexTableName = TableName.valueOf("testBalanceCluster_index");
        createUserAndIndexTable(tableName, indexTableName);
        HTableDescriptor htd1 = new HTableDescriptor(TableName.valueOf("testBalanceCluster1"));
        htd1.addFamily(new HColumnDescriptor("fam1"));
        char c = 'A';
        byte[][] split1 = new byte[12][];
        for (int i = 0; i < 12; i++) {
            byte[] b = { (byte) c };
            split1[i] = b;
            c++;
        }
        admin.setBalancerRunning(false, false);
        admin.createTable(htd1, split1);
        admin.disableTable(tableName);
        admin.enableTable(tableName);
        admin.setBalancerRunning(true, false);
        admin.balancer();
        boolean isRegionsColocated =
                checkForColocation(master, tableName.getNameAsString(), indexTableName
                        .getNameAsString());
        assertTrue("User regions and index regions should colocate.", isRegionsColocated);
    }

    @Test(timeout = 180000)
    public void testBalanceByTable() throws Exception {
        ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
        MiniHBaseCluster cluster = UTIL.getHBaseCluster();
        HMaster master = cluster.getMaster();
        master.getConfiguration().setBoolean("hbase.master.loadbalance.bytable", true);
        TableName tableName = TableName.valueOf("testBalanceByTable");
        TableName indexTableName = TableName.valueOf("testBalanceByTable_index");
        createUserAndIndexTable(tableName, indexTableName);
        HTableDescriptor htd1 = new HTableDescriptor(TableName.valueOf("testBalanceByTable1"));
        htd1.addFamily(new HColumnDescriptor("fam1"));
        char c = 'A';
        byte[][] split1 = new byte[12][];
        for (int i = 0; i < 12; i++) {
            byte[] b = { (byte) c };
            split1[i] = b;
            c++;
        }
        admin.disableTable(tableName);
        admin.enableTable(tableName);
        admin.setBalancerRunning(true, false);
        boolean isRegionColocated =
                checkForColocation(master, tableName.getNameAsString(), indexTableName
                        .getNameAsString());
        assertTrue("User regions and index regions should colocate.", isRegionColocated);
        admin.balancer();
        Thread.sleep(10000);
        ZKAssign.blockUntilNoRIT(zkw);
        while (master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
            Threads.sleep(1000);
        }
        isRegionColocated =
                checkForColocation(master, tableName.getNameAsString(), indexTableName
                        .getNameAsString());
        assertTrue("User regions and index regions should colocate.", isRegionColocated);
    }

    @Test(timeout = 180000)
    public void testRoundRobinAssignmentAfterRegionServerDown() throws Exception {
        ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
        MiniHBaseCluster cluster = UTIL.getHBaseCluster();
        HMaster master = cluster.getMaster();
        TableName tableName = TableName.valueOf("testRoundRobinAssignmentAfterRegionServerDown");
        TableName indexTableName =
                TableName.valueOf("testRoundRobinAssignmentAfterRegionServerDown_index");
        createUserAndIndexTable(tableName, indexTableName);
        HRegionServer regionServer = cluster.getRegionServer(1);
        regionServer.abort("Aborting to test random assignment after region server down");
        while (master.getServerManager().areDeadServersInProgress()) {
            Thread.sleep(1000);
        }
        ZKAssign.blockUntilNoRIT(zkw);
        while (master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
            Threads.sleep(1000);
        }
        boolean isRegionColocated =
                checkForColocation(master, tableName.getNameAsString(), indexTableName
                        .getNameAsString());
        assertTrue("User regions and index regions should colocate.", isRegionColocated);

    }

    @Test(timeout = 180000)
    public void testRetainAssignmentDuringMasterStartUp() throws Exception {
        ZooKeeperWatcher zkw = UTIL.getZooKeeperWatcher(UTIL);
        MiniHBaseCluster cluster = UTIL.getHBaseCluster();
        HMaster master = cluster.getMaster();
        master.getConfiguration().setBoolean("hbase.master.startup.retainassign", true);
        TableName tableName = TableName.valueOf("testRetainAssignmentDuringMasterStartUp");
        TableName indexTableName =
                TableName.valueOf("testRetainAssignmentDuringMasterStartUp_index");
        createUserAndIndexTable(tableName, indexTableName);
        UTIL.shutdownMiniHBaseCluster();
        UTIL.startMiniHBaseCluster(1, 4);
        cluster = UTIL.getHBaseCluster();
        master = cluster.getMaster();
        if (admin != null) {
            admin.close();
            admin = new HBaseAdmin(master.getConfiguration());
        }
        ZKAssign.blockUntilNoRIT(zkw);
        while (master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
            Threads.sleep(1000);
        }
        boolean isRegionColocated =
                checkForColocation(master, tableName.getNameAsString(), indexTableName
                        .getNameAsString());
        assertTrue("User regions and index regions should colocate.", isRegionColocated);

    }

    @Test(timeout = 300000)
    public void testRoundRobinAssignmentDuringMasterStartUp() throws Exception {
        MiniHBaseCluster cluster = UTIL.getHBaseCluster();
        HMaster master = cluster.getMaster();
        UTIL.getConfiguration().setBoolean("hbase.master.startup.retainassign", false);

        TableName tableName = TableName.valueOf("testRoundRobinAssignmentDuringMasterStartUp");
        TableName indexTableName =
                TableName.valueOf("testRoundRobinAssignmentDuringMasterStartUp_index");
        createUserAndIndexTable(tableName, indexTableName);
        UTIL.shutdownMiniHBaseCluster();
        cluster.waitUntilShutDown();
        UTIL.startMiniHBaseCluster(1, 4);
        cluster = UTIL.getHBaseCluster();
        if (admin != null) {
            admin.close();
            admin = new HBaseAdmin(cluster.getMaster().getConfiguration());
        }
        master = cluster.getMaster();
        while (master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
            Threads.sleep(1000);
        }
        boolean isRegionColocated =
                checkForColocation(master, tableName.getNameAsString(), indexTableName
                        .getNameAsString());
        assertTrue("User regions and index regions should colocate.", isRegionColocated);
    }

    private void createUserAndIndexTable(TableName tableName, TableName indexTableName)
            throws IOException {
        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor("cf"));
        char c = 'A';
        byte[][] split = new byte[20][];
        for (int i = 0; i < 20; i++) {
            byte[] b = { (byte) c };
            split[i] = b;
            c++;
        }
        admin.createTable(htd, split);
        HTableDescriptor iHtd = new HTableDescriptor(indexTableName);
        iHtd.addFamily(new HColumnDescriptor("cf"));
        iHtd.setValue(IndexLoadBalancer.PARENT_TABLE_KEY, tableName.toBytes());
        admin.createTable(iHtd, split);
    }

    private List<Pair<byte[], ServerName>> getStartKeysAndLocations(HMaster master, String tableName)
            throws IOException, InterruptedException {

        List<Pair<HRegionInfo, ServerName>> tableRegionsAndLocations =
                MetaTableAccessor.getTableRegionsAndLocations(master.getZooKeeper(), master.getConnection(),
                        TableName.valueOf(tableName));
        List<Pair<byte[], ServerName>> startKeyAndLocationPairs =
                new ArrayList<Pair<byte[], ServerName>>(tableRegionsAndLocations.size());
        Pair<byte[], ServerName> startKeyAndLocation = null;
        for (Pair<HRegionInfo, ServerName> regionAndLocation : tableRegionsAndLocations) {
            startKeyAndLocation =
                    new Pair<byte[], ServerName>(regionAndLocation.getFirst().getStartKey(),
                            regionAndLocation.getSecond());
            startKeyAndLocationPairs.add(startKeyAndLocation);
        }
        return startKeyAndLocationPairs;

    }

    public boolean checkForColocation(HMaster master, String tableName, String indexTableName)
            throws IOException, InterruptedException {
        List<Pair<byte[], ServerName>> uTableStartKeysAndLocations =
                getStartKeysAndLocations(master, tableName);
        List<Pair<byte[], ServerName>> iTableStartKeysAndLocations =
                getStartKeysAndLocations(master, indexTableName);

        boolean regionsColocated = true;
        if (uTableStartKeysAndLocations.size() != iTableStartKeysAndLocations.size()) {
            regionsColocated = false;
        } else {
            for (int i = 0; i < uTableStartKeysAndLocations.size(); i++) {
                Pair<byte[], ServerName> uStartKeyAndLocation = uTableStartKeysAndLocations.get(i);
                Pair<byte[], ServerName> iStartKeyAndLocation = iTableStartKeysAndLocations.get(i);

                if (Bytes.compareTo(uStartKeyAndLocation.getFirst(), iStartKeyAndLocation
                        .getFirst()) == 0) {
                    if (uStartKeyAndLocation.getSecond().equals(iStartKeyAndLocation.getSecond())) {
                        continue;
                    }
                }
                regionsColocated = false;
            }
        }
        return regionsColocated;
    }
}
