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

package org.apache.hadoop.hbase.regionserver.wal;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.hbase.index.write.recovery.PerRegionIndexWriteCache;
import org.apache.phoenix.hbase.index.write.recovery.StoreFailuresInCachePolicy;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
public class WALRecoveryRegionPostOpenIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WALRecoveryRegionPostOpenIT.class);

    private static final String DATA_TABLE_NAME="DATA_POST_OPEN";

    private static final String INDEX_TABLE_NAME="INDEX_POST_OPEN";

    private static final long ONE_SEC = 1000;
    private static final long ONE_MIN = 60 * ONE_SEC;
    private static final long TIMEOUT = ONE_MIN;

    private static volatile CountDownLatch handleFailureCountDownLatch= null;

    private static volatile Multimap<HTableInterfaceReference, Mutation> tableReferenceToMutation=null;

    private static volatile int handleFailureCalledCount=0;

    private static volatile boolean failIndexTableWrite=false;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put("hbase.coprocessor.region.classes", IndexTableFailingRegionObserver.class.getName());
        serverProps.put(Indexer.RecoveryFailurePolicyKeyForTesting, ReleaseLatchOnFailurePolicy.class.getName());
        serverProps.put(IndexWriterUtils.INDEX_WRITER_RPC_RETRIES_NUMBER, "2");
        serverProps.put(HConstants.HBASE_RPC_TIMEOUT_KEY, "10000");
        serverProps.put(IndexWriterUtils.INDEX_WRITER_RPC_PAUSE, "5000");
        serverProps.put("data.tx.snapshot.dir", "/tmp");
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB, Boolean.FALSE.toString());
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.FALSE.toString());
        clientProps.put(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB, Boolean.FALSE.toString());
        NUM_SLAVES_BASE = 2;
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
        getUtility().getHBaseCluster().getMaster().balanceSwitch(false);
    }

    public static class IndexTableFailingRegionObserver extends SimpleRegionObserver {

        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> observerContext, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {

            if (observerContext.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString().contains(INDEX_TABLE_NAME) && failIndexTableWrite) {
                throw new DoNotRetryIOException();
            }
            Mutation operation = miniBatchOp.getOperation(0);
            Set<byte[]> keySet = operation.getFamilyCellMap().keySet();
            for(byte[] family: keySet) {
                if(Bytes.toString(family).startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX) && failIndexTableWrite) {
                    throw new DoNotRetryIOException();
                }
            }
            super.preBatchMutate(observerContext, miniBatchOp);
        }
    }


    public static class ReleaseLatchOnFailurePolicy extends StoreFailuresInCachePolicy {

        public ReleaseLatchOnFailurePolicy(PerRegionIndexWriteCache failedIndexEdits) {
            super(failedIndexEdits);
        }

        @Override
        public void handleFailure(Multimap<HTableInterfaceReference, Mutation> attempted, Exception cause) throws IOException
        {
            LOGGER.info("Found index update failure!");
            handleFailureCalledCount++;
            tableReferenceToMutation=attempted;
            LOGGER.info("failed index update on WAL recovery - allowing index table can be write.");
            failIndexTableWrite=false;
            super.handleFailure(attempted, cause);

            if(handleFailureCountDownLatch!=null) {
                handleFailureCountDownLatch.countDown();
            }
         }
    }

    @Test
    public void testRecoveryRegionPostOpen() throws Exception {
        handleFailureCountDownLatch= null ;
        tableReferenceToMutation=null;
        handleFailureCalledCount=0;
        failIndexTableWrite=false;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);


        try (Connection conn = driver.connect(url, props)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("CREATE TABLE " + DATA_TABLE_NAME
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) ");


            conn.createStatement().execute(
                    "CREATE " +  "INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_NAME + " (v1) INCLUDE (v2)");
            String query = "SELECT * FROM " + DATA_TABLE_NAME;
            ResultSet resultSet = conn.createStatement().executeQuery(query);
            assertFalse(resultSet.next());

            MiniHBaseCluster miniHBaseCluster = getUtility().getMiniHBaseCluster();
            this.moveIndexTableRegionIfSameRegionSErver(miniHBaseCluster);
            this.assertRegionServerDifferent(miniHBaseCluster);

            //load one row into the table
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_NAME + " VALUES(?,?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "x");
            stmt.setString(3, "1");
            stmt.execute();

            this.assertRegionServerDifferent(miniHBaseCluster);

            Scan scan = new Scan();
            org.apache.hadoop.hbase.client.Connection hbaseConn = ConnectionFactory.createConnection(getUtility().getConfiguration());
            Table primaryTable = hbaseConn.getTable(TableName.valueOf(DATA_TABLE_NAME));
            ResultScanner resultScanner = primaryTable.getScanner(scan);
            int count = 0;
             for (Result result : resultScanner) {
                 count++;
             }
             assertEquals("Got an unexpected found of data rows", 1, count);

            // begin to kill data table regionServer,and data table region would move to the other regionSever,
            // and then recover data table's WAL
            handleFailureCountDownLatch=new CountDownLatch(1);
            failIndexTableWrite=true;

            ServerName dataTableRegionServerName=this.getRegionServerName(miniHBaseCluster, DATA_TABLE_NAME);

            miniHBaseCluster.killRegionServer(dataTableRegionServerName);
            miniHBaseCluster.waitForRegionServerToStop(dataTableRegionServerName, TIMEOUT);

            //there are only one regionServer now.
            assertEquals("miniHBaseCluster.getLiveRegionServerThreads()", miniHBaseCluster.getLiveRegionServerThreads().size(),1);
            HRegionServer liveRegionServer=miniHBaseCluster.getLiveRegionServerThreads().get(0).getRegionServer();

            //verify handleFailure is called.
            handleFailureCountDownLatch.await();
            assertTrue(handleFailureCalledCount==1);
            Map<HTableInterfaceReference, Collection<Mutation>> tableReferenceToMutations=tableReferenceToMutation.asMap();
            assertEquals("tableReferenceToMutation.size()", 1, tableReferenceToMutations.size());
            Iterator<Map.Entry<HTableInterfaceReference, Collection<Mutation>>> iter=tableReferenceToMutations.entrySet().iterator();
            assertTrue(iter.hasNext());
            Map.Entry<HTableInterfaceReference, Collection<Mutation>> entry=iter.next();
            assertTrue(entry.getKey().getTableName().equals(INDEX_TABLE_NAME));
            Mutation[] mutations=entry.getValue().toArray(new Mutation[0]);
            assertEquals("mutations size "+mutations[0], 1, mutations.length);
            assertTrue(mutations[0] instanceof Put);
            assertTrue(!Arrays.equals(mutations[0].getRow(),Bytes.toBytes("a")));

            //wait for data table region repoen.
            List<HRegion> dataTableRegions=null;

            for(int i=1;i<=200;i++) {
                dataTableRegions=liveRegionServer.getRegions(TableName.valueOf(DATA_TABLE_NAME));
                if(dataTableRegions.size() > 0) {
                    break;
                }
                Thread.sleep(ONE_SEC);
            }

            dataTableRegions=liveRegionServer.getRegions(TableName.valueOf(DATA_TABLE_NAME));
            assertTrue(dataTableRegions.size()==1);


            // the index table is one row
            Table indexTable = hbaseConn.getTable(TableName.valueOf(INDEX_TABLE_NAME));
            resultScanner = indexTable.getScanner(scan);
            count = 0;
            for (Result result : resultScanner) {
                count++;
            }
            assertEquals("Got an unexpected found of index rows", 1, count);
            resultScanner.close();
            indexTable.close();

            scan = new Scan();
            primaryTable.close();
            primaryTable = hbaseConn.getTable(TableName.valueOf(DATA_TABLE_NAME));
            ((ClusterConnection)hbaseConn).clearRegionLocationCache();
            resultScanner = primaryTable.getScanner(scan);
            count = 0;
            for (Result result : resultScanner) {
                LOGGER.info("Got data table result:" + result);
                count++;
            }
            assertEquals("Got an unexpected found of data rows", 1, count);

            // cleanup
            primaryTable.close();
        }
    }

    private ServerName getRegionServerName(MiniHBaseCluster miniHBaseCluster,String tableName) throws IOException {
        List<HRegion> regions = miniHBaseCluster.getRegions(Bytes.toBytes(tableName));
        assertEquals(1, regions.size());
        HRegion region=regions.get(0);
        return miniHBaseCluster.getServerHoldingRegion(TableName.valueOf(tableName),region.getRegionInfo().getRegionName());
    }

    private void assertRegionServerDifferent(MiniHBaseCluster miniHBaseCluster) throws IOException {
        ServerName dataTableRegionServerName=
                this.getRegionServerName(miniHBaseCluster, DATA_TABLE_NAME);
        ServerName indexTableRegionServerName=
                this.getRegionServerName(miniHBaseCluster, INDEX_TABLE_NAME);
        assertTrue(!dataTableRegionServerName.equals(indexTableRegionServerName));
    }

    private void moveIndexTableRegionIfSameRegionSErver(MiniHBaseCluster miniHBaseCluster) throws IOException, InterruptedException {
        List<HRegion> dataTableRegions = miniHBaseCluster.getRegions(Bytes.toBytes(DATA_TABLE_NAME));
        assertEquals(1, dataTableRegions.size());
        List<HRegion> indexTableRegions = miniHBaseCluster.getRegions(Bytes.toBytes(INDEX_TABLE_NAME));
        assertEquals(1, indexTableRegions.size());

        HRegion dataTableRegion=dataTableRegions.get(0);
        HRegion indexTableRegion=indexTableRegions.get(0);
        int dataTableRegionServerIndex = miniHBaseCluster.getServerWith(dataTableRegion.getRegionInfo().getRegionName());
        int indexTableRegionServerIndex=miniHBaseCluster.getServerWith(indexTableRegion.getRegionInfo().getRegionName());
        if(dataTableRegionServerIndex != indexTableRegionServerIndex) {
            return;
        }


        int newRegionServerIndex=0;
        while(newRegionServerIndex == indexTableRegionServerIndex) {
            newRegionServerIndex++;
        }

        HRegionServer newRegionServer = miniHBaseCluster.getRegionServer(newRegionServerIndex);
        this.moveRegionAndWait(miniHBaseCluster,indexTableRegion, newRegionServer);
    }


    private void moveRegionAndWait(MiniHBaseCluster miniHBaseCluster,HRegion destRegion, HRegionServer destRegionServer) throws IOException, InterruptedException {
        HMaster master = miniHBaseCluster.getMaster();
        getUtility().getAdmin().move(
                destRegion.getRegionInfo().getEncodedNameAsBytes(),
                destRegionServer.getServerName());
        while (true) {
            ServerName currentRegionServerName =
                    master.getAssignmentManager().getRegionStates().getRegionServerOfRegion(destRegion.getRegionInfo());
            if (currentRegionServerName != null && currentRegionServerName.equals(destRegionServer.getServerName())) {
                getUtility().assertRegionOnServer(
                        destRegion.getRegionInfo(), currentRegionServerName, 200);
                break;
            }
            Thread.sleep(10);
        }
    }
}
