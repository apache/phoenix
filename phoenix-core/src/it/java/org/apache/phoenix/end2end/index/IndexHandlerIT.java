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
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.DelegatingPayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.controller.ServerRpcControllerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.TableName;
import org.apache.phoenix.query.QueryServicesOptions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Comprehensive test that ensures we are adding custom index handlers
 */
public class IndexHandlerIT {

    public static class CountingIndexClientRpcFactory extends RpcControllerFactory {

        private ServerRpcControllerFactory delegate;

        public CountingIndexClientRpcFactory(Configuration conf) {
            super(conf);
            this.delegate = new ServerRpcControllerFactory(conf);
        }

        @Override
        public PayloadCarryingRpcController newController() {
            PayloadCarryingRpcController controller = delegate.newController();
            return new CountingIndexClientRpcController(controller);
        }

        @Override
        public PayloadCarryingRpcController newController(CellScanner cellScanner) {
            PayloadCarryingRpcController controller = delegate.newController(cellScanner);
            return new CountingIndexClientRpcController(controller);
        }

        @Override
        public PayloadCarryingRpcController newController(List<CellScannable> cellIterables) {
            PayloadCarryingRpcController controller = delegate.newController(cellIterables);
            return new CountingIndexClientRpcController(controller);
        }
    }

    public static class CountingIndexClientRpcController extends
            DelegatingPayloadCarryingRpcController {

        private static Map<Integer, Integer> priorityCounts = new HashMap<Integer, Integer>();

        public CountingIndexClientRpcController(PayloadCarryingRpcController delegate) {
            super(delegate);
        }

        @Override
        public void setPriority(int pri) {
            Integer count = priorityCounts.get(pri);
            if (count == 0) {
                count = new Integer(0);
            }
            count = count.intValue() + 1;
            priorityCounts.put(pri, count);

        }
    }

    private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

    private static final byte[] row = Bytes.toBytes("row");
    private static final byte[] family = Bytes.toBytes("FAM");
    private static final byte[] qual = Bytes.toBytes("qual");
    private static final HColumnDescriptor FAM1 = new HColumnDescriptor(family);

    @Rule
    public TableName TestTable = new TableName();

    @BeforeClass
    public static void setupCluster() throws Exception {
        UTIL.startMiniCluster();
    }

    @AfterClass
    public static void shutdownCluster() throws Exception {
        UTIL.shutdownMiniCluster();
    }

    @Before
    public void setup() throws Exception {
        HTableDescriptor desc =
                new HTableDescriptor(org.apache.hadoop.hbase.TableName.valueOf(TestTable
                        .getTableNameString()));
        desc.addFamily(FAM1);

        // create the table
        HBaseAdmin admin = UTIL.getHBaseAdmin();
        admin.createTable(desc);
    }

    @After
    public void cleanup() throws Exception {
        HBaseAdmin admin = UTIL.getHBaseAdmin();
        admin.disableTable(TestTable.getTableName());
        admin.deleteTable(TestTable.getTableName());
    }

    @Test
    public void testClientWritesWithPriority() throws Exception {
        Configuration conf = new Configuration(UTIL.getConfiguration());
        // add the keys for our rpc factory
        conf.set(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY,
            CountingIndexClientRpcFactory.class.getName());
        // and set the index table as the current table
//        conf.setStrings(PhoenixRpcControllerFactory.INDEX_TABLE_NAMES_KEY,
//            TestTable.getTableNameString());
        HTable table = new HTable(conf, TestTable.getTableName());

        // do a write to the table
        Put p = new Put(row);
        p.add(family, qual, new byte[] { 1, 0, 1, 0 });
        table.put(p);
        table.flushCommits();

        // check the counts on the rpc controller
        assertEquals("Didn't get the expected number of index priority writes!", 1,
            (int) CountingIndexClientRpcController.priorityCounts
                    .get(QueryServicesOptions.DEFAULT_INDEX_PRIORITY));

        table.close();
    }
}