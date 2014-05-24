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
package org.apache.phoenix.hbase.index;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.DelegatingPayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.PhoenixIndexRpcSchedulerFactory;

/**
 * {@link RpcControllerFactory} that overrides the standard {@link PayloadCarryingRpcController} to
 * allow the configured index tables (via {@link #INDEX_TABLE_NAMES_KEY}) to use the Index priority.
 */
public class IndexQosRpcControllerFactory extends RpcControllerFactory {

    public static final String INDEX_TABLE_NAMES_KEY = "phoenix.index.rpc.controller.index-tables";

    public IndexQosRpcControllerFactory(Configuration conf) {
        super(conf);
    }

    @Override
    public PayloadCarryingRpcController newController() {
        PayloadCarryingRpcController delegate = super.newController();
        return new IndexQosRpcController(delegate, conf);
    }

    @Override
    public PayloadCarryingRpcController newController(CellScanner cellScanner) {
        PayloadCarryingRpcController delegate = super.newController(cellScanner);
        return new IndexQosRpcController(delegate, conf);
    }

    @Override
    public PayloadCarryingRpcController newController(List<CellScannable> cellIterables) {
        PayloadCarryingRpcController delegate = super.newController(cellIterables);
        return new IndexQosRpcController(delegate, conf);
    }

    /**
     * @param tableName name of the index table
     * @return configuration key for if a table should have Index QOS writes (its a target index
     *         table)
     */
    public static String getTableIndexQosConfKey(String tableName) {
        return "phoenix.index.table.qos._" + tableName;
    }

    private class IndexQosRpcController extends DelegatingPayloadCarryingRpcController {

        private Configuration conf;
        private int priority;

        public IndexQosRpcController(PayloadCarryingRpcController delegate, Configuration conf) {
            super(delegate);
            this.conf = conf;
            this.priority = PhoenixIndexRpcSchedulerFactory.getMinPriority(conf);
        }

        @Override
        public void setPriority(final TableName tn) {
            // if its an index table, then we override to the index priority
            if (isIndexTable(tn)) {
                setPriority(this.priority);
            } else {
                super.setPriority(tn);
            }
        }

        private boolean isIndexTable(TableName tn) {
            return conf.get(getTableIndexQosConfKey(tn.getNameAsString())) == null;
        }
    }
}