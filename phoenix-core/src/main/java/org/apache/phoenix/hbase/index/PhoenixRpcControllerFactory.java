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
import org.apache.phoenix.hbase.index.ipc.PhoenixRpcSchedulerFactory;
import org.apache.phoenix.util.SchemaUtil;

/**
 * {@link RpcControllerFactory} that overrides the standard {@link PayloadCarryingRpcController} to
 * allow the configured index tables (via {@link #INDEX_TABLE_NAMES_KEY}) to use the Index priority.
 */
public class PhoenixRpcControllerFactory extends RpcControllerFactory {

    public static final String INDEX_TABLE_NAMES_KEY = "phoenix.index.rpc.controller.index-tables";

    public PhoenixRpcControllerFactory(Configuration conf) {
        super(conf);
    }

    @Override
    public PayloadCarryingRpcController newController() {
        PayloadCarryingRpcController delegate = super.newController();
        return new PhoenixRpcController(delegate, conf);
    }

    @Override
    public PayloadCarryingRpcController newController(CellScanner cellScanner) {
        PayloadCarryingRpcController delegate = super.newController(cellScanner);
        return new PhoenixRpcController(delegate, conf);
    }

    @Override
    public PayloadCarryingRpcController newController(List<CellScannable> cellIterables) {
        PayloadCarryingRpcController delegate = super.newController(cellIterables);
        return new PhoenixRpcController(delegate, conf);
    }

    private class PhoenixRpcController extends DelegatingPayloadCarryingRpcController {

        private int indexPriority;
        private int metadataPriority;

        public PhoenixRpcController(PayloadCarryingRpcController delegate, Configuration conf) {
            super(delegate);
            this.indexPriority = PhoenixRpcSchedulerFactory.getIndexMinPriority(conf);
            this.metadataPriority = PhoenixRpcSchedulerFactory.getMetadataMinPriority(conf);
        }
        @Override
        public void setPriority(final TableName tn) {
            // this is function is called for hbase system tables, phoenix system tables and index tables 
            if (SchemaUtil.isSystemDataTable(tn.getNameAsString())) {
                setPriority(this.metadataPriority);
            } 
            else if (!tn.isSystemTable()) {
                setPriority(this.indexPriority);
            } 
            else {
                super.setPriority(tn);
            }
        }

    }
}