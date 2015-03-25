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
package org.apache.hadoop.hbase.ipc.controller;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

/**
 * {@link RpcControllerFactory} that sets the priority of metadata rpc calls to be processed
 * in its own queue.
 */
public class ClientRpcControllerFactory extends RpcControllerFactory {

    public ClientRpcControllerFactory(Configuration conf) {
        super(conf);
    }

    @Override
    public PayloadCarryingRpcController newController() {
        PayloadCarryingRpcController delegate = super.newController();
        return getController(delegate);
    }

    @Override
    public PayloadCarryingRpcController newController(CellScanner cellScanner) {
        PayloadCarryingRpcController delegate = super.newController(cellScanner);
        return getController(delegate);
    }

    @Override
    public PayloadCarryingRpcController newController(List<CellScannable> cellIterables) {
        PayloadCarryingRpcController delegate = super.newController(cellIterables);
        return getController(delegate);
    }
    
    private PayloadCarryingRpcController getController(PayloadCarryingRpcController delegate) {
		return new MetadataRpcController(delegate, conf);
    }
    
}