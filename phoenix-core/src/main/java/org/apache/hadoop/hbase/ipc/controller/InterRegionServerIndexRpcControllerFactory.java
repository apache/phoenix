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
package org.apache.hadoop.hbase.ipc.controller;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.phoenix.compat.hbase.CompatHBaseRpcController;
import org.apache.phoenix.compat.hbase.CompatRpcControllerFactory;

/**

 * RpcControllerFactory that should only be used when creating HTable for
 * making remote RPCs to the region servers hosting global mutable index table regions.
 * This controller factory shouldn't be globally configured anywhere and is meant to be used
 * only internally by Phoenix indexing code.
 */
public class InterRegionServerIndexRpcControllerFactory extends CompatRpcControllerFactory {

    public InterRegionServerIndexRpcControllerFactory(Configuration conf) {
        super(conf);
    }

    @Override
    public CompatHBaseRpcController newController() {
        CompatHBaseRpcController delegate = super.newController();
        return getController(delegate);
    }

    @Override
    public CompatHBaseRpcController newController(CellScanner cellScanner) {
        CompatHBaseRpcController delegate = super.newController(cellScanner);
        return getController(delegate);
    }

    @Override
    public CompatHBaseRpcController newController(List<CellScannable> cellIterables) {
        CompatHBaseRpcController delegate = super.newController(cellIterables);
        return getController(delegate);
    }

    private CompatHBaseRpcController getController(CompatHBaseRpcController delegate) {
        // construct a chain of controllers: metadata, index and standard controller
        CompatHBaseRpcController indexRpcController = new IndexRpcController(delegate, conf);
        return new MetadataRpcController(indexRpcController, conf);
    }

}
