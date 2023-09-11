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

import com.google.protobuf.RpcController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.util.List;

/**
 * {@link RpcController} that sets the appropriate priority of server-server RPC calls destined
 * for Phoenix SYSTEM tables.
 */
public class ServerToServerRpcControllerImpl extends ServerRpcController implements
        ServerToServerRpcController {

    private int priority;
    // list of system tables that can possibly have server-server rpc's
    private static final List<String> SYSTEM_TABLE_NAMES = new ImmutableList.Builder<String>()
            .add(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME)
            .add(PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME)
            .add(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME)
            .add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, true)
                    .getNameAsString())
            .add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES, true)
                    .getNameAsString())
            .add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME_BYTES, true)
                    .getNameAsString())
            .build();

    public ServerToServerRpcControllerImpl(
            Configuration conf) {
        super();
        this.priority = IndexUtil.getServerSidePriority(conf);
    }

    @Override
    public void setPriority(final TableName tn) {
        if (SYSTEM_TABLE_NAMES.contains(tn.getNameAsString())) {
            setPriority(this.priority);
        }
    }


    @Override public void setPriority(int priority) {
        this.priority = priority;
    }


    @Override public int getPriority() {
        return this.priority;
    }
}
