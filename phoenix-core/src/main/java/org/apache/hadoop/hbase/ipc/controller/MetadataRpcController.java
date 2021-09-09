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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.DelegatingHBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.RpcController;

/**
 * {@link RpcController} that sets the appropriate priority of RPC calls destined for Phoenix SYSTEM
 * tables
 */
class MetadataRpcController extends DelegatingHBaseRpcController {

	private int priority;
	// list of system tables
	private static final List<String> SYSTEM_TABLE_NAMES = new ImmutableList.Builder<String>()
			.add(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME)
			.add(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME)
			.add(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME)
			.add(PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME)
            .add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, true)
                    .getNameAsString())
            .add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_STATS_NAME_BYTES, true)
                    .getNameAsString())
            .add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_SEQUENCE_NAME_BYTES, true)
                    .getNameAsString())
            .add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_FUNCTION_NAME_BYTES, true)
                    .getNameAsString())
            .build();

	public MetadataRpcController(HBaseRpcController delegate,
			Configuration conf) {
		super(delegate);
		this.priority = PhoenixRpcSchedulerFactory.getMetadataPriority(conf);
	}

	@Override
	public void setPriority(final TableName tn) {
		if (SYSTEM_TABLE_NAMES.contains(tn.getNameAsString())) {
			setPriority(this.priority);
		} else {
			super.setPriority(tn);
		}
	}

}
