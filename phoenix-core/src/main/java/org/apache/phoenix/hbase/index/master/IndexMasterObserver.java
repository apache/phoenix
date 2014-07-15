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
package org.apache.phoenix.hbase.index.master;

import java.io.IOException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.phoenix.hbase.index.balancer.IndexLoadBalancer;

/**
 * Defines of coprocessor hooks(to support secondary indexing) of operations on
 * {@link org.apache.hadoop.hbase.master.HMaster} process.
 */
public class IndexMasterObserver extends BaseMasterObserver {
    IndexLoadBalancer balancer = null;

    @Override
    public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
        LoadBalancer loadBalancer =
                ctx.getEnvironment().getMasterServices().getAssignmentManager().getBalancer();
        if (loadBalancer instanceof IndexLoadBalancer) {
            balancer = (IndexLoadBalancer) loadBalancer;
        }
        super.preMasterInitialization(ctx);
    }

    @Override
    public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
            HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
        TableName userTableName = null;
        if (balancer != null && desc.getValue(IndexLoadBalancer.PARENT_TABLE_KEY) != null) {
            userTableName =
                    TableName.valueOf(desc.getValue(IndexLoadBalancer.PARENT_TABLE_KEY));
            balancer.addTablesToColocate(userTableName, desc.getTableName());
        }
        if (userTableName != null) balancer.populateRegionLocations(userTableName);
        super.preCreateTableHandler(ctx, desc, regions);
    }

    @Override
    public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
            TableName tableName) throws IOException {
        if (balancer != null && balancer.isTableColocated(tableName)) {
            balancer.removeTablesFromColocation(tableName);
        }
    }
}
