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
package org.apache.phoenix.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ipc.BalancedQueueRpcExecutor;
import org.apache.hadoop.hbase.ipc.PhoenixRpcScheduler;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcExecutor;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.mockito.Mockito;

public class TestPhoenixIndexRpcSchedulerFactory extends PhoenixRpcSchedulerFactory {
    
    private static RpcExecutor indexRpcExecutor = Mockito.spy(new BalancedQueueRpcExecutor("test-index-queue", 30, 1,
            300));
    private static RpcExecutor metadataRpcExecutor = Mockito.spy(new BalancedQueueRpcExecutor("test-metataqueue", 30,
            1, 300));

    @Override
    public RpcScheduler create(Configuration conf, PriorityFunction priorityFunction, Abortable abortable) {
        PhoenixRpcScheduler phoenixIndexRpcScheduler = (PhoenixRpcScheduler)super.create(conf, priorityFunction, abortable);
        phoenixIndexRpcScheduler.setIndexExecutorForTesting(indexRpcExecutor);
        phoenixIndexRpcScheduler.setMetadataExecutorForTesting(metadataRpcExecutor);
        return phoenixIndexRpcScheduler;
    }
    
    @Override
    public RpcScheduler create(Configuration configuration, PriorityFunction priorityFunction) {
        return create(configuration, priorityFunction, null);
    }
    
    public static RpcExecutor getIndexRpcExecutor() {
        return indexRpcExecutor;
    }
    
    public static RpcExecutor getMetadataRpcExecutor() {
        return metadataRpcExecutor;
    }
    
    public static void reset() {
        Mockito.reset(metadataRpcExecutor);
        Mockito.reset(indexRpcExecutor);
    }
}


