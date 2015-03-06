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
package org.apache.phoenix.query;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.phoenix.job.JobManager;
import org.apache.phoenix.memory.GlobalMemoryManager;
import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.optimize.QueryOptimizer;
import org.apache.phoenix.util.ReadOnlyProps;



/**
 * 
 * Base class for QueryService implementors.
 *
 * 
 * @since 0.1
 */
public abstract class BaseQueryServicesImpl implements QueryServices {
    private final ThreadPoolExecutor executor;
    private final MemoryManager memoryManager;
    private final ReadOnlyProps props;
    private final QueryOptimizer queryOptimizer;
    
    public BaseQueryServicesImpl(ReadOnlyProps defaultProps, QueryServicesOptions options) {
        this.executor =  JobManager.createThreadPoolExec(
                options.getKeepAliveMs(), 
                options.getThreadPoolSize(), 
                options.getQueueSize(),
                options.isMetricsEnabled());
        this.memoryManager = new GlobalMemoryManager(
                Runtime.getRuntime().maxMemory() * options.getMaxMemoryPerc() / 100,
                options.getMaxMemoryWaitMs());
        this.props = options.getProps(defaultProps);
        this.queryOptimizer = new QueryOptimizer(this);
    }
    
    @Override
    public ThreadPoolExecutor getExecutor() {
        return executor;
    }

    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public final ReadOnlyProps getProps() {
        return props;
    }

    @Override
    public void close() {
        // Do not shutdown the executor as it prevents the Driver from being able
        // to attempt to open a connection in the future.
    }

    @Override
    public QueryOptimizer getOptimizer() {
        return queryOptimizer;
    }   
}
