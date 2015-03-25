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
package org.apache.hadoop.hbase.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;

import com.google.common.base.Preconditions;

/**
 * Factory to create a {@link PhoenixRpcScheduler}. In this package so we can access the
 * {@link SimpleRpcSchedulerFactory}.
 */
public class PhoenixRpcSchedulerFactory implements RpcSchedulerFactory {

    private static final Log LOG = LogFactory.getLog(PhoenixRpcSchedulerFactory.class);

    private static final String VERSION_TOO_OLD_FOR_INDEX_RPC =
            "Running an older version of HBase (less than 0.98.4), Phoenix index RPC handling cannot be enabled.";

    @Override
    public RpcScheduler create(Configuration conf, PriorityFunction priorityFunction, Abortable abortable) {
        // create the delegate scheduler
        RpcScheduler delegate;
        try {
            // happens in <=0.98.4 where the scheduler factory is not visible
            delegate = new SimpleRpcSchedulerFactory().create(conf, priorityFunction, abortable);
        } catch (IllegalAccessError e) {
            LOG.fatal(VERSION_TOO_OLD_FOR_INDEX_RPC);
            throw e;
        }

        // get the index priority configs
        int indexPriority = getIndexPriority(conf);
        validatePriority(indexPriority);
        // get the metadata priority configs
        int metadataPriority = getMetadataPriority(conf);
        validatePriority(metadataPriority);

        // validate index and metadata priorities are not the same
        Preconditions.checkArgument(indexPriority != metadataPriority, "Index and Metadata priority must not be same "+ indexPriority);
        LOG.info("Using custom Phoenix Index RPC Handling with index rpc priority " + indexPriority + " and metadata rpc priority " + metadataPriority);

        PhoenixRpcScheduler scheduler =
                new PhoenixRpcScheduler(conf, delegate, indexPriority, metadataPriority);
        return scheduler;
    }

    @Override
    public RpcScheduler create(Configuration configuration, PriorityFunction priorityFunction) {
        return create(configuration, priorityFunction, null);
    }

    /**
     * Validates that the given priority does not overlap with the HBase priority range
     */
    private void validatePriority(int priority) {
        Preconditions.checkArgument( priority < HConstants.NORMAL_QOS || priority > HConstants.HIGH_QOS, "priority cannot be within hbase priority range " 
        			+ HConstants.NORMAL_QOS +" to " + HConstants.HIGH_QOS ); 
    }

    public static int getIndexPriority(Configuration conf) {
        return conf.getInt(QueryServices.INDEX_PRIOIRTY_ATTRIB, QueryServicesOptions.DEFAULT_INDEX_PRIORITY);
    }
    
    public static int getMetadataPriority(Configuration conf) {
        return conf.getInt(QueryServices.METADATA_PRIOIRTY_ATTRIB, QueryServicesOptions.DEFAULT_METADATA_PRIORITY);
    }
    
}