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
package org.apache.phoenix.hbase.index.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.PhoenixRpcScheduler;
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
    public RpcScheduler create(Configuration conf, RegionServerServices services) {
        // create the delegate scheduler
        RpcScheduler delegate;
        try {
            // happens in <=0.98.4 where the scheduler factory is not visible
            delegate = new SimpleRpcSchedulerFactory().create(conf, services);
        } catch (IllegalAccessError e) {
            LOG.fatal(VERSION_TOO_OLD_FOR_INDEX_RPC);
            throw e;
        }
        try {
            // make sure we are on a version that phoenix can support
            Class.forName("org.apache.hadoop.hbase.ipc.RpcExecutor");
        } catch (ClassNotFoundException e) {
            LOG.error(VERSION_TOO_OLD_FOR_INDEX_RPC
                    + " Instead, using falling back to Simple RPC scheduling.");
            return delegate;
        }

        // get the index priority configs
        int indexHandlerCount = conf.getInt(QueryServices.INDEX_HANDLER_COUNT_ATTRIB, QueryServicesOptions.DEFAULT_INDEX_HANDLER_COUNT);
        int indexMinPriority = getIndexMinPriority(conf);
        int indexMaxPriority = conf.getInt(QueryServices.MAX_INDEX_PRIOIRTY_ATTRIB, QueryServicesOptions.DEFAULT_INDEX_MAX_PRIORITY);
        validatePriority(indexMinPriority, indexMaxPriority);
        
        // get the metadata priority configs
        int metadataHandlerCount = conf.getInt(QueryServices.INDEX_HANDLER_COUNT_ATTRIB, QueryServicesOptions.DEFAULT_METADATA_HANDLER_COUNT);
        int metadataMinPriority = getMetadataMinPriority(conf);
        int metadataMaxPriority = conf.getInt(QueryServices.MAX_INDEX_PRIOIRTY_ATTRIB, QueryServicesOptions.DEFAULT_METADATA_MAX_PRIORITY);
        validatePriority(indexMinPriority, indexMaxPriority);
        
        //validate index and metadata priorities do not overlap
        Preconditions.checkArgument(doesNotOverlap(indexMinPriority, indexMaxPriority, metadataMinPriority, metadataMaxPriority), "Priority ranges ("
                + indexMinPriority + ",  " + indexMaxPriority + ") and  (" + metadataMinPriority + ", " + metadataMaxPriority
                + ") must not overlap");

        LOG.info("Using custom Phoenix Index RPC Handling with " + indexHandlerCount
                + " index handlers and priority range [" + indexMinPriority + ", " + indexMaxPriority + " and " + metadataHandlerCount
                + " metadata handlers and priority range [" + metadataMinPriority + ", " + metadataMaxPriority + ")");

        PhoenixRpcScheduler scheduler =
                new PhoenixRpcScheduler(indexHandlerCount, metadataHandlerCount, 
                        conf, delegate, indexMinPriority, metadataMinPriority);
        return scheduler;
    }

    /**
     * Validates that the given priority range does not overlap with the HBase priority range
     */
    private void validatePriority(int minPriority, int maxPriority) {
        Preconditions.checkArgument(maxPriority > minPriority, "Max index priority (" + maxPriority
                + ") must be larger than min priority (" + minPriority + ")");
        Preconditions.checkArgument(
                doesNotOverlap(minPriority, maxPriority, HConstants.NORMAL_QOS, HConstants.HIGH_QOS),
                "Index priority range (" + minPriority + ",  " + maxPriority
                        + ") must be outside HBase priority range (" + HConstants.NORMAL_QOS + ", "
                        + HConstants.HIGH_QOS + ")");
    }

    /**
     * Returns true if the two priority ranges overlap
     */
    private boolean doesNotOverlap(int minPriority1, int maxPriority1, int minPriority2, int maxPriority2) {
        return minPriority1 > maxPriority2 || maxPriority1 < minPriority2;
    }

    public static int getIndexMinPriority(Configuration conf) {
        return conf.getInt(QueryServices.MIN_INDEX_PRIOIRTY_ATTRIB, QueryServicesOptions.DEFAULT_INDEX_MIN_PRIORITY);
    }
    
    public static int getMetadataMinPriority(Configuration conf) {
        return conf.getInt(QueryServices.MIN_METADATA_PRIOIRTY_ATTRIB, QueryServicesOptions.DEFAULT_METADATA_MIN_PRIORITY);
    }
    
}