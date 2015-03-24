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
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.PhoenixIndexRpcScheduler;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;

import com.google.common.base.Preconditions;

/**
 * Factory to create a {@link PhoenixIndexRpcScheduler}. In this package so we can access the
 * {@link SimpleRpcSchedulerFactory}.
 */
public class PhoenixIndexRpcSchedulerFactory implements RpcSchedulerFactory {

    private static final Log LOG = LogFactory.getLog(PhoenixIndexRpcSchedulerFactory.class);

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

        int indexHandlerCount = conf.getInt(QueryServices.INDEX_HANDLER_COUNT_ATTRIB, QueryServicesOptions.DEFAULT_INDEX_HANDLER_COUNT);
        int minPriority = getMinPriority(conf);
        int maxPriority = conf.getInt(QueryServices.MAX_INDEX_PRIOIRTY_ATTRIB, QueryServicesOptions.DEFAULT_INDEX_MAX_PRIORITY);
        // make sure the ranges are outside the warning ranges
        Preconditions.checkArgument(maxPriority > minPriority, "Max index priority (" + maxPriority
                + ") must be larger than min priority (" + minPriority + ")");
        boolean allSmaller =
                minPriority < HConstants.REPLICATION_QOS
                        && maxPriority < HConstants.REPLICATION_QOS;
        boolean allLarger = minPriority > HConstants.HIGH_QOS;
        Preconditions.checkArgument(allSmaller || allLarger, "Index priority range (" + minPriority
                + ",  " + maxPriority + ") must be outside HBase priority range ("
                + HConstants.REPLICATION_QOS + ", " + HConstants.HIGH_QOS + ")");

        LOG.info("Using custom Phoenix Index RPC Handling with " + indexHandlerCount
                + " handlers and priority range [" + minPriority + ", " + maxPriority + ")");

        PhoenixIndexRpcScheduler scheduler =
                new PhoenixIndexRpcScheduler(indexHandlerCount, conf, delegate, minPriority,
                        maxPriority);
        return scheduler;
    }

    @Override
    public RpcScheduler create(Configuration configuration, PriorityFunction priorityFunction) {
        return create(configuration, priorityFunction, null);
    }

    public static int getMinPriority(Configuration conf) {
        return conf.getInt(QueryServices.MIN_INDEX_PRIOIRTY_ATTRIB, QueryServicesOptions.DEFAULT_INDEX_MIN_PRIORITY);
    }
}