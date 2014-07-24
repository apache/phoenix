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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

/**
 * {@link RpcScheduler} that first checks to see if this is an index update before passing off the
 * call to the delegate {@link RpcScheduler}.
 * <p>
 * We reserve the range (1000, 1050], by default (though it is configurable), for index priority
 * writes. Currently, we don't do any prioritization within that range - all index writes are
 * treated with the same priority and put into the same queue.
 */
public class PhoenixIndexRpcScheduler extends RpcScheduler {

    // copied from org.apache.hadoop.hbase.ipc.SimpleRpcScheduler in HBase 0.98.4
    public static final String CALL_QUEUE_READ_SHARE_CONF_KEY = "ipc.server.callqueue.read.share";
    public static final String CALL_QUEUE_HANDLER_FACTOR_CONF_KEY =
            "ipc.server.callqueue.handler.factor";
    private static final int DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER = 10;

    private RpcScheduler delegate;
    private int minPriority;
    private int maxPriority;
    private RpcExecutor callExecutor;

    public PhoenixIndexRpcScheduler(int indexHandlerCount, Configuration conf,
            RpcScheduler delegate, int minPriority, int maxPriority) {
        int maxQueueLength =
                conf.getInt("ipc.server.max.callqueue.length", indexHandlerCount
                        * DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);

        // copied from org.apache.hadoop.hbase.ipc.SimpleRpcScheduler in HBase 0.98.4
        float callQueuesHandlersFactor = conf.getFloat(CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 0);
        int numCallQueues =
                Math.max(1, Math.round(indexHandlerCount * callQueuesHandlersFactor));

        this.minPriority = minPriority;
        this.maxPriority = maxPriority;
        this.delegate = delegate;

        this.callExecutor =
                new BalancedQueueRpcExecutor("Index", indexHandlerCount, numCallQueues,
                        maxQueueLength);
    }

    @Override
    public void init(Context context) {
        delegate.init(context);
    }

    @Override
    public void start() {
        delegate.start();
    }

    @Override
    public void stop() {
        delegate.stop();
        callExecutor.stop();
    }

    @Override
    public void dispatch(CallRunner callTask) throws InterruptedException, IOException {
        RpcServer.Call call = callTask.getCall();
        int priority = call.header.getPriority();
        if (minPriority <= priority && priority < maxPriority) {
            callExecutor.dispatch(callTask);
        } else {
            delegate.dispatch(callTask);
        }
    }

    @Override
    public int getGeneralQueueLength() {
        // not the best way to calculate, but don't have a better way to hook
        // into metrics at the moment
        return this.delegate.getGeneralQueueLength() + this.callExecutor.getQueueLength();
    }

    @Override
    public int getPriorityQueueLength() {
        return this.delegate.getPriorityQueueLength();
    }

    @Override
    public int getReplicationQueueLength() {
        return this.delegate.getReplicationQueueLength();
    }

    @Override
    public int getActiveRpcHandlerCount() {
        return this.delegate.getActiveRpcHandlerCount() + this.callExecutor.getActiveHandlerCount();
    }

    @VisibleForTesting
    public void setExecutorForTesting(RpcExecutor executor) {
        this.callExecutor = executor;
    }
}