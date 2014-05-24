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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * {@link RpcScheduler} that first checks to see if this is an index update before passing off the
 * call to the delegate {@link RpcScheduler}.
 * <p>
 * We reserve the range (200, 250], by default (though it is configurable), for index priority
 * writes. Currently, we don't do any prioritization within that range - all index writes are
 * treated with the same priority and put into the same queue.
 */
public class PhoenixIndexRpcScheduler implements RpcScheduler {

    private LinkedBlockingQueue<CallRunner> indexCallQueue;
    private RpcScheduler delegate;
    private final int handlerCount;
    private volatile boolean running;
    private int port;
    private final List<Thread> handlers = Lists.newArrayList();
    private int minPriority;
    private int maxPriority;

    public PhoenixIndexRpcScheduler(int indexHandlerCount, Configuration conf,
            RpcScheduler delegate, int minPriority, int maxPriority) {
        int maxQueueLength =
                conf.getInt("ipc.server.max.callqueue.length", indexHandlerCount
                        * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
        this.minPriority = minPriority;
        this.maxPriority = maxPriority;

        this.indexCallQueue = new LinkedBlockingQueue<CallRunner>(maxQueueLength);
        this.handlerCount = indexHandlerCount;
        this.delegate = delegate;
    }

    @Override
    public void init(Context context) {
        delegate.init(context);
        this.port = context.getListenerAddress().getPort();
    }

    @Override
    public void start() {
        delegate.start();
        running = true;
        startHandlers(handlerCount, indexCallQueue, "PhoenixIndexing.");
    }

    @Override
    public void stop() {
        running = false;
        for (Thread handler : handlers) {
            handler.interrupt();
        }
        delegate.stop();
    }

    @Override
    public void dispatch(CallRunner callTask) throws InterruptedException, IOException {
        RpcServer.Call call = callTask.getCall();
        int priority = call.header.getPriority();
        if (minPriority <= priority && priority < maxPriority) {
            indexCallQueue.put(callTask);
        } else {
            delegate.dispatch(callTask);
        }
    }

    @Override
    public int getGeneralQueueLength() {
        // not the best way to calculate, but don't have a better way to hook
        // into metrics at the moment
        return this.delegate.getGeneralQueueLength() + indexCallQueue.size();
    }

    @Override
    public int getPriorityQueueLength() {
        return this.delegate.getPriorityQueueLength();
    }

    @Override
    public int getReplicationQueueLength() {
        return this.delegate.getReplicationQueueLength();
    }

    // ****************************************************
    // Below copied from SimpleRpcScheduler for visibility
    // *****************************************************
    private void startHandlers(int handlerCount, final BlockingQueue<CallRunner> callQueue,
            String threadNamePrefix) {
        for (int i = 0; i < handlerCount; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    consumerLoop(callQueue);
                }
            });
            t.setDaemon(true);
            t.setName(Strings.nullToEmpty(threadNamePrefix) + "RpcServer.handler=" + i + ",port="
                    + port);
            t.start();
            handlers.add(t);
        }
    }

    private void consumerLoop(BlockingQueue<CallRunner> myQueue) {
        boolean interrupted = false;
        try {
            while (running) {
                try {
                    CallRunner task = myQueue.take();
                    task.run();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @VisibleForTesting
    public void setIndexCallQueueForTesting(LinkedBlockingQueue<CallRunner> indexQueue) {
        this.indexCallQueue = indexQueue;
    }
}
