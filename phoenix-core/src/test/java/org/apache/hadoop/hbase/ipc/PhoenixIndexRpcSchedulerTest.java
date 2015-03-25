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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.RpcScheduler.Context;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test that the rpc scheduler schedules index writes to the index handler queue and sends
 * everything else to the standard queues
 */
public class PhoenixIndexRpcSchedulerTest {

    private static final Configuration conf = HBaseConfiguration.create();
    private static final InetSocketAddress isa = new InetSocketAddress("localhost", 0);

    @Test
    public void testIndexPriorityWritesToIndexHandler() throws Exception {
        RpcScheduler mock = Mockito.mock(RpcScheduler.class);

        PhoenixRpcScheduler scheduler = new PhoenixRpcScheduler(conf, mock, 200, 250);
        BalancedQueueRpcExecutor executor = new BalancedQueueRpcExecutor("test-queue", 1, 1, 1);
        scheduler.setIndexExecutorForTesting(executor);
        dispatchCallWithPriority(scheduler, 200);
        List<BlockingQueue<CallRunner>> queues = executor.getQueues();
        assertEquals(1, queues.size());
        BlockingQueue<CallRunner> queue = queues.get(0);
        queue.poll(20, TimeUnit.SECONDS);

        // try again, this time we tweak the ranges we support
        scheduler = new PhoenixRpcScheduler(conf, mock, 101, 110);
        scheduler.setIndexExecutorForTesting(executor);
        dispatchCallWithPriority(scheduler, 101);
        queue.poll(20, TimeUnit.SECONDS);

        Mockito.verify(mock, Mockito.times(2)).init(Mockito.any(Context.class));
        Mockito.verifyNoMoreInteractions(mock);
    }

    /**
     * Test that we delegate to the passed {@link RpcScheduler} when the call priority is outside
     * the index range
     * @throws Exception
     */
    @Test
    public void testDelegateWhenOutsideRange() throws Exception {
        RpcScheduler mock = Mockito.mock(RpcScheduler.class);
        PhoenixRpcScheduler scheduler = new PhoenixRpcScheduler(conf, mock, 200, 250);
        dispatchCallWithPriority(scheduler, 100);
        dispatchCallWithPriority(scheduler, 251);

        // try again, this time we tweak the ranges we support
        scheduler = new PhoenixRpcScheduler(conf, mock, 101, 110);
        dispatchCallWithPriority(scheduler, 200);
        dispatchCallWithPriority(scheduler, 111);

        Mockito.verify(mock, Mockito.times(4)).init(Mockito.any(Context.class));
        Mockito.verify(mock, Mockito.times(4)).dispatch(Mockito.any(CallRunner.class));
        Mockito.verifyNoMoreInteractions(mock);
    }

    private void dispatchCallWithPriority(RpcScheduler scheduler, int priority) throws Exception {
        CallRunner task = Mockito.mock(CallRunner.class);
        RequestHeader header = RequestHeader.newBuilder().setPriority(priority).build();
        RpcServer server = new RpcServer(null, "test-rpcserver", null, isa, conf, scheduler);
        RpcServer.Call call =
                server.new Call(0, null, null, header, null, null, null, null, 10, null);
        Mockito.when(task.getCall()).thenReturn(call);

        scheduler.dispatch(task);

        Mockito.verify(task).getCall();
        Mockito.verifyNoMoreInteractions(task);
    }
}