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

import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.RpcScheduler.Context;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Test that the rpc scheduler schedules index writes to the index handler queue and sends
 * everything else to the standard queues
 */
public class PhoenixIndexRpcSchedulerTest {

    private static final Configuration conf = HBaseConfiguration.create();
    private static final InetSocketAddress isa = new InetSocketAddress("localhost", 0);

    
    private class AbortServer implements Abortable {
      private boolean aborted = false;

      @Override
      public void abort(String why, Throwable e) {
        aborted = true;
      }

      @Override
      public boolean isAborted() {
        return aborted;
      }
    }

    @Test
    public void testIndexPriorityWritesToIndexHandler() throws Exception {
        RpcScheduler mock = Mockito.mock(RpcScheduler.class);
        PriorityFunction qosFunction = Mockito.mock(PriorityFunction.class);
        Abortable abortable = new AbortServer();
        PhoenixRpcScheduler scheduler = new PhoenixRpcScheduler(conf, mock, 200, 250, 225, qosFunction,abortable);
        BalancedQueueRpcExecutor executor = new BalancedQueueRpcExecutor("test-queue", 1, 1,qosFunction,conf,abortable);
        scheduler.setIndexExecutorForTesting(executor);
        dispatchCallWithPriority(scheduler, 200);
        List<BlockingQueue<CallRunner>> queues = executor.getQueues();
        assertEquals(1, queues.size());
        BlockingQueue<CallRunner> queue = queues.get(0);
        queue.poll(20, TimeUnit.SECONDS);

        // try again, this time we tweak the ranges we support
        scheduler = new PhoenixRpcScheduler(conf, mock, 101, 110, 105, qosFunction,abortable);
        scheduler.setIndexExecutorForTesting(executor);
        dispatchCallWithPriority(scheduler, 101);
        queue.poll(20, TimeUnit.SECONDS);

        Mockito.verify(mock, Mockito.times(2)).init(Mockito.any(Context.class));
        scheduler.stop();
        executor.stop();
    }

    @Test
    public void testServerSideRPCalls() throws Exception {
        RpcScheduler mock = Mockito.mock(RpcScheduler.class);
        PriorityFunction qosFunction = Mockito.mock(PriorityFunction.class);
        Abortable abortable = new AbortServer();
        PhoenixRpcScheduler scheduler1 = new PhoenixRpcScheduler(conf, mock, 200, 250, 100, qosFunction,abortable);
        RpcExecutor executor1  = scheduler1.getServerSideExecutorForTesting();
        for (int c = 0; c < 10; c++) {
            dispatchCallWithPriority(scheduler1, 100);
        }
        List<BlockingQueue<CallRunner>> queues1 = executor1.getQueues();
        int numDispatches1 = 0;
        for (BlockingQueue<CallRunner> queue1 : queues1) {
            if (queue1.size() > 0) {
                numDispatches1 += queue1.size();
                for (int i = 0; i < queue1.size(); i++) {
                    queue1.poll(20, TimeUnit.SECONDS);
                }
            }
        }
        assertEquals(10, numDispatches1);
        scheduler1.stop();

        // try again, with the incorrect executor
        PhoenixRpcScheduler scheduler2 = new PhoenixRpcScheduler(conf, mock, 101, 110, 50, qosFunction,abortable);
        RpcExecutor executor2  = scheduler2.getIndexExecutorForTesting();
        dispatchCallWithPriority(scheduler2, 50);
        List<BlockingQueue<CallRunner>> queues2 = executor2.getQueues();
        int numDispatches2 = 0;
        for (BlockingQueue<CallRunner> queue2 : queues2) {
            if (queue2.size() > 0) {
                numDispatches2++;
                queue2.poll(20, TimeUnit.SECONDS);
            }
        }
        assertEquals(0, numDispatches2);
        scheduler2.stop();

        Mockito.verify(mock, Mockito.times(numDispatches1+1)).init(Mockito.any(Context.class));
        //Verify no dispatches to the default delegate handler
        Mockito.verify(mock, Mockito.times(0)).dispatch(Mockito.any(CallRunner.class));
    }

    /**
     * Test that we delegate to the passed {@link RpcScheduler} when the call priority is outside
     * the index range
     * @throws Exception
     */
    @Test
    public void testDelegateWhenOutsideRange() throws Exception {
        PriorityFunction qosFunction = Mockito.mock(PriorityFunction.class);
        Abortable abortable = new AbortServer();
        RpcScheduler mock = Mockito.mock(RpcScheduler.class);
        PhoenixRpcScheduler scheduler = new PhoenixRpcScheduler(conf, mock, 200, 250, 225, qosFunction,abortable);
        dispatchCallWithPriority(scheduler, 100);
        dispatchCallWithPriority(scheduler, 251);

        // try again, this time we tweak the ranges we support
        scheduler = new PhoenixRpcScheduler(conf, mock, 101, 110, 105, qosFunction,abortable);
        dispatchCallWithPriority(scheduler, 200);
        dispatchCallWithPriority(scheduler, 111);

        Mockito.verify(mock, Mockito.times(4)).init(Mockito.any(Context.class));
        Mockito.verify(mock, Mockito.times(4)).dispatch(Mockito.any(CallRunner.class));
        scheduler.stop();
    }

    private void dispatchCallWithPriority(RpcScheduler scheduler, int priority) throws Exception {
        CallRunner task = Mockito.mock(CallRunner.class);
        RequestHeader header = RequestHeader.newBuilder().setPriority(priority).build();
        RpcServer server = RpcServerFactory.createRpcServer(null, "test-rpcserver", Lists.newArrayList(new BlockingServiceAndInterface(
                SERVICE, null)), isa, conf, scheduler);
        ServerCall call = Mockito.mock(ServerCall.class);
        when(call.getHeader()).thenReturn(header);
        when(call.getRequestUser()).thenReturn(Optional.empty());
        Mockito.when(task.getRpcCall()).thenReturn(call);

        scheduler.dispatch(task);

        Mockito.verify(task).getRpcCall();
        Mockito.verifyNoMoreInteractions(task);
        server.stop();
    }
}