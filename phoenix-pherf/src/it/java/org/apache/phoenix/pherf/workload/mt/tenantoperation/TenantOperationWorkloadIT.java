/*
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


package org.apache.phoenix.pherf.workload.mt.tenantoperation;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.WorkHandler;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.Workload;
import org.apache.phoenix.pherf.workload.mt.OperationStats;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationWorkload.TenantOperationEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TenantOperationWorkloadIT extends MultiTenantOperationBaseIT {

    private static class EventCountingWorkHandler implements
            WorkHandler<TenantOperationEvent>, LifecycleAware {
        private final String handlerId;
        private final TenantOperationFactory tenantOperationFactory;
        private static final Logger LOGGER = LoggerFactory.getLogger(EventCountingWorkHandler.class);
        private final Map<String, CountDownLatch> latches;
        public EventCountingWorkHandler(TenantOperationFactory tenantOperationFactory,
                String handlerId, Map<String, CountDownLatch> latches) {
            this.handlerId = handlerId;
            this.tenantOperationFactory = tenantOperationFactory;
            this.latches = latches;
        }

        @Override public void onStart() {}

        @Override public void onShutdown() {}

        @Override public void onEvent(TenantOperationEvent event)
                throws Exception {
            TenantOperationInfo input = event.getTenantOperationInfo();
            TenantOperationImpl op = tenantOperationFactory.getOperation(input);
            OperationStats stats = op.getMethod().apply(input);
            LOGGER.info(tenantOperationFactory.getPhoenixUtil().getGSON().toJson(stats));
            assertTrue(stats.getStatus() == 0);
            latches.get(handlerId).countDown();
        }
    }

    @Test
    public void testWorkloadWithOneHandler() throws Exception {
        int numOpGroups = 5;
        int numHandlers = 1;
        int totalOperations = 50;
        int perHandlerCount = 50;

        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(numHandlers);
            PhoenixUtil pUtil = PhoenixUtil.create();
            DataModel model = readTestDataModel("/scenario/test_mt_workload.xml");
            for (Scenario scenario : model.getScenarios()) {
                // Set the total number of operations for this load profile
                scenario.getLoadProfile().setNumOperations(totalOperations);
                TenantOperationFactory opFactory = new TenantOperationFactory(pUtil, model, scenario);
                assertTrue("operation group size from the factory is not as expected: ",
                        opFactory.getOperationsForScenario().size() == numOpGroups);

                // populate the handlers and countdown latches.
                String handlerId = String.format("%s.%d", InetAddress.getLocalHost().getHostName(), numHandlers);
                List<WorkHandler> workers = Lists.newArrayList();
                Map<String, CountDownLatch> latches = Maps.newConcurrentMap();
                workers.add(new EventCountingWorkHandler(opFactory, handlerId, latches));
                latches.put(handlerId, new CountDownLatch(perHandlerCount));
                // submit the workload
                Workload workload = new TenantOperationWorkload(pUtil, model, scenario, workers, properties);
                Future status = executor.submit(workload.execute());
                // Just make sure there are no exceptions
                status.get();

                // Wait for the handlers to count down
                for (Map.Entry<String, CountDownLatch> latch : latches.entrySet()) {
                    assertTrue(latch.getValue().await(60, TimeUnit.SECONDS));
                }
            }
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    @Test
    public void testWorkloadWithManyHandlers() throws Exception {
        int numOpGroups = 5;
        int numHandlers = 5;
        int totalOperations = 500;
        int perHandlerCount = 50;

        ExecutorService executor = Executors.newFixedThreadPool(numHandlers);
        PhoenixUtil pUtil = PhoenixUtil.create();
        DataModel model = readTestDataModel("/scenario/test_mt_workload.xml");
        for (Scenario scenario : model.getScenarios()) {
            // Set the total number of operations for this load profile
            scenario.getLoadProfile().setNumOperations(totalOperations);
            TenantOperationFactory opFactory = new TenantOperationFactory(pUtil, model, scenario);
            assertTrue("operation group size from the factory is not as expected: ",
                    opFactory.getOperationsForScenario().size() == numOpGroups);

            // populate the handlers and countdown latches.
            List<WorkHandler> workers = Lists.newArrayList();
            Map<String, CountDownLatch> latches = Maps.newConcurrentMap();
            for (int i=0;i<numHandlers;i++) {
                String handlerId = String.format("%s.%d", InetAddress.getLocalHost().getHostName(), i);
                workers.add(new EventCountingWorkHandler(opFactory, handlerId, latches));
                latches.put(handlerId, new CountDownLatch(perHandlerCount));
            }
            // submit the workload
            Workload workload = new TenantOperationWorkload(pUtil, model, scenario, workers, properties);
            Future status = executor.submit(workload.execute());
            // Just make sure there are no exceptions
            status.get();
            // Wait for the handlers to count down
            for (Map.Entry<String, CountDownLatch> latch : latches.entrySet()) {
                assertTrue(latch.getValue().await(60, TimeUnit.SECONDS));
            }
        }
        executor.shutdown();
    }

}
