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

import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.WorkHandler;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.Workload;
import org.apache.phoenix.pherf.workload.mt.Operation;
import org.apache.phoenix.pherf.workload.mt.OperationStats;
import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.base.Supplier;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiTenantTestUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTenantTestUtils.class);
    enum TestOperationGroup {
        upsertOp, queryOp1, queryOp2, idleOp, udfOp
    }

    public void testWorkloadWithOneHandler(Properties properties, DataModel model,
            String scenarioName, int expectedTenantGroups, int expectedOpGroups) throws Exception {

        int numHandlers = 1;
        int totalOperations = 50;
        int perHandlerCount = 50;

        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(numHandlers);
            PhoenixUtil pUtil = PhoenixUtil.create();
            for (Scenario scenario : model.getScenarios()) {
                if (scenarioName != null && !scenarioName.isEmpty()
                        && scenario.getName().compareTo(scenarioName) != 0) {
                    continue;
                }
                LOGGER.debug(String.format("Testing %s", scenario.getName()));
                LoadProfile loadProfile = scenario.getLoadProfile();

                // Set the total number of operations for this load profile
                loadProfile.setNumOperations(totalOperations);
                TenantOperationFactory opFactory = new TenantOperationFactory(pUtil, model,
                        scenario);
                assertEquals("tenant group size is not as expected: ", expectedTenantGroups,
                        loadProfile.getTenantDistribution().size());
                assertEquals("operation group size from the factory is not as expected: ",
                        expectedOpGroups, opFactory.getOperations().size());

                // populate the handlers and countdown latches.
                String handlerId = String
                        .format("%s.%d", InetAddress.getLocalHost().getHostName(), numHandlers);
                List<WorkHandler> workers = Lists.newArrayList();
                Map<String, CountDownLatch> latches = Maps.newConcurrentMap();
                workers.add(new EventCountingWorkHandler(opFactory, handlerId, latches));
                latches.put(handlerId, new CountDownLatch(perHandlerCount));
                // submit the workload
                Workload workload = new TenantOperationWorkload(pUtil, model, scenario, workers,
                        properties);
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

    public void testWorkloadWithManyHandlers(Properties properties, DataModel model,
            String scenarioName, int expectedTenantGroups, int expectedOpGroups) throws Exception {

        int numHandlers = 5;
        int totalOperations = 500;
        int perHandlerCount = 50;

        ExecutorService executor = Executors.newFixedThreadPool(numHandlers);
        PhoenixUtil pUtil = PhoenixUtil.create();
        for (Scenario scenario : model.getScenarios()) {
            if (scenarioName != null && !scenarioName.isEmpty()
                    && scenario.getName().compareTo(scenarioName) != 0) {
                continue;
            }
            LOGGER.debug(String.format("Testing %s", scenario.getName()));
            LoadProfile loadProfile = scenario.getLoadProfile();

            // Set the total number of operations for this load profile
            loadProfile.setNumOperations(totalOperations);
            TenantOperationFactory opFactory = new TenantOperationFactory(pUtil, model, scenario);
            assertEquals("tenant group size is not as expected: ", expectedTenantGroups,
                    loadProfile.getTenantDistribution().size());

            assertEquals("operation group size from the factory is not as expected: ",
                    expectedOpGroups, opFactory.getOperations().size());

            // populate the handlers and countdown latches.
            List<WorkHandler> workers = Lists.newArrayList();
            Map<String, CountDownLatch> latches = Maps.newConcurrentMap();
            for (int i = 0; i < numHandlers; i++) {
                String handlerId = String
                        .format("%s.%d", InetAddress.getLocalHost().getHostName(), i);
                workers.add(new EventCountingWorkHandler(opFactory, handlerId, latches));
                latches.put(handlerId, new CountDownLatch(perHandlerCount));
            }
            // submit the workload
            Workload workload = new TenantOperationWorkload(pUtil, model, scenario, workers,
                    properties);
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

    public void testVariousOperations(Properties properties, DataModel model, String scenarioName,
            int expectedTenantGroups, int expectedOpGroups) throws Exception {

        int numRuns = 10;
        int numOperations = 10;

        PhoenixUtil pUtil = PhoenixUtil.create();
        for (Scenario scenario : model.getScenarios()) {
            if (scenarioName != null && !scenarioName.isEmpty()
                    && scenario.getName().compareTo(scenarioName) != 0) {
                continue;
            }
            LOGGER.debug(String.format("Testing %s", scenario.getName()));
            LoadProfile loadProfile = scenario.getLoadProfile();
            assertEquals("tenant group size is not as expected: ", expectedTenantGroups,
                    loadProfile.getTenantDistribution().size());
            assertEquals("operation group size is not as expected: ", expectedOpGroups,
                    loadProfile.getOpDistribution().size());

            TenantOperationFactory opFactory = new TenantOperationFactory(pUtil, model, scenario);
            TenantOperationEventGenerator evtGen = new TenantOperationEventGenerator(
                    opFactory.getOperations(), model, scenario);

            assertEquals("operation group size from the factory is not as expected: ",
                    expectedOpGroups, opFactory.getOperations().size());

            int numRowsInserted = 0;
            for (int i = 0; i < numRuns; i++) {
                int ops = numOperations;
                loadProfile.setNumOperations(ops);
                while (ops-- > 0) {
                    TenantOperationInfo info = evtGen.next();
                    Supplier<Function<TenantOperationInfo, OperationStats>> opSupplier = opFactory
                            .getOperationSupplier(info);
                    OperationStats stats = opSupplier.get().apply(info);
                    LOGGER.info(PhoenixUtil.getGSON().toJson(stats));
                    if (info.getOperation().getType() == Operation.OperationType.PRE_RUN) continue;
                    assertTrue(stats.getStatus() != -1);
                    switch (TestOperationGroup
                            .valueOf(info.getOperationGroupId())) {
                    case upsertOp:
                        assertTrue(opSupplier.getClass()
                                .isAssignableFrom(UpsertOperationSupplier.class));
                        numRowsInserted += stats.getRowCount();
                        break;
                    case queryOp1:
                    case queryOp2:
                        assertTrue(opFactory.getOperationSupplier(info).getClass()
                                .isAssignableFrom(QueryOperationSupplier.class));

                        // expected row count > 0
                        // Since the same view/table is being used by many tests.
                        // Keeping query return values would require lot of housekeeping
                        assertTrue(stats.getRowCount() > 0);
                        break;
                    case idleOp:
                        assertTrue(opFactory.getOperationSupplier(info).getClass()
                                .isAssignableFrom(IdleTimeOperationSupplier.class));
                        assertEquals(0, stats.getRowCount());
                        // expected think time (no-op) to be ~50ms
                        assertTrue(40 < stats.getDurationInMs() && stats.getDurationInMs() < 60);
                        break;
                    case udfOp:
                        assertTrue(opFactory.getOperationSupplier(info).getClass()
                                .isAssignableFrom(UserDefinedOperationSupplier.class));
                        assertEquals(0, stats.getRowCount());
                        break;
                    default:
                        Assert.fail();
                    }
                }
            }
        }
    }

    private static class EventCountingWorkHandler
            implements WorkHandler<TenantOperationWorkload.TenantOperationEvent>, LifecycleAware {
        private final String handlerId;
        private final TenantOperationFactory tenantOperationFactory;
        private final Map<String, CountDownLatch> latches;

        public EventCountingWorkHandler(TenantOperationFactory tenantOperationFactory,
                String handlerId, Map<String, CountDownLatch> latches) {
            this.handlerId = handlerId;
            this.tenantOperationFactory = tenantOperationFactory;
            this.latches = latches;
        }

        @Override public void onStart() {
        }

        @Override public void onShutdown() {
        }

        @Override public void onEvent(TenantOperationWorkload.TenantOperationEvent event)
                throws Exception {
            TenantOperationInfo input = event.getTenantOperationInfo();
            Supplier<Function<TenantOperationInfo, OperationStats>>
                    opSupplier
                    = tenantOperationFactory.getOperationSupplier(input);
            OperationStats stats = opSupplier.get().apply(input);
            LOGGER.info(PhoenixUtil.getGSON().toJson(stats));
            assertEquals(0, stats.getStatus());
            latches.get(handlerId).countDown();
        }
    }

}
