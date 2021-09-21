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

package org.apache.phoenix.pherf.workload.mt;

import com.lmax.disruptor.LifecycleAware;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.XMLConfigParserTest;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.schema.SchemaReader;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.Workload;
import org.apache.phoenix.pherf.workload.WorkloadExecutor;
import org.apache.phoenix.pherf.workload.mt.generators.LoadEventGenerator;
import org.apache.phoenix.pherf.workload.mt.generators.TenantLoadEventGeneratorFactory;
import org.apache.phoenix.pherf.workload.mt.generators.TenantOperationInfo;
import org.apache.phoenix.pherf.workload.mt.handlers.PherfWorkHandler;
import org.apache.phoenix.pherf.workload.mt.operations.IdleTimeOperationSupplier;
import org.apache.phoenix.pherf.workload.mt.operations.Operation;
import org.apache.phoenix.pherf.workload.mt.operations.OperationStats;
import org.apache.phoenix.pherf.workload.mt.operations.QueryOperationSupplier;
import org.apache.phoenix.pherf.workload.mt.operations.TenantOperationFactory;
import org.apache.phoenix.pherf.workload.mt.operations.UpsertOperationSupplier;
import org.apache.phoenix.pherf.workload.mt.operations.UserDefinedOperationSupplier;
import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.base.Supplier;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.pherf.workload.mt.generators.BaseLoadEventGenerator.TenantOperationEvent;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MultiTenantTestUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTenantTestUtils.class);
    enum TestOperationGroup {
        upsertOp, queryOp1, queryOp2, idleOp, udfOp
    }

    public static class TestConfigAndExpectations {
        List<TenantGroup> tenantGroups;
        int expectedTenantGroups;
        int expectedOpGroups;
    }

    public SchemaReader applySchema(PhoenixUtil util, String matcherSchema) throws Exception {
        PherfConstants constants = PherfConstants.create();

        PhoenixUtil.setZookeeper("localhost");
        SchemaReader reader = new SchemaReader(util, matcherSchema);
        reader.applySchema();
        List<Path> resources = new ArrayList<>(reader.getResourceList());

        assertTrue("Could not pull list of schema files.", resources.size() > 0);
        assertNotNull("Could not read schema file.", reader.resourceToString(resources.get(0)));
        return reader;
    }

    public DataModel readTestDataModel(String resourceName) throws Exception {
        URL scenarioUrl = XMLConfigParserTest.class.getResource(resourceName);
        assertNotNull(scenarioUrl);
        Path p = Paths.get(scenarioUrl.toURI());
        return XMLConfigParser.readDataModel(p);
    }

    public void testWorkloadWithCountingHandlers(Properties properties, DataModel model,
            String scenarioName,
            int numHandlers,
            int expectedTenantGroups, int expectedOpGroups) throws Exception {

        int totalOperations = 500;
        int perHandlerCount = 50;

        List<Workload> workloads = new ArrayList<>();
        WorkloadExecutor workloadExecutor = new WorkloadExecutor(properties, workloads, false);
        try {
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
                List<PherfWorkHandler> workers = Lists.newArrayList();
                Map<String, CountDownLatch> latches = Maps.newConcurrentMap();
                for (int i = 0; i < numHandlers; i++) {
                    String handlerId = String
                            .format("%s.%d", InetAddress.getLocalHost().getHostName(), i);
                    workers.add(new EventCountingWorkHandler(opFactory, handlerId, latches));
                    latches.put(handlerId, new CountDownLatch(perHandlerCount));
                }

                // submit the workload
                Workload workload = new MultiTenantWorkload(pUtil, model, scenario, workers,
                        properties);
                workloads.add(workload);
                workloadExecutor.add(workload);
                // Just make sure there are no exceptions
                workloadExecutor.get();

                // Wait for the handlers to count down
                for (Map.Entry<String, CountDownLatch> latch : latches.entrySet()) {
                    assertTrue(latch.getValue().await(60, TimeUnit.SECONDS));
                }
            }
        } finally {
            if (!workloads.isEmpty()) {
                for (Workload workload : workloads) {
                    workload.complete();
                }
            }
            workloadExecutor.complete();
            workloadExecutor.shutdown();
        }

    }

    public void testWorkloadWithHandlers(Properties properties, DataModel model,
            String scenarioName,
            int numHandlers,
            int expectedTenantGroups, int expectedOpGroups) throws Exception {

        int totalOperations = 500;

        List<Workload> workloads = new ArrayList<>();
        WorkloadExecutor workloadExecutor = new WorkloadExecutor(properties, workloads, false);
        try {
            PhoenixUtil pUtil = PhoenixUtil.create();
            for (Scenario scenario : model.getScenarios()) {
                if (scenarioName != null && !scenarioName.isEmpty()
                        && scenario.getName().compareTo(scenarioName) != 0) {
                    continue;
                }

                Map<String, String> scenarioProperties = Maps.newHashMap();
                scenarioProperties.put("pherf.mt.handlers_per_scenario", String.valueOf(numHandlers));
                scenario.setPhoenixProperties(scenarioProperties);
                LOGGER.debug(String.format("Testing %s", scenario.getName()));
                LoadProfile loadProfile = scenario.getLoadProfile();

                // Set the total number of operations for this load profile
                loadProfile.setNumOperations(totalOperations);
                TenantOperationFactory opFactory = new TenantOperationFactory(pUtil, model, scenario);
                assertEquals("tenant group size is not as expected: ", expectedTenantGroups,
                        loadProfile.getTenantDistribution().size());

                assertEquals("operation group size from the factory is not as expected: ",
                        expectedOpGroups, opFactory.getOperations().size());
                // submit the workload
                Workload workload = new MultiTenantWorkload(pUtil, model, scenario,
                        properties);
                workloads.add(workload);
                workloadExecutor.add(workload);
                // Just make sure there are no exceptions
                workloadExecutor.get();

            }
        } finally {
            if (!workloads.isEmpty()) {
                for (Workload workload : workloads) {
                    workload.complete();
                }
            }
            workloadExecutor.complete();
            workloadExecutor.shutdown();
        }

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

            TenantLoadEventGeneratorFactory eventGenFactory = new TenantLoadEventGeneratorFactory();
            LoadEventGenerator evtGen = eventGenFactory.newLoadEventGenerator(
                    pUtil, model, scenario, properties);
            TenantOperationFactory opFactory = evtGen.getOperationFactory();

            assertEquals("operation group size from the factory is not as expected: ",
                    expectedOpGroups, opFactory.getOperations().size());

            int numRowsInserted = 0;
            for (int i = 0; i < numRuns; i++) {
                int ops = numOperations;
                loadProfile.setNumOperations(ops);
                while (ops-- > 0) {
                    TenantOperationInfo info = evtGen.next();
                    opFactory.initializeTenant(info);
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

                        // expected row count >= 0
                        // Since the same view/table is being used by many tests.
                        // Keeping query return values would require lot of housekeeping
                        assertTrue(stats.getRowCount() >= 0);
                        break;
                    case idleOp:
                        assertTrue(opFactory.getOperationSupplier(info).getClass()
                                .isAssignableFrom(IdleTimeOperationSupplier.class));
                        assertEquals(0, stats.getRowCount());
                        // expected think time (no-op) to be ~50ms
                        assertTrue(25 < stats.getDurationInMs() && stats.getDurationInMs() < 75);
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
            implements PherfWorkHandler<TenantOperationEvent>, LifecycleAware {
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

        @Override public void onEvent(TenantOperationEvent event)
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

        @Override public List<ResultValue<OperationStats>> getResults() {
            return null;
        }
    }

}
