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

import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.Operation;
import org.apache.phoenix.pherf.workload.mt.OperationStats;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationFactory.NoopTenantOperationImpl;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationFactory.QueryTenantOperationImpl;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationFactory.UpsertTenantOperationImpl;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationFactory.UserDefinedOperationImpl;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TenantOperationIT extends MultiTenantOperationBaseIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantOperationIT.class);

    @Test
    public void testVariousOperations() throws Exception {
        int numTenantGroups = 3;
        int numOpGroups = 5;
        int numRuns = 10;
        int numOperations = 10;

        PhoenixUtil pUtil = PhoenixUtil.create();
        DataModel model = readTestDataModel("/scenario/test_mt_workload.xml");
        for (Scenario scenario : model.getScenarios()) {
            LOGGER.debug(String.format("Testing %s", scenario.getName()));
            LoadProfile loadProfile = scenario.getLoadProfile();
            assertTrue("tenant group size is not as expected: ",
                    loadProfile.getTenantDistribution().size() == numTenantGroups);
            assertTrue("operation group size is not as expected: ",
                    loadProfile.getOpDistribution().size() == numOpGroups);

            TenantOperationFactory opFactory = new TenantOperationFactory(pUtil, model, scenario);
            TenantOperationEventGenerator evtGen = new TenantOperationEventGenerator(
                    opFactory.getOperationsForScenario(), model, scenario);

            assertTrue("operation group size from the factory is not as expected: ",
                    opFactory.getOperationsForScenario().size() == numOpGroups);

            int numRowsInserted = 0;
            for (int i = 0; i < numRuns; i++) {
                int ops = numOperations;
                loadProfile.setNumOperations(ops);
                while (ops-- > 0) {
                    TenantOperationInfo info = evtGen.next();
                    TenantOperationImpl op = opFactory.getOperation(info);
                    int row = TestOperationGroup.valueOf(info.getOperationGroupId()).ordinal();
                    OperationStats stats = op.getMethod().apply(info);
                    LOGGER.info(pUtil.getGSON().toJson(stats));
                    if (info.getOperation().getType() == Operation.OperationType.PRE_RUN) continue;
                    switch (row) {
                    case 0:
                        assertTrue(op.getClass()
                                .isAssignableFrom(UpsertTenantOperationImpl.class));
                        numRowsInserted += stats.getRowCount();
                        break;
                    case 1:
                    case 2:
                        assertTrue(opFactory.getOperation(info).getClass()
                                .isAssignableFrom(QueryTenantOperationImpl.class));

                        // expected row count == num rows inserted
                        assertEquals(numRowsInserted, stats.getRowCount());
                        break;
                    case 3:
                        assertTrue(opFactory.getOperation(info).getClass()
                                .isAssignableFrom(NoopTenantOperationImpl.class));
                        assertEquals(0, stats.getRowCount());
                        // expected think time (no-op) to be ~50ms
                        assertTrue(40 < stats.getDurationInMs() && stats.getDurationInMs() < 60);
                        break;
                    case 4:
                        assertTrue(opFactory.getOperation(info).getClass()
                                .isAssignableFrom(UserDefinedOperationImpl.class));
                        assertEquals(0, stats.getRowCount());
                        break;
                    default:
                        Assert.fail();
                    }
                }
            }
        }
    }
}
