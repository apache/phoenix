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

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.XMLConfigParserTest;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.util.PhoenixUtil;

import org.apache.phoenix.pherf.workload.mt.generators.TenantOperationInfo;
import org.apache.phoenix.pherf.workload.mt.generators.WeightedRandomLoadEventGenerator;
import org.apache.phoenix.pherf.workload.mt.operations.IdleTimeOperationSupplier;
import org.apache.phoenix.pherf.workload.mt.operations.QueryOperationSupplier;
import org.apache.phoenix.pherf.workload.mt.operations.TenantOperationFactory;
import org.apache.phoenix.pherf.workload.mt.operations.UpsertOperationSupplier;
import org.apache.phoenix.pherf.workload.mt.operations.UserDefinedOperationSupplier;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * Tests the various operation supplier outcomes based on scenario, model and load profile.
 */
public class TenantOperationFactoryTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantOperationFactoryTest.class);

    private enum TestOperationGroup {
        upsertOp, queryOp1, queryOp2, idleOp, udfOp
    }

    private enum TestTenantGroup {
        tg1, tg2, tg3
    }

    public DataModel readTestDataModel(String resourceName) throws Exception {
        URL scenarioUrl = XMLConfigParserTest.class.getResource(resourceName);
        assertNotNull(scenarioUrl);
        Path p = Paths.get(scenarioUrl.toURI());
        return XMLConfigParser.readDataModel(p);
    }

    @Test public void testVariousOperations() throws Exception {
        int numTenantGroups = 3;
        int numOpGroups = 5;
        int numRuns = 10;
        int numOperations = 10;

        PhoenixUtil pUtil = PhoenixUtil.create();
        Properties properties = PherfConstants
                .create().getProperties(PherfConstants.PHERF_PROPERTIES, false);

        DataModel model = readTestDataModel("/scenario/test_evt_gen1.xml");
        for (Scenario scenario : model.getScenarios()) {
            LOGGER.debug(String.format("Testing %s", scenario.getName()));
            LoadProfile loadProfile = scenario.getLoadProfile();
            assertEquals("tenant group size is not as expected: ",
                    numTenantGroups, loadProfile.getTenantDistribution().size());
            assertEquals("operation group size is not as expected: ",
                    numOpGroups, loadProfile.getOpDistribution().size());

            WeightedRandomLoadEventGenerator evtGen = new WeightedRandomLoadEventGenerator(
                    pUtil, model, scenario, properties);
            TenantOperationFactory opFactory = evtGen.getOperationFactory();
            assertEquals("operation group size from the factory is not as expected: ",
                    numOpGroups, opFactory.getOperations().size());

            for (int i = 0; i < numRuns; i++) {
                int ops = numOperations;
                loadProfile.setNumOperations(ops);
                while (ops-- > 0) {
                    TenantOperationInfo info = evtGen.next();
                    switch (TestOperationGroup.valueOf(info.getOperationGroupId())) {
                    case upsertOp:
                        assertTrue(opFactory.getOperationSupplier(info).getClass()
                                .isAssignableFrom(UpsertOperationSupplier.class));
                        break;
                    case queryOp1:
                    case queryOp2:
                        assertTrue(opFactory.getOperationSupplier(info).getClass()
                                .isAssignableFrom(QueryOperationSupplier.class));
                        break;
                    case idleOp:
                        assertTrue(opFactory.getOperationSupplier(info).getClass()
                                .isAssignableFrom(IdleTimeOperationSupplier.class));
                        break;
                    case udfOp:
                        assertTrue(opFactory.getOperationSupplier(info).getClass()
                                .isAssignableFrom(UserDefinedOperationSupplier.class));
                        break;
                    default:
                        Assert.fail();

                    }
                }
            }
        }
    }
}
