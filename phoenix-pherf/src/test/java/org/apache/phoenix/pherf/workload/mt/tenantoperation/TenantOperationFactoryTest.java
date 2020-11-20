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

import org.apache.phoenix.pherf.XMLConfigParserTest;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationFactory.NoopTenantOperationImpl;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationFactory.QueryTenantOperationImpl;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationFactory.UpsertTenantOperationImpl;
import org.apache.phoenix.pherf.workload.mt.tenantoperation.TenantOperationFactory.UserDefinedOperationImpl;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.UnmarshalException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TenantOperationFactoryTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantOperationFactoryTest.class);

    private static enum TestOperationGroup {
        op1, op2, op3, op4, op5
    }

    private static enum TestTenantGroup {
        tg1, tg2, tg3
    }

    public DataModel readTestDataModel(String resourceName) throws Exception {
        URL scenarioUrl = XMLConfigParserTest.class.getResource(resourceName);
        assertNotNull(scenarioUrl);
        Path p = Paths.get(scenarioUrl.toURI());
        try {
            return XMLConfigParser.readDataModel(p);
        } catch (UnmarshalException e) {
            // If we don't parse the DTD, the variable 'name' won't be defined in the XML
            LOGGER.warn("Caught expected exception", e);
        }
        return null;
    }

    @Test public void testVariousOperations() throws Exception {
        int numTenantGroups = 3;
        int numOpGroups = 5;
        int numRuns = 10;
        int numOperations = 10;

        PhoenixUtil pUtil = PhoenixUtil.create();
        DataModel model = readTestDataModel("/scenario/test_evt_gen1.xml");
        for (Scenario scenario : model.getScenarios()) {
            LOGGER.debug(String.format("Testing %s", scenario.getName()));
            LoadProfile loadProfile = scenario.getLoadProfile();
            assertTrue("tenant group size is not as expected: ",
                    loadProfile.getTenantDistribution().size() == numTenantGroups);
            assertTrue("operation group size is not as expected: ",
                    loadProfile.getOpDistribution().size() == numOpGroups);

            TenantOperationFactory opFactory = new TenantOperationFactory(pUtil, model, scenario);
            assertTrue("operation group size from the factory is not as expected: ",
                    opFactory.getOperationsForScenario().size() == numOpGroups);

            for (int i = 0; i < numRuns; i++) {
                int ops = numOperations;
                loadProfile.setNumOperations(ops);
                TenantOperationEventGenerator evtGen = new TenantOperationEventGenerator(
                        opFactory.getOperationsForScenario(), model, scenario);
                while (ops-- > 0) {
                    TenantOperationInfo info = evtGen.next();
                    int row = TestOperationGroup.valueOf(info.getOperationGroupId()).ordinal();
                    switch (row) {
                    case 0:
                        assertTrue(opFactory.getOperation(info).getClass()
                                .isAssignableFrom(UpsertTenantOperationImpl.class));
                        break;
                    case 1:
                    case 2:
                        assertTrue(opFactory.getOperation(info).getClass()
                                .isAssignableFrom(QueryTenantOperationImpl.class));
                        break;
                    case 3:
                        assertTrue(opFactory.getOperation(info).getClass()
                                .isAssignableFrom(NoopTenantOperationImpl.class));
                        break;
                    case 4:
                        assertTrue(opFactory.getOperation(info).getClass()
                                .isAssignableFrom(UserDefinedOperationImpl.class));
                        break;
                    default:
                        Assert.fail();

                    }
                }
            }
        }
    }
}
