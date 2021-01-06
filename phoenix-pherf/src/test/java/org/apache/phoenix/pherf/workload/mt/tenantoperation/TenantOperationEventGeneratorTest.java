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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.UnmarshalException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the various event generation outcomes based on scenario, model and load profile.
 */
public class TenantOperationEventGeneratorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantOperationEventGeneratorTest.class);
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

    /**
     * Case 1 : where some operations have zero weight
     * Case 2 : where some tenant groups have zero weight
     * Case 3 : where no operations and tenant groups have zero weight
     * Case 4 : where some combinations of operation and tenant groups have zero weight
     *
     * @throws Exception
     */
    @Test
    public void testVariousEventGeneration() throws Exception {
        int numRuns = 10;
        int numOperations = 100000;
        int allowedVariance = 1000;
        int normalizedOperations = (numOperations * numRuns) / 10000;
        int numTenantGroups = 3;
        int numOpGroups = 5;

        PhoenixUtil pUtil = PhoenixUtil.create();
        DataModel model = readTestDataModel("/scenario/test_evt_gen1.xml");
        for (Scenario scenario : model.getScenarios()) {
            LOGGER.debug(String.format("Testing %s", scenario.getName()));
            LoadProfile loadProfile = scenario.getLoadProfile();
            assertEquals("tenant group size is not as expected: ",
                    numTenantGroups, loadProfile.getTenantDistribution().size());
            assertEquals("operation group size is not as expected: ",
                    numOpGroups, loadProfile.getOpDistribution().size());
            // Calculate the expected distribution.
            int[][] expectedDistribution = new int[numOpGroups][numTenantGroups];
            for (int r = 0; r < numOpGroups; r++) {
                for (int c = 0; c < numTenantGroups; c++) {
                    int tenantWeight = loadProfile.getTenantDistribution().get(c).getWeight();
                    int opWeight = loadProfile.getOpDistribution().get(r).getWeight();
                    expectedDistribution[r][c] = normalizedOperations * (tenantWeight * opWeight);
                    LOGGER.debug(String.format("Expected [%d,%d] = %d", r, c, expectedDistribution[r][c]));
                }
            }
            TenantOperationFactory opFactory = new TenantOperationFactory(pUtil, model, scenario);

            // Calculate the actual distribution.
            int[][] distribution = new int[numOpGroups][numTenantGroups];
            for (int i = 0; i < numRuns; i++) {
                int ops = numOperations;
                loadProfile.setNumOperations(ops);
                TenantOperationEventGenerator evtGen = new TenantOperationEventGenerator(
                        opFactory.getOperationsForScenario(), model, scenario);
                while (ops-- > 0) {
                    TenantOperationInfo info = evtGen.next();
                    int row = TestOperationGroup.valueOf(info.getOperationGroupId()).ordinal();
                    int col = TestTenantGroup.valueOf(info.getTenantGroupId()).ordinal();
                    distribution[row][col]++;
                }
            }

            // Validate that the expected and actual distribution
            // is within the margin of allowed variance.
            for (int r = 0; r < numOpGroups; r++) {
                for (int c = 0; c < numTenantGroups; c++) {
                    LOGGER.debug(String.format("Actual[%d,%d] = %d", r, c, distribution[r][c]));
                    int diff = Math.abs(expectedDistribution[r][c] - distribution[r][c]);
                    boolean isAllowed = diff < allowedVariance;
                    assertTrue("Difference is outside the allowed variance", isAllowed);
                }
            }
        }
    }
}
