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
 * Tests the various event generation outcomes based on scenario, model and load profile.
 */
public class WeightedRandomLoadEventGeneratorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            WeightedRandomLoadEventGeneratorTest.class);
    private enum TestOperationGroup {
        upsertOp, queryOp1, queryOp2, idleOp, udfOp
    }

    private enum TestOperationGroup2 {
        upsertOp, queryOp1, queryOp2, queryOp3, queryOp4, queryOp5, queryOp6, queryOp7, queryOp8, idleOp, udfOp
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
     * Case : where no operations and tenant groups have zero weight
     * @throws Exception
     */
    @Test
    public void testVariousEventGeneration() throws Exception {
        int numRuns = 10;
        int numOperations = 100000;
        double normalizedOperations = (double) (numOperations * numRuns) / 10000.0f;
        int numTenantGroups = 3;
        int numOpGroups = 5;

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
            // Calculate the expected distribution.
            double[][] expectedDistribution = new double[numOpGroups][numTenantGroups];
            for (int r = 0; r < numOpGroups; r++) {
                for (int c = 0; c < numTenantGroups; c++) {
                    int tenantWeight = loadProfile.getTenantDistribution().get(c).getWeight();
                    int opWeight = loadProfile.getOpDistribution().get(r).getWeight();
                    expectedDistribution[r][c] = normalizedOperations * (tenantWeight * opWeight);
                    LOGGER.debug(String.format("Expected [%d,%d] = %f", r, c, expectedDistribution[r][c]));
                }
            }

            WeightedRandomLoadEventGenerator evtGen = new WeightedRandomLoadEventGenerator(
                    pUtil, model, scenario, properties);

            // Calculate the actual distribution.
            double[][] distribution = new double[numOpGroups][numTenantGroups];
            for (int i = 0; i < numRuns; i++) {
                int ops = numOperations;
                loadProfile.setNumOperations(ops);
                while (ops-- > 0) {
                    TenantOperationInfo info = evtGen.next();
                    int row = TestOperationGroup.valueOf(info.getOperationGroupId()).ordinal();
                    int col = TestTenantGroup.valueOf(info.getTenantGroupId()).ordinal();
                    distribution[row][col]++;
                }
            }
            validateResults(numOpGroups, numTenantGroups, expectedDistribution, distribution);
        }
    }

    /**
     * Case  : where some operations have zero weight
     */
    @Test
    public void testAutoAssignedPMFs() throws Exception {
        int numRuns = 50;
        int numOperations = 100000;
        double normalizedOperations = (double) (numOperations * numRuns) / 10000.0f;
        int numTenantGroups = 3;
        int numOpGroups = 11;

        PhoenixUtil pUtil = PhoenixUtil.create();
        Properties properties = PherfConstants
                .create().getProperties(PherfConstants.PHERF_PROPERTIES, false);

        DataModel model = readTestDataModel("/scenario/test_evt_gen2.xml");
        for (Scenario scenario : model.getScenarios()) {
            LOGGER.debug(String.format("Testing %s", scenario.getName()));
            LoadProfile loadProfile = scenario.getLoadProfile();
            assertEquals("tenant group size is not as expected: ",
                    numTenantGroups, loadProfile.getTenantDistribution().size());
            assertEquals("operation group size is not as expected: ",
                    numOpGroups, loadProfile.getOpDistribution().size());

            float totalOperationWeight = 0.0f;
            float autoAssignedOperationWeight = 0.0f;
            float remainingOperationWeight = 0.0f;
            int numAutoWeightedOperations = 0;
            for (int r = 0; r < numOpGroups; r++) {
                int opWeight = loadProfile.getOpDistribution().get(r).getWeight();
                if (opWeight > 0.0f) {
                    totalOperationWeight += opWeight;
                } else {
                    numAutoWeightedOperations++;
                }
            }
            remainingOperationWeight = 100.0f - totalOperationWeight;
            if (numAutoWeightedOperations > 0) {
                autoAssignedOperationWeight = remainingOperationWeight/((float) numAutoWeightedOperations);
            }
            LOGGER.debug(String.format("Auto [%d,%f] = %f", numAutoWeightedOperations,
                    remainingOperationWeight, autoAssignedOperationWeight ));

            // Calculate the expected distribution.
            double[][] expectedDistribution = new double[numOpGroups][numTenantGroups];
            for (int r = 0; r < numOpGroups; r++) {
                for (int c = 0; c < numTenantGroups; c++) {
                    float tenantWeight = loadProfile.getTenantDistribution().get(c).getWeight();
                    float opWeight = loadProfile.getOpDistribution().get(r).getWeight();
                    if (opWeight <= 0.0f) {
                        opWeight = autoAssignedOperationWeight;
                    }
                    expectedDistribution[r][c] = Math.round(normalizedOperations * (tenantWeight * opWeight));
                    LOGGER.debug(String.format("Expected [%d,%d] = %f", r, c, expectedDistribution[r][c]));
                }
            }

            WeightedRandomLoadEventGenerator evtGen = new WeightedRandomLoadEventGenerator(
                    pUtil, model, scenario, properties);

            // Calculate the actual distribution.
            double[][] distribution = new double[numOpGroups][numTenantGroups];
            for (int i = 0; i < numRuns; i++) {
                int ops = numOperations;
                loadProfile.setNumOperations(ops);
                while (ops-- > 0) {
                    TenantOperationInfo info = evtGen.next();
                    int row = TestOperationGroup2.valueOf(info.getOperationGroupId()).ordinal();
                    int col = TestTenantGroup.valueOf(info.getTenantGroupId()).ordinal();
                    distribution[row][col]++;
                }
            }
            validateResults(numOpGroups, numTenantGroups, expectedDistribution, distribution);
        }
    }

    private void validateResults(int numOpGroups, int numTenantGroups,
            double[][] expectedDistribution,
            double[][] actualDistribution) throws Exception {

        double variancePercent = 0.05f; // 5 percent

        // Validate that the expected and actual distribution
        // is within the margin of allowed variance.
        for (int r = 0; r < numOpGroups; r++) {
            for (int c = 0; c < numTenantGroups; c++) {
                double allowedVariance = expectedDistribution[r][c] * variancePercent;
                double diff = Math.abs(expectedDistribution[r][c] - actualDistribution[r][c]);
                boolean isAllowed = diff < allowedVariance;
                LOGGER.debug(String.format("Actual[%d,%d] = %f, %f, %f",
                        r, c, actualDistribution[r][c], diff, allowedVariance));
                assertTrue(String.format("Difference is outside the allowed variance "
                        + "[expected = %f, actual = %f]", allowedVariance, diff), isAllowed);
            }
        }
    }
}
