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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import com.lmax.disruptor.WorkHandler;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.generators.TenantLoadEventGeneratorFactory.GeneratorType;
import org.apache.phoenix.pherf.workload.mt.MultiTenantTestUtils.TestConfigAndExpectations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Tests focused on tenant tablee operations and their validations
 * Tests focused on tenant operation workloads {@link MultiTenantWorkload}
 * and workload handlers {@link WorkHandler}
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class TenantTableOperationWorkloadIT extends ParallelStatsDisabledIT {

    private final MultiTenantTestUtils multiTenantTestUtils = new MultiTenantTestUtils();
    private final Properties properties = PherfConstants.create().getProperties(PherfConstants.PHERF_PROPERTIES, false);
    private final PhoenixUtil util = PhoenixUtil.create(true);
    private GeneratorType generatorType;

    public TenantTableOperationWorkloadIT(String generatorType) throws Exception {
        this.generatorType = GeneratorType.valueOf(generatorType);
    }

    @Parameterized.Parameters( name = "generator_type={0}" )
    public static synchronized Collection<Object[]> data() {
        List<Object[]> testCases = Lists.newArrayList();
        testCases.add(new Object[] { "WEIGHTED" });
        testCases.add(new Object[] { "UNIFORM" });
        testCases.add(new Object[] { "SEQUENTIAL" });
        return testCases;
    }

    @Before
    public void setup() throws Exception {
        multiTenantTestUtils.applySchema(util, ".*datamodel/.*test_tbl_schema_simple.sql");
    }

    @After
    public void cleanup() throws Exception {
        util.deleteTables("PHERF.*MULTI_TENANT_TABLE");
    }

    @Test
    public void testVariousOperations() throws Exception {
        DataModel model = multiTenantTestUtils.readTestDataModel(
                "/scenario/test_tbl_workload_template.xml");
        for (Scenario scenario : model.getScenarios()) {
            TestConfigAndExpectations settings = getTestConfigAndExpectations(scenario, generatorType);
            scenario.setGeneratorName(generatorType.name());
            scenario.getLoadProfile().setTenantDistribution(settings.tenantGroups);
            multiTenantTestUtils.testVariousOperations(properties, model, scenario.getName(),
                    settings.expectedTenantGroups, settings.expectedOpGroups);
        }
    }

    @Test
    public void testWorkloadWithOneHandler() throws Exception {
        int numHandlers = 1;

        DataModel model = multiTenantTestUtils.readTestDataModel(
                "/scenario/test_tbl_workload_template.xml");
        for (Scenario scenario : model.getScenarios()) {
            TestConfigAndExpectations settings = getTestConfigAndExpectations(scenario, generatorType);
            scenario.setGeneratorName(generatorType.name());
            scenario.getLoadProfile().setTenantDistribution(settings.tenantGroups);
            multiTenantTestUtils.testWorkloadWithHandlers(properties, model, scenario.getName(),
                    numHandlers, settings.expectedTenantGroups, settings.expectedOpGroups);
        }
    }

    @Test
    public void testWorkloadWithManyHandlers() throws Exception {
        int numHandlers = 5;
        DataModel model = multiTenantTestUtils.readTestDataModel(
                "/scenario/test_tbl_workload_template.xml");
        for (Scenario scenario : model.getScenarios()) {
            TestConfigAndExpectations settings = getTestConfigAndExpectations(scenario, generatorType);
            scenario.setGeneratorName(generatorType.name());
            scenario.getLoadProfile().setTenantDistribution(settings.tenantGroups);
            multiTenantTestUtils.testWorkloadWithHandlers(properties, model, scenario.getName(),
                    numHandlers, settings.expectedTenantGroups, settings.expectedOpGroups);
        }
    }

    private TestConfigAndExpectations getTestConfigAndExpectations(
            Scenario scenario, GeneratorType generatorType) {

        TestConfigAndExpectations
                settings = new TestConfigAndExpectations();

        switch (generatorType) {
        case WEIGHTED:
            settings.tenantGroups = scenario.getLoadProfile().getTenantDistribution();
            settings.expectedOpGroups = scenario.getLoadProfile().getOpDistribution().size();
            settings.expectedTenantGroups = scenario.getLoadProfile().getTenantDistribution().size();
        default:
            List<TenantGroup> tenantGroups = new ArrayList<>();
            TenantGroup tg1 = new TenantGroup();
            tg1.setId("tg1");
            tg1.setNumTenants(10);
            tg1.setWeight(100);
            tenantGroups.add(tg1);
            settings.tenantGroups = tenantGroups;
            settings.expectedTenantGroups = 1;
            settings.expectedOpGroups = scenario.getLoadProfile().getOpDistribution().size();;
            break;
        }
        return settings;
    }
}
