/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataOverride;
import org.apache.phoenix.pherf.configuration.DataSequence;
import org.apache.phoenix.pherf.configuration.DataTypeMapping;
import org.apache.phoenix.pherf.configuration.Ddl;
import org.apache.phoenix.pherf.configuration.ExecutionType;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.configuration.QuerySet;
import org.apache.phoenix.pherf.configuration.WriteParams;
import org.apache.phoenix.thirdparty.com.google.common.collect.Sets;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.workload.mt.generators.TenantLoadEventGeneratorFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import static org.junit.Assert.*;

public class ConfigurationParserTest extends ResultBaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationParserTest.class);

    @Test
    public void testReadWriteWorkloadReader() throws Exception {
        String scenarioName = "testScenarioRW";
        String testResourceName = "/scenario/test_scenario.xml";
        List<Scenario> scenarioList = getScenarios(testResourceName);
        Scenario target = null;
        for (Scenario scenario : scenarioList) {
            if (scenarioName.equals(scenario.getName())) {
                target = scenario;
            }
        }
        assertNotNull("Could not find scenario: " + scenarioName, target);
        WriteParams params = target.getWriteParams();

        assertNotNull("Could not find writeParams in scenario: " + scenarioName, params);
        assertNotNull("Could not find batch size: ", params.getBatchSize());
        assertNotNull("Could not find execution duration: ", params.getExecutionDurationInMs());
        assertNotNull("Could not find sleep duration: ", params.getThreadSleepDuration());
        assertNotNull("Could not find writer count: ", params.getWriterThreadCount());
    }

    @Test
    // TODO Break this into multiple smaller tests.
    public void testConfigReader() {
        try {
            String testResourceName = "/scenario/test_scenario.xml";
            LOGGER.debug("DataModel: " + writeXML());
            List<Scenario> scenarioList = getScenarios(testResourceName);
            List<Column> dataMappingColumns = getDataModel(testResourceName).getDataMappingColumns();
            assertTrue("Could not load the data columns from xml.",
                    (dataMappingColumns != null) && (dataMappingColumns.size() > 0));
            assertTrue("Could not load the data DataValue list from xml.",
                    (dataMappingColumns.get(8).getDataValues() != null)
                            && (dataMappingColumns.get(8).getDataValues().size() > 0));

            assertDateValue(dataMappingColumns);
            assertCurrentDateValue(dataMappingColumns);

            // Validate column mappings
            for (Column column : dataMappingColumns) {
                assertNotNull("Column (" + column.getName() + ") is missing its type",
                        column.getType());
            }

            Scenario scenario = scenarioList.get(1);
            assertNotNull(scenario);
            assertEquals("PHERF.TEST_TABLE", scenario.getTableName());
            assertEquals(30, scenario.getRowCount());
            assertEquals(1, scenario.getDataOverride().getColumn().size());
            QuerySet qs = scenario.getQuerySet().get(0);
            assertEquals(ExecutionType.SERIAL, qs.getExecutionType());
            assertEquals(5000, qs.getExecutionDurationInMs());
            assertEquals(2, qs.getQuery().size());

            Query firstQuery = qs.getQuery().get(0);
            assertEquals("1-3", qs.getConcurrency());
            assertEquals(1, qs.getMinConcurrency());
            assertEquals(3, qs.getMaxConcurrency());
            assertEquals(100, qs.getNumberOfExecutions());
            assertEquals("select count(*) from PHERF.TEST_TABLE", firstQuery.getStatement());
            assertEquals("123456789012345", firstQuery.getTenantId());
            assertEquals(null, firstQuery.getDdl());
            assertEquals(0, (long) firstQuery.getExpectedAggregateRowCount());

            Query secondQuery = qs.getQuery().get(1);
            assertEquals("Could not get statement.", "select sum(SOME_INT) from PHERF.TEST_TABLE",
                    secondQuery.getStatement());
            assertEquals("Could not get queryGroup.", "g1", secondQuery.getQueryGroup());

            // Make sure anything in the overrides matches a real column in the data mappings
            DataOverride override = scenario.getDataOverride();
            for (Column column : override.getColumn()) {
                assertTrue("Could not lookup Column (" + column.getName()
                        + ") in DataMapping columns: " + dataMappingColumns,
                        dataMappingColumns.contains(column));
            }

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testWorkloadWithLoadProfile() throws Exception {
        String testResourceName = "/scenario/test_workload_with_load_profile.xml";
        Set<String> scenarioNames = Sets.newHashSet("scenario_11", "scenario_12");
        List<Scenario> scenarioList = getScenarios(testResourceName);
        Scenario target = null;
        for (Scenario scenario : scenarioList) {
            if (scenarioNames.contains(scenario.getName())) {
                target = scenario;
            }
            assertNotNull("Could not find scenario: " + scenario.getName(), target);
        }

        Scenario testScenarioWithLoadProfile = scenarioList.get(0);
        Map<String, String> props = testScenarioWithLoadProfile.getPhoenixProperties();
        assertEquals("Number of properties(size) not as expected: ",
                2, props.size());
        TenantLoadEventGeneratorFactory.GeneratorType
                type = TenantLoadEventGeneratorFactory.GeneratorType.valueOf(
                        testScenarioWithLoadProfile.getGeneratorName());
        assertEquals("Unknown generator type: ",
                TenantLoadEventGeneratorFactory.GeneratorType.UNIFORM, type);

        LoadProfile loadProfile = testScenarioWithLoadProfile.getLoadProfile();
        assertEquals("batch size not as expected: ",
                1, loadProfile.getBatchSize());
        assertEquals("num operations not as expected: ",
                1000, loadProfile.getNumOperations());
        assertEquals("tenant group size is not as expected: ",
                3, loadProfile.getTenantDistribution().size());
        assertEquals("operation group size is not as expected: ",
                5,loadProfile.getOpDistribution().size());
        assertEquals("UDFs size is not as expected  ",
                1, testScenarioWithLoadProfile.getUdfs().size());
        assertNotNull("UDFs clazzName cannot be null ",
                testScenarioWithLoadProfile.getUdfs().get(0).getClazzName());
        assertEquals("UDFs args size is not as expected  ",
                2, testScenarioWithLoadProfile.getUdfs().get(0).getArgs().size());
        assertEquals("UpsertSet size is not as expected ",
                1, testScenarioWithLoadProfile.getUpserts().size());
        assertEquals("#Column within the first upsert is not as expected ",
                7, testScenarioWithLoadProfile.getUpserts().get(0).getColumn().size());
        assertEquals("QuerySet size is not as expected ",
                1, testScenarioWithLoadProfile.getQuerySet().size());
        assertEquals("#Queries within the first querySet is not as expected ",
                2, testScenarioWithLoadProfile.getQuerySet().get(0).getQuery().size());

        // Test configuration for global connection
        Scenario testScenarioWithGlobalConn = scenarioList.get(2);
        LoadProfile loadProfileWithGlobalConn = testScenarioWithGlobalConn.getLoadProfile();
        assertEquals("batch size not as expected: ",
                1, loadProfileWithGlobalConn.getBatchSize());
        assertEquals("num operations not as expected: ",
                1000, loadProfileWithGlobalConn.getNumOperations());
        assertEquals("tenant group size is not as expected: ",
                1, loadProfileWithGlobalConn.getTenantDistribution().size());
        assertEquals("global tenant is not as expected: ",
                1, loadProfileWithGlobalConn.getTenantDistribution().get(0).getNumTenants());
        assertEquals("global tenant id is not as expected: ",
                "GLOBAL", loadProfileWithGlobalConn.getTenantDistribution().get(0).getId());
        assertEquals("global tenant weight is not as expected: ",
                100, loadProfileWithGlobalConn.getTenantDistribution().get(0).getWeight());
        assertEquals("operation group size is not as expected: ",
                1,loadProfileWithGlobalConn.getOpDistribution().size());
        assertEquals("UpsertSet size is not as expected ",
                1, testScenarioWithGlobalConn.getUpserts().size());
        assertEquals("#Column within the first upsert is not as expected ",
                7, testScenarioWithGlobalConn.getUpserts().get(0).getColumn().size());
        assertEquals("Upsert operation not using global connection as expected ",
                true, testScenarioWithGlobalConn.getUpserts().get(0).isUseGlobalConnection());
    }

    private URL getResourceUrl(String resourceName) {
        URL resourceUrl = getClass().getResource(resourceName);
        assertNotNull("Test data XML file is missing", resourceUrl);
        return resourceUrl;
    }

    private List<Scenario> getScenarios(String resourceName) throws Exception {
        DataModel data = getDataModel(resourceName);
        List<Scenario> scenarioList = data.getScenarios();
        assertTrue("Could not load the scenarios from xml.",
                (scenarioList != null) && (scenarioList.size() > 0));
        return scenarioList;
    }

    private DataModel getDataModel(String resourceName) throws Exception {
        Path resourcePath = Paths.get(getResourceUrl(resourceName).toURI());
        return XMLConfigParser.readDataModel(resourcePath);
    }

    private void assertDateValue(List<Column> dataMappingColumns) {
        for (Column dataMapping : dataMappingColumns) {
            if ((dataMapping.getType() == DataTypeMapping.DATE) && (dataMapping.getName()
                    .equals("SOME_DATE"))) {
                // First rule should have min/max set
                assertNotNull(dataMapping.getDataValues().get(0).getMinValue());
                assertNotNull(dataMapping.getDataValues().get(0).getMaxValue());

                // Second rule should have only value set
                assertNotNull(dataMapping.getDataValues().get(1).getValue());

                // Third rule should have min/max set
                assertNotNull(dataMapping.getDataValues().get(2).getMinValue());
                assertNotNull(dataMapping.getDataValues().get(2).getMaxValue());
                return;
            }
        }
        fail("We should have found a Rule value that matched.");
    }

    private void assertCurrentDateValue(List<Column> dataMappingColumns) {
        for (Column dataMapping : dataMappingColumns) {
            if ((dataMapping.getType() == DataTypeMapping.DATE) && (dataMapping.getName()
                    .equals("PRESENT_DATE"))) {
                //First rule should have use current date value set
                assertNotNull(dataMapping.getDataValues().get(0).getUseCurrentDate());

                //Second rule should have use current date value set
                assertNotNull(dataMapping.getDataValues().get(1).getUseCurrentDate());
                return;
            }
        }
        fail("We should have found a Rule value that matched.");
    }

    /*
        Used for debugging to dump out a simple xml filed based on the bound objects.
     */
    private String writeXML() {
        DataModel data = new DataModel();
        try {
            DataValue dataValue = new DataValue();
            dataValue.setDistribution(20);
            dataValue.setValue("jnhgGhHminwiajn");
            List<DataValue> dataValueList = new ArrayList<>();
            dataValueList.add(dataValue);
            Column column = new Column();
            column.setLength(15);
            column.setDataSequence(DataSequence.RANDOM);
            column.setName("TEST_COL");
            column.setUserDefined(true);
            column.setDataValues(dataValueList);
            List<Column> columnList = new ArrayList<>();
            columnList.add(column);

            data.setDataMappingColumns(columnList);

            Scenario scenario = new Scenario();
            scenario.setName("scenario1");
            scenario.setTenantId("00DXXXXXX");
        	List<Ddl> preScenarioDdls = new ArrayList<Ddl>();
        	preScenarioDdls.add(new Ddl("CREATE INDEX IF NOT EXISTS ? ON FHA (NEWVAL_NUMBER) ASYNC", "FHAIDX_NEWVAL_NUMBER"));
        	preScenarioDdls.add(new Ddl("CREATE LOCAL INDEX IF NOT EXISTS ? ON FHA (NEWVAL_NUMBER)", "FHAIDX_NEWVAL_NUMBER"));
			scenario.setPreScenarioDdls(preScenarioDdls);
            scenario.setPhoenixProperties(new HashMap<String, String>());
            scenario.getPhoenixProperties().put("phoenix.query.threadPoolSize", "200");
            scenario.setDataOverride(new DataOverride());
            scenario.setTableName("tableName");
            scenario.setRowCount(10);
            QuerySet querySet = new QuerySet();
            querySet.setExecutionType(ExecutionType.PARALLEL);
            querySet.setExecutionDurationInMs(10000);
            scenario.getQuerySet().add(querySet);
            Query query = new Query();
            querySet.getQuery().add(query);
            querySet.setConcurrency("15");
            querySet.setNumberOfExecutions(20);
            query.setStatement("select * from FHA");
            Scenario scenario2 = new Scenario();
            scenario2.setName("scenario2");
            scenario2.setPhoenixProperties(new HashMap<String, String>());
            scenario2.setDataOverride(new DataOverride());
            scenario2.setTableName("tableName2");
            scenario2.setRowCount(500);
            List<Scenario> scenarios = new ArrayList<Scenario>();
            scenarios.add(scenario);
            scenarios.add(scenario2);
            data.setScenarios(scenarios);

            // create JAXB context and initializing Marshaller
            JAXBContext jaxbContext = JAXBContext.newInstance(DataModel.class);
            Marshaller jaxbMarshaller = jaxbContext.createMarshaller();

            // for getting nice formatted output
            jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

            // Writing to console
            jaxbMarshaller.marshal(data, System.out);
        } catch (JAXBException e) {
            // some exception occured
            e.printStackTrace();
        }
        return data.toString();
    }
}
