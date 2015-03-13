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

import org.apache.phoenix.pherf.configuration.*;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.workload.WorkloadExecutor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import static org.junit.Assert.*;

public class ConfigurationParserTest {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationParserTest.class);

    @Test
    public void testConfigFilesParsing() {
        try {
        	WorkloadExecutor workloadExec = new WorkloadExecutor();
            List<Scenario> scenarioList = workloadExec.getParser().getScenarios();
            assertTrue("Could not load the scenarios from xml.", (scenarioList != null) && (scenarioList.size() > 0));
            logger.info("Number of scenarios loaded: " + scenarioList.size());

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

	@Test
    // TODO Break this into multiple smaller tests.
    public void testConfigReader(){
		URL resourceUrl = getClass().getResource("/scenario/test_scenario.xml");
        assertNotNull("Test data XML file is missing", resourceUrl);

		try {
//            writeXML();
			Path resourcePath = Paths.get(resourceUrl.toURI());
            DataModel data = XMLConfigParser.readDataModel(resourcePath);
            List<Scenario> scenarioList = data.getScenarios();
            assertTrue("Could not load the scenarios from xml.", (scenarioList != null) && (scenarioList.size() > 0));
            List<Column> dataMappingColumns = data.getDataMappingColumns();
            assertTrue("Could not load the data columns from xml.", (dataMappingColumns != null) && (dataMappingColumns.size() > 0));
            assertTrue("Could not load the data DataValue list from xml.",
                    (dataMappingColumns.get(6).getDataValues() != null)
                    && (dataMappingColumns.get(6).getDataValues().size() > 0));

            assertDateValue(dataMappingColumns);

            // Validate column mappings
            for (Column column : dataMappingColumns) {
                assertNotNull("Column ("+ column.getName() + ") is missing its type",column.getType());
            }

            Scenario scenario = scenarioList.get(0);
            assertNotNull(scenario);
            assertEquals("PHERF.TEST_TABLE", scenario.getTableName());
            assertEquals(50, scenario.getRowCount());
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
            assertEquals(0, (long)firstQuery.getExpectedAggregateRowCount());

            Query secondQuery = qs.getQuery().get(1);
            assertEquals("Could not get statement.", "select sum(SOME_INT) from PHERF.TEST_TABLE", secondQuery.getStatement());
            assertEquals("Could not get queryGroup.", "g1", secondQuery.getQueryGroup());

            // Make sure anything in the overrides matches a real column in the data mappings
            DataOverride override = scenario.getDataOverride();
            for (Column column : override.getColumn()) {
                assertTrue("Could not lookup Column (" + column.getName() + ") in DataMapping columns: " + dataMappingColumns, dataMappingColumns.contains(column));
            }

		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

    private void assertDateValue(List<Column> dataMappingColumns) {
        for (Column dataMapping : dataMappingColumns) {
            if ((dataMapping.getType() == DataTypeMapping.DATE) && (dataMapping.getName().equals("CREATED_DATE"))) {
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

    /*
        Used for debugging to dump out a simple xml filed based on the bound objects.
     */
	private void writeXML() {
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

            DataModel data = new DataModel();
            data.setRelease("192");
            data.setDataMappingColumns(columnList);

            Scenario scenario = new Scenario();
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
    }
}
