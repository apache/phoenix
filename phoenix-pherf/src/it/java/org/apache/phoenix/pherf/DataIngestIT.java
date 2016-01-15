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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.pherf.PherfConstants.GeneratePhoenixStats;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.DataTypeMapping;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.QueryExecutor;
import org.apache.phoenix.pherf.workload.Workload;
import org.apache.phoenix.pherf.workload.WorkloadExecutor;
import org.apache.phoenix.pherf.workload.WriteWorkload;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.Outcome;

public class DataIngestIT extends ResultBaseTestIT {

    @Before
    public void applySchema() throws Exception {
        reader.applySchema();
        resources = new ArrayList<>(reader.getResourceList());

        assertTrue("Could not pull list of schema files.", resources.size() > 0);
        assertNotNull("Could not read schema file.", reader.resourceToString(resources.get(0)));
    }

    @Test
    public void testColumnRulesApplied() {

        Scenario scenario = null;
        try {
            scenario = parser.getScenarioByName("testScenario");
            List<Column>
                    columnListFromPhoenix =
                    util.getColumnsFromPhoenix(scenario.getSchemaName(),
                            scenario.getTableNameWithoutSchemaName(), util.getConnection());
            assertTrue("Could not get phoenix columns.", columnListFromPhoenix.size() > 0);

            WriteWorkload loader = new WriteWorkload(util, parser, scenario, GeneratePhoenixStats.NO);
            WorkloadExecutor executor = new WorkloadExecutor();
            executor.add(loader);
            executor.get();
            executor.shutdown();
            
            RulesApplier rulesApplier = loader.getRulesApplier();
            List<Map> modelList = rulesApplier.getModelList();
            assertTrue("Could not generate the modelList", modelList.size() > 0);

            for (Column column : columnListFromPhoenix) {
                DataValue data = rulesApplier.getDataForRule(scenario, column);

                // We are generating data values
                // so the value should have been specified by this point.
                assertTrue("Failed to retrieve data for column type: " + column.getType(),
                        data != null);

                // Test that we still retrieve the GENERAL_CHAR rule even after an override is
                // applied to another CHAR type. NEWVAL_STRING Column does not specify an override
                // so we should get the default rule.
                if ((column.getType() == DataTypeMapping.VARCHAR) && (column.getName()
                        .equals("NEWVAL_STRING"))) {
                    assertTrue("Failed to retrieve data for column type: ",
                            data.getDistribution() == Integer.MIN_VALUE);
                }
            }

            // Run some queries
            executor = new WorkloadExecutor();
            Workload query = new QueryExecutor(parser, util, executor);
            executor.add(query);
            executor.get();
            executor.shutdown();
            PhoenixUtil.create().deleteTables("ALL");
        } catch (Exception e) {
            fail("We had an exception: " + e.getMessage());
        }
    }

    @Test
    public void testRWWorkload() throws Exception {

        Connection connection = util.getConnection();

        WorkloadExecutor executor = new WorkloadExecutor();
        DataModel dataModel = parser.getDataModelByName("test_scenario");
        List<DataModel> dataModels = new ArrayList<>();
        dataModels.add(dataModel);
        QueryExecutor
                qe =
                new QueryExecutor(parser, util, executor, dataModels, null, false);
        executor.add(qe);
        Scenario scenario = parser.getScenarioByName("testScenarioRW");

        String sql = "select count(*) from " + scenario.getTableName();

        try {
            // Wait for data to load up.
            executor.get();
            executor.shutdown();

            // Verify data has been loaded
            Integer count = new JdbcSession(connection).sql(sql).select(new Outcome<Integer>() {
                @Override public Integer handle(ResultSet resultSet, Statement statement)
                        throws SQLException {
                    while (resultSet.next()) {
                        return resultSet.getInt(1);
                    }
                    return null;
                }
            });
            assertNotNull("Could not retrieve count. " + count);

            // It would be better to sum up all the rowcounts for the scenarios, but this is fine
            assertTrue("Could not query any rows for in " + scenario.getTableName(), count > 0);
        } catch (Exception e) {
            fail("Failed to load data. An exception was thrown: " + e.getMessage());
        }
    }


    @Test
    /**
     * Validates that Pherf can write data to a Multi-Tenant View in addition to 
     * standard Phoenix tables.
     */
    public void testMultiTenantViewWriteWorkload() throws Exception {
        // Arrange
        Scenario scenario = parser.getScenarioByName("testMTWriteScenario");
        WorkloadExecutor executor = new WorkloadExecutor();
        executor.add(new WriteWorkload(util, parser, scenario, GeneratePhoenixStats.NO));
        
        // Act
        try {
            // Wait for data to load up.
            executor.get();
            executor.shutdown();
        } catch (Exception e) {
            fail("Failed to load data. An exception was thrown: " + e.getMessage());
        }

        assertExpectedNumberOfRecordsWritten(scenario);
    }

    
    @Test
    public void testMultiTenantScenarioRunBeforeWriteWorkload() throws Exception {
        // Arrange
        Scenario scenario = parser.getScenarioByName("testMTDdlWriteScenario");
        WorkloadExecutor executor = new WorkloadExecutor();
        executor.add(new WriteWorkload(util, parser, scenario, GeneratePhoenixStats.NO));
        
        // Act
        try {
            // Wait for data to load up.
            executor.get();
            executor.shutdown();
        } catch (Exception e) {
            fail("Failed to load data. An exception was thrown: " + e.getMessage());
        }

        assertExpectedNumberOfRecordsWritten(scenario);
    }
    
    private void assertExpectedNumberOfRecordsWritten(Scenario scenario) throws Exception,
            SQLException {
        Connection connection = util.getConnection(scenario.getTenantId());
        String sql = "select count(*) from " + scenario.getTableName();
        Integer count = new JdbcSession(connection).sql(sql).select(new Outcome<Integer>() {
            @Override public Integer handle(ResultSet resultSet, Statement statement)
                    throws SQLException {
                while (resultSet.next()) {
                    return resultSet.getInt(1);
                }
                return null;
            }
        });
        assertNotNull("Could not retrieve count. " + count);
        assertEquals("Expected 100 rows to have been inserted",
                scenario.getRowCount(), count.intValue());
    }

}
