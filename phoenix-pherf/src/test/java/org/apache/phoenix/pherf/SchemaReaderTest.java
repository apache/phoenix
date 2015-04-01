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

import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.schema.SchemaReader;
import org.junit.Test;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchemaReaderTest extends BaseTestWithCluster {

	@Test
    public void testSchemaReader() {
        // Test for the unit test version of the schema files.
        assertApplySchemaTest();
    }

    private void assertApplySchemaTest() {
        try {
            SchemaReader reader = new SchemaReader(".*datamodel/.*test.*sql");

            List<Path> resources = new ArrayList<>(reader.getResourceList());
            assertTrue("Could not pull list of schema files.", resources.size() > 0);
            assertNotNull("Could not read schema file.", this.getClass().getResourceAsStream(
                    PherfConstants.RESOURCE_DATAMODEL + "/" + resources.get(0).getFileName().toString()));
            assertNotNull("Could not read schema file.", reader.resourceToString(resources.get(0)));
            reader.applySchema();

            Connection connection = null;
            URL resourceUrl = getClass().getResource("/scenario/test_scenario.xml");
            assertNotNull("Test data XML file is missing", resourceUrl);
            connection = util.getConnection();
            Path resourcePath = Paths.get(resourceUrl.toURI());
            DataModel data = XMLConfigParser.readDataModel(resourcePath);
            List<Scenario> scenarioList = data.getScenarios();
            Scenario scenario = scenarioList.get(0);
            List<Column> columnList = util.getColumnsFromPhoenix(scenario.getSchemaName(), scenario.getTableNameWithoutSchemaName(), connection);
            assertTrue("Could not retrieve Metadata from Phoenix", columnList.size() > 0);
        } catch (Exception e) {
            fail("Could not initialize SchemaReader");
            e.printStackTrace();
        }
    }
}
