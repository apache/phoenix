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
import org.apache.phoenix.pherf.configuration.DataTypeMapping;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.loaddata.DataLoader;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.pherf.schema.SchemaReader;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataIngestTest extends BaseTestWithCluster {
    static final String matcherScenario = ".*scenario/.*test.*xml";
    static final String matcherSchema = ".*datamodel/.*test.*sql";

    @Test
    public void generateData() throws Exception {
        SchemaReader reader = new SchemaReader(matcherSchema);
        XMLConfigParser parser = new XMLConfigParser(matcherScenario);

        // 1. Generate table schema from file
        List<Path> resources = new ArrayList<>(reader.getResourceList());
        assertTrue("Could not pull list of schema files.", resources.size() > 0);
        assertNotNull("Could not read schema file.", reader.resourceToString(resources.get(0)));
        reader.applySchema();

        // 2. Load the metadata of for the test tables
        Scenario scenario = parser.getScenarios().get(0);
        List<Column> columnListFromPhoenix = util.getColumnsFromPhoenix(scenario.getSchemaName(), scenario.getTableNameWithoutSchemaName(), util.getConnection());
        assertTrue("Could not get phoenix columns.", columnListFromPhoenix.size() > 0);
        DataLoader loader = new DataLoader(parser);
        RulesApplier rulesApplier = loader.getRulesApplier();
        List<Map> modelList = rulesApplier.getModelList();
        assertTrue("Could not generate the modelList", modelList.size() > 0);

        for (Column column : columnListFromPhoenix) {
            DataValue data = rulesApplier.getDataForRule(scenario, column);

            // We are generating data values so the value should have been specified by this point.
            assertTrue("Failed to retrieve data for column type: " + column.getType(), data != null);

            // Test that we still retrieve the GENERAL_CHAR rule even after an override is applied to another CHAR type.
            // FIELD_HISTORY_ARCHIVE_ID Column does not  specify an override so we should get the default rule.
            if ((column.getType() == DataTypeMapping.CHAR) && (column.getName().equals("FIELD_HISTORY_ARCHIVE_ID"))) {
                assertTrue("Failed to retrieve data for column type: ", data.getDistribution() == Integer.MIN_VALUE);
            }
        }

        loader.execute();
    }
}
