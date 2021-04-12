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

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.XMLConfigParserTest;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.schema.SchemaReader;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class MultiTenantViewOperationBaseIT extends ParallelStatsDisabledIT {
    protected static final String matcherScenario = ".*scenario/.*test_mt_workload.*xml";
    protected static final String matcherSchema = ".*datamodel/.*test_schema_mt*.*sql";

    protected static PhoenixUtil util = PhoenixUtil.create(true);
    protected static Properties properties;
    protected static SchemaReader reader;
    protected static XMLConfigParser parser;
    protected static List<Path> resources;

    @BeforeClass public static synchronized void setUp() throws Exception {
        PherfConstants constants = PherfConstants.create();
        properties = constants.getProperties(PherfConstants.PHERF_PROPERTIES, false);

        PhoenixUtil.setZookeeper("localhost");
        reader = new SchemaReader(util, matcherSchema);
        parser = new XMLConfigParser(matcherScenario);
        reader.applySchema();
        resources = new ArrayList<>(reader.getResourceList());

        assertTrue("Could not pull list of schema files.", resources.size() > 0);
        assertNotNull("Could not read schema file.", reader.resourceToString(resources.get(0)));

    }

    public DataModel readTestDataModel(String resourceName) throws Exception {
        URL scenarioUrl = XMLConfigParserTest.class.getResource(resourceName);
        assertNotNull(scenarioUrl);
        Path p = Paths.get(scenarioUrl.toURI());
        return XMLConfigParser.readDataModel(p);
    }

    @AfterClass public static synchronized void tearDown() throws Exception {
        dropNonSystemTables();
    }

}
