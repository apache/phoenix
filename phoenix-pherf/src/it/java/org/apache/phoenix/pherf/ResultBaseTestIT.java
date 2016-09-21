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

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.result.ResultUtil;
import org.apache.phoenix.pherf.schema.SchemaReader;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ResultBaseTestIT extends BaseHBaseManagedTimeIT {
    protected static final String matcherScenario = ".*scenario/.*test.*xml";
    protected static final String matcherSchema = ".*datamodel/.*test.*sql";

    protected static PhoenixUtil util = PhoenixUtil.create(true);
    protected static Properties properties;
    protected static SchemaReader reader;
    protected static XMLConfigParser parser;
    protected static List<Path> resources;
    protected static ResultUtil resultUtil = new ResultUtil();

    @BeforeClass public static void setUp() throws Exception {

        PherfConstants constants = PherfConstants.create();
        properties = constants.getProperties(PherfConstants.PHERF_PROPERTIES, false);
        String dir = properties.getProperty("pherf.default.results.dir");
        resultUtil.ensureBaseDirExists(dir);

        util.setZookeeper("localhost");
        reader = new SchemaReader(util, matcherSchema);
        parser = new XMLConfigParser(matcherScenario);
    }
    
    @AfterClass public static void tearDown() throws Exception {
    	resultUtil.deleteDir(properties.getProperty("pherf.default.results.dir"));
    }
}
