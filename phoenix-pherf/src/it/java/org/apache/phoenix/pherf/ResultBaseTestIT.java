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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.result.ResultUtil;
import org.apache.phoenix.pherf.schema.SchemaReader;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.MultiTenantTestUtils;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

public abstract class ResultBaseTestIT extends ParallelStatsDisabledIT {
    protected static final String matcherScenario = ".*scenario/.*test_scenario.xml";
    protected static final String matcherSchema = ".*datamodel/.*test_schema.sql";

    protected static PhoenixUtil util = PhoenixUtil.create(true);
    protected static Properties properties;
    protected static SchemaReader reader;
    protected static XMLConfigParser parser;
    protected static List<Path> resources;
    protected static ResultUtil resultUtil = new ResultUtil();

    protected static Configuration getTestClusterConfig() {
        // don't want callers to modify config.
        return new Configuration(config);
    }

    @BeforeClass public static synchronized void setUp() throws Exception {

        PherfConstants constants = PherfConstants.create();
        properties = constants.getProperties(PherfConstants.PHERF_PROPERTIES, false);
        String dir = properties.getProperty("pherf.default.results.dir");
        resultUtil.ensureBaseDirExists(dir);

        reader = new SchemaReader(util, matcherSchema);
        parser = new XMLConfigParser(matcherScenario);

        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);

        PhoenixUtil.setZookeeper(MultiTenantTestUtils.getZookeeperFromUrl(url));
    }

    @AfterClass public static synchronized void tearDown() throws Exception {
        dropNonSystemTables();
        resultUtil.deleteDir(properties.getProperty("pherf.default.results.dir"));
    }

    @After
    public void cleanUpAfterTest() throws Exception {
        deletePriorMetaData(HConstants.LATEST_TIMESTAMP, getUrl());
    }
}
