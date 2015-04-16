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

import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;

public class BaseTestWithCluster {
    static final String matcherScenario = PherfConstants.SCENARIO_ROOT_PATTERN + ".xml";
    private static final Logger logger = LoggerFactory.getLogger(BaseTestWithCluster.class);
    protected static PhoenixUtil util;

    @BeforeClass
    public static void initQuorum() {
        util = new PhoenixUtil();
        String zookeeper = ((System.getProperty("ZK_QUORUM") == null) || System.getProperty("ZK_QUORUM").equals("")) ? "localhost" : System.getProperty("ZK_QUORUM");
        PhoenixUtil.setZookeeper(zookeeper);
        logger.info("Using quorum:" + zookeeper);
    }

    /**
     * Get the configuration for what scenarios will run and how.
     *
     * @return {@link java.nio.file.Path}
     */
    public Path getTestConfiguration() {
        URL resourceUrl = getUrl();
        assertNotNull("Test data XML file is missing", resourceUrl);
        Path resourcePath = null;
        try {
            resourcePath = Paths.get(resourceUrl.toURI());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return resourcePath;
    }

    public URL getUrl() {
        return getClass().getResource("/scenario/test_scenario.xml");
    }

}
