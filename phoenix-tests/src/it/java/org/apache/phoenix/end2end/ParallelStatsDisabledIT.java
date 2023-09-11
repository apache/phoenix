/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.end2end;

import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.QueryBuilder;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;



/**
 * Base class for tests whose methods run in parallel with
 * 1. Statistics enabled on server side (QueryServices#STATS_COLLECTION_ENABLED is true)
 * 2. Guide Post Width for all relevant tables is 0. Stats are disabled at table level.
 *
 * See {@link org.apache.phoenix.schema.stats.NoOpStatsCollectorIT} for tests that disable
 * stats collection from server side.
 *
 * You must create unique names using {@link #generateUniqueName()} for each
 * table and sequence used to prevent collisions.
 */
public abstract class ParallelStatsDisabledIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @AfterClass
    public static synchronized void freeResources() throws Exception {
        BaseTest.freeResourcesIfBeyondThreshold();
    }

    protected ResultSet executeQueryThrowsException(Connection conn, QueryBuilder queryBuilder,
            String expectedPhoenixExceptionMsg, String expectedSparkExceptionMsg) {
        ResultSet rs = null;
        try {
            rs = executeQuery(conn, queryBuilder);
            fail();
        }
        catch(Exception e) {
            assertTrue(e.getMessage().contains(expectedPhoenixExceptionMsg));
        }
        return rs;
    }

    public static void validateQueryPlan(Connection conn, QueryBuilder queryBuilder, String expectedPhoenixPlan, String expectedSparkPlan) throws SQLException {
        if (StringUtils.isNotBlank(expectedPhoenixPlan)) {
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + queryBuilder.build());
            assertEquals(expectedPhoenixPlan, QueryUtil.getExplainPlan(rs));
        }
    }
}
