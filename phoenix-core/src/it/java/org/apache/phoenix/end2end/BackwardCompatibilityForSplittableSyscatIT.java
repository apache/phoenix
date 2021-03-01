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

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.MavenCoordinates;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;

import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.CREATE_TMP_TABLE;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.QUERY_DELETE_FOR_SPLITTABLE_SYSCAT;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.QUERY_SELECT_AND_DROP_TABLE;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.assertExpectedOutput;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.checkForPreConditions;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.computeClientVersions;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.executeQueriesWithCurrentVersion;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.executeQueryWithClientVersion;
import static org.apache.phoenix.end2end.BackwardCompatibilityTestUtil.UpgradeProps.NONE;

/**
 * This class is meant for specifically testing syscat with all compatible client versions.
 */

@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class BackwardCompatibilityForSplittableSyscatIT extends SplitSystemCatalogIT {
    private final MavenCoordinates compatibleClientVersion;
    private String zkQuorum;
    private String url;

    @Parameterized.Parameters(name = "BackwardCompatibilityForSplitableSyscatIT_compatibleClientVersion={0}")
    public static synchronized Collection<MavenCoordinates> data() throws Exception {
        return computeClientVersions();
    }

    public BackwardCompatibilityForSplittableSyscatIT(MavenCoordinates compatibleClientVersion) {
        this.compatibleClientVersion = compatibleClientVersion;
    }

    @Before
    public synchronized void setup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        doSetup(serverProps);
        zkQuorum = "localhost:" + getZKClientPort(config);
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        checkForPreConditions(compatibleClientVersion, config);
    }

    @Test
    public void testSplittableSyscatWithOldClientForAddingDataAndDelete() throws Exception {
        executeQueryWithClientVersion(compatibleClientVersion,
                QUERY_DELETE_FOR_SPLITTABLE_SYSCAT, zkQuorum);
        assertExpectedOutput(QUERY_DELETE_FOR_SPLITTABLE_SYSCAT);
    }

    @Test
    public void testSplittableSyscatWithNewClientForAddingDataAndDelete() throws Exception {
        executeQueriesWithCurrentVersion(QUERY_DELETE_FOR_SPLITTABLE_SYSCAT, url, NONE);
        assertExpectedOutput(QUERY_DELETE_FOR_SPLITTABLE_SYSCAT);
    }

    @Test
    public void testSplittableSyscatWithOldClientLoadDataAndNewClientQueryAndDelete() throws Exception {
        executeQueryWithClientVersion(compatibleClientVersion, CREATE_TMP_TABLE, zkQuorum);
        executeQueriesWithCurrentVersion(QUERY_SELECT_AND_DROP_TABLE, url, NONE);
        assertExpectedOutput(QUERY_SELECT_AND_DROP_TABLE);
    }

    @Test
    public void testSplittableSyscatWithNewClientLoadDataAndOldClientQueryAndDelete() throws Exception {
        executeQueriesWithCurrentVersion(CREATE_TMP_TABLE, url, NONE);
        executeQueryWithClientVersion(compatibleClientVersion, QUERY_SELECT_AND_DROP_TABLE, zkQuorum);
        assertExpectedOutput(QUERY_SELECT_AND_DROP_TABLE);
    }
}
