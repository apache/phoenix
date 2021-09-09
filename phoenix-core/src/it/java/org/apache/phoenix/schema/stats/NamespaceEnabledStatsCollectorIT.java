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
package org.apache.phoenix.schema.stats;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

@Category(NeedsOwnMiniClusterTest.class)
public class NamespaceEnabledStatsCollectorIT extends BaseStatsCollectorIT {

    public NamespaceEnabledStatsCollectorIT(boolean userTableNamespaceMapped, boolean collectStatsOnSnapshot) {
        super(userTableNamespaceMapped, collectStatsOnSnapshot);
    }

    @Parameterized.Parameters(name = "userTableNamespaceMapped={0},collectStatsOnSnapshot={1}")
    public static Collection<Object[]> provideData() {
        return Arrays.asList(
                new Object[][] {
                        // Collect stats on snapshots using UpdateStatisticsTool
                        { true, true },
                        // Collect stats via `UPDATE STATISTICS` SQL
                        { true, false }
                }
        );
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        // enable name space mapping at global level on both client and server side
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(7);
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        clientProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

}
