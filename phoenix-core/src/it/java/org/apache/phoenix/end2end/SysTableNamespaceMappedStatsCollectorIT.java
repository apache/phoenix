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

import java.util.Map;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;

import com.google.common.collect.Maps;

public class SysTableNamespaceMappedStatsCollectorIT extends StatsCollectorIT {
    
    public SysTableNamespaceMappedStatsCollectorIT(boolean mutable, boolean transactional, boolean userTableNamespaceMapped, boolean columnEncoded) {
        super(mutable, transactional, userTableNamespaceMapped, columnEncoded);
    }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        // enable name space mapping at global level on both client and server side
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(7);
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        serverProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        serverProps.put(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, "true");
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        clientProps.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        clientProps.put(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE, "true");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }


}
