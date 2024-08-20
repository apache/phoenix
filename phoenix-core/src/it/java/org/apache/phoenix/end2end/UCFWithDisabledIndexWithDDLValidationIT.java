/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

/**
 * Tests that use UPDATE_CACHE_FREQUENCY with some of the disabled index states that require
 * clients to override UPDATE_CACHE_FREQUENCY and perform metadata calls to retrieve PTable.
 * The cluster is brought up with required configs at client and server side to enable
 * metadata caching redesign.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class UCFWithDisabledIndexWithDDLValidationIT extends UCFWithDisabledIndexIT {

    private static void initCluster() throws Exception {
        Map<String, String> props = Maps.newConcurrentMap();
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(60 * 60 * 1000));
        props.put(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB, "ALWAYS");
        props.put(QueryServices.LAST_DDL_TIMESTAMP_VALIDATION_ENABLED, Boolean.toString(true));
        props.put(QueryServices.PHOENIX_METADATA_INVALIDATE_CACHE_ENABLED, Boolean.toString(true));
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        initCluster();
    }

    @Test
    public void testUcfWithNoGetTableCalls() throws Throwable {
        // Uncomment with PHOENIX-7381
        //super.testUcfWithNoGetTableCalls();
    }

    @Test
    public void testUcfWithDisabledIndex1() throws Throwable {
        // Uncomment with PHOENIX-7381
        //super.testUcfWithDisabledIndex1();
    }

    @Test
    public void testUcfWithDisabledIndex2() throws Throwable {
        // Uncomment with PHOENIX-7381
        //super.testUcfWithDisabledIndex2();
    }

    @Test
    public void testUcfWithDisabledIndex3() throws Throwable {
        // Uncomment with PHOENIX-7381
        //super.testUcfWithDisabledIndex3();
    }

}
