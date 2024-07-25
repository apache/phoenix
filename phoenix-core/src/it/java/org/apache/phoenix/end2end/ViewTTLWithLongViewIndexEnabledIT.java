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

import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

@Category(NeedsOwnMiniClusterTest.class)
public class ViewTTLWithLongViewIndexEnabledIT extends BaseViewTTLIT {

    @BeforeClass
    public static final void doSetup() throws Exception {
        // Turn on the TTL feature
        Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>() {{
            put(QueryServices.PHOENIX_TABLE_TTL_ENABLED, String.valueOf(true));
            put(QueryServices.LONG_VIEW_INDEX_ENABLED_ATTRIB, String.valueOf(true));
            put("hbase.procedure.remote.dispatcher.delay.msec", "0");
            // no max lookback
            put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(0));
            put(QueryServices.PHOENIX_VIEW_TTL_ENABLED, Boolean.toString(true));
            put(QueryServices.PHOENIX_VIEW_TTL_TENANT_VIEWS_PER_SCAN_LIMIT, String.valueOf(1));
        }};

        setUpTestDriver(new ReadOnlyProps(ReadOnlyProps.EMPTY_PROPS,
                DEFAULT_PROPERTIES.entrySet().iterator()));
    }

    @Test
    public void testMajorCompactWithSimpleIndexedBaseTables() throws Exception {
        super.testMajorCompactWithSimpleIndexedBaseTables();
    }

    @Test
    public void testMajorCompactFromMultipleGlobalIndexes() throws Exception {
        super.testMajorCompactFromMultipleGlobalIndexes();
    }

    @Test
    public void testMajorCompactFromMultipleTenantIndexes() throws Exception {
        super.testMajorCompactFromMultipleTenantIndexes();
    }
    @Test
    public void testMajorCompactWithOnlyTenantView() throws Exception {
        super.testMajorCompactWithOnlyTenantView();
    }
    @Test
    public void testMajorCompactWithSaltedIndexedBaseTables() throws Exception {
        super.testMajorCompactWithSaltedIndexedBaseTables();
    }
    @Test
    public void testMajorCompactWithSaltedIndexedTenantView() throws Exception {
        super.testMajorCompactWithSaltedIndexedTenantView();
    }

    @Test
    public void testMajorCompactWithVariousViewsAndOptions() throws Exception {
        super.testMajorCompactWithVariousViewsAndOptions();
    }
    @Test
    public void testMajorCompactWithVariousTenantIdTypesAndRegions() throws Exception {
        super.testMajorCompactWithVariousTenantIdTypesAndRegions(PVarchar.INSTANCE);
        super.testMajorCompactWithVariousTenantIdTypesAndRegions(PInteger.INSTANCE);
        super.testMajorCompactWithVariousTenantIdTypesAndRegions(PLong.INSTANCE);
    }
    @Test
    public void testMajorCompactWhenTTLSetForSomeTenants() throws Exception {
        super.testMajorCompactWhenTTLSetForSomeTenants();
    }
    @Test
    public void testTenantViewsWIthOverlappingRowPrefixes() throws Exception {
        super.testTenantViewsWIthOverlappingRowPrefixes();
    }
    @Test
    public void testMajorCompactWithGlobalAndTenantViewHierarchy() throws Exception {
        super.testMajorCompactWithGlobalAndTenantViewHierarchy();
    }
}
