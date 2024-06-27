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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Base class for tests that run with split SYSTEM.CATALOG.
 * 
 */
public abstract class SplitSystemCatalogIT extends BaseTest {

    protected static String SCHEMA1 = "SCHEMA1";
    protected static String SCHEMA2 = "SCHEMA2";
    protected static String SCHEMA3 = "SCHEMA3";
    protected static String SCHEMA4 = "SCHEMA4";

    protected static String TENANT1 = "tenant1";
    protected static String TENANT2 = "tenant2";

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
       doSetup(null);
    }

    public static synchronized void doSetup(Map<String, String> props)
            throws Exception {
        NUM_SLAVES_BASE = 6;
        if (props == null) {
            props = Collections.emptyMap();
        }
        boolean splitSystemCatalog = (driver == null);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        // Split SYSTEM.CATALOG once after the mini-cluster is started
        if (splitSystemCatalog) {
            // splitSystemCatalog is incompatible with the balancer chore
            getUtility().getHBaseCluster().getMaster().balanceSwitch(false);
            splitSystemCatalog();
        }
    }
    
    protected static void splitSystemCatalog() throws Exception {
        try (Connection ignored = DriverManager.getConnection(getUrl())) {
        }
        String tableName = "TABLE";
        String fullTableName1 = SchemaUtil.getTableName(SCHEMA1, tableName);
        String fullTableName2 = SchemaUtil.getTableName(SCHEMA2, tableName);
        String fullTableName3 = SchemaUtil.getTableName(SCHEMA3, tableName);
        String fullTableName4 = SchemaUtil.getTableName(SCHEMA4, tableName);
        ArrayList<String> tableList = Lists.newArrayList(fullTableName1,
                fullTableName2, fullTableName3);
        Map<String, List<String>> tenantToTableMap = Maps.newHashMap();
        tenantToTableMap.put(null, tableList);
        tenantToTableMap.put(TENANT1, Lists.newArrayList(fullTableName2,
                fullTableName3));
        tenantToTableMap.put(TENANT2, Lists.newArrayList(fullTableName4));
        splitSystemCatalog(tenantToTableMap);
    }

}
