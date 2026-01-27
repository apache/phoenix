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


import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import org.apache.phoenix.mapreduce.index.IndexToolTableUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.mapreduce.index.IndexToolTableUtil.RESULT_TABLE_NAME;
import static org.apache.phoenix.query.QueryConstants.SYSTEM_SCHEMA_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


public class UpgradeIndexToolTablesIT extends LoadSystemTableSnapshotBase {
    protected String nameSpaceMapping = "true";

    @Before
    public synchronized void doSetup() throws Exception {
        setupCluster(nameSpaceMapping);
    }

    public synchronized void setupCluster(String nameSpaceMappingEnabled) throws Exception {
        HashMap<String, String> snapshotsToLoad = new HashMap<>();
        snapshotsToLoad.put("phoenixtoolresultsnapshot", "PHOENIX_INDEX_TOOL_RESULT");
        setupCluster(false, "indexToolsnapshot.tar.gz", "indexToolResultSnapshot/",  snapshotsToLoad, nameSpaceMappingEnabled);
    }

    @Test
    public void testPhoenixUpgradeIndexToolTables() throws Exception {
        try (Admin admin = utility.getAdmin()) {
            // we load the  RESULT_TABLE_NAME from snapshot
            assertTrue(admin.tableExists(TableName.valueOf(IndexToolTableUtil.RESULT_TABLE_NAME)));
            assertFalse(admin.tableExists(TableName.valueOf(IndexToolTableUtil.RESULT_TABLE_FULL_NAME)));
            // we don't load the OUTPUT_TABLE_NAME
            assertFalse(admin.tableExists(TableName.valueOf(IndexToolTableUtil.OUTPUT_TABLE_NAME)));
            assertFalse(admin.tableExists(TableName.valueOf(IndexToolTableUtil.OUTPUT_TABLE_FULL_NAME)));
        }

        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, nameSpaceMapping);
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, nameSpaceMapping);


        //Now we can start Phoenix
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet()
                .iterator()));
        assertTrue(true);


        // Check the IndexTool Tables after upgrade
        try (Admin admin = utility.getAdmin()) {
            assertFalse(admin.tableExists(TableName.valueOf(IndexToolTableUtil.OUTPUT_TABLE_NAME)));
            assertFalse(admin.tableExists(TableName.valueOf(IndexToolTableUtil.RESULT_TABLE_NAME)));
            assertTrue(admin.tableExists(TableName.valueOf(IndexToolTableUtil.OUTPUT_TABLE_FULL_NAME)));
            assertTrue(admin.tableExists(TableName.valueOf(IndexToolTableUtil.RESULT_TABLE_FULL_NAME)));
        }

        String tableName = SchemaUtil.getTableName(SYSTEM_SCHEMA_NAME, RESULT_TABLE_NAME);
        if (nameSpaceMapping.equals("true")) {
            assertEquals(IndexToolTableUtil.RESULT_TABLE_FULL_NAME, tableName.replace(QueryConstants.NAME_SEPARATOR,
                    QueryConstants.NAMESPACE_SEPARATOR));
        } else {
            assertEquals(IndexToolTableUtil.RESULT_TABLE_FULL_NAME, tableName);
        }

    }

}
