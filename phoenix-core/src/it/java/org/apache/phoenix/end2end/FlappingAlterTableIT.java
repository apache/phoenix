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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class FlappingAlterTableIT extends ParallelStatsDisabledIT {
    private String dataTableFullName;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        /**
         * This test checks Table properties at ColumnFamilyDescriptor level, turing phoenix_table_ttl
         * to false for them to test TTL and other props at HBase level. TTL being set at phoenix level
         * is being tested in {@link TTLAsPhoenixTTLIT}
         */
        props.put(QueryServices.PHOENIX_TABLE_TTL_ENABLED, Boolean.toString(false));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Before
    public void setupTableNames() throws Exception {
        String schemaName = "";
        String dataTableName = generateUniqueName();
        dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
    }

    @Test
    public void testAddColumnForNewColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2))";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " ADD CF.STRING VARCHAR";
        conn1.createStatement().execute(ddl);
        try (Admin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            ColumnFamilyDescriptor[] columnFamilies = admin.getDescriptor(TableName.valueOf(dataTableFullName)).getColumnFamilies();
            assertEquals(2, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_TTL, columnFamilies[0].getTimeToLive());
            assertEquals("CF", columnFamilies[1].getNameAsString());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_TTL, columnFamilies[1].getTimeToLive());
        }
    }

    @Test
    public void testNewColumnFamilyInheritsTTLOfEmptyCF() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) TTL = 1000";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " ADD CF.STRING VARCHAR";
        conn1.createStatement().execute(ddl);
        try (Admin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
            ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            assertEquals(2, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(1000, columnFamilies[0].getTimeToLive());
            assertEquals("CF", columnFamilies[1].getNameAsString());
            assertEquals(1000, columnFamilies[1].getTimeToLive());
        }
    }


}
