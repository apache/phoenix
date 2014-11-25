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

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;

import java.sql.SQLException;
import java.util.Map;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;

import com.google.common.collect.Maps;


public abstract class BaseTenantSpecificTablesIT extends BaseOwnClusterClientManagedTimeIT {
    protected static final String TENANT_ID = "ZZTop";
    protected static final String TENANT_TYPE_ID = "abc";
    protected static String PHOENIX_JDBC_TENANT_SPECIFIC_URL;
    protected static final String TENANT_ID2 = "Styx";
    protected static String PHOENIX_JDBC_TENANT_SPECIFIC_URL2;
    
    protected static final String PARENT_TABLE_NAME = "PARENT_TABLE";
    protected static final String PARENT_TABLE_DDL = "CREATE TABLE " + PARENT_TABLE_NAME + " ( \n" + 
            "                user VARCHAR ,\n" + 
            "                tenant_id VARCHAR(5) NOT NULL,\n" + 
            "                tenant_type_id VARCHAR(3) NOT NULL, \n" + 
            "                id INTEGER NOT NULL\n" + 
            "                CONSTRAINT pk PRIMARY KEY (tenant_id, tenant_type_id, id)) MULTI_TENANT=true, IMMUTABLE_ROWS=true";
    
    protected static final String TENANT_TABLE_NAME = "TENANT_TABLE";
    protected static final String TENANT_TABLE_DDL = "CREATE VIEW " + TENANT_TABLE_NAME + " ( \n" + 
            "                tenant_col VARCHAR) AS SELECT *\n" + 
            "                FROM " + PARENT_TABLE_NAME + " WHERE tenant_type_id= '" + TENANT_TYPE_ID + "'";
    
    protected static final String PARENT_TABLE_NAME_NO_TENANT_TYPE_ID = "PARENT_TABLE_NO_TENANT_TYPE_ID";
    protected static final String PARENT_TABLE_DDL_NO_TENANT_TYPE_ID = "CREATE TABLE " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID + " ( \n" + 
            "                user VARCHAR ,\n" + 
            "                tenant_id VARCHAR(5) NOT NULL,\n" + 
            "                id INTEGER NOT NULL,\n" + 
            "                CONSTRAINT pk PRIMARY KEY (tenant_id, id)) MULTI_TENANT=true, IMMUTABLE_ROWS=true";
    
    protected static final String TENANT_TABLE_NAME_NO_TENANT_TYPE_ID = "TENANT_TABLE_NO_TENANT_TYPE_ID";
    protected static final String TENANT_TABLE_DDL_NO_TENANT_TYPE_ID = "CREATE VIEW " + TENANT_TABLE_NAME_NO_TENANT_TYPE_ID + " ( \n" + 
            "                tenant_col VARCHAR) AS SELECT *\n" + 
            "                FROM " + PARENT_TABLE_NAME_NO_TENANT_TYPE_ID;
    
    
    @Before
    public void createTables() throws SQLException {
        createTestTable(getUrl(), PARENT_TABLE_DDL, null, nextTimestamp());
        createTestTable(getUrl(), PARENT_TABLE_DDL_NO_TENANT_TYPE_ID, null, nextTimestamp());
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_DDL, null, nextTimestamp());
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_DDL_NO_TENANT_TYPE_ID, null, nextTimestamp());
    }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        PHOENIX_JDBC_TENANT_SPECIFIC_URL = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + TENANT_ID;
        PHOENIX_JDBC_TENANT_SPECIFIC_URL2 = getUrl() + ';' + TENANT_ID_ATTRIB + '=' + TENANT_ID2;
    }
}
