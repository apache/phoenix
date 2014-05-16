/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pig;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.HBaseManagedTimeTest;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(HBaseManagedTimeTest.class)
public class PhoenixPigConfigurationIT extends BaseHBaseManagedTimeIT {
    private static final String zkQuorum = TestUtil.LOCALHOST + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    
    @Test
    public void testUpsertStatement() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        final String tableName = "TEST_TABLE";
        try {
            String ddl = "CREATE TABLE "+ tableName + 
                    "  (a_string varchar not null, a_binary varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n";
            createTestTable(getUrl(), ddl);
            final PhoenixPigConfiguration configuration = newConfiguration (tableName);
            final String upserStatement = configuration.getUpsertStatement();
            final String expectedUpsertStatement = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)"; 
            assertEquals(expectedUpsertStatement, upserStatement);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelectStatement() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        final String tableName = "TEST_TABLE";
        try {
            String ddl = "CREATE TABLE "+ tableName + 
                    "  (a_string varchar not null, a_binary varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n";
            createTestTable(getUrl(), ddl);
            final PhoenixPigConfiguration configuration = newConfiguration (tableName);
            final String selectStatement = configuration.getSelectStatement();
            final String expectedSelectStatement = "SELECT \"A_STRING\",\"A_BINARY\",\"0\".\"COL1\" FROM " + SchemaUtil.getEscapedArgument(tableName) ; 
            assertEquals(expectedSelectStatement, selectStatement);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelectStatementForSpecificColumns() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        final String tableName = "TEST_TABLE";
        try {
            String ddl = "CREATE TABLE "+ tableName + 
                    "  (a_string varchar not null, a_binary varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n";
            createTestTable(getUrl(), ddl);
            final PhoenixPigConfiguration configuration = newConfiguration (tableName);
            configuration.setSelectColumns("a_binary");
            final String selectStatement = configuration.getSelectStatement();
            final String expectedSelectStatement = "SELECT \"A_BINARY\" FROM " + SchemaUtil.getEscapedArgument(tableName) ; 
            assertEquals(expectedSelectStatement, selectStatement);
        } finally {
            conn.close();
        }
    }

    private PhoenixPigConfiguration newConfiguration(String tableName) {
        final Configuration configuration = new Configuration();
        final PhoenixPigConfiguration phoenixConfiguration = new PhoenixPigConfiguration(configuration);
        phoenixConfiguration.configure(zkQuorum, tableName.toUpperCase(), 100);
        return phoenixConfiguration;
    }
}
