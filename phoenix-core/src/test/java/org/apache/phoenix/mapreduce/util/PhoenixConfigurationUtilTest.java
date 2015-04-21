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
package org.apache.phoenix.mapreduce.util;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.SchemaType;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

/**
 * Test for {@link PhoenixConfigurationUtil}
 */
public class PhoenixConfigurationUtilTest extends BaseConnectionlessQueryTest {
    private static final String ORIGINAL_CLUSTER_QUORUM = "myzookeeperhost";
    private static final String OVERRIDE_CLUSTER_QUORUM = "myoverridezookeeperhost";
    @Test
    public void testUpsertStatement() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
        final String tableName = "TEST_TABLE";
        try {
            String ddl = "CREATE TABLE "+ tableName + 
                    "  (a_string varchar not null, a_binary varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n";
            conn.createStatement().execute(ddl);
            final Configuration configuration = new Configuration ();
            configuration.set(HConstants.ZOOKEEPER_QUORUM, getUrl());
            PhoenixConfigurationUtil.setOutputTableName(configuration, tableName);
            final String upserStatement = PhoenixConfigurationUtil.getUpsertStatement(configuration);
            final String expectedUpsertStatement = "UPSERT INTO " + tableName + " VALUES (?, ?, ?)"; 
            assertEquals(expectedUpsertStatement, upserStatement);
        } finally {
            conn.close();
        }
     }

    @Test
    public void testSelectStatement() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
        final String tableName = "TEST_TABLE";
        try {
            String ddl = "CREATE TABLE "+ tableName + 
                    "  (a_string varchar not null, a_binary varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n";
            conn.createStatement().execute(ddl);
            final Configuration configuration = new Configuration ();
            configuration.set(HConstants.ZOOKEEPER_QUORUM, getUrl());
            PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
            final String selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration);
            final String expectedSelectStatement = "SELECT \"A_STRING\",\"A_BINARY\",\"0\".\"COL1\" FROM " + SchemaUtil.getEscapedArgument(tableName) ; 
            assertEquals(expectedSelectStatement, selectStatement);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelectStatementForSpecificColumns() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
        final String tableName = "TEST_TABLE";
        try {
            String ddl = "CREATE TABLE "+ tableName + 
                    "  (a_string varchar not null, a_binary varbinary not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n";
            conn.createStatement().execute(ddl);
            final Configuration configuration = new Configuration ();
            configuration.set(HConstants.ZOOKEEPER_QUORUM, getUrl());
            PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
            PhoenixConfigurationUtil.setSelectColumnNames(configuration, "A_BINARY");
            final String selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration);
            final String expectedSelectStatement = "SELECT \"A_BINARY\" FROM " + SchemaUtil.getEscapedArgument(tableName) ; 
            assertEquals(expectedSelectStatement, selectStatement);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSelectStatementForArrayTypes() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
        final String tableName = "TEST_TABLE";
        try {
            String ddl = "CREATE TABLE "+ tableName + 
                    "  (ID BIGINT NOT NULL PRIMARY KEY, VCARRAY VARCHAR[])\n";
            conn.createStatement().execute(ddl);
            final Configuration configuration = new Configuration ();
            configuration.set(HConstants.ZOOKEEPER_QUORUM, getUrl());
            PhoenixConfigurationUtil.setSelectColumnNames(configuration,"ID,VCARRAY");
            PhoenixConfigurationUtil.setSchemaType(configuration, SchemaType.QUERY);
            PhoenixConfigurationUtil.setInputTableName(configuration, tableName);
            final String selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration);
            final String expectedSelectStatement = "SELECT \"ID\",\"0\".\"VCARRAY\" FROM " + SchemaUtil.getEscapedArgument(tableName) ; 
            assertEquals(expectedSelectStatement, selectStatement);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testInputClusterOverride() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, ORIGINAL_CLUSTER_QUORUM);
        String zkQuorum = PhoenixConfigurationUtil.getInputCluster(configuration);
        assertEquals(zkQuorum, ORIGINAL_CLUSTER_QUORUM);

        configuration.set(PhoenixConfigurationUtil.MAPREDUCE_INPUT_CLUSTER_QUORUM,
            OVERRIDE_CLUSTER_QUORUM);
        String zkQuorumOverride = PhoenixConfigurationUtil.getInputCluster(configuration);
        assertEquals(zkQuorumOverride, OVERRIDE_CLUSTER_QUORUM);

        final Configuration configuration2 = new Configuration();
        PhoenixConfigurationUtil.setInputCluster(configuration2, OVERRIDE_CLUSTER_QUORUM);
        String zkQuorumOverride2 =
                PhoenixConfigurationUtil.getInputCluster(configuration2);
        assertEquals(zkQuorumOverride2, OVERRIDE_CLUSTER_QUORUM);

        final Job job = Job.getInstance();
        PhoenixMapReduceUtil.setInputCluster(job, OVERRIDE_CLUSTER_QUORUM);
        Configuration configuration3 = job.getConfiguration();
        String zkQuorumOverride3 =
                PhoenixConfigurationUtil.getInputCluster(configuration3);
        assertEquals(zkQuorumOverride3, OVERRIDE_CLUSTER_QUORUM);

    }

    @Test
    public void testOutputClusterOverride() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, ORIGINAL_CLUSTER_QUORUM);
        String zkQuorum = PhoenixConfigurationUtil.getOutputCluster(configuration);
        assertEquals(zkQuorum, ORIGINAL_CLUSTER_QUORUM);

        configuration.set(PhoenixConfigurationUtil.MAPREDUCE_OUTPUT_CLUSTER_QUORUM,
            OVERRIDE_CLUSTER_QUORUM);
        String zkQuorumOverride = PhoenixConfigurationUtil.getOutputCluster(configuration);
        assertEquals(zkQuorumOverride, OVERRIDE_CLUSTER_QUORUM);

        final Configuration configuration2 = new Configuration();
        PhoenixConfigurationUtil.setOutputCluster(configuration2, OVERRIDE_CLUSTER_QUORUM);
        String zkQuorumOverride2 =
                PhoenixConfigurationUtil.getOutputCluster(configuration2);
        assertEquals(zkQuorumOverride2, OVERRIDE_CLUSTER_QUORUM);

        final Job job = Job.getInstance();
        PhoenixMapReduceUtil.setOutputCluster(job, OVERRIDE_CLUSTER_QUORUM);
        Configuration configuration3 = job.getConfiguration();
        String zkQuorumOverride3 =
                PhoenixConfigurationUtil.getOutputCluster(configuration3);
        assertEquals(zkQuorumOverride3, OVERRIDE_CLUSTER_QUORUM);

    }
}
