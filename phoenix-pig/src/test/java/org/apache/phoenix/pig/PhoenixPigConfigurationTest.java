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

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


/**
 * Tests for PhoenixPigConfiguration. 
 *
 */
public class PhoenixPigConfigurationTest {

  
    @Test
    public void testBasicConfiguration() throws SQLException {
        Configuration conf = new Configuration();
        final PhoenixPigConfiguration phoenixConfiguration = new PhoenixPigConfiguration(conf);
        final String zkQuorum = "localhost";
        final String tableName = "TABLE";
        final long batchSize = 100;
        phoenixConfiguration.configure(zkQuorum, tableName, batchSize);
        assertEquals(zkQuorum,phoenixConfiguration.getServer());
        assertEquals(tableName,phoenixConfiguration.getTableName());
        assertEquals(batchSize,phoenixConfiguration.getBatchSize());
     }
    
   /* @Test
    public void testConfiguration() throws SQLException {
        Configuration configuration = new Configuration();
        final PhoenixPigConfiguration phoenixConfiguration = new PhoenixPigConfiguration(configuration);
        final String zkQuorum = "localhost";
        final String tableName = "TABLE";
        final long batchSize = 100;
        phoenixConfiguration.configure(zkQuorum, tableName, batchSize);
        PhoenixPigConfigurationUtil util = Mockito.mock(PhoenixPigConfigurationUtil.class);
        phoenixConfiguration.setUtil(util);
        phoenixConfiguration.getColumnMetadataList();
        Mockito.verify(util).getUpsertColumnMetadataList(configuration, tableName);
        Mockito.verifyNoMoreInteractions(util);
        
        phoenixConfiguration.getSelectStatement();
        Mockito.verify(util).getSelectStatement(configuration, tableName);
        Mockito.verifyNoMoreInteractions(util);
     }
    
    @Test
    public void testWithSpy() throws SQLException {
        Configuration configuration = new Configuration();
        final PhoenixPigConfiguration phoenixConfiguration = new PhoenixPigConfiguration(configuration);
        final String zkQuorum = "localhost";
        final String tableName = "TABLE";
        final long batchSize = 100;
        phoenixConfiguration.configure(zkQuorum, tableName, batchSize);
        phoenixConfiguration.setSelectStatement("SELECT 1 from TABLE");
        PhoenixPigConfigurationUtil util = new PhoenixPigConfigurationUtil();
        PhoenixPigConfigurationUtil spied = Mockito.spy(util);
        phoenixConfiguration.setUtil(spied);
          
        phoenixConfiguration.getSelectStatement();
        Mockito.verify(spied,Mockito.times(1)).getSelectStatement(configuration, tableName);
        Mockito.verifyNoMoreInteractions(util);
     }*/
}
