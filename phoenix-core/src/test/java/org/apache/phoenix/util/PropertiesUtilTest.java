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
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

import org.junit.Test;

public class PropertiesUtilTest {

    private static final String SOME_TENANT_ID = "00Dxx0000001234";
    private static final String SOME_OTHER_PROPERTY_KEY = "some_other_property";
    private static final String SOME_OTHER_PROPERTY_VALUE = "some_other_value";
    
    @Test
    public void testCopy() throws Exception{
        final Properties propsWithTenant = new Properties();
        propsWithTenant.put(PhoenixRuntime.TENANT_ID_ATTRIB, SOME_TENANT_ID);

        verifyValidCopy(propsWithTenant);
    }

    @Test
    public void testCopyOnWrappedProperties() throws Exception{
        final Properties propsWithTenant = new Properties();
        propsWithTenant.put(PhoenixRuntime.TENANT_ID_ATTRIB, SOME_TENANT_ID);

        verifyValidCopy(new Properties(propsWithTenant));
    }

    @Test
    public void testCopyFromConfiguration() throws Exception{
        //make sure that we don't only copy the ZK quorum, but all
        //properties
        final Configuration conf = HBaseConfiguration.create();
        final Properties props = new Properties();
        
        conf.set(HConstants.ZOOKEEPER_QUORUM, HConstants.LOCALHOST);
        conf.set(PropertiesUtilTest.SOME_OTHER_PROPERTY_KEY, 
                PropertiesUtilTest.SOME_OTHER_PROPERTY_VALUE);
        PropertiesUtil.extractProperties(props, conf);
        assertEquals(props.getProperty(HConstants.ZOOKEEPER_QUORUM),
                conf.get(HConstants.ZOOKEEPER_QUORUM));
        assertEquals(props.getProperty(PropertiesUtilTest.SOME_OTHER_PROPERTY_KEY),
                conf.get(PropertiesUtilTest.SOME_OTHER_PROPERTY_KEY));
    }
    private void verifyValidCopy(Properties props) throws SQLException {

        Properties copy = PropertiesUtil.deepCopy(props);
        copy.containsKey(PhoenixRuntime.TENANT_ID_ATTRIB); //This checks the map and NOT the defaults in java.util.Properties
        assertEquals(SOME_TENANT_ID, copy.getProperty(PhoenixRuntime.TENANT_ID_ATTRIB));
    }
}
