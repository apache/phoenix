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
package org.apache.phoenix.query;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class PropertyPolicyProviderTest extends BaseConnectionlessQueryTest{
    @Test
    public void testPropertyPolicyProvider() {
        PropertyPolicy provided = PropertyPolicyProvider.getPropertyPolicy();
        assertTrue(provided instanceof TestPropertyPolicy);
    }

    @Test(expected = PropertyNotAllowedException.class)
    public void testPropertyPolicyBlacklisted() throws SQLException {
        Properties properties=new Properties();
        properties.put("DisallowedProperty","value");
        try(Connection conn = DriverManager.getConnection(getUrl(),properties);
        ){}
    }

    @Test
    public void testPropertyPolicyWhitelisted() throws SQLException {
        Properties properties=new Properties();
        properties.put("allowedProperty","value");
        try(
        Connection conn = DriverManager.getConnection(getUrl(),properties);
        ){}
    }

    @Test
    public void testDisablePropertyPolicyProvider() throws SQLException {
        Properties properties=new Properties();
        properties.put("DisallowedProperty","value");
        properties.put(QueryServices.PROPERTY_POLICY_PROVIDER_ENABLED, "false");
        try(
                Connection conn = DriverManager.getConnection(getUrl(), properties)
        ){}
    }
}
