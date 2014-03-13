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
package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

public class PhoenixDriverTest extends BaseConnectionlessQueryTest {


    @Test
    public void testFirstConnectionWhenPropsHasTenantId() throws Exception {
        final String url = getUrl();
        verifyConnectionValid(url);
    }

    private void verifyConnectionValid(String url) throws SQLException {
        Driver driver = DriverManager.getDriver(url);

        Properties props = new Properties();
        final String tenantId = "00Dxx0000001234";
        props.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);

        Connection connection = driver.connect(url, props);
        assertEquals(tenantId, connection.getClientInfo(PhoenixRuntime.TENANT_ID_ATTRIB));
    }
}
