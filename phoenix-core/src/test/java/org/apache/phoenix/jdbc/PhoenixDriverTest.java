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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

public class PhoenixDriverTest extends BaseConnectionlessQueryTest {


    @Test
    public void testFirstConnectionWhenPropsHasTenantId() throws Exception {
        Properties props = new Properties();
        final String tenantId = "00Dxx0000001234";
        props.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);

        Connection connection = new PhoenixTestDriver().connect(getUrl(), props);
        assertEquals(tenantId, connection.getClientInfo(PhoenixRuntime.TENANT_ID_ATTRIB));
    }

    @Test
    public void testFirstConnectionWhenUrlHasTenantId() throws Exception {
        final String tenantId = "00Dxx0000001234";
        String url = getUrl() + ";" + PhoenixRuntime.TENANT_ID_ATTRIB + "=" + tenantId;
        Driver driver = new PhoenixTestDriver();

        driver.connect(url, new Properties());
    }

    @Test
    public void testMaxMutationSizeSetCorrectly() throws Exception {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB,"100");
        connectionProperties.setProperty(QueryServices.IMMUTABLE_ROWS_ATTRIB,"100");
        Connection connection = DriverManager.getConnection(getUrl(), connectionProperties);

        PreparedStatement stmt = connection.prepareStatement("upsert into " + ATABLE + " (organization_id, entity_id, a_integer) values (?,?,?)");
        try {
            for (int i = 0; i < 200; i++) {
                stmt.setString(1, "AAAA" + i);
                stmt.setString(2, "BBBB" + i);
                stmt.setInt(3, 1);
                stmt.execute();
            }
            fail("Upsert should have failed since the number of upserts (200) is greater than the MAX_MUTATION_SIZE_ATTRIB (100)");
        } catch (IllegalArgumentException expected) {}
    }
}
