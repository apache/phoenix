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
package org.apache.phoenix.end2end.registry;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.MasterRegistry;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;


@Category(NeedsOwnMiniClusterTest.class)
public class HRpcRegistryIT extends ParallelStatsDisabledIT {
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Collections.singletonMap(QueryUtil.IS_HREGISTRY_CONNECTION, "true");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(serverProps.entrySet().iterator()));
    }


    @Test
    public void testReadOnlyUsingHRpc() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        // Use HRpc client
        String tmpUrl = getUrl();
        Connection conn = DriverManager.getConnection(tmpUrl, props);
        createTableAndReadFromIt(conn);

        // Switch to ZK client
        config.unset(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY);
        String newUrl = getLocalClusterUrl(utility);
        initAndRegisterTestDriver(newUrl, ReadOnlyProps.EMPTY_PROPS);
        conn = DriverManager.getConnection(newUrl, props);
        createTableAndReadFromIt(conn);
    }

    private void createTableAndReadFromIt(Connection conn) throws Exception {
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable + " " +
                "  (r varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (r))\n";
        createTestTable(getUrl(), ddl);

        String query = "UPSERT INTO " + testTable + "(r, col1) VALUES('row1', 777)";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + testTable + "");
        assertTrue(rs.next());

    }
}
