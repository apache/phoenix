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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ConnectionDriverForDiffTenantIT {
    protected static Configuration conf = HBaseConfiguration.create();
    protected static HBaseTestingUtility utility;
    private static String url;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        utility = new HBaseTestingUtility(conf);
        utility.startMiniCluster(1);
        url = QueryUtil.getConnectionUrl(new Properties(), utility.getConfiguration());
    }

    @AfterClass
    public static void shutDown() throws Exception {
        utility.shutdownMiniCluster();
    }

    @Test
    public void testConnectionDriverNormalizeDiffTenants() throws Exception {
        PhoenixDriver driver1 = new PhoenixDriver();
        Admin admin1 = driver1.connect(url + PhoenixRuntime.TENANT_ID_ATTRIB + "=1",
                new Properties()).unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        Admin admin2 = driver1.connect(url + PhoenixRuntime.TENANT_ID_ATTRIB + "=2",
                new Properties()).unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        Admin admin3 = driver1.connect(url + PhoenixRuntime.TENANT_ID_ATTRIB + "=1",
                new Properties()).unwrap(PhoenixConnection.class).getQueryServices().getAdmin();

        assertEquals(admin1.getConnection(), admin3.getConnection());
        assertNotEquals(admin1.getConnection(), admin2.getConnection());
    }
}
