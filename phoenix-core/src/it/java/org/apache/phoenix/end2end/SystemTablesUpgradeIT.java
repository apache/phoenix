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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.TaskMetaDataEndpoint;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesTestImpl;
import org.apache.phoenix.schema.SystemTaskSplitPolicy;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Tests for upgrades of System tables.
 */
//FIXME this class has no @Category, and will not be run by Maven
public class SystemTablesUpgradeIT extends BaseTest {
    private static boolean reinitialize;
    private static int countUpgradeAttempts;
    private static long systemTableVersion = MetaDataProtocol.getPriorVersion();

    private static class PhoenixUpgradeCountingServices extends ConnectionQueryServicesImpl {
        public PhoenixUpgradeCountingServices(QueryServices services, ConnectionInfo connectionInfo, Properties info) {
            super(services, connectionInfo, info);
        }

        @Override
        protected void setUpgradeRequired() {
            super.setUpgradeRequired();
            countUpgradeAttempts++;
        }

        @Override
        protected long getSystemTableVersion() {
            return systemTableVersion;
        }

        @Override
        protected boolean isInitialized() {
            return !reinitialize && super.isInitialized();
        }
    }

    public static class PhoenixUpgradeCountingDriver extends PhoenixTestDriver {
        private ConnectionQueryServices cqs;
        private final ReadOnlyProps overrideProps;

        public PhoenixUpgradeCountingDriver(ReadOnlyProps props) {
            overrideProps = props;
        }

        @Override
        public boolean acceptsURL(String url) throws SQLException {
            return true;
        }

        @Override // public for testing
        public synchronized ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException {
            if (cqs == null) {
                cqs = new PhoenixUpgradeCountingServices(new QueryServicesTestImpl(getDefaultProps(), overrideProps), ConnectionInfo.create(url), info);
                cqs.init(url, info);
            } else if (reinitialize) {
                cqs.init(url, info);
                reinitialize = false;
            }
            return cqs;
        }
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newConcurrentMap();
        props.put(BaseTest.DRIVER_CLASS_NAME_ATTRIB, PhoenixUpgradeCountingDriver.class.getName());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testUpgradeOnlyHappensOnce() throws Exception {
        ConnectionQueryServices services = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class).getQueryServices();
        assertTrue(services instanceof PhoenixUpgradeCountingServices);
        // Check if the timestamp version is changing between the current version and prior version
        boolean wasTimestampChanged = systemTableVersion != MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP;
        reinitialize = true;
        systemTableVersion = MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP;
        DriverManager.getConnection(getUrl());
        // Confirm that if the timestamp changed, that an upgrade was performed (and that if it
        // didn't, that an upgrade wasn't attempted).
        assertEquals(wasTimestampChanged ? 1 : 0, countUpgradeAttempts);
        // Confirm that another connection does not increase the number of times upgrade was attempted
        DriverManager.getConnection(getUrl());
        assertEquals(wasTimestampChanged ? 1 : 0, countUpgradeAttempts);
        // Additional test for PHOENIX-6125
        // Confirm that SYSTEM.TASK has split policy set as
        // SystemTaskSplitPolicy (which is extending DisabledRegionSplitPolicy
        // as of this writing)
        try (Admin admin = services.getAdmin()) {
            TableDescriptor td = admin.getDescriptor(TableName.valueOf(
                PhoenixDatabaseMetaData.SYSTEM_TASK_NAME));
            String taskSplitPolicy = td.getRegionSplitPolicyClassName();
            assertEquals(SystemTaskSplitPolicy.class.getName(),
                taskSplitPolicy);
            assertTrue(td.hasCoprocessor(TaskMetaDataEndpoint.class.getName()));
        }
    }

}