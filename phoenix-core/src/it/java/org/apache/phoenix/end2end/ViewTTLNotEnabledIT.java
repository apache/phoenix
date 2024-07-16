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

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.PhoenixTTLRegionObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.fail;

@Category(NeedsOwnMiniClusterTest.class)
public class ViewTTLNotEnabledIT extends ParallelStatsDisabledIT {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        props.put(QueryServices.PHOENIX_VIEW_TTL_ENABLED, Boolean.toString(false));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    @Test
    public void testCreateViewWithTTLWithConfigFalse() throws Exception {
        PhoenixTestBuilder.SchemaBuilder schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());
        PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions globalViewOptions = PhoenixTestBuilder.SchemaBuilder.
                GlobalViewOptions.withDefaults();
        globalViewOptions.setTableProps("TTL = 10000");
        try {
            schemaBuilder.withTableOptions(PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults()).withGlobalViewOptions(
                    globalViewOptions).build();
            fail();
        } catch (SQLException sqe) {
            Assert.assertEquals(sqe.getErrorCode(), SQLExceptionCode.VIEW_TTL_NOT_ENABLED.getErrorCode());
        }
    }

    @Test
    public void testAlterViewWithTTLWithConfigFalse() throws Exception {
        PhoenixTestBuilder.SchemaBuilder schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());
        PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions globalViewOptions = PhoenixTestBuilder.SchemaBuilder.
                GlobalViewOptions.withDefaults();
        schemaBuilder.withTableOptions(PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults()).withGlobalViewOptions(
                    globalViewOptions).build();

        String dml = "ALTER VIEW " + schemaBuilder.getEntityGlobalViewName() + " SET TTL = 10000";
        try (Connection connection = DriverManager.getConnection(getUrl())){
            try {
                connection.createStatement().execute(dml);
                fail();
            } catch (SQLException sqe) {
                Assert.assertEquals(sqe.getErrorCode(), SQLExceptionCode.VIEW_TTL_NOT_ENABLED.getErrorCode());
            }

        }
    }

    @Test
    public void testSettingTTLFromTableToViewWithConfigDisabled() throws Exception {
        PhoenixTestBuilder.SchemaBuilder schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());
        PhoenixTestBuilder.SchemaBuilder.TableOptions tableOptions = PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        tableOptions.setTableProps("TTL = 10000");
        PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions globalViewOptions = PhoenixTestBuilder.SchemaBuilder.
                GlobalViewOptions.withDefaults();
        schemaBuilder.withTableOptions(tableOptions).withGlobalViewOptions(
                globalViewOptions).build();

        try (Connection connection = DriverManager.getConnection(getUrl())){

            String dml = "ALTER TABLE " + schemaBuilder.getEntityTableName() + " SET TTL = NONE";
            connection.createStatement().execute(dml);

            //Clearing cache as  metaDataCaching is not there for TTL usecase
            connection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            try {
                dml = "ALTER VIEW " + schemaBuilder.getEntityGlobalViewName() + " SET TTL = 10000";
                connection.createStatement().execute(dml);
                fail();
            } catch (SQLException sqe) {
                Assert.assertEquals(sqe.getErrorCode(), SQLExceptionCode.VIEW_TTL_NOT_ENABLED.getErrorCode());
            }

        }
    }

}
