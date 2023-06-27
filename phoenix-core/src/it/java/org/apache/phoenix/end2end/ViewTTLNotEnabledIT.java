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
import org.apache.phoenix.coprocessor.PhoenixTTLRegionObserver;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;

@Category(NeedsOwnMiniClusterTest.class)
public class ViewTTLNotEnabledIT extends ParallelStatsDisabledIT {

    @Test
    public void testPhoenixTTLNotEnabled() throws Exception {

        // PHOENIX TTL is set in seconds (for e.g 10 secs)
        long phoenixTTL = 10;
        PhoenixTestBuilder.SchemaBuilder.TableOptions
                tableOptions = PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        tableOptions.getTableColumns().clear();
        tableOptions.getTableColumnTypes().clear();

        PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                tenantViewOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewOptions.withDefaults();
        tenantViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", phoenixTTL));

        // Define the test schema.
        final PhoenixTestBuilder.SchemaBuilder schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(url);
        schemaBuilder
                .withTableOptions(tableOptions)
                .withTenantViewOptions(tenantViewOptions)
                .build();

        String viewName = schemaBuilder.getEntityTenantViewName();

        Properties props = new Properties();
        String tenantConnectUrl =
                url + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId();

        // Test the coproc is not registered
        org.apache.hadoop.hbase.client.Connection hconn = getUtility().getConnection();
        Admin admin = hconn.getAdmin();
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(
                TableName.valueOf(schemaBuilder.getEntityTableName()));
        Assert.assertFalse("Coprocessor " + PhoenixTTLRegionObserver.class.getName()
                        + " should not have been added: ",
                tableDescriptor.hasCoprocessor(PhoenixTTLRegionObserver.class.getName()));


        // Test masking expired rows property are not set
        try (Connection conn = DriverManager.getConnection(tenantConnectUrl, props);
                final Statement statement = conn.createStatement()) {
            conn.setAutoCommit(true);

            final String stmtString = String.format("select * from  %s", viewName);
            Preconditions.checkNotNull(stmtString);
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            final QueryPlan queryPlan = pstmt.optimizeQuery(stmtString);

            PhoenixResultSet
                    rs = pstmt.newResultSet(queryPlan.iterator(), queryPlan.getProjector(), queryPlan.getContext());
            Assert.assertFalse("Should not have any rows", rs.next());
            Assert.assertEquals("Should have at least one element", 1, queryPlan.getScans().size());
            Assert.assertEquals("PhoenixTTL should not be set",
                    0, ScanUtil.getPhoenixTTL(queryPlan.getScans().get(0).get(0)));
            Assert.assertFalse("Masking attribute should not be set",
                    ScanUtil.isMaskTTLExpiredRows(queryPlan.getScans().get(0).get(0)));
            Assert.assertFalse("Delete Expired attribute should not set",
                    ScanUtil.isDeleteTTLExpiredRows(queryPlan.getScans().get(0).get(0)));
        }
    }

}
