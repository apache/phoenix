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
import org.apache.phoenix.mapreduce.util.DefaultPhoenixMultiViewListProvider;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMultiInputUtil;
import org.apache.phoenix.mapreduce.util.ViewInfoWritable;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.apache.phoenix.mapreduce.PhoenixTTLTool.DELETE_ALL_VIEWS;
import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_QUERY_BATCH_SIZE;
import static org.junit.Assert.assertEquals;

@Category(NeedsOwnMiniClusterTest.class)
public class DefaultPhoenixMultiViewListProviderIT extends ParallelStatsDisabledIT {
    private final String BASE_TABLE_DDL = "CREATE TABLE %s (TENANT_ID CHAR(10) NOT NULL, " +
            "ID CHAR(10) NOT NULL, NUM BIGINT CONSTRAINT " +
            "PK PRIMARY KEY (TENANT_ID,ID)) MULTI_TENANT=true, COLUMN_ENCODED_BYTES = 0";
    private final String VIEW_DDL = "CREATE VIEW %s (" +
            "PK1 BIGINT PRIMARY KEY,A BIGINT, B BIGINT, C BIGINT, D BIGINT)" +
            " AS SELECT * FROM %s ";
    private final String VIEW_DDL_WITH_ID_PREFIX_AND_TTL =
            VIEW_DDL + " PHOENIX_TTL = 1000";
    private final String VIEW_INDEX_DDL = "CREATE INDEX %s ON %s(%s)";
    private final String TENANT_VIEW_DDL =
            "CREATE VIEW %s (E BIGINT, F BIGINT) AS SELECT * FROM %s";
    private final String TENANT_VIEW_DDL_WITH_TTL = TENANT_VIEW_DDL + " PHOENIX_TTL = 1000";;

    @Test
    public void testGetPhoenixMultiViewList() throws Exception {
        String schema = generateUniqueName();
        String baseTableFullName = schema + "." + generateUniqueName();
        String globalViewName1 = schema + "." + generateUniqueName();
        String globalViewName2 = schema + "." + generateUniqueName();
        String tenantViewName1 = schema + "." + generateUniqueName();
        String tenantViewName2 = schema + "." + generateUniqueName();
        String tenantViewName3 = schema + "." + generateUniqueName();
        String tenantViewName4 = schema + "." + generateUniqueName();
        String indexTable1 = generateUniqueName() + "_IDX";
        String indexTable2 = generateUniqueName() + "_IDX";
        String indexTable3 = generateUniqueName() + "_IDX";
        String indexTable4 = generateUniqueName() + "_IDX";
        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();
        DefaultPhoenixMultiViewListProvider defaultPhoenixMultiViewListProvider =
                new DefaultPhoenixMultiViewListProvider();
        Configuration cloneConfig = PropertiesUtil.cloneConfig(config);

        cloneConfig.set(PhoenixConfigurationUtil.MAPREDUCE_PHOENIX_TTL_DELETE_JOB_ALL_VIEWS,
                DELETE_ALL_VIEWS);
        cloneConfig.set(MAPREDUCE_MULTI_INPUT_QUERY_BATCH_SIZE,"2");
        List<ViewInfoWritable> result =
                defaultPhoenixMultiViewListProvider.getPhoenixMultiViewList(cloneConfig);

        /*
            Case 1 : no view
         */
        assertEquals(0, result.size());

         /*
            Case 2 :
                    BaseMultiTenantTable
                  GlobalView1 with TTL(1 ms)
                Index1                 Index2
                TenantView1,   TenantView2
        */
        try (Connection globalConn = DriverManager.getConnection(url);
             Connection tenant1Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(url, tenant1);
             Connection tenant2Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(url, tenant2)) {

            globalConn.createStatement().execute(String.format(BASE_TABLE_DDL, baseTableFullName));
            globalConn.createStatement().execute(String.format(VIEW_DDL_WITH_ID_PREFIX_AND_TTL,
                    globalViewName1, baseTableFullName));

            globalConn.createStatement().execute(
                    String.format(VIEW_INDEX_DDL, indexTable1, globalViewName1, "A,B"));
            globalConn.createStatement().execute(
                    String.format(VIEW_INDEX_DDL, indexTable2, globalViewName1, "C,D"));

            tenant1Connection.createStatement().execute(
                    String.format(TENANT_VIEW_DDL,tenantViewName1, globalViewName1));
            tenant2Connection.createStatement().execute(
                    String.format(TENANT_VIEW_DDL,tenantViewName2, globalViewName1));
        }

        result = defaultPhoenixMultiViewListProvider.getPhoenixMultiViewList(cloneConfig);
        // view with 2 index views is issuing 3 deletion jobs
        // 1 from the data table, 2 from the index table.
        assertEquals(3, result.size());

         /*
            Case 3: globalView2 without TTL
                                     BaseMultiTenantTable
             GlobalView1 with TTL(1 ms)            GlobalView2 without TTL
        Index1                 Index2            Index3                    Index4
            TenantView1,   TenantView2
        */
        try (Connection globalConn = DriverManager.getConnection(url)) {
            globalConn.createStatement().execute(String.format(VIEW_DDL,
                    globalViewName2, baseTableFullName));
            globalConn.createStatement().execute(
                    String.format(VIEW_INDEX_DDL, indexTable3, globalViewName2, "A,B"));
            globalConn.createStatement().execute(
                    String.format(VIEW_INDEX_DDL, indexTable4, globalViewName2, "C,D"));
        }
        result = defaultPhoenixMultiViewListProvider.getPhoenixMultiViewList(cloneConfig);
        assertEquals(3, result.size());

         /*
            Case 4: adding tenant3 and tenant4 with TTL
                                     BaseMultiTenantTable
             GlobalView1 with TTL(1 ms)            GlobalView2 without TTL
        Index1                 Index2            Index3                    Index4
            TenantView1,   TenantView2          TenantView3 with TTL,   TenantView4 without TTL
        */

        try (Connection tenant1Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(url, tenant1);
             Connection tenant2Connection =
                     PhoenixMultiInputUtil.buildTenantConnection(url, tenant2)) {
            tenant1Connection.createStatement().execute(
                    String.format(TENANT_VIEW_DDL_WITH_TTL,tenantViewName3, globalViewName2));
            tenant2Connection.createStatement().execute(
                    String.format(TENANT_VIEW_DDL,tenantViewName4, globalViewName2));
        }
        result = defaultPhoenixMultiViewListProvider.getPhoenixMultiViewList(cloneConfig);
        assertEquals(6, result.size());

        /*
            Testing tenant specific case. Even tenant1 created 2 leaf views, one of them was created
            under a global view with TTL. This will not add to the deletion list.
         */
        cloneConfig = PropertiesUtil.cloneConfig(config);
        cloneConfig.set(MAPREDUCE_MULTI_INPUT_QUERY_BATCH_SIZE,"2");
        cloneConfig.set(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID, tenant1);
        result = defaultPhoenixMultiViewListProvider.getPhoenixMultiViewList(cloneConfig);
        assertEquals(3, result.size());

        /*
            Deleting tenant1 with tenantViewName1 will NOT add any deletion job to the list because
            the parent global view has TTL value.
         */
        cloneConfig.set(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID, tenant1);
        cloneConfig.set(PhoenixConfigurationUtil.MAPREDUCE_PHOENIX_TTL_DELETE_JOB_PER_VIEW,
                tenantViewName1);
        result = defaultPhoenixMultiViewListProvider.getPhoenixMultiViewList(cloneConfig);
        assertEquals(0, result.size());

        /*
            Without tenant id, it will not add the job to the list even the tenant view name is
            provided.
         */
        cloneConfig = PropertiesUtil.cloneConfig(config);
        cloneConfig.set(MAPREDUCE_MULTI_INPUT_QUERY_BATCH_SIZE,"2");
        cloneConfig.set(PhoenixConfigurationUtil.MAPREDUCE_PHOENIX_TTL_DELETE_JOB_PER_VIEW,
                tenantViewName3);
        result = defaultPhoenixMultiViewListProvider.getPhoenixMultiViewList(cloneConfig);
        assertEquals(0, result.size());

        /*
            tenant id + tenant view name will add 3 job to the list.
            1 for data table and 2 for the index table.
         */
        cloneConfig.set(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID, tenant1);
        cloneConfig.set(PhoenixConfigurationUtil.MAPREDUCE_PHOENIX_TTL_DELETE_JOB_PER_VIEW,
                tenantViewName3);
        result = defaultPhoenixMultiViewListProvider.getPhoenixMultiViewList(cloneConfig);
        assertEquals(3, result.size());

        /*
            tenant id + tenant view name will NOT add ot the list because tenantViewName4 did NOT
            have TTL value.
         */
        cloneConfig.set(PhoenixConfigurationUtil.MAPREDUCE_TENANT_ID, tenant2);
        cloneConfig.set(PhoenixConfigurationUtil.MAPREDUCE_PHOENIX_TTL_DELETE_JOB_PER_VIEW,
                tenantViewName4);
        result = defaultPhoenixMultiViewListProvider.getPhoenixMultiViewList(cloneConfig);
        assertEquals(0, result.size());
    }
}
