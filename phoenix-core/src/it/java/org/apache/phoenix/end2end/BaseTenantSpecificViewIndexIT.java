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

import static org.apache.phoenix.thirdparty.com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public abstract class BaseTenantSpecificViewIndexIT extends SplitSystemCatalogIT {
    
    public static final String NON_STRING_TENANT_ID = "1234";
    
    protected Set<Pair<String, String>> tenantViewsToDelete = newHashSet();

    protected void testUpdatableView(Integer saltBuckets) throws Exception {
        testUpdatableView(saltBuckets, false);
    }
    
    protected void testUpdatableView(Integer saltBuckets, boolean localIndex) throws Exception {
        String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String viewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        createBaseTable(tableName, saltBuckets, true);
        Connection conn = createTenantConnection(TENANT1);
        try {
            createAndPopulateTenantView(conn, TENANT1, tableName, "", viewName);
            createAndVerifyIndex(conn, viewName, tableName, saltBuckets, TENANT1, "", localIndex,0L);
            verifyViewData(conn, viewName, "");
        } finally {
            try { conn.close();} catch (Exception ignored) {}
        }
    }

    protected void testUpdatableViewNonString(Integer saltBuckets, boolean localIndex) throws Exception {
        String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String viewName = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        createBaseTable(tableName, saltBuckets, false);
        Connection conn = createTenantConnection(NON_STRING_TENANT_ID);
        try {
            createAndPopulateTenantView(conn, NON_STRING_TENANT_ID, tableName, "", viewName);
            createAndVerifyIndexNonStringTenantId(conn, viewName, tableName, NON_STRING_TENANT_ID, "");
            verifyViewData(conn, viewName, "");
        } finally {
            try { conn.close();} catch (Exception ignored) {}
        }
    }
    
    protected void testUpdatableViewsWithSameNameDifferentTenants(Integer saltBuckets) throws Exception {
        testUpdatableViewsWithSameNameDifferentTenants(saltBuckets, false);
    }

    protected void testUpdatableViewsWithSameNameDifferentTenants(Integer saltBuckets, boolean localIndex) throws Exception {
        String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String viewName1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String viewName2 = SchemaUtil.getTableName(SCHEMA4, generateUniqueName());
        createBaseTable(tableName, saltBuckets, true);
        Connection conn1 = createTenantConnection(TENANT1);
        Connection conn2 = createTenantConnection(TENANT2);
        try {
            String prefixForTenant1Data = "TI";
            String prefixForTenant2Data = "TII";
            
            // tenant views with same name for two different tables
            createAndPopulateTenantView(conn1, TENANT1, tableName, prefixForTenant1Data, viewName1);
            createAndPopulateTenantView(conn2, TENANT2, tableName, prefixForTenant2Data, viewName2);
            
            createAndVerifyIndex(conn1, viewName1, tableName, saltBuckets, TENANT1, prefixForTenant1Data, localIndex,0L);
            createAndVerifyIndex(conn2, viewName2, tableName, saltBuckets, TENANT2, prefixForTenant2Data, localIndex,1L);
            
            verifyViewData(conn1, viewName1, prefixForTenant1Data);
            verifyViewData(conn2, viewName2, prefixForTenant2Data);
        } finally {
            try { conn1.close();} catch (Exception ignored) {}
            try { conn2.close();} catch (Exception ignored) {}
        }
    }
    
    private void createBaseTable(String tableName, Integer saltBuckets, boolean hasStringTenantId) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String tenantIdType = hasStringTenantId ? "VARCHAR" : "BIGINT";
        String ddl = "CREATE TABLE " + tableName + " (t_id " + tenantIdType + " NOT NULL,\n" +
                "k1 INTEGER NOT NULL,\n" +
                "k2 INTEGER NOT NULL,\n" +
                "v1 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n" +
                "multi_tenant=true" + (saltBuckets == null ? "" : (",salt_buckets="+saltBuckets));
        conn.createStatement().execute(ddl);
        conn.close();
    }
    
    private String createAndPopulateTenantView(Connection conn, String tenantId, String baseTable, String valuePrefix, String viewName) throws SQLException {
        String ddl = "CREATE VIEW " + viewName + "(v2 VARCHAR) AS SELECT * FROM " + baseTable + " WHERE k1 = 1";
        conn.createStatement().execute(ddl);
        tenantViewsToDelete.add(new Pair<String, String>(tenantId, viewName ));
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO " + viewName + "(k2,v1,v2) VALUES(" + i + ",'" + valuePrefix + "v1-" + (i%5) + "','" + valuePrefix + "v2-" + (i%2) + "')");
        }
        conn.commit();
        return viewName;
    }
    
    private void createAndVerifyIndex(Connection conn, String viewName, String tableName, Integer saltBuckets, String tenantId, String valuePrefix, boolean localIndex, Long expectedIndexIdOffset) throws SQLException {
        String indexName = generateUniqueName();
        if(localIndex){
            conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + viewName + "(v2)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + viewName + "(v2)");
        }
        conn.createStatement().execute("UPSERT INTO " + viewName + "(k2,v1,v2) VALUES (-1, 'blah', 'superblah')"); // sanity check that we can upsert after index is there
        conn.commit();
        ExplainPlan plan = conn.prepareStatement("SELECT k1, k2, v2 FROM "
                + viewName + " WHERE v2='" + valuePrefix + "v2-1'")
            .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
            .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
            plan.getPlanStepsAsAttributes();
        final String iteratorTypeAndScanSize;
        final String clientSortAlgo;
        final String expectedTableName;
        final String keyRanges;

        if (localIndex) {
            if (saltBuckets == null) {
                iteratorTypeAndScanSize = "PARALLEL 1-WAY";
            } else {
                iteratorTypeAndScanSize = "PARALLEL 3-WAY";
            }
            clientSortAlgo = "CLIENT MERGE SORT";
            expectedTableName = tableName;
            keyRanges = " [" + (1L + expectedIndexIdOffset) + ",'" + tenantId
                + "','" + valuePrefix + "v2-1']";
        } else {
            if (saltBuckets == null) {
                iteratorTypeAndScanSize = "PARALLEL 1-WAY";
                clientSortAlgo = null;
                keyRanges = " [" + (Short.MIN_VALUE + expectedIndexIdOffset)
                    + ",'" + tenantId + "','" + valuePrefix + "v2-1']";
            } else {
                iteratorTypeAndScanSize = "PARALLEL 3-WAY";
                clientSortAlgo = "CLIENT MERGE SORT";
                keyRanges = " [0," + (Short.MIN_VALUE + expectedIndexIdOffset)
                    + ",'" + tenantId + "','" + valuePrefix + "v2-1'] - ["
                    + (saltBuckets - 1) + "," + (Short.MIN_VALUE + expectedIndexIdOffset)
                    + ",'" + tenantId + "','" + valuePrefix + "v2-1']";
            }
            expectedTableName = "_IDX_" + tableName;
        }
        assertEquals(iteratorTypeAndScanSize,
            explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
            explainPlanAttributes.getServerWhereFilter());
        assertEquals("RANGE SCAN ", explainPlanAttributes.getExplainScanType());
        assertEquals(clientSortAlgo, explainPlanAttributes.getClientSortAlgo());
        assertEquals(expectedTableName, explainPlanAttributes.getTableName());
        assertEquals(keyRanges, explainPlanAttributes.getKeyRanges());
    }

    private void createAndVerifyIndexNonStringTenantId(Connection conn, String viewName, String tableName, String tenantId, String valuePrefix) throws SQLException {
        String indexName = generateUniqueName();
        conn.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + viewName + "(v2)");
        conn.createStatement().execute("UPSERT INTO " + viewName + "(k2,v1,v2) VALUES (-1, 'blah', 'superblah')"); // sanity check that we can upsert after index is there
        conn.commit();
        ExplainPlan plan = conn.prepareStatement("SELECT k1, k2, v2 FROM "
                + viewName + " WHERE v2='" + valuePrefix + "v2-1'")
            .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
            .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
            plan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
            explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
            explainPlanAttributes.getServerWhereFilter());
        assertEquals("RANGE SCAN ", explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
        assertEquals(" [1," + tenantId + ",'" + valuePrefix + "v2-1']",
            explainPlanAttributes.getKeyRanges());
        assertEquals("CLIENT MERGE SORT",
            explainPlanAttributes.getClientSortAlgo());
    }
    
    private Connection createTenantConnection(String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), props);
    }
    
    @SuppressWarnings("unchecked")
    private void verifyViewData(Connection conn, String viewName, String valuePrefix) throws SQLException {
        String query = "SELECT k1, k2, v2 FROM " + viewName + " WHERE v2='" + valuePrefix + "v2-1'";
        ResultSet rs = conn.createStatement().executeQuery(query);
        List<List<Object>> expectedResultsA = Lists.newArrayList(
            Arrays.<Object>asList(1,1, valuePrefix + "v2-1"),
            Arrays.<Object>asList(1,3, valuePrefix + "v2-1"),
            Arrays.<Object>asList(1,5, valuePrefix + "v2-1"),
            Arrays.<Object>asList(1,7, valuePrefix + "v2-1"),
            Arrays.<Object>asList(1,9, valuePrefix + "v2-1"));
        assertValuesEqualsResultSet(rs,expectedResultsA);
    }
}
