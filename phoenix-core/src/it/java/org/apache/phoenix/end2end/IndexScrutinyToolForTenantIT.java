/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexScrutinyMapperForTest;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.OutputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.index.SourceTargetColumnNames;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.mapreduce.index.IndexScrutinyTableOutput.OUTPUT_TABLE_NAME;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.INVALID_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.VALID_ROW_COUNT;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link IndexScrutinyTool}
 */
@Category(NeedsOwnMiniClusterTest.class)
public  class IndexScrutinyToolForTenantIT extends IndexScrutinyToolBaseIT {
    private Connection connGlobal = null;
    private Connection connTenant = null;

    private String tenantId;
    private String tenantViewName;
    private String indexNameTenant;
    private String multiTenantTable;
    private String viewIndexTableName;

    private final String createViewStr = "CREATE VIEW %s AS SELECT * FROM %s";
    private final String upsertQueryStr = "UPSERT INTO %s (COL1, ID, NAME) VALUES('%s' , %d, '%s')";
    private final String createIndexStr = "CREATE INDEX %s ON %s (NAME) ";

    /**
     * Create the test data
     */
    @Before public void setup() throws SQLException {
        tenantId = generateUniqueName();
        tenantViewName = generateUniqueName();
        indexNameTenant = generateUniqueName();
        multiTenantTable = generateUniqueName();
        viewIndexTableName = "_IDX_" + multiTenantTable;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        connGlobal = DriverManager.getConnection(getUrl(), props);

        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        connTenant = DriverManager.getConnection(getUrl(), props);
        String createTblStr = "CREATE TABLE %s (COL1 VARCHAR(15) NOT NULL,ID INTEGER NOT NULL" + ", NAME VARCHAR, CONSTRAINT PK_1 PRIMARY KEY (COL1, ID)) MULTI_TENANT=true";

        createTestTable(getUrl(), String.format(createTblStr, multiTenantTable));

        connTenant.createStatement()
                .execute(String.format(createViewStr, tenantViewName, multiTenantTable));

        String idxStmtTenant = String.format(createIndexStr, indexNameTenant, tenantViewName);
        connTenant.createStatement().execute(idxStmtTenant);
        connTenant.commit();
        connGlobal.commit();
    }

    @After
    public void teardown() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        if (connGlobal != null) {
            connGlobal.close();
        }
        if (connTenant != null) {
            connTenant.close();
        }
        assertFalse("refCount leaked", refCountLeaked);
    }

    /**
     * Tests that the config for max number of output rows is observed
     */
    @Test public void testTenantViewAndIndexEqual() throws Exception {
        connTenant.createStatement()
                .execute(String.format(upsertQueryStr, tenantViewName, tenantId, 1, "x"));
        connTenant.commit();

        String[]
                argValues =
                getArgValues("", tenantViewName, indexNameTenant, 10L, SourceTable.INDEX_TABLE_SOURCE, false, null, null, tenantId,
                        EnvironmentEdgeManager.currentTimeMillis());

        List<Job> completedJobs = runScrutiny(IndexScrutinyMapperForTest.class, argValues);
        // Sunny case, both index and view are equal. 1 row
        assertEquals(1, completedJobs.size());
        for (Job job : completedJobs) {
            assertTrue(job.isSuccessful());
            Counters counters = job.getCounters();
            assertEquals(1, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(0, getCounterValue(counters, INVALID_ROW_COUNT));
        }
    }

    /**
     * Tests global view on multi-tenant table should work too
     **/
    @Test public void testGlobalViewOnMultiTenantTable() throws Exception {
        String globalViewName = generateUniqueName();
        String indexNameGlobal = generateUniqueName();

        connGlobal.createStatement()
                .execute(String.format(createViewStr, globalViewName, multiTenantTable));

        String idxStmtGlobal = String.format(createIndexStr, indexNameGlobal, globalViewName);
        connGlobal.createStatement().execute(idxStmtGlobal);
        connGlobal.createStatement()
                .execute(String.format(upsertQueryStr, globalViewName, "global", 5, "x"));
        connGlobal.commit();
        String[]
                argValues =
                getArgValues("", globalViewName, indexNameGlobal, 10L, SourceTable.INDEX_TABLE_SOURCE, false, null, null, null,
                        EnvironmentEdgeManager.currentTimeMillis());
        List<Job> completedJobs = runScrutiny(IndexScrutinyMapperForTest.class, argValues);
        // Sunny case, both index and view are equal. 1 row
        assertEquals(1, completedJobs.size());
        for (Job job : completedJobs) {
            assertTrue(job.isSuccessful());
            Counters counters = job.getCounters();
            assertEquals(1, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(0, getCounterValue(counters, INVALID_ROW_COUNT));
        }
    }

    @Test public void testColumnsForSelectQueryOnMultiTenantTable() throws Exception {
        String indexNameGlobal = generateUniqueName();
        connGlobal.createStatement()
                .execute(String.format(createIndexStr, indexNameGlobal, multiTenantTable));

        PhoenixConnection pconn = connGlobal.unwrap(PhoenixConnection.class);
        PTable pDataTable = pconn.getTable(new PTableKey(null, multiTenantTable));
        PTable pIndexTable = pconn.getTable(new PTableKey(null, indexNameGlobal));

        SourceTargetColumnNames
                columnNames =
                new SourceTargetColumnNames.IndexSourceColNames(pDataTable, pIndexTable);
        String targetPksCsv = Joiner.on(",").join(SchemaUtil.getEscapedFullColumnNames(columnNames.getTargetPkColNames()));
        String
                selectQuery =
                QueryUtil.constructSelectStatement(indexNameGlobal, columnNames.getCastedTargetColNames(), targetPksCsv,
                        HintNode.Hint.NO_INDEX, false);
        assertEquals(selectQuery,
                "SELECT /*+ NO_INDEX */ CAST(\"COL1\" AS VARCHAR(15)) , CAST(\"ID\" AS INTEGER) , CAST(\"0\".\"NAME\" AS VARCHAR) FROM "
                        + indexNameGlobal + " WHERE (\"COL1\",\"ID\")");
    }

    /**
     * Use Both as source. Add 1 row to tenant view and disable index.
     * Add 1 more to view and add a wrong row to index.
     * Both have 1 invalid row, 1 valid row.
     **/
    @Test public void testOneValidOneInvalidUsingBothAsSource() throws Exception {
        connTenant.createStatement()
                .execute(String.format(upsertQueryStr, tenantViewName, tenantId, 1, "x"));
        connTenant.commit();
        connTenant.createStatement().execute(
                String.format("ALTER INDEX %s ON %S disable", indexNameTenant, tenantViewName));

        connTenant.createStatement()
                .execute(String.format(upsertQueryStr, tenantViewName, tenantId, 2, "x2"));

        connTenant.createStatement().execute(String.format("UPSERT INTO %s (\":ID\", \"0:NAME\") values (%d, '%s')",
                indexNameTenant, 5555, "wrongName"));
        connTenant.commit();

        String[]
                argValues =
                getArgValues("", tenantViewName, indexNameTenant, 10L, SourceTable.BOTH, false, null, null, tenantId, EnvironmentEdgeManager.currentTimeMillis());
        List<Job> completedJobs = runScrutiny(IndexScrutinyMapperForTest.class, argValues);

        assertEquals(2, completedJobs.size());
        for (Job job : completedJobs) {
            assertTrue(job.isSuccessful());
            Counters counters = job.getCounters();
            assertEquals(1, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(1, getCounterValue(counters, INVALID_ROW_COUNT));
        }
    }

    /**
     * Add 3 rows to Tenant view.
     * Empty index table and observe they are not equal.
     * Use data table as source and output to file.
     **/
    @Test public void testWithEmptyIndexTableOutputToFile() throws Exception {
        testWithOutput(OutputFormat.FILE);
    }

    @Test public void testWithEmptyIndexTableOutputToTable() throws Exception {
        testWithOutput(OutputFormat.TABLE);
        assertEquals(3, countRows(connGlobal, OUTPUT_TABLE_NAME));
    }

    private void testWithOutput(OutputFormat outputFormat) throws Exception {
        connTenant.createStatement()
                .execute(String.format(upsertQueryStr, tenantViewName, tenantId, 1, "x"));
        connTenant.createStatement()
                .execute(String.format(upsertQueryStr, tenantViewName, tenantId, 2, "x2"));
        connTenant.createStatement()
                .execute(String.format(upsertQueryStr, tenantViewName, tenantId, 3, "x3"));
        connTenant.createStatement().execute(String.format("UPSERT INTO %s (\":ID\", \"0:NAME\") values (%d, '%s')",
                indexNameTenant, 5555, "wrongName"));
        connTenant.commit();

        ConnectionQueryServices queryServices = connGlobal.unwrap(PhoenixConnection.class).getQueryServices();
        Admin admin = queryServices.getAdmin();
        TableName tableName = TableName.valueOf(viewIndexTableName);
        admin.disableTable(tableName);
        admin.truncateTable(tableName, false);

        String[]
                argValues =
                getArgValues("", tenantViewName, indexNameTenant, 10L, SourceTable.DATA_TABLE_SOURCE, true, outputFormat, null,
                        tenantId, EnvironmentEdgeManager.currentTimeMillis());
        List<Job> completedJobs = runScrutiny(IndexScrutinyMapperForTest.class, argValues);

        assertEquals(1, completedJobs.size());
        for (Job job : completedJobs) {
            assertTrue(job.isSuccessful());
            Counters counters = job.getCounters();
            assertEquals(0, getCounterValue(counters, VALID_ROW_COUNT));
            assertEquals(3, getCounterValue(counters, INVALID_ROW_COUNT));
        }
    }
}

