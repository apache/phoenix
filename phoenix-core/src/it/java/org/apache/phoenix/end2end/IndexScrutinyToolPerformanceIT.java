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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.INVALID_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixScrutinyJobCounters.VALID_ROW_COUNT;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;


/**
 * Tests for the {@link IndexScrutinyTool}
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class IndexScrutinyToolPerformanceIT extends IndexScrutinyToolBaseIT {
    private String dataTableDdl;
    private String indexTableDdl;
    private String viewDdl;

    private static final String UPSERT_SQL = "UPSERT INTO %s VALUES(?,?,?,?)";

    private String schemaName;
    private String dataTableName;
    private String dataTableFullName;
    private String indexTableName;
    private String indexTableFullName;
    private String viewName;
    private String viewFullName;

    private Connection conn;

    private PreparedStatement dataTableUpsertStmt;
    private PreparedStatement viewUpsertStmt;

    private long testTime;
    private Properties props;

    private static final Log LOGGER = LogFactory.getLog(IndexScrutinyToolPerformanceIT.class);
    @Parameterized.Parameters public static Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayListWithExpectedSize(15);
        list.add(new Object[] {
                "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER, EMPLOY_DATE TIMESTAMP, EMPLOYER VARCHAR)",
                null, "CREATE INDEX %s ON %s (NAME, EMPLOY_DATE) INCLUDE (ZIP)" });
        list.add(new Object[] {
                "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER, EMPLOY_DATE TIMESTAMP, EMPLOYER VARCHAR)",
                null, "CREATE LOCAL INDEX %s ON %s (NAME, EMPLOY_DATE) INCLUDE (ZIP)" });
        list.add(new Object[] {
                "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER, EMPLOY_DATE TIMESTAMP, EMPLOYER VARCHAR)",
                null, "CREATE INDEX %s ON %s (NAME, ID, ZIP, EMPLOYER, EMPLOY_DATE)" });
        list.add(new Object[] {
                "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER, EMPLOY_DATE TIMESTAMP, EMPLOYER VARCHAR)",
                null, "CREATE INDEX %s ON %s (NAME, ZIP, EMPLOY_DATE, EMPLOYER)" });
        list.add(new Object[] {
                "CREATE TABLE %s (ID INTEGER NOT NULL, NAME VARCHAR NOT NULL, ZIP INTEGER, EMPLOY_DATE TIMESTAMP, EMPLOYER VARCHAR CONSTRAINT PK_1 PRIMARY KEY (ID, NAME)) ",
                null, "CREATE INDEX %s ON %s (EMPLOY_DATE) INCLUDE (ZIP)" });
        list.add(new Object[] {
                "CREATE TABLE %s (ID INTEGER NOT NULL, NAME VARCHAR NOT NULL, ZIP INTEGER, EMPLOY_DATE TIMESTAMP, EMPLOYER VARCHAR CONSTRAINT PK_1 PRIMARY KEY (ID, NAME)) ",
                null, "CREATE INDEX %s ON %s (EMPLOY_DATE, ZIP) INCLUDE (EMPLOYER)" });
        list.add(new Object[] {
                "CREATE TABLE %s (ID INTEGER NOT NULL, NAME VARCHAR NOT NULL, ZIP INTEGER, EMPLOY_DATE TIMESTAMP, EMPLOYER VARCHAR CONSTRAINT PK_1 PRIMARY KEY (ID, NAME)) SALT_BUCKETS=2",
                null, "CREATE INDEX %s ON %s (EMPLOY_DATE, ZIP) INCLUDE (EMPLOYER) " });
        list.add(new Object[] {
                "CREATE TABLE %s (COL1 VARCHAR(15) NOT NULL,ID INTEGER NOT NULL, NAME VARCHAR, ZIP INTEGER CONSTRAINT PK_1 PRIMARY KEY (COL1, ID)) MULTI_TENANT=true",
                null, "CREATE INDEX %s ON %s (NAME) " });
        list.add(new Object[] {
                "CREATE TABLE %s (COL1 VARCHAR(15) NOT NULL,ID INTEGER NOT NULL, NAME VARCHAR NOT NULL, ZIP INTEGER CONSTRAINT PK_1 PRIMARY KEY (COL1, ID, NAME)) MULTI_TENANT=true",
                null, "CREATE INDEX %s ON %s (ZIP) " });
        list.add(new Object[] {
                "CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, ZIP INTEGER, EMPLOY_DATE TIMESTAMP, EMPLOYER VARCHAR)",
                "CREATE VIEW %s AS SELECT * FROM %s", "CREATE INDEX %s ON %s (NAME) " });
        list.add(new Object[] {
                "CREATE TABLE %s (COL1 VARCHAR(15) NOT NULL,ID INTEGER NOT NULL, NAME VARCHAR, ZIP INTEGER CONSTRAINT PK_1 PRIMARY KEY (COL1, ID)) MULTI_TENANT=true",
                "CREATE VIEW %s AS SELECT * FROM %s", "CREATE INDEX %s ON %s (NAME) " });
        list.add(new Object[] {
                "CREATE TABLE %s (COL1 VARCHAR(15) NOT NULL,ID INTEGER NOT NULL, NAME VARCHAR, ZIP INTEGER CONSTRAINT PK_1 PRIMARY KEY (COL1, ID)) MULTI_TENANT=true",
                null, "CREATE INDEX %s  ON %s (NAME, ID, ZIP) " });
        return list;
    }

    public IndexScrutinyToolPerformanceIT(String dataTableDdl, String viewDdl, String indexTableDdl) {
        this.dataTableDdl = dataTableDdl;
        this.viewDdl = viewDdl;
        this.indexTableDdl = indexTableDdl;
    }

    /**
     * Create the test data and index tables
     */
    @Before
    public void setup() throws SQLException {
        generateUniqueTableNames();
        createTestTable(getUrl(), String.format(dataTableDdl, dataTableFullName));
        if (!Strings.isNullOrEmpty(viewDdl)) {
            createTestTable(getUrl(), String.format(viewDdl, viewFullName, dataTableFullName));
            createTestTable(getUrl(), String.format(indexTableDdl, indexTableName, viewFullName));
        } else {
            createTestTable(getUrl(), String.format(indexTableDdl, indexTableName, dataTableFullName));
        }
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        String dataTableUpsert = String.format(UPSERT_SQL, dataTableFullName);
        dataTableUpsertStmt = conn.prepareStatement(dataTableUpsert);
        String viewUpsert = String.format(UPSERT_SQL, viewFullName);
        viewUpsertStmt = conn.prepareStatement(viewUpsert);
        conn.setAutoCommit(false);
        testTime = EnvironmentEdgeManager.currentTimeMillis() - 1000;

    }

    @After
    public void teardown() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    public void testSkipScan() throws Exception {
        String baseTableName = dataTableName;
        PreparedStatement upsertStmt = dataTableUpsertStmt;
        if (!Strings.isNullOrEmpty(viewDdl)) {
            baseTableName = viewName;
            upsertStmt = viewUpsertStmt;
        }

        for (int i=0; i < 2; i++) {
            if (dataTableDdl.contains("MULTI_TENANT")) {
                upsertRowMultiTenant(upsertStmt, "val"+i, i,"name-" + i, 19999);
            } else {
                upsertRow(upsertStmt, i, "name-" + i, 19999);
            }
            conn.commit();
        }

        List<Job> completedJobs = IndexScrutinyToolIT.runScrutiny(schemaName, baseTableName,
                indexTableName, null, SourceTable.DATA_TABLE_SOURCE);
        PreparedStatement targetStmt = IndexScrutinyTool.getTargetTableQueryForScrutinyTool();
        assertSkipScanFilter(targetStmt);
        Job job = completedJobs.get(0);
        Counters counters = job.getCounters();
        assertEquals(2, getCounterValue(counters, VALID_ROW_COUNT));
        assertEquals(0, getCounterValue(counters, INVALID_ROW_COUNT));

        completedJobs = IndexScrutinyToolIT.runScrutiny(schemaName, baseTableName,
                indexTableName, null, SourceTable.INDEX_TABLE_SOURCE);
        targetStmt = IndexScrutinyTool.getTargetTableQueryForScrutinyTool();
        assertSkipScanFilter(targetStmt);

        job = completedJobs.get(0);
        counters = job.getCounters();
        assertEquals(2, getCounterValue(counters, VALID_ROW_COUNT));
        assertEquals(0, getCounterValue(counters, INVALID_ROW_COUNT));
    }

    private void assertSkipScanFilter(PreparedStatement targetStmt) throws SQLException {
        QueryPlan plan = targetStmt.unwrap(PhoenixStatement.class).getQueryPlan();
        Scan scan = plan.getScans().get(0).get(0);
        Filter filter = scan.getFilter();
        if (filter instanceof FilterList) {
            FilterList filterList = (FilterList) scan.getFilter();

            boolean isSkipScanUsed = false;
            for (Filter f : filterList.getFilters()) {
                if (f instanceof SkipScanFilter) {
                    isSkipScanUsed = true;
                    break;
                }
            }
            assertEquals(true, isSkipScanUsed);
        } else {
            assertEquals(SkipScanFilter.class, filter.getClass());
        }
    }
    
    private void generateUniqueTableNames() {
        schemaName = "S_" + generateUniqueName();
        dataTableName = "D_" + generateUniqueName();
        dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        viewName = "V_" + generateUniqueName();
        viewFullName = SchemaUtil.getTableName(schemaName, viewName);
        indexTableName = "I_" + generateUniqueName();
        indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);
    }

    private void upsertRow(PreparedStatement stmt, int id, String name, int zip) throws SQLException {
        int index = 1;
        // insert row
        stmt.setInt(index++, id);
        stmt.setString(index++, name);
        stmt.setInt(index++, zip);
        stmt.setTimestamp(index++, new Timestamp(testTime));
        stmt.executeUpdate();
    }

    private void upsertRowMultiTenant(PreparedStatement stmt, String col, int id, String name, int zip) throws SQLException {
        int index = 1;
        // insert row
        stmt.setString(index++, col);
        stmt.setInt(index++, id);
        stmt.setString(index++, name);
        stmt.setInt(index++, zip);
        stmt.executeUpdate();
    }
}
