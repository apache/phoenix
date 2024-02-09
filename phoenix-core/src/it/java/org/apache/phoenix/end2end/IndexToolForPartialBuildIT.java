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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Tests for the {@link IndexToolForPartialBuildIT}
 */
@Category(NeedsOwnMiniClusterTest.class)
public class IndexToolForPartialBuildIT extends BaseOwnClusterIT {
    
    protected boolean isNamespaceEnabled = false;
    protected final String tableDDLOptions;
    
    public IndexToolForPartialBuildIT() {
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append(" SPLIT ON(1,2)");
        this.tableDDLOptions = optionBuilder.toString();
    }
    
    public static Map<String, String> getServerProperties() {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        serverProps.put(" yarn.scheduler.capacity.maximum-am-resource-percent", "1.0");
        serverProps.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "2");
        serverProps.put(HConstants.HBASE_RPC_TIMEOUT_KEY, "10000");
        serverProps.put("hbase.client.pause", "5000");
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB, Boolean.FALSE.toString());
        serverProps.put(QueryServices.INDEX_FAILURE_DISABLE_INDEX, Boolean.TRUE.toString());
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_FORWARD_TIME_ATTRIB, Long.toString(2000));
        return serverProps;
    }
    
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = getServerProperties();
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), ReadOnlyProps.EMPTY_PROPS);
    }
    
    @Test
    public void testSecondaryIndex() throws Exception {
        String schemaName = generateUniqueName();
        String dataTableName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, dataTableName);
        final String indxTable = String.format("%s_IDX", dataTableName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        props.setProperty(QueryServices.EXPLAIN_ROW_COUNT_ATTRIB, Boolean.FALSE.toString());
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceEnabled));
        final PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(),
                props);
        Statement stmt = conn.createStatement();
        try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();){
            if (isNamespaceEnabled) {
                conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            }
            stmt.execute(
                    String.format("CREATE IMMUTABLE TABLE %s (ID BIGINT NOT NULL, NAME VARCHAR, ZIP INTEGER CONSTRAINT PK PRIMARY KEY(ID ROW_TIMESTAMP)) %s",
                            fullTableName, tableDDLOptions));
            String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", fullTableName);
            PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
            // insert two rows
            upsertRow(stmt1, 1000);
            upsertRow(stmt1, 2000);

            conn.commit();
            stmt.execute(String.format("CREATE INDEX %s ON %s  (LPAD(UPPER(NAME),11,'x')||'_xyz') ", indxTable, fullTableName));
            upsertRow(stmt1, 3000);
            upsertRow(stmt1, 4000);
            upsertRow(stmt1, 5000);
            conn.commit();

            // delete these indexes
            PTable pindexTable = conn.getTable(SchemaUtil.getTableName(schemaName, indxTable));
            try (Table hTable = admin.getConnection().
                    getTable(TableName.valueOf(pindexTable.getPhysicalName().toString()));) {
                Scan scan = new Scan();
                ResultScanner scanner = hTable.getScanner(scan);
                int cnt = 0;
                for (Result res = scanner.next(); res != null; res = scanner.next()) {
                    cnt++;
                    if (cnt > 2) {
                        hTable.delete(new Delete(res.getRow()));
                    }
                }
            }
            TestUtil.doMajorCompaction(conn, pindexTable.getPhysicalName().toString());

            conn.createStatement()
                    .execute(String.format("ALTER INDEX %s on %s REBUILD ASYNC", indxTable, fullTableName));
            
            ResultSet rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schemaName), indxTable,
                    new String[] { PTableType.INDEX.toString() });
            assertTrue(rs.next());
            assertEquals(indxTable, rs.getString(3));
            String indexState = rs.getString("INDEX_STATE");
            assertEquals(PIndexState.BUILDING.toString(), indexState);            
            assertFalse(rs.next());
            upsertRow(stmt1, 6000);
            upsertRow(stmt1, 7000);
            conn.commit();

            String selectSql = String.format("SELECT LPAD(UPPER(NAME),11,'x')||'_xyz',ID FROM %s", fullTableName);

            ExplainPlan plan = conn.prepareStatement(selectSql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            // assert we are pulling from data table.
            assertEquals(SchemaUtil.getPhysicalHBaseTableName(schemaName,
                dataTableName, isNamespaceEnabled).toString(),
                explainPlanAttributes.getTableName());

            rs = stmt1.executeQuery(selectSql);
            for (int i = 1; i <= 7; i++) {
                assertTrue(rs.next());
                assertEquals("xxUNAME" + i*1000 + "_xyz", rs.getString(1));
            }

            // Validate Index table data till disabled timestamp
            rs = stmt1.executeQuery(String.format("SELECT * FROM %s", SchemaUtil.getTableName(schemaName, indxTable)));
            for (int i = 1; i <= 2; i++) {
                assertTrue(rs.next());
                assertEquals("xxUNAME" + i*1000 + "_xyz", rs.getString(1));
            }
            for (int i = 6; i <= 7; i++) {
                assertTrue(rs.next());
                assertEquals("xxUNAME" + i*1000 + "_xyz", rs.getString(1));
            }
            assertFalse(rs.next());
            // run the index MR job.
            final IndexTool indexingTool = new IndexTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            conf.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceEnabled));
            indexingTool.setConf(conf);

            final String[] cmdArgs = getArgValues(schemaName, dataTableName, indxTable);
            int status = indexingTool.run(cmdArgs);
            assertEquals(0, status);

            // insert two more rows
            upsertRow(stmt1, 8000);
            upsertRow(stmt1, 9000);
            conn.commit();

            plan = conn.prepareStatement(selectSql)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            // assert we are pulling from index table.
            assertEquals(SchemaUtil.getPhysicalHBaseTableName(schemaName,
                indxTable, isNamespaceEnabled).toString(),
                explainPlanAttributes.getTableName());

            rs = stmt.executeQuery(selectSql);

            for (int i = 1; i <= 9; i++) {
                assertTrue(rs.next());
                assertEquals("xxUNAME" + i*1000 + "_xyz", rs.getString(1));
            }
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    public String[] getArgValues(String schemaName, String dataTable, String indexName) {
        final List<String> args = Lists.newArrayList();
        if (schemaName!=null) {
            args.add("-s");
            args.add(schemaName);
        }
        args.add("-dt");
        args.add(dataTable);
        // complete index rebuild
        args.add("-it");
        args.add(indexName);
        args.add("-runfg");
        args.add("-op");
        args.add("/tmp/output/partialTable_");
        return args.toArray(new String[0]);
    }

    public static void upsertRow(PreparedStatement stmt, int i) throws SQLException {
        // insert row
        stmt.setInt(1, i);
        stmt.setString(2, "uname" + String.valueOf(i));
        stmt.setInt(3, 95050 + i);
        stmt.executeUpdate();
    }
    
}
