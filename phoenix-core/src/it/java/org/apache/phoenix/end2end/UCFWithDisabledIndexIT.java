/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.coprocessor.MetaDataEndpointImpl;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.query.QueryServices.DISABLE_VIEW_SUBTREE_VALIDATION;

/**
 * Tests that use UPDATE_CACHE_FREQUENCY with some of the disabled index states that require
 * clients to override UPDATE_CACHE_FREQUENCY and perform metadata calls to retrieve PTable.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class UCFWithDisabledIndexIT extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(UCFWithDisabledIndexIT.class);

    private static void initCluster() throws Exception {
        Map<String, String> props = Maps.newConcurrentMap();
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(60 * 60 * 1000));
        props.put(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB, "ALWAYS");
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        props.put(DISABLE_VIEW_SUBTREE_VALIDATION, "true");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        initCluster();
    }

    public static class TestMetaDataEndpointImpl extends MetaDataEndpointImpl {

        @Override
        public void getTable(RpcController controller, MetaDataProtos.GetTableRequest request,
                             RpcCallback<MetaDataProtos.MetaDataResponse> done) {
            LOGGER.error("Not expected to get getTable() for {}",
                    Bytes.toString(request.getTableName().toByteArray()));
            ProtobufUtil.setControllerException(controller,
                    ClientUtil.createIOException(
                            SchemaUtil.getPhysicalTableName(
                                            PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                                            false).toString(),
                            new DoNotRetryIOException("Not allowed")));

        }

    }

    @Test
    public void testUcfWithNoGetTableCalls() throws Throwable {
        final String tableName = generateUniqueName();
        final String indexName = "IDX_" + tableName;
        final String view01 = "v01_" + tableName;
        final String view02 = "v02_" + tableName;
        final String index_view01 = "idx_v01_" + tableName;
        final String index_view02 = "idx_v02_" + tableName;
        final String index_view03 = "idx_v03_" + tableName;
        final String index_view04 = "idx_v04_" + tableName;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            final Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE " + tableName
                    + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                    + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))"
                    + " UPDATE_CACHE_FREQUENCY=20000");
            stmt.execute("CREATE INDEX " + indexName
                    + " ON " + tableName + " (COL3) INCLUDE (COL4)");
            stmt.execute("CREATE VIEW " + view01
                    + " (VCOL1 CHAR(8), COL5 VARCHAR) AS SELECT * FROM " + tableName
                    + " WHERE COL1 = 'col1'");
            stmt.execute("CREATE VIEW " + view02 + " (VCOL2 CHAR(10), COL6 VARCHAR)"
                    + " AS SELECT * FROM " + view01 + " WHERE VCOL1 = 'vcol1'");

            stmt.execute("CREATE INDEX " + index_view01 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL1, COL2, COL3)");
            stmt.execute("CREATE INDEX " + index_view02 + " ON " + view02 + " (COL6) INCLUDE "
                    + "(COL1, COL2, COL3)");
            stmt.execute("CREATE INDEX " + index_view03 + " ON " + view01 + " (COL5) INCLUDE "
                    + "(COL2, COL1)");
            stmt.execute("CREATE INDEX " + index_view04 + " ON " + view02 + " (COL6) INCLUDE "
                    + "(COL2, COL1)");

            stmt.execute("UPSERT INTO " + view02
                    + " (col2, vcol2, col5, col6) values ('0001', 'vcol2_01', 'col5_01', " +
                    "'col6_01')");
            conn.commit();

            TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
            TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);

            stmt.execute("UPSERT INTO " + view02
                    +
                    " (col2, vcol2, col5, col6) values ('0002', 'vcol2_02', 'col5_02', 'col6_02')");
            stmt.execute("UPSERT INTO " + view02
                    +
                    " (col2, vcol2, col5, col6) values ('0003', 'vcol2_03', 'col5_03', 'col6_03')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0004', 'vcol2', 'col3_04', 'col4_04', 'col5_04')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0005', 'vcol-2', 'col3_05', 'col4_05', 'col5_05')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0006', 'vcol-1', 'col3_06', 'col4_06', 'col5_06')");
            stmt.execute("UPSERT INTO " + view01 + " (col2, vcol1, col3, col4, col5) values "
                    + "('0007', 'vcol1', 'col3_07', 'col4_07', 'col5_07')");
            stmt.execute("UPSERT INTO " + view02
                    +
                    " (col2, vcol2, col5, col6) values ('0008', 'vcol2_08', 'col5_08', 'col6_02')");
            conn.commit();

            final Statement statement = conn.createStatement();
            ResultSet rs =
                    statement.executeQuery(
                            "SELECT COL2, VCOL1, VCOL2, COL5, COL6 FROM " + view02);
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.next());

            String query = "SELECT COL1, COL2, COL3, COL4 FROM " + tableName + " WHERE COL3 = " +
                    "'col3_04'";
            rs = statement.executeQuery(query);
            Assert.assertTrue(rs.next());
            Assert.assertFalse(rs.next());

            ExplainPlan explainPlan = conn.prepareStatement(query)
                    .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                    .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                    explainPlan.getPlanStepsAsAttributes();
            Assert.assertEquals(indexName, explainPlanAttributes.getTableName());

            TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);
            TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
        }
    }

    @Test
    public void testUcfWithDisabledIndex1() throws Throwable {
        final String tableName = generateUniqueName();
        final String indexName = "IDX_" + tableName;

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            final Statement stmt = createIndexAndUpdateCoproc(conn, tableName, indexName, true);

            stmt.execute("UPSERT INTO " + tableName
                    + " (col1, col2, col3, col4) values ('c011', "
                    + "'c012', 'c013', 'c014')");
            conn.commit();
            throw new RuntimeException("Should not reach here");
        } catch (PhoenixIOException e) {
            LOGGER.error("Error thrown. ", e);
            Assert.assertTrue(e.getCause() instanceof DoNotRetryIOException);
            Assert.assertTrue(e.getCause().getMessage().contains("Not allowed"));
        } finally {
            updateIndexToRebuild(conn, tableName, indexName);
            TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);
            TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
            verifyTableAndIndexRows(conn, tableName, indexName, false);
            conn.close();
        }
    }

    private static void verifyTableAndIndexRows(Connection conn,
                                                String tableName,
                                                String indexName,
                                                boolean isCreateDisableCase)
            throws SQLException {
        ResultSet rs1 = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        ResultSet rs2 = conn.createStatement().executeQuery("SELECT * FROM " + indexName);
        if (!isCreateDisableCase) {
            Assert.assertTrue(rs1.next());
            Assert.assertEquals("c000", rs1.getString(1));
            Assert.assertEquals("c002", rs1.getString(2));
            Assert.assertEquals("c003", rs1.getString(3));
            Assert.assertEquals("c004", rs1.getString(4));
            Assert.assertTrue(rs1.next());
            Assert.assertEquals("c001", rs1.getString(1));
            Assert.assertEquals("c002", rs1.getString(2));
            Assert.assertEquals("c003", rs1.getString(3));
            Assert.assertEquals("c004", rs1.getString(4));
        }
        Assert.assertTrue(rs1.next());
        Assert.assertEquals("c011", rs1.getString(1));
        Assert.assertEquals("c012", rs1.getString(2));
        Assert.assertEquals("c013", rs1.getString(3));
        Assert.assertEquals("c014", rs1.getString(4));
        Assert.assertTrue(rs1.next());
        Assert.assertEquals("c0112", rs1.getString(1));
        Assert.assertEquals("c012", rs1.getString(2));
        Assert.assertEquals("c013", rs1.getString(3));
        Assert.assertEquals("c014", rs1.getString(4));
        Assert.assertFalse(rs1.next());

        if (!isCreateDisableCase) {
            Assert.assertTrue(rs2.next());
            Assert.assertEquals("c003", rs2.getString(1));
            Assert.assertEquals("c000", rs2.getString(2));
            Assert.assertEquals("c002", rs2.getString(3));
            Assert.assertEquals("c004", rs2.getString(4));
            Assert.assertTrue(rs2.next());
            Assert.assertEquals("c003", rs2.getString(1));
            Assert.assertEquals("c001", rs2.getString(2));
            Assert.assertEquals("c002", rs2.getString(3));
            Assert.assertEquals("c004", rs2.getString(4));
        }
        Assert.assertTrue(rs2.next());
        Assert.assertEquals("c013", rs2.getString(1));
        Assert.assertEquals("c011", rs2.getString(2));
        Assert.assertEquals("c012", rs2.getString(3));
        Assert.assertEquals("c014", rs2.getString(4));
        Assert.assertTrue(rs2.next());
        Assert.assertEquals("c013", rs2.getString(1));
        Assert.assertEquals("c0112", rs2.getString(2));
        Assert.assertEquals("c012", rs2.getString(3));
        Assert.assertEquals("c014", rs2.getString(4));
        Assert.assertFalse(rs2.next());
    }

    private static void updateIndexToRebuild(Connection conn, String tableName, String indexName)
            throws Exception {
        // re-attach original coproc
        TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);
        TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);

        final Statement stmt = conn.createStatement();
        stmt.execute("UPSERT INTO " + tableName
                + " (col1, col2, col3, col4) values ('c011', "
                + "'c012', 'c013', 'c014')");
        // expected to call getTable
        conn.commit();

        stmt.execute("ALTER INDEX " + indexName + " ON " + tableName + " REBUILD");

        PTable indexTable = conn.unwrap(PhoenixConnection.class).getTableNoCache(indexName);
        Assert.assertEquals(PIndexState.ACTIVE, indexTable.getIndexState());

        // attach coproc that does not allow getTable RPC call
        TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
        TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);

        stmt.execute("UPSERT INTO " + tableName
                + " (col1, col2, col3, col4) values ('c0112', "
                + "'c012', 'c013', 'c014')");
        // not expected to call getTable
        conn.commit();
    }

    private static Statement createIndexAndUpdateCoproc(Connection conn,
                                                        String tableName,
                                                        String indexName,
                                                        boolean alterIndexState)
            throws Exception {
        final Statement stmt = conn.createStatement();

        stmt.execute("CREATE TABLE " + tableName
                + " (COL1 CHAR(10) NOT NULL, COL2 CHAR(5) NOT NULL, COL3 VARCHAR,"
                + " COL4 VARCHAR CONSTRAINT pk PRIMARY KEY(COL1, COL2))"
                + " UPDATE_CACHE_FREQUENCY=10000");
        stmt.execute("CREATE INDEX " + indexName
                + " ON " + tableName + " (COL3) INCLUDE (COL4)");

        if (alterIndexState) {
            stmt.execute("ALTER INDEX " + indexName + " ON " + tableName + " DISABLE");
        }

        // expected to call getTable
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
        Assert.assertFalse(rs.next());

        // attach coproc that does not allow getTable RPC call
        TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
        TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);

        stmt.execute("UPSERT INTO " + tableName
                + " (col1, col2, col3, col4) values ('c000', "
                + "'c002', 'c003', 'c004')");
        // not expected to call getTable unless index is created in CREATE_DISABLE state
        conn.commit();

        // re-attach original coproc
        TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);
        TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);

        Thread.sleep(11000);
        stmt.execute("UPSERT INTO " + tableName
                + " (col1, col2, col3, col4) values ('c001', "
                + "'c002', 'c003', 'c004')");
        // expected to call getTable
        conn.commit();

        PTable indexTable = conn.unwrap(PhoenixConnection.class).getTableNoCache(indexName);
        if (alterIndexState) {
            Assert.assertEquals(PIndexState.DISABLE, indexTable.getIndexState());
        } else {
            Assert.assertEquals(PIndexState.CREATE_DISABLE, indexTable.getIndexState());
        }

        // attach coproc that does not allow getTable RPC call
        TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
        TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);
        return stmt;
    }

    @Test
    public void testUcfWithDisabledIndex2() throws Throwable {
        final String tableName = generateUniqueName();
        final String indexName = "IDX_" + tableName;

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            final Statement stmt = createIndexAndUpdateCoproc(conn, tableName, indexName, true);

            stmt.execute("SELECT * FROM " + indexName);
            throw new RuntimeException("Should not reach here");
        } catch (PhoenixIOException e) {
            LOGGER.error("Error thrown. ", e);
            Assert.assertTrue(e.getCause() instanceof DoNotRetryIOException);
            Assert.assertTrue(e.getCause().getMessage().contains("Not allowed"));
        } finally {
            updateIndexToRebuild(conn, tableName, indexName);
            TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);
            TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
            verifyTableAndIndexRows(conn, tableName, indexName, false);
            conn.close();
        }
    }

    @Test
    public void testUcfWithDisabledIndex3() throws Throwable {
        final String tableName = generateUniqueName();
        final String indexName = "IDX_" + tableName;

        Properties props = new Properties();
        props.setProperty(QueryServices.INDEX_CREATE_DEFAULT_STATE,
                PIndexState.CREATE_DISABLE.toString());
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            final Statement stmt = createIndexAndUpdateCoproc(conn, tableName,
                    indexName, false);

            stmt.execute("UPSERT INTO " + tableName
                    + " (col1, col2, col3, col4) values ('c011', "
                    + "'c012', 'c013', 'c014')");
            conn.commit();
            throw new RuntimeException("Should not reach here");
        } catch (PhoenixIOException e) {
            LOGGER.error("Error thrown. ", e);
            Assert.assertTrue(e.getCause() instanceof DoNotRetryIOException);
            Assert.assertTrue(e.getCause().getMessage().contains("Not allowed"));
        } finally {
            updateIndexToRebuild(conn, tableName, indexName);
            TestUtil.removeCoprocessor(conn, "SYSTEM.CATALOG", TestMetaDataEndpointImpl.class);
            TestUtil.addCoprocessor(conn, "SYSTEM.CATALOG", MetaDataEndpointImpl.class);
            verifyTableAndIndexRows(conn, tableName, indexName, true);
            conn.close();
        }
    }

}
