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

import static org.apache.phoenix.monitoring.MetricType.DELETE_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_FAILED_SIZE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.end2end.index.ImmutableIndexIT;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class MutationBatchFailedStateMetricIT extends ParallelStatsDisabledIT {

    String create_table =
            "CREATE TABLE IF NOT EXISTS %s(ID VARCHAR NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER)";
    String indexName = generateUniqueName();
    String create_index = "CREATE INDEX " + indexName + " ON %s(VAL1 DESC) INCLUDE (VAL2)";
    String upsertStatement = "UPSERT INTO %s VALUES(?, ?, ?)";
    String deleteTableName = generateUniqueName();
    private final boolean transactional;

    public MutationBatchFailedStateMetricIT(String transactionProvider) {
        this.transactional = transactionProvider != null;
        if (this.transactional) {
            create_table =
                    create_table + (this.transactional
                            ? (" TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + transactionProvider
                                    + "'")
                            : "");
            System.out.println(create_table);
        }
        createTables();
        populateTables();
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(3);
        serverProps.put("hbase.coprocessor.abortonerror", "false");
        serverProps.put(Indexer.CHECK_VERSION_CONF_KEY, "false");
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, "true");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
            new ReadOnlyProps(clientProps.entrySet().iterator()));

    }

    @Parameterized.Parameters(name = "MutationBatchFailedStateMetricIT_transactionProvider={0}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { "OMID" }, { null } });
    }

    /**
     * We will have 5 rows in table and we will try deleting 4 out of 5 rows
     * In this test intention is to fail the delete and see how metric value isupdated when that happens
     * for transactional table and non transactional table
     * @throws SQLException
     */
    @Test
    public void testFailedDelete() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            long rowCount = TestUtil.getRowCount(conn, deleteTableName);
            System.out.println("Row Count:" + rowCount);

            Connection finalConn = conn;
            boolean autoCommit = conn.getAutoCommit();
            System.out.println("Auto commit : " + autoCommit);
            Statement stmt = conn.createStatement();
            // adding coprocessor which basically overrides prebatchmutate whiich is called for all the mutations
            // it is called before applying mutation to region
            TestUtil.addCoprocessor(conn, deleteTableName,
                ImmutableIndexIT.DeleteFailingRegionObserver.class);
            // trying to delete 4 rows with this single delete statement
            String dml = String.format("DELETE FROM %s where val1 >  1", deleteTableName);
            boolean execute = stmt.execute(dml);
            conn.commit();

            TestUtil.removeCoprocessor(conn, deleteTableName,
                ImmutableIndexIT.DeleteFailingRegionObserver.class);

            rowCount = TestUtil.getRowCount(conn, deleteTableName);
            System.out.println("Row Count:" + rowCount);
        } catch (SQLException e) {
            Map<String, Map<MetricType, Long>> mutationMetrics =
                    conn.unwrap(PhoenixConnection.class).getMutationMetrics();
            int mfs = mutationMetrics.get(deleteTableName).get(MUTATION_BATCH_FAILED_SIZE).intValue();
            int dbfs = mutationMetrics.get(deleteTableName).get(DELETE_BATCH_FAILED_SIZE).intValue();
            System.out.println("mfs = " + mfs);
            System.out.println("dbfs = " + dbfs);
            if (transactional) {
                Assert.assertEquals(4, dbfs);
                Assert.assertEquals(1, mfs);
            } else {
                Assert.assertEquals(4, dbfs);
                Assert.assertEquals(4, mfs);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void populateTables() {
        final int NROWS = 5;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (PreparedStatement dataPreparedStatement =
                    conn.prepareStatement(String.format(upsertStatement, deleteTableName))) {
                for (int i = 1; i <= NROWS; i++) {
                    dataPreparedStatement.setString(1, "ROW_" + i);
                    dataPreparedStatement.setInt(2, i);
                    dataPreparedStatement.setInt(3, i * 2);
                    dataPreparedStatement.execute();
                }
            }
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createTables() {
        try (Connection con = DriverManager.getConnection(getUrl())) {
            Statement stmt = con.createStatement();
            stmt.execute(String.format(create_table, deleteTableName));
            stmt.execute(String.format(create_index, deleteTableName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
