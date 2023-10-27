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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;


public class BaseMutationBatchFailedStateMetricIT extends ParallelStatsDisabledIT {
    String create_table =
            "CREATE TABLE IF NOT EXISTS %s(ID VARCHAR NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER)";
    String indexName = generateUniqueName();
    String create_index = "CREATE INDEX " + indexName + " ON %s(VAL1 DESC) INCLUDE (VAL2)";
    String upsertStatement = "UPSERT INTO %s VALUES(?, ?, ?)";
    String deleteTableName = generateUniqueName();
    private final boolean transactional;
    private String transactionProvider;

    public BaseMutationBatchFailedStateMetricIT(String transactionProvider) {
        this.transactional = transactionProvider != null;
        if (this.transactional) {
            create_table =
                    create_table + (this.transactional
                            ? (" TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + transactionProvider
                            + "'")
                            : "");
        }
        this.transactionProvider = transactionProvider;
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

    @Parameterized.Parameters(name = "MutationBatchFailedStateMetricWithAllDeleteIT_transactionProvider={0}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { "OMID" }, { null } });
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
