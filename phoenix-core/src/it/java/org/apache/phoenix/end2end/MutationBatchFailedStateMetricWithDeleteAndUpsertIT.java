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

import static org.apache.phoenix.monitoring.MetricType.DELETE_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.UPSERT_BATCH_FAILED_SIZE;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_FAILED_SIZE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_FAILED_COUNT;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.end2end.index.ImmutableIndexIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class MutationBatchFailedStateMetricWithDeleteAndUpsertIT extends BaseMutationBatchFailedStateMetricIT {
    String transactionProvider;
    public MutationBatchFailedStateMetricWithDeleteAndUpsertIT(String transactionProvider) {
        super(transactionProvider);
        this.transactionProvider = transactionProvider;
    }

    @Test
    public void testFailedUpsertAndDelete() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(getUrl(), props);

            Statement stmt = conn.createStatement();
            // adding coprocessor which basically overrides pre batch mutate which is called for all the mutations
            // Note :- it is called before applying mutation to region
            TestUtil.addCoprocessor(conn, deleteTableName,
                    ImmutableIndexIT.DeleteFailingRegionObserver.class);
            // trying to delete 4 rows with this single delete statement
            String dml = String.format("DELETE FROM %s where val1 >  1", deleteTableName);
            stmt.execute(dml);

            String upsertSQL = String.format(upsertStatement, deleteTableName);
            PreparedStatement preparedStatement = conn.prepareStatement(upsertSQL);
            preparedStatement.setString(1, "ROW_6");
            preparedStatement.setInt(2, 6);
            preparedStatement.setInt(3, 12);
            preparedStatement.execute();

            conn.commit();

            Assert.fail("Commit should not have succeeded");
        } catch (SQLException e) {
            Map<String, Map<MetricType, Long>> mutationMetrics =
                    conn.unwrap(PhoenixConnection.class).getMutationMetrics();
            int mfs =
                    mutationMetrics.get(deleteTableName).get(MUTATION_BATCH_FAILED_SIZE).intValue();
            int dbfs =
                    mutationMetrics.get(deleteTableName).get(DELETE_BATCH_FAILED_SIZE).intValue();
            int upfs =
                    mutationMetrics.get(deleteTableName).get(UPSERT_BATCH_FAILED_SIZE).intValue();
            long gfs =
                    GLOBAL_MUTATION_BATCH_FAILED_COUNT.getMetric().getValue();

            Assert.assertEquals(4, dbfs);
            Assert.assertEquals(5, mfs);
            Assert.assertEquals(1, upfs);
            //for the second parameter the global mutation batch failed count is double the original parameter because
            //global metrics do not reset to 0 for each parameter
            if(this.transactionProvider == null){
                Assert.assertEquals(10, gfs);
            } else if(this.transactionProvider.equals("OMID")){
                Assert.assertEquals(5, gfs);
            }
        }

    }
}