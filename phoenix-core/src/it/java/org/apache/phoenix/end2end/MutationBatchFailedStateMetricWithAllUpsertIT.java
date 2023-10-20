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
public class MutationBatchFailedStateMetricWithAllUpsertIT extends BaseMutationBatchFailedStateMetricIT {
    String transactionProvider;
    public MutationBatchFailedStateMetricWithAllUpsertIT(String transactionProvider) {
        super(transactionProvider);
        this.transactionProvider = transactionProvider;
    }

    /**
     * We will try upserting 1 row. The intention of the test is to fail the upsert and see how
     * metric value is updated when that happens for transactional table and non-transactional table
     * @throws SQLException
     */
    @Test
    public void testFailedUpsert() throws Exception {
        int numUpsertCount = 3;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);

            // adding coprocessor which basically overrides pre batch mutate which is called for all the mutations
            // Note :- it is called before applying mutation to region
            TestUtil.addCoprocessor(conn, deleteTableName,
                    ImmutableIndexIT.DeleteFailingRegionObserver.class);

            for(int i = 0; i < numUpsertCount; i++){
                String upsertSQL = String.format(upsertStatement, deleteTableName);
                PreparedStatement preparedStatement = conn.prepareStatement(upsertSQL);
                preparedStatement.setString(1, "ROW_"+i);
                preparedStatement.setInt(2, i);
                preparedStatement.setInt(3, 2*i);
                preparedStatement.execute();
            }
            conn.commit();

            Assert.fail("Commit should not have succeeded");
        } catch (SQLException e) {
            Map<String, Map<MetricType, Long>> mutationMetrics =
                    conn.unwrap(PhoenixConnection.class).getMutationMetrics();
            int dbfs =
                    mutationMetrics.get(deleteTableName).get(DELETE_BATCH_FAILED_SIZE).intValue();
            int upfs =
                    mutationMetrics.get(deleteTableName).get(UPSERT_BATCH_FAILED_SIZE).intValue();
            int mfs =
                    mutationMetrics.get(deleteTableName).get(MUTATION_BATCH_FAILED_SIZE).intValue();
            long gfs = GLOBAL_MUTATION_BATCH_FAILED_COUNT.getMetric().getValue();

            Assert.assertEquals(0, dbfs);
            Assert.assertEquals(numUpsertCount, upfs);
            Assert.assertEquals(numUpsertCount, mfs);
            //for the second parameter the global mutation batch failed count is double the original parameter because
            //global metrics do not reset to 0 for each parameter
            if(this.transactionProvider == null){
                Assert.assertEquals(2*numUpsertCount, gfs);
            } else if(this.transactionProvider.equals("OMID")){
                Assert.assertEquals(numUpsertCount, gfs);
            }
        }
    }
}