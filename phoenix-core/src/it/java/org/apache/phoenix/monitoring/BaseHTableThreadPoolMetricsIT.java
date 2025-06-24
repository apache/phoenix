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
package org.apache.phoenix.monitoring;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.monitoring.HTableThreadPoolHistograms.HistogramName;

public class BaseHTableThreadPoolMetricsIT extends BaseTest {
    private static final int ROWS_TO_LOAD_INITIALLY = 10000;

    protected void assertHistogramTags(
            Map<String, List<HistogramDistribution>> htableThreadPoolHistograms,
            Map<String, String> expectedTagKeyValues, String histogramKey) throws SQLException {
        List<HistogramDistribution> histograms = htableThreadPoolHistograms.get(histogramKey);
        Assert.assertEquals(2, histograms.size());
        for (HistogramDistribution histogram : histograms) {
            Map<String, String> tags = histogram.getTags();

            // Assert that there only expected tag names are there
            Assert.assertEquals(expectedTagKeyValues.keySet(), tags.keySet());

            for (Map.Entry<String, String> tag : tags.entrySet()) {
                String tagName = tag.getKey();
                Assert.assertEquals(expectedTagKeyValues.get(tagName), tags.get(tagName));
            }
        }
    }

    protected void assertHTableThreadPoolUsed(
            Map<String, List<HistogramDistribution>> htableThreadPoolHistograms, String histogramKey) {
        boolean foundActiveThreadsCountHistogram = false;
        boolean foundQueueSizeHistogram = false;

        List<HistogramDistribution> histograms = htableThreadPoolHistograms.get(histogramKey);
        // Assert each HTableThreadPoolHistograms has 2 HdrHistograms
        Assert.assertEquals(2, histograms.size());
        for (HistogramDistribution histogram : histograms) {
            if (histogram.getHistoName().equals(HistogramName.ActiveThreadsCount.name())) {

                // Assert that at least row count no. of requests were processed by
                // HTableThreadPool as we are fetching 1 row per scan RPC
                Assert.assertTrue(ROWS_TO_LOAD_INITIALLY
                        <= histogram.getPercentileDistributionMap().get(
                        PercentileHistogram.NUM_OPS_METRIC_NAME).intValue());
                foundActiveThreadsCountHistogram = true;
            }
            else if (histogram.getHistoName().equals(HistogramName.QueueSize.name())) {
                foundQueueSizeHistogram = true;
            }
        }

        // Assert that the HTableThreadPool expected to be used was actually used
        Assert.assertTrue(foundActiveThreadsCountHistogram);
        Assert.assertTrue(foundQueueSizeHistogram);
    }

    protected void assertHTableThreadPoolNotUsed(
            Map<String, List<HistogramDistribution>> htableThreadPoolHistograms,
            String histogramKey) {
        boolean foundActiveThreadsCountHistogram = false;
        boolean foundQueueSizeHistogram = false;
        List<HistogramDistribution> histograms = htableThreadPoolHistograms.get(histogramKey);
        Assert.assertEquals(2, histograms.size());
        for (HistogramDistribution histogram : histograms) {
            if (histogram.getHistoName().equals(HistogramName.ActiveThreadsCount.name())) {
                foundActiveThreadsCountHistogram = true;
            }
            else if (histogram.getHistoName().equals(HistogramName.QueueSize.name())) {
                foundQueueSizeHistogram = true;
            }
            Assert.assertFalse(histogram.getPercentileDistributionMap().isEmpty());
            for (long value: histogram.getPercentileDistributionMap().values()) {
                Assert.assertEquals(0, value);
            }
        }
        Assert.assertTrue(foundActiveThreadsCountHistogram);
        Assert.assertTrue(foundQueueSizeHistogram);
    }

    protected Map<String, List<HistogramDistribution>> runQueryAndGetHistograms(Connection conn,
                                                                              String tableName)
            throws SQLException {
        Map<String, List<HistogramDistribution>> htableThreadPoolHistograms;

        try (Statement stmt = conn.createStatement()) {
            // Per row submit one task for execution in HTable thread pool
            stmt.setFetchSize(1);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                int rowsRead = 0;
                // Reset the histograms
                PhoenixRuntime.getHTableThreadPoolHistograms();
                while (rs.next()) {
                    rowsRead++;
                }
                Assert.assertEquals(ROWS_TO_LOAD_INITIALLY, rowsRead);
                htableThreadPoolHistograms = PhoenixRuntime.getHTableThreadPoolHistograms();
            }
        }
        return htableThreadPoolHistograms;
    }

    protected void createTableAndUpsertData(Connection conn, String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, v "
                    + "VARCHAR)");
        }
        try (PreparedStatement stmt =conn.prepareStatement("UPSERT INTO " + tableName
                + " VALUES (?, ?)")) {
            for (int i = 1; i <= ROWS_TO_LOAD_INITIALLY; i++) {
                stmt.setString(1, "k" + i);
                stmt.setString(2, "v" + i);
                stmt.executeUpdate();
            }
            conn.commit();
        }
    }
}
