package org.apache.phoenix.monitoring;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_ENABLED;
import static org.apache.phoenix.query.QueryServices.HTABLE_THREAD_POOL_METRICS_ENABLED;

@Category(NeedsOwnMiniClusterTest.class)
public class HTableThreadPoolMetricsIT extends BaseTest {
    private static final int ROWS_TO_LOAD_INITIALLY = 10;


    @BeforeClass
    public static void setUp() throws Exception {
        setUpTestDriver(new ReadOnlyProps(Collections.<String, String>emptyMap().entrySet().iterator()));
    }

    @Test
    public void testCSQIThreadPoolMetrics() throws Exception {
        String tableName = generateUniqueName();
        Properties props = new Properties();
        props.setProperty(HTABLE_THREAD_POOL_METRICS_ENABLED, "true");
        props.setProperty(CQSI_THREAD_POOL_ENABLED, "true");
        try (Connection conn = DriverManager.getConnection(getUrl("service1"), props)) {
            createTableAndUpsertData(conn, tableName);

            try (Statement stmt = conn.createStatement()) {
                int concurrencyLevel = 100;
                ThreadPoolExecutor threadPoolExecutor =
                        (ThreadPoolExecutor) Executors.newFixedThreadPool(concurrencyLevel);
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            stmt.setFetchSize(1);
                            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                                int rowCount = 0;
                                while (rs.next()) {
                                    rowCount++;
                                }
                                Assert.assertEquals(ROWS_TO_LOAD_INITIALLY, rowCount);
                            }
                        }
                        catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                for (int i = 0; i < concurrencyLevel; i++) {
                    threadPoolExecutor.execute(runnable);
                }
                threadPoolExecutor.shutdown();
                threadPoolExecutor.awaitTermination(60, TimeUnit.SECONDS);
            }
            System.out.println("All queries completed");
        }
        System.out.println("Checking thread pool statistics");
        Map<String, List<HistogramDistribution>> htableThreadPoolHistograms =
                PhoenixRuntime.getHTableThreadPoolHistograms();
        for (List<HistogramDistribution> histograms : htableThreadPoolHistograms.values()) {
            for (HistogramDistribution histogram : histograms) {
                System.out.println(histogram.getHistoName());
                System.out.println(histogram.getPercentileDistributionMap());
                System.out.println(histogram.getTags());
            }
        }
        System.out.println("Recheck the thread pool statistics");
        htableThreadPoolHistograms = PhoenixRuntime.getHTableThreadPoolHistograms();
        for (List<HistogramDistribution> histograms : htableThreadPoolHistograms.values()) {
            for (HistogramDistribution histogram : histograms) {
                System.out.println(histogram.getHistoName());
                System.out.println(histogram.getPercentileDistributionMap());
                System.out.println(histogram.getTags());
            }
        }
    }

    private void createTableAndUpsertData(Connection conn, String tableName) throws SQLException {
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
        }
        conn.commit();
    }
}
