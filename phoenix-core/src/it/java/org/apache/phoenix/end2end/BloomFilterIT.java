package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.flush;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServer;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServerWrapper;
import org.apache.phoenix.thirdparty.com.google.common.base.MoreObjects;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
public class BloomFilterIT extends ParallelStatsDisabledIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterIT.class);
    private static class BloomFilterMetrics {
        // total lookup requests
        private long requestsCount;
        // requests where key does not exist
        private long negativeResultsCount;
        // potential lookup requests rejected because no bloom filter present in storefile
        private long eligibleRequestsCount;

        private BloomFilterMetrics() {
            this.requestsCount = 0;
            this.negativeResultsCount = 0;
            this.eligibleRequestsCount = 0;
        }

        private BloomFilterMetrics(long requestsCount, long negativeResultsCount, long eligibleRequestsCount) {
            this.requestsCount = requestsCount;
            this.negativeResultsCount = negativeResultsCount;
            this.eligibleRequestsCount = eligibleRequestsCount;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            BloomFilterMetrics rhs = (BloomFilterMetrics)obj;
            return (this.requestsCount == rhs.requestsCount &&
                    this.negativeResultsCount == rhs.negativeResultsCount &&
                    this.eligibleRequestsCount == rhs.eligibleRequestsCount);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("requestsCount", requestsCount)
                    .add("negativeResultsCount", negativeResultsCount)
                    .add("eligibleRequestsCount", eligibleRequestsCount)
                    .toString();
        }
    }
    private BloomFilterMetrics beforeMetrics;

    private BloomFilterMetrics getBloomFilterMetrics() {
        HBaseTestingUtility util = getUtility();
        HRegionServer regionServer = util.getHBaseCluster().getRegionServer(0);
        MetricsRegionServer regionServerMetrics = regionServer.getMetrics();
        MetricsRegionServerWrapper regionServerWrapper = regionServerMetrics.getRegionServerWrapper();
        long requestsCount = regionServerWrapper.getBloomFilterRequestsCount();
        long negativeResultsCount = regionServerWrapper.getBloomFilterNegativeResultsCount();
        long eligibleRequestsCount = regionServerWrapper.getBloomFilterEligibleRequestsCount();
        return new BloomFilterMetrics(requestsCount, negativeResultsCount, eligibleRequestsCount);
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(HConstants.REGIONSERVER_METRICS_PERIOD, Long.toString(1000));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void testSetup() {
        beforeMetrics = getBloomFilterMetrics();
    }

    @Test
    public void testPointLookup() throws Exception {
        String tableName = generateUniqueName();
        BloomFilterMetrics expectedMetrics = new BloomFilterMetrics();
        //String ddl = String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, V1 VARCHAR) BLOOMFILTER='NONE'", tableName);
        String ddl = String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, V1 VARCHAR)", tableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            populateTable(conn, tableName);
            // flush the memstore to storefiles which will write the bloom filter
            flush(getUtility(), TableName.valueOf(tableName));
            // negative key point lookup
            String dql = String.format("SELECT * FROM %s where id = 7", tableName);
            try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
                assertFalse(rs.next());
                expectedMetrics.requestsCount +=1;
                expectedMetrics.negativeResultsCount +=1;
            }
            // key exists
            dql = String.format("SELECT * FROM %s where id = 4", tableName);
            try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1));
                // negative request shouldn't be incremented since key exists
                expectedMetrics.requestsCount +=1;
            }
            // multiple keys point lookup
            dql = String.format("SELECT * FROM %s where id IN (4,7)", tableName);
            try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1));
                assertFalse(rs.next());
                // bloom filter won't be used since scan start/stop key is different
            }
            verifyBloomFilterMetrics(expectedMetrics);
        }
    }

    @Test
    public void testPointLookupOnSaltedTable() throws Exception {
        String tableName = generateUniqueName();
        BloomFilterMetrics expectedMetrics = new BloomFilterMetrics();
        String ddl = String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, V1 VARCHAR) SALT_BUCKETS=3", tableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            populateTable(conn, tableName);
            flush(getUtility(), TableName.valueOf(tableName));
            // negative key point lookup
            String dql = String.format("SELECT * FROM %s where id = 7", tableName);
            try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
                assertFalse(rs.next());
                expectedMetrics.requestsCount +=1;
                expectedMetrics.negativeResultsCount +=1;
            }
        }
        verifyBloomFilterMetrics(expectedMetrics);
    }

    @Test
    public void testAlterBloomFilter() throws Exception {
        String tableName = generateUniqueName();
        BloomFilterMetrics expectedMetrics = new BloomFilterMetrics();
        String ddl = String.format("CREATE TABLE %s (ID INTEGER NOT NULL PRIMARY KEY, V1 VARCHAR) BLOOMFILTER='NONE'", tableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            populateTable(conn, tableName);
            flush(getUtility(), TableName.valueOf(tableName));
            String dql = String.format("SELECT * FROM %s where id = 7", tableName);
            try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
                assertFalse(rs.next());
                // since bloom filter is not enabled
                expectedMetrics.eligibleRequestsCount +=1;
                verifyBloomFilterMetrics(expectedMetrics);
            }
            ddl = String.format("ALTER TABLE %s SET BLOOMFILTER='ROW'", tableName);
            conn.createStatement().execute(ddl);
            // alter table changes the table descriptor and table region re-opens reset metrics
            beforeMetrics = getBloomFilterMetrics();
            expectedMetrics = new BloomFilterMetrics();
            // Insert 2 more rows
            String dml = String.format("UPSERT INTO %s VALUES (100, 'val_100')", tableName);
            conn.createStatement().execute(dml);
            dml = String.format("UPSERT INTO %s VALUES (200, 'val_200')", tableName);
            conn.createStatement().execute(dml);
            conn.commit();
            // A new storefile should be created with bloom filter
            flush(getUtility(), TableName.valueOf(tableName));
            dql = String.format("SELECT * FROM %s where id = 150", tableName);
            try (ResultSet rs = conn.createStatement().executeQuery(dql)) {
                assertFalse(rs.next());
                expectedMetrics.requestsCount +=1;
                expectedMetrics.negativeResultsCount +=1;
                verifyBloomFilterMetrics(expectedMetrics);
            }
        }
    }

    private void verifyBloomFilterMetrics(BloomFilterMetrics expectedMetrics) throws InterruptedException {
        long metricsDelay = config.getLong(HConstants.REGIONSERVER_METRICS_PERIOD,
                HConstants.DEFAULT_REGIONSERVER_METRICS_PERIOD);
        // Ensure that the region server accumulates the metrics
        Thread.sleep(metricsDelay + 1000);
        BloomFilterMetrics afterMetrics = getBloomFilterMetrics();
        LOGGER.info("Before={} After={} Expected={}", beforeMetrics, afterMetrics, expectedMetrics);

        BloomFilterMetrics deltaMetrics = new BloomFilterMetrics(
                afterMetrics.requestsCount - beforeMetrics.requestsCount,
                afterMetrics.negativeResultsCount - beforeMetrics.negativeResultsCount,
                afterMetrics.eligibleRequestsCount - beforeMetrics.eligibleRequestsCount);

        Assert.assertEquals(expectedMetrics, deltaMetrics);
    }

    private void populateTable(Connection conn, String tableName) throws SQLException {
        try(PreparedStatement ps = conn.prepareStatement(String.format("UPSERT INTO %s VALUES (?, ?)", tableName))) {
            for (int i = 0; i < 16; i = i + 2) {
                ps.setInt(1, i);
                ps.setString(2, "val_" + i);
                ps.executeUpdate();
            }
            conn.commit();
        }
    }
}
