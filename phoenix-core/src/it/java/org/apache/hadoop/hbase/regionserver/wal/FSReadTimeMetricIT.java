package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

@Category(NeedsOwnMiniClusterTest.class)
public class FSReadTimeMetricIT extends BaseTest {
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
        setUpTestDriver(new ReadOnlyProps(props));
    }

    @Test
    public void testFsReadTimeMetric() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        // create table with small block size and upsert enough rows to have at least 2 blocks
        stmt.execute("CREATE TABLE " + tableName
                + " (A UNSIGNED_LONG NOT NULL PRIMARY KEY, Z UNSIGNED_LONG) BLOCKSIZE=200");
        for (int i = 1; i <= 20; i++) {
            String sql = String.format("UPSERT INTO %s VALUES (%d, %d)", tableName, i, i);
            stmt.execute(sql);
        }
        conn.commit();
        String SELECT_ALL_QUERY = "SELECT * FROM " + tableName;

        // read from memory
        long time0 = getFsReadTimeFromSql(stmt, SELECT_ALL_QUERY);
        Assert.assertEquals(0, time0);

        // flush and clear cache
        TestUtil.flush(utility, TableName.valueOf(tableName));
        TestUtil.clearBlockCache(utility, TableName.valueOf(tableName));

        // read from disk
        long time1 = getFsReadTimeFromSql(stmt, SELECT_ALL_QUERY);
        Assert.assertTrue(time1 > 0);

        // read from cache
        long time2 = getFsReadTimeFromSql(stmt, SELECT_ALL_QUERY);
        Assert.assertEquals(0, time2);
    }

    private long getFsReadTimeFromSql(Statement stmt, String sql) throws SQLException {
        return TestUtil.getMetricFromSql(stmt, sql, MetricType.FS_READ_TIME);
    }
}
