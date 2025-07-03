package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;

@Category(NeedsOwnMiniClusterTest.class)
public class CDCStreamTTLIT extends CDCBaseIT  {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        props.put(QueryServices.PHOENIX_CDC_STREAM_PARTITION_EXPIRY_MIN_AGE_MS, Long.toString(10*1000));
        props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        taskRegionEnvironment =
                getUtility()
                        .getRSForFirstRegionInTable(
                                PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .getRegions(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .get(0).getCoprocessorHost()
                        .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
    }

    @Test
    public void testCDCStreamTTL() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("m"));
        String sql = "SELECT PARTITION_END_TIME FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(3, count);
        Thread.sleep(11000);
        rs = conn.createStatement().executeQuery(sql);
        int newCount = 0;
        while (rs.next()) {
            // parent partition row with non-zero end time should have expired
            if (rs.getLong(1) > 0) {
                Assert.fail("Closed partition should have expired after TTL.");
            }
            newCount++;
        }
        Assert.assertEquals(2, newCount);
    }
}
