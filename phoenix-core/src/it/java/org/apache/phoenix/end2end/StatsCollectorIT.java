package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(HBaseManagedTimeTest.class)
public class StatsCollectorIT extends BaseHBaseManagedTimeIT {
    private static String url;
    private static HBaseTestingUtility util;
    private static int frequency = 4000;

    @BeforeClass
    public static void doSetup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        setUpConfigForMiniCluster(conf);
        conf.setInt("hbase.client.retries.number", 2);
        conf.setInt("hbase.client.pause", 5000);
        conf.setLong(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_TIME_ATTRIB, 0);
        util = new HBaseTestingUtility(conf);
        util.startMiniCluster();
        String clientPort = util.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
        url = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR + clientPort
                + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
        int histogramDepth = 60;
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.HISTOGRAM_BYTE_DEPTH_CONF_KEY, Integer.toString(histogramDepth));
        props.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Integer.toString(frequency));
        driver = initAndRegisterDriver(url, new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testUpdateStatsForTheTable() throws Throwable {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        // props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(url, props);
        conn.createStatement().execute(
                "CREATE TABLE t ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) \n");
        String[] s;
        Array array;
        conn = upsertValues(props, "t");
        // CAll the update statistics query here. If already major compaction has run this will not get executed.
        stmt = conn.prepareStatement("ANALYZE T");
        stmt.execute();
        stmt = upsertStmt(conn, "t");
        stmt.setString(1, "z");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        // This analyze would not work
        stmt = conn.prepareStatement("ANALYZE T");
        stmt.execute();
        rs = conn.createStatement().executeQuery("SELECT k FROM T");
        assertTrue(rs.next());
        conn.close();
    }

    @Test
    public void testUpdateStatsWithMultipleTables() throws Throwable {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        // props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(url, props);
        conn.createStatement().execute(
                "CREATE TABLE x ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) \n");
        conn.createStatement().execute(
                "CREATE TABLE z ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) \n");
        String[] s;
        Array array;
        conn = upsertValues(props, "x");
        conn = upsertValues(props, "z");
        // CAll the update statistics query here
        stmt = conn.prepareStatement("ANALYZE X");
        stmt.execute();
        stmt = conn.prepareStatement("ANALYZE Z");
        stmt.execute();
        stmt = upsertStmt(conn, "x");
        stmt.setString(1, "z");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        stmt = upsertStmt(conn, "z");
        stmt.setString(1, "z");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.close();
        conn = DriverManager.getConnection(getUrl(), props);
        // This analyze would not work
        stmt = conn.prepareStatement("ANALYZE Z");
        stmt.execute();
        rs = conn.createStatement().executeQuery("SELECT k FROM Z");
        assertTrue(rs.next());
        conn.close();
    }

    private Connection upsertValues(Properties props, String tableName) throws SQLException, IOException,
            InterruptedException {
        Connection conn;
        PreparedStatement stmt;
        // props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(url, props);
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "a");
        String[] s = new String[] { "abc", "def", "ghi", "jkll", null, null, "xxx" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        flush(tableName);
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "b");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        flush(tableName);
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "c");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        flush(tableName);
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "d");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        flush(tableName);
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "b");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        flush(tableName);
        stmt = upsertStmt(conn, tableName);
        stmt.setString(1, "e");
        s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        flush(tableName);
        return conn;
    }

    private void flush(String tableName) throws IOException, InterruptedException {
        util.getHBaseAdmin().flush(tableName.toUpperCase());
    }

    private PreparedStatement upsertStmt(Connection conn, String tableName) throws SQLException {
        PreparedStatement stmt;
        stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        return stmt;
    }

}