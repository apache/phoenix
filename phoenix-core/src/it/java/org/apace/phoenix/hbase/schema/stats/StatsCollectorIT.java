package org.apace.phoenix.hbase.schema.stats;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;

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
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.HBaseManagedTimeTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stat.StatisticsConstants;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Maps;

@Category(HBaseManagedTimeTest.class)
public class StatsCollectorIT extends BaseHBaseManagedTimeIT {
  private static String url;
  private static PhoenixTestDriver driver;
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
    url = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR
        + clientPort + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    int targetQueryConcurrency = 3;
    int maxQueryConcurrency = 5;
    int histogramDepth = 60;
    Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
    props.put(QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB, Integer.toString(maxQueryConcurrency));
    props.put(QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB, Integer.toString(targetQueryConcurrency));
    props.put(StatisticsConstants.HISTOGRAM_BYTE_DEPTH_CONF_KEY, Integer.toString(histogramDepth));
    props.put(QueryServices.MAX_INTRA_REGION_PARALLELIZATION_ATTRIB, Integer.toString(Integer.MAX_VALUE));
    props.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Integer.toString(frequency));
    driver = initAndRegisterDriver(url, new ReadOnlyProps(props.entrySet().iterator()));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      destroyDriver(driver);
    } finally {
      util.shutdownMiniCluster();
    }
  }

  protected static TableRef getTableRef(Connection conn, long ts, PTable pTable)
      throws SQLException {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    if (pTable == null) {
      return new TableRef(null, pconn.getMetaDataCache().getTable(
          new PTableKey(pconn.getTenantId(), "T")), ts, false);
    } else {
      return new TableRef(null, pTable, ts, false);
    }
  }

  @Test
  public void testUpdateStatsForTheTable() throws Throwable {
    Connection conn;
    PreparedStatement stmt;
    ResultSet rs;
    long ts = nextTimestamp();
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    //props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
    conn = DriverManager.getConnection(url, props);
    conn.createStatement()
        .execute(
            "CREATE TABLE t ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) \n");
    //props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
    conn = DriverManager.getConnection(url, props);
    stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
    stmt.setString(1, "a");
    String[] s = new String[] { "abc", "def", "ghi", "jkll", null, null, "xxx" };
    Array array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(2, array);
    s = new String[] { "abc", "def", "ghi", "jkll", null, null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(3, array);
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
    stmt.setString(1, "b");
    s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(2, array);
    s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(3, array);
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
    stmt.setString(1, "c");
    s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(2, array);
    s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(3, array);
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
    stmt.setString(1, "d");
    s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(2, array);
    s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(3, array);
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
    stmt.setString(1, "b");
    s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(2, array);
    s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(3, array);
    stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
    stmt.setString(1, "e");
    s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(2, array);
    s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(3, array);
    stmt.execute();
    conn.commit();
    // CAll the update statistics query here
    stmt = conn.prepareStatement("ANALYZE T");
    stmt.execute();
    stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
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
    rs = conn.createStatement().executeQuery("SELECT b_string_array FROM T");
    assertTrue(rs.next());
    conn.close();
  }

}
