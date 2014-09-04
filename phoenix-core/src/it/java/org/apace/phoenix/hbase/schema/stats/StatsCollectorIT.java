package org.apace.phoenix.hbase.schema.stats;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.end2end.HBaseManagedTimeTest;
import org.apache.phoenix.iterate.DefaultParallelIteratorRegionSplitter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.stat.StatisticsConstants;
import org.apache.phoenix.util.PhoenixRuntime;
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

  private static List<KeyRange> getSplits(Connection conn, long ts, final Scan scan, PTable pTable)
      throws SQLException {
    TableRef tableRef = getTableRef(conn, ts, pTable);
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    final List<HRegionLocation> regions = pconn.getQueryServices().getAllTableRegions(
        tableRef.getTable().getPhysicalName().getBytes());
    PhoenixStatement statement = new PhoenixStatement(pconn);
    StatementContext context = new StatementContext(statement, null, scan, new SequenceManager(
        statement));
    DefaultParallelIteratorRegionSplitter splitter = new DefaultParallelIteratorRegionSplitter(
        context, tableRef, HintNode.EMPTY_HINT_NODE) {
      @Override
      protected List<HRegionLocation> getAllRegions() throws SQLException {
        return DefaultParallelIteratorRegionSplitter.filterRegions(regions, scan.getStartRow(),
            scan.getStopRow());
      }
    };
    List<KeyRange> keyRanges = splitter.getSplits();
    Collections.sort(keyRanges, new Comparator<KeyRange>() {
      @Override
      public int compare(KeyRange o1, KeyRange o2) {
        return Bytes.compareTo(o1.getLowerRange(), o2.getLowerRange());
      }
    });
    return keyRanges;
  }

  @Test
  public void testUpdateStatsForTheTable() throws Throwable {
    Connection conn;
    PreparedStatement stmt;
    ResultSet rs;
    long ts = nextTimestamp();
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
    conn = DriverManager.getConnection(url, props);
    conn.createStatement()
        .execute(
            "CREATE TABLE t ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) \n");
    conn.close();
    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
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
/*    ConnectionQueryServices services = driver.getConnectionQueryServices(url,
        PropertiesUtil.deepCopy(TEST_PROPERTIES));
    // This getRef does not have the stats. We may need to update the stats
    // using getTable
    TableRef table = getTableRef(conn, ts, null);
    Scan scan = new Scan(); 
    HTable ht = new HTable(util.getConfiguration(), "SYSTEM.STATS");
    ResultScanner scanner = ht.getScanner(scan);
    Result[] next = scanner.next(4);
    for (Result res : next) {
      CellScanner cellScanner = res.cellScanner();
      cellScanner.advance();
      Cell current = cellScanner.current();
    } 
    MetaDataMutationResult result = services.getTable(null, table.getTable().getSchemaName()
        .getBytes(), table.getTable().getName().getBytes(), table.getTimeStamp(),
        ((PhoenixConnection) conn).getSCN());
    scan.setStartRow(HConstants.EMPTY_START_ROW);
    List<KeyRange> keyRanges = getSplits(conn, ts, scan, result.getTable());*/
    stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
    stmt.setString(1, "z");
    s = new String[] { "xyz", "def", "ghi", "jkll", null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(2, array);
    s = new String[] { "zya", "def", "ghi", "jkll", null, null, null, "xxx" };
    array = conn.createArrayOf("VARCHAR", s);
    stmt.setArray(3, array);
    stmt.execute();
    stmt = conn.prepareStatement("ANALYZE T");
    stmt.execute();
/*    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
    conn = DriverManager.getConnection(url, props);*/
    rs = conn.createStatement().executeQuery("SELECT b_string_array FROM T");
    System.out.println();
  }

  @Test
  public void testUpdateStatsAndUseIt() throws Exception {

  }
}
