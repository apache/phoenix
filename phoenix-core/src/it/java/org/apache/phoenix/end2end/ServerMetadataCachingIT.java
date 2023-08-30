package org.apache.phoenix.end2end;


import org.apache.phoenix.cache.ServerMetadataCache;
import org.apache.phoenix.coprocessor.PhoenixRegionServerEndpoint;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

@Category({NeedsOwnMiniClusterTest.class })
public class ServerMetadataCachingIT extends BaseTest {

    private final Random RANDOM = new Random(42);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(REGIONSERVER_COPROCESSOR_CONF_KEY,
                PhoenixRegionServerEndpoint.class.getName());
        props.put(QueryServices.LAST_DDL_TIMESTAMP_VALIDATION_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    private void createTable(Connection conn, String tableName, long updateCacheFrequency) throws SQLException {
        conn.createStatement().execute("CREATE TABLE " + tableName
                + "(k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER, v2 INTEGER)"
                + (updateCacheFrequency == 0 ? "" : "UPDATE_CACHE_FREQUENCY="+updateCacheFrequency));
    }

    private void upsert(Connection conn, String tableName) throws SQLException {
        conn.createStatement().execute("UPSERT INTO " + tableName +
                " (k, v1, v2) VALUES ("+  RANDOM.nextInt() +", " + RANDOM.nextInt() + ", " + RANDOM.nextInt() +")");
        conn.commit();
    }

    private void query(Connection conn, String tableName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
        rs.next();
    }

    private void alterTableAddColumn(Connection conn, String tableName, String columnName) throws SQLException {
        conn.createStatement().execute("ALTER TABLE " + tableName + " ADD IF NOT EXISTS "
                + columnName + " INTEGER");
    }

    @Test
    public void testSelectQueryWithOldDDLTimestamp() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");
        String tableName = generateUniqueName();

        try (Connection conn1 = DriverManager.getConnection(url1);
             Connection conn2 = DriverManager.getConnection(url2)) {

            // create table and upsert data using client-1
            createTable(conn1, tableName, Long.MAX_VALUE);
            upsert(conn1, tableName);

            // select query from client-2 to populate client side metadata cache
            query(conn2, tableName);

            // add column using client-1 to update last ddl timestamp
            alterTableAddColumn(conn1, tableName, "newCol1");

            // invalidate region server cache
            ServerMetadataCache.resetCache();

            // select query from client-2
            query(conn2, tableName);
        }
    }
}
