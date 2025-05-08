package org.apache.phoenix.query;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_ALLOW_CORE_THREAD_TIMEOUT;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_CORE_POOL_SIZE;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_ENABLED;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_KEEP_ALIVE_SECONDS;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_MAX_QUEUE;
import static org.apache.phoenix.query.QueryServices.CQSI_THREAD_POOL_MAX_THREADS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class ConnectionQueryServicesImplThreadPoolIT extends BaseTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ConnectionQueryServicesImplThreadPoolIT.class);
    private AtomicInteger counter = new AtomicInteger();
    private static HBaseTestingUtility hbaseTestUtil;
    private String tableName;
    private static final String CONN_QUERY_SERVICE_CREATE_TABLE = "CONN_QUERY_SERVICE_CREATE_TABLE";
    private static final String CONN_QUERY_SERVICE_1 = "CONN_QUERY_SERVICE_1";
    private static final String CONN_QUERY_SERVICE_2 = "CONN_QUERY_SERVICE_2";
    private static final int TEST_CQSI_THREAD_POOL_KEEP_ALIVE_SECONDS = 13;
    private static final int TEST_CQSI_THREAD_POOL_CORE_POOL_SIZE = 17;
    private static final int TEST_CQSI_THREAD_POOL_MAX_THREADS = 19;
    private static final int TEST_CQSI_THREAD_POOL_MAX_QUEUE = 23;



    @BeforeClass
    public static void doSetup() throws Exception {
        InstanceResolver.clearSingletons();
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override public Configuration getConfiguration() {
                Configuration conf = HBaseConfiguration.create();
                conf.set(CQSI_THREAD_POOL_ENABLED, Boolean.toString(true));
                conf.set(CQSI_THREAD_POOL_KEEP_ALIVE_SECONDS, Integer.toString(TEST_CQSI_THREAD_POOL_KEEP_ALIVE_SECONDS));
                conf.set(CQSI_THREAD_POOL_CORE_POOL_SIZE,  Integer.toString(TEST_CQSI_THREAD_POOL_CORE_POOL_SIZE));
                conf.set(CQSI_THREAD_POOL_MAX_THREADS,  Integer.toString(TEST_CQSI_THREAD_POOL_MAX_THREADS));
                conf.set(CQSI_THREAD_POOL_MAX_QUEUE,  Integer.toString(TEST_CQSI_THREAD_POOL_MAX_QUEUE));
                conf.set(CQSI_THREAD_POOL_ALLOW_CORE_THREAD_TIMEOUT, Boolean.toString(true));
                return conf;
            }

            @Override public Configuration getConfiguration(Configuration confToClone) {
                Configuration conf = HBaseConfiguration.create();
                conf.set(CQSI_THREAD_POOL_ENABLED, Boolean.toString(true));
                conf.set(CQSI_THREAD_POOL_KEEP_ALIVE_SECONDS, Integer.toString(TEST_CQSI_THREAD_POOL_KEEP_ALIVE_SECONDS));
                conf.set(CQSI_THREAD_POOL_CORE_POOL_SIZE,  Integer.toString(TEST_CQSI_THREAD_POOL_CORE_POOL_SIZE));
                conf.set(CQSI_THREAD_POOL_MAX_THREADS,  Integer.toString(TEST_CQSI_THREAD_POOL_MAX_THREADS));
                conf.set(CQSI_THREAD_POOL_MAX_QUEUE,  Integer.toString(TEST_CQSI_THREAD_POOL_MAX_QUEUE));
                conf.set(CQSI_THREAD_POOL_ALLOW_CORE_THREAD_TIMEOUT, Boolean.toString(true));
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });
        Configuration conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        hbaseTestUtil.startMiniCluster();
        String zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
    }

    @AfterClass
    public static void tearDownMiniCluster() {
        try {
            if (hbaseTestUtil != null) {
                hbaseTestUtil.shutdownMiniCluster();
            }
        } catch (Exception e) {
            // ignore
        } finally {
            ServerMetadataCacheTestImpl.resetCache();
        }
    }

    @Before
    public void setUp() throws Exception {
        tableName = generateUniqueName();
        createTable(tableName);
    }

    private String connUrlWithPrincipal(String principalName) throws SQLException {
        return ConnectionInfo.create(url, null, null).withPrincipal(principalName).toUrl();
    }

    @Test
    public void checkHTableThreadPoolExecutorSame() throws Exception {
        Table table = createCQSI(null).getTable(tableName.getBytes());
        assertTrue(table instanceof HTable);
        HTable hTable = (HTable) table;
        Field props = hTable.getClass().getDeclaredField("pool");
        props.setAccessible(true);
        validateThreadPoolExecutor((ThreadPoolExecutor) props.get(hTable));
    }

    @Test
    public void testMultipleCQSIThreadPoolsInParallel() throws Exception {
        ConnectionQueryServices cqsiExternal1  = createCQSI(CONN_QUERY_SERVICE_1);
        ConnectionQueryServices cqsiExternal2  = createCQSI(CONN_QUERY_SERVICE_2);
        Thread cqsiThread1 = new Thread(() -> {
            try {
                ConnectionQueryServices cqsi  = createCQSI(CONN_QUERY_SERVICE_1);
                checkSameThreadPool(cqsiExternal1, cqsi);
                checkDifferentThreadPool(cqsiExternal2, cqsi);
                validateThreadPoolExecutor(extractThreadPoolExecutorFromCQSI(cqsi));
                counter.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread cqsiThread2 = new Thread(() -> {
            try {
                ConnectionQueryServices cqsi  = createCQSI(CONN_QUERY_SERVICE_1);
                checkSameThreadPool(cqsiExternal1, cqsi);
                checkDifferentThreadPool(cqsiExternal2, cqsi);
                validateThreadPoolExecutor(extractThreadPoolExecutorFromCQSI(cqsi));
                counter.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread cqsiThread3 = new Thread(() -> {
            try {
                ConnectionQueryServices cqsi  = createCQSI(CONN_QUERY_SERVICE_2);
                checkSameThreadPool(cqsiExternal2, cqsi);
                checkDifferentThreadPool(cqsiExternal1, cqsi);
                validateThreadPoolExecutor(extractThreadPoolExecutorFromCQSI(cqsi));
                counter.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread cqsiThread4 = new Thread(() -> {
            try {
                ConnectionQueryServices cqsi  = createCQSI(CONN_QUERY_SERVICE_2);
                checkSameThreadPool(cqsiExternal2, cqsi);
                checkDifferentThreadPool(cqsiExternal1, cqsi);
                validateThreadPoolExecutor(extractThreadPoolExecutorFromCQSI(cqsi));
                counter.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });


        cqsiThread1.start();
        cqsiThread2.start();
        cqsiThread3.start();
        cqsiThread4.start();
        cqsiThread1.join();
        cqsiThread2.join();
        cqsiThread3.join();
        cqsiThread4.join();

        assertEquals(4, counter.get());
    }

    private void checkSameThreadPool(ConnectionQueryServices cqsi1, ConnectionQueryServices cqsi2) throws NoSuchFieldException, IllegalAccessException {
        assertSame(extractThreadPoolExecutorFromCQSI(cqsi1), extractThreadPoolExecutorFromCQSI(cqsi2));
    }

    private void checkDifferentThreadPool(ConnectionQueryServices cqsi1, ConnectionQueryServices cqsi2) throws NoSuchFieldException, IllegalAccessException {
        assertNotSame(extractThreadPoolExecutorFromCQSI(cqsi1), extractThreadPoolExecutorFromCQSI(cqsi2));
    }

    private ConnectionQueryServices createCQSI(String serviceName) throws SQLException {
        String principalURL = connUrlWithPrincipal(serviceName);
        Connection conn = DriverManager.getConnection(principalURL);
        return conn.unwrap(PhoenixConnection.class).getQueryServices();
    }

    private void validateThreadPoolExecutor(ThreadPoolExecutor threadPoolExecutor) {
        assertEquals(TEST_CQSI_THREAD_POOL_KEEP_ALIVE_SECONDS, threadPoolExecutor.getKeepAliveTime(TimeUnit.SECONDS));
        assertEquals(TEST_CQSI_THREAD_POOL_CORE_POOL_SIZE, threadPoolExecutor.getCorePoolSize());
        assertEquals(TEST_CQSI_THREAD_POOL_MAX_THREADS, threadPoolExecutor.getMaximumPoolSize());
        assertEquals(TEST_CQSI_THREAD_POOL_MAX_QUEUE, threadPoolExecutor.getQueue().remainingCapacity());
    }


    private void createTable(String tableName) throws SQLException {
        String CREATE_TABLE_DDL = "CREATE TABLE IF NOT EXISTS %s (K VARCHAR(10) NOT NULL"
                + " PRIMARY KEY, V VARCHAR)";
        String princURL = connUrlWithPrincipal(CONN_QUERY_SERVICE_CREATE_TABLE);
        LOGGER.info("Connection Query Service : " + CONN_QUERY_SERVICE_CREATE_TABLE + " URL : " + princURL);
        try (Connection conn = DriverManager.getConnection(princURL);
             Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(CREATE_TABLE_DDL, tableName));
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
