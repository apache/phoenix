package org.apache.phoenix.replication;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.replication.tool.LogFileAnalyzer;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
public class ReplicationLogIT extends ParallelStatsDisabledIT {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogIT.class);

    @Rule
    public TestName name = new TestName();

    private int syscatLogCountBeforeTest;
    private int childLinkLogCountBeforeTest;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.SYNCHRONOUS_REPLICATION_ENABLED, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest() throws Exception {
        LOG.info("Starting test {}", name.getMethodName());
        //getReplicationLog().rotateLog(RotationReason.TIME);
        Map<String, List<Mutation>> logsByTable = groupLogsByTable();
        dumpTableLogCount(logsByTable);
        syscatLogCountBeforeTest = getCountForTable(logsByTable, SYSTEM_CATALOG_NAME);
        childLinkLogCountBeforeTest = getCountForTable(logsByTable, SYSTEM_CHILD_LINK_NAME);
        LOG.info("Before Test {} syscat={} child_links={}", name.getMethodName(),
                syscatLogCountBeforeTest, childLinkLogCountBeforeTest);
    }

    private ReplicationLog getReplicationLog() throws IOException {
        HRegionServer rs = getUtility().getHBaseCluster().getRegionServer(0);
        return ReplicationLog.get(config, rs.getServerName());
    }

    private Map<String, List<Mutation>> groupLogsByTable() throws Exception {
        ReplicationLog log = getReplicationLog();
        // close the current writer to write the Trailer
        log.closeCurrentWriter();
        LogFileAnalyzer analyzer = new LogFileAnalyzer();
        analyzer.setConf(config);
        String[] args = {"--check", standbyUri.getPath()};
        assertEquals(0, analyzer.run(args));
        return analyzer.groupLogsByTable(standbyUri.getPath());
    }

    private int getCountForTable(Map<String, List<Mutation>> logsByTable,
                                 String tableName) throws Exception {
        List<Mutation> mutations = logsByTable.get(tableName);
        return mutations != null ? mutations.size() : 0;
    }

    private void verifyReplication(Connection conn,
                                   Map<String, Integer> expected) throws Exception {
        Map<String, List<Mutation>> mutationsByTable = groupLogsByTable();
        dumpTableLogCount(mutationsByTable);
        for (Map.Entry<String, Integer> entry : expected.entrySet()) {
            String tableName = entry.getKey();
            int expectedMutationCount = entry.getValue();
            List<Mutation> mutations = mutationsByTable.get(tableName);
            int actualMutationCount = mutations != null ? mutations.size() : 0;
            try {
                if (!tableName.equals(SYSTEM_CATALOG_NAME)) {
                    assertEquals(String.format("For table %s", tableName),
                            expectedMutationCount, actualMutationCount);
                } else {
                    // special handling for syscat
                    assertTrue("For SYSCAT", actualMutationCount >= expectedMutationCount);
                }
            } catch (AssertionError e) {
                TestUtil.dumpTable(conn, TableName.valueOf(tableName));
                throw e;
            }
        }
    }

    private void dumpTableLogCount(Map<String, List<Mutation>> mutationsByTable) {
        LOG.info("Dump table log count for test {}", name.getMethodName());
        for  (Map.Entry<String, List<Mutation>> table : mutationsByTable.entrySet()) {
            LOG.info("#Log entries for {} = {}", table.getKey(), table.getValue().size());
        }
    }

    private void moveRegionToServer(TableName tableName, ServerName sn) throws Exception {
        HBaseTestingUtility util = getUtility();
        try (RegionLocator locator = util.getConnection().getRegionLocator(tableName)) {
            String regEN = locator.getAllRegionLocations().get(0).getRegionInfo().getEncodedName();
            while (!sn.equals(locator.getAllRegionLocations().get(0).getServerName())) {
                LOG.info("Moving region {} of table {} to server {}", regEN, tableName, sn);
                util.getAdmin().move(Bytes.toBytes(regEN), sn);
                Thread.sleep(100);
            }
            LOG.info("Moved region {} of table {} to server {}", regEN, tableName, sn);
        }
    }

    private PhoenixTestBuilder.SchemaBuilder createViewHierarchy() throws Exception {
        // Define the test schema.
        // 1. Table with columns => (ORG_ID, KP, COL1, COL2, COL3), PK => (ORG_ID, KP)
        // 2. GlobalView with columns => (ID, COL4, COL5, COL6), PK => (ID)
        // 3. Tenant with columns => (ZID, COL7, COL8, COL9), PK => (ZID)
        final PhoenixTestBuilder.SchemaBuilder schemaBuilder = new PhoenixTestBuilder.SchemaBuilder(getUrl());
        PhoenixTestBuilder.SchemaBuilder.TableOptions tableOptions =
                PhoenixTestBuilder.SchemaBuilder.TableOptions.withDefaults();
        PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions
                globalViewOptions = PhoenixTestBuilder.SchemaBuilder.GlobalViewOptions.withDefaults();
        PhoenixTestBuilder.SchemaBuilder.TenantViewOptions
                tenantViewWithOverrideOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewOptions.withDefaults();
        PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions
                tenantViewIndexOverrideOptions = PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions.withDefaults();

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            schemaBuilder.withTableOptions(tableOptions)
                    .withGlobalViewOptions(globalViewOptions)
                    .withTenantViewOptions(tenantViewWithOverrideOptions)
                    .withTenantViewIndexOptions(tenantViewIndexOverrideOptions)
                    .buildWithNewTenant();
        }
        return schemaBuilder;
    }

    @Test
    public void testAppendAndSync() throws Exception {
        final String tableName = "T_" + generateUniqueName();
        final String indexName1 = "I_" + generateUniqueName();
        final String indexName2 = "I_" + generateUniqueName();
        final String indexName3 = "L_" + generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format("create table %s (id1 integer not null, " +
                    "id2 integer not null, val1 varchar, val2 varchar " +
                    "constraint pk primary key (id1, id2))", tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format("create index %s on %s (val1) include (val2)",
                    indexName1, tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format("create index %s on %s (val2) include (val1)",
                    indexName2, tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format("create local index %s on %s (id2,val1) include (val2)",
                    indexName3, tableName);
            conn.createStatement().execute(ddl);
            conn.commit();
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + tableName + " VALUES(?, ?, ?, ?)");
            // upsert 50 rows
            int rowCount = 50;
            for (int i = 0; i < 5; ++i) {
                for (int j = 0; j < 10; ++j) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, j);
                    stmt.setString(3, "abcdefghijklmnopqrstuvwxyz");
                    stmt.setString(4, null);
                    stmt.executeUpdate();
                }
                conn.commit();
            }
            // do some atomic upserts which will be ignored and therefore not replicated
            stmt = conn.prepareStatement("upsert into " + tableName + " VALUES(?, ?, ?) " +
                    "ON DUPLICATE KEY IGNORE");
            conn.setAutoCommit(true);
            for (int i = 0; i < 5; ++i) {
                for (int j = 0; j < 2; ++j) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, j);
                    stmt.setString(3, null);
                    assertEquals(0, stmt.executeUpdate());
                }
            }
            // verify the correctness of the index
            IndexToolIT.verifyIndexTable(tableName, indexName1, conn);
            // verify replication
            Map<String, Integer> expected = Maps.newHashMap();
            // mutation count will be equal to row count since the atomic upsert mutations will be
            // ignored and therefore not replicated
            expected.put(tableName, rowCount * 3); // Put + Delete + local index update
            // for index1 unverified + verified + delete (Delete column)
            expected.put(indexName1, rowCount * 3);
            // for index2 unverified + verified  since the null column is part of row key
            expected.put(indexName2, rowCount * 2);
            // we didn't create any tenant views so no change in the syscat entries
            expected.put(SYSTEM_CATALOG_NAME, syscatLogCountBeforeTest);
            expected.put(SYSTEM_CHILD_LINK_NAME, childLinkLogCountBeforeTest);
            verifyReplication(conn, expected);
        }
    }

    @Test
    public void testPreWALRestoreSkip() throws Exception {
        HBaseTestingUtility util = getUtility();
        MiniHBaseCluster cluster = util.getHBaseCluster();
        final String tableName = "T_" + generateUniqueName();
        final String indexName = "I_" + generateUniqueName();
        TableName table = TableName.valueOf(tableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String ddl = String.format("create table %s (id1 integer not null, " +
                    "id2 integer not null, val1 varchar, val2 varchar " +
                    "constraint pk primary key (id1, id2))", tableName);
            conn.createStatement().execute(ddl);
            ddl = String.format("create index %s on %s (val1) include (val2)",
                    indexName, tableName);
            conn.createStatement().execute(ddl);
            conn.commit();
        }
        JVMClusterUtil.RegionServerThread rs2 = cluster.startRegionServer();
        ServerName sn2 = rs2.getRegionServer().getServerName();
        moveRegionToServer(table, sn2);
        moveRegionToServer(TableName.valueOf(SYSTEM_CATALOG_NAME), sn2);
        moveRegionToServer(TableName.valueOf(SYSTEM_CHILD_LINK_NAME), sn2);
        int rowCount = 50;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " + tableName + " VALUES(?, ?, ?, ?)");
            // upsert 50 rows
            for (int i = 0; i < 5; ++i) {
                for (int j = 0; j < 10; ++j) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, j);
                    stmt.setString(3, "abcdefghijklmnopqrstuvwxyz");
                    stmt.setString(4, null); // Generate a DeleteColumn cell
                    stmt.executeUpdate();
                }
                // we want to simulate RS crash after updating memstore and WAL
                IndexRegionObserver.setIgnoreSyncReplicationForTesting(true);
                conn.commit();
            }
            // Create tenant views for syscat and child link replication
            createViewHierarchy();
        } finally {
            IndexRegionObserver.setIgnoreSyncReplicationForTesting(false);
        }
        cluster.killRegionServer(rs2.getRegionServer().getServerName());
        Threads.sleep(20000); // just to be sure that the kill has fully started.
        util.waitUntilAllRegionsAssigned(table);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Map<String, Integer> expected = Maps.newHashMap();
            // For each row 1 Put + 1 Delete (DeleteColumn)
            expected.put(tableName, rowCount * 2);
            // unverified + verified + delete (Delete column)
            expected.put(indexName, rowCount * 3);
            // 1 tenant view was created
            expected.put(SYSTEM_CHILD_LINK_NAME, childLinkLogCountBeforeTest + 1);
            // atleast 1 log entry for syscat
            expected.put(SYSTEM_CATALOG_NAME, syscatLogCountBeforeTest + 1);
            verifyReplication(conn, expected);
        }
    }

    @Test
    public void testSystemTables() throws Exception {
        createViewHierarchy();
        Map<String, List<Mutation>> logsByTable = groupLogsByTable();
        dumpTableLogCount(logsByTable);
        Map<String, List<Mutation>> systemTables = logsByTable.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(QueryConstants.SYSTEM_SCHEMA_NAME))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertEquals(2, systemTables.size());
        assertEquals(childLinkLogCountBeforeTest + 1,
                getCountForTable(systemTables, SYSTEM_CHILD_LINK_NAME));
        assertTrue(getCountForTable(systemTables, SYSTEM_CATALOG_NAME) >
                syscatLogCountBeforeTest);
    }
}
