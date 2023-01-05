package org.apache.phoenix;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.getAllSplits;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixMasterObserverIT extends ParallelStatsDisabledIT {

    String create_table =
            "CREATE TABLE IF NOT EXISTS %s(ID VARCHAR NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) SALT_BUCKETS=4";
    String indexName = generateUniqueName();
    String create_index = "CREATE INDEX " + indexName + " ON %s(VAL1 DESC) INCLUDE (VAL2)";
    String upsertStatement = "UPSERT INTO %s VALUES(?, ?, ?)";
    String deleteTableName = generateUniqueName();

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(3);
        serverProps.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
            new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @Test
    public void checkMergeOperationsOnSaltedTables() throws Exception {

        createTables();
        populateTables();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        List<KeyRange> allSplits = getAllSplits(conn, deleteTableName);

        allSplits.stream().forEach(System.out::println);

        try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            Table table = admin.getConnection().getTable(TableName.valueOf(deleteTableName));
            TableName name = table.getName();
            RegionLocator regionLocator = table.getRegionLocator();
            List<HRegionLocation> allRegionLocations = regionLocator.getAllRegionLocations();
            
            for (int i = 0; i < allRegionLocations.size();) {
                RegionInfo lastRegion = allRegionLocations.get(i++).getRegion();
                RegionInfo thisRegion = allRegionLocations.get(i++).getRegion();
                admin.mergeRegions(lastRegion.getRegionName(), thisRegion.getRegionName(), false);
            }
        }

    }

    private void populateTables() throws SQLException {
        int batchSize = 1000;
        int numberOfBatches= 2;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (PreparedStatement dataPreparedStatement =
                    conn.prepareStatement(String.format(upsertStatement, deleteTableName))) {
                for (int i = 1; i <= numberOfBatches; i++) {
                    for (int j=1;j<=batchSize;j++) {
                        int i1 = (i - 1) * batchSize + j;
                        dataPreparedStatement.setString(1, "ROW_" + i1);
                        dataPreparedStatement.setInt(2, i1);
                        dataPreparedStatement.setInt(3, i1 * 2);
                        dataPreparedStatement.execute();
                    }
                    conn.commit();
                }
            }
        }
    }

    private void createTables() throws SQLException {
        try (Connection con = DriverManager.getConnection(getUrl())) {
            Statement stmt = con.createStatement();
            stmt.execute(String.format(create_table, deleteTableName));
            stmt.execute(String.format(create_index, deleteTableName));
        }
    }
}
