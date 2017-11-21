package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.After;
import org.junit.Test;

public class SystemCatalogIT {
    private HBaseTestingUtility testUtil = null;

    @After
    public void cleanup() throws Exception {
        if (null != testUtil) {
          testUtil.shutdownMiniCluster();
          testUtil = null;
        }
    }

    /**
     * Make sure that SYSTEM.CATALOG cannot be split, even with schemas and multi-tenant views
     */
    @Test
    public void testSystemTableSplit() throws Exception {
        testUtil = new HBaseTestingUtility();
        testUtil.startMiniCluster(1);
        for (int i=0; i<10; i++) {
            createTable("schema"+i+".table_"+i);
        }
        TableName systemCatalog = TableName.valueOf("SYSTEM.CATALOG");
        assertEquals(1, testUtil.getHBaseAdmin().getTableRegions(systemCatalog).size());

        // now attempt to split SYSTEM.CATALOG
        testUtil.getHBaseAdmin().split(systemCatalog.getName());

        // make sure the split finishes (there's no synchronous splitting before HBase 2.x)
        testUtil.getHBaseAdmin().disableTable(systemCatalog);
        testUtil.getHBaseAdmin().enableTable(systemCatalog);

        // test again... Must still be exactly one region.
        assertEquals(1, testUtil.getHBaseAdmin().getTableRegions(systemCatalog).size());
    }

    private void createTable(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl());
            Statement stmt = conn.createStatement();) {
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            stmt.execute("CREATE TABLE " + tableName
                + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR CONSTRAINT PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT=true");
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + tableName + "_view AS SELECT * FROM " + tableName;
                tenant1Conn.createStatement().execute(view1DDL);
            }
            conn.commit();
        }
    }

    private String getJdbcUrl() {
        return "jdbc:phoenix:localhost:" + testUtil.getZkCluster().getClientPort() + ":/hbase";
    }

    private Connection getTenantConnection(String tenantId) throws SQLException {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getJdbcUrl(), tenantProps);
    }
}
