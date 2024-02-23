package org.apache.phoenix.end2end;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.TestUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.util.MetaDataUtil.getViewIndexPhysicalName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CDCBaseIT extends ParallelStatsDisabledIT {
    protected void createTable(Connection conn, String table_sql,
                               PTable.QualifierEncodingScheme encodingScheme)
            throws Exception {
        createTable(conn, table_sql, encodingScheme, false, 0);
    }

    protected void createTable(Connection conn, String table_sql,
                               PTable.QualifierEncodingScheme encodingScheme, boolean multitenant)
            throws Exception {
        createTable(conn, table_sql, encodingScheme, multitenant, 0);
    }

    protected void createTable(Connection conn, String table_sql,
                               PTable.QualifierEncodingScheme encodingScheme, boolean multitenant, int nSaltBuckets)
            throws Exception {
        List<String> props = new ArrayList<>();
        if (encodingScheme != null && encodingScheme.getSerializedMetadataValue() !=
                QueryServicesOptions.DEFAULT_COLUMN_ENCODED_BYTES) {
            props.add("COLUMN_ENCODED_BYTES=" +
                    String.valueOf(encodingScheme.getSerializedMetadataValue()));
        }
        if (multitenant) {
            props.add("MULTI_TENANT=true");
        }
        if (nSaltBuckets != 0) {
            props.add("SALT_BUCKETS=" + nSaltBuckets);
        }
        conn.createStatement().execute(table_sql + " " + String.join(", ", props));
    }

    protected void createCDCAndWait(Connection conn, String tableName, String cdcName,
                                    String cdc_sql) throws Exception {
        createCDCAndWait(conn, tableName, cdcName, cdc_sql, null, 0);
    }

    protected void createCDCAndWait(Connection conn, String tableName, String cdcName,
                                    String cdc_sql, PTable.QualifierEncodingScheme encodingScheme,
                                    int nSaltBuckets) throws Exception {
        // For CDC, multitenancy gets derived automatically via the parent table.
        createTable(conn, cdc_sql, encodingScheme, false, nSaltBuckets);
        IndexToolIT.runIndexTool(false, null, tableName,
                "\""+CDCUtil.getCDCIndexName(cdcName)+"\"");
        TestUtil.waitForIndexState(conn, CDCUtil.getCDCIndexName(cdcName), PIndexState.ACTIVE);
    }

    protected void assertCDCState(Connection conn, String cdcName, String expInclude,
                                  int idxType) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT cdc_include FROM " +
                "system.catalog WHERE table_name = '" + cdcName +
                "' AND column_name IS NULL and column_family IS NULL")) {
            assertEquals(true, rs.next());
            assertEquals(expInclude, rs.getString(1));
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT index_type FROM " +
                "system.catalog WHERE table_name = '" + CDCUtil.getCDCIndexName(cdcName) +
                "' AND column_name IS NULL and column_family IS NULL")) {
            assertEquals(true, rs.next());
            assertEquals(idxType, rs.getInt(1));
        }
    }

    protected void assertPTable(String cdcName, Set<PTable.CDCChangeScope> expIncludeScopes,
                                String tableName, String datatableName)
            throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PTable table = PhoenixRuntime.getTable(conn, cdcName);
        assertEquals(expIncludeScopes, table.getCDCIncludeScopes());
        assertEquals(expIncludeScopes, TableProperty.INCLUDE.getPTableValue(table));
        assertNull(table.getIndexState()); // Index state should be null for CDC.
        assertNull(table.getIndexType()); // This is not an index.
        assertEquals(tableName, table.getParentName().getString());
        assertEquals(table.getPhysicalName().getString(),
                tableName == datatableName ? CDCUtil.getCDCIndexName(cdcName) :
                        getViewIndexPhysicalName(datatableName));
    }

    protected void assertSaltBuckets(String cdcName, Integer nbuckets) throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PTable cdcTable = PhoenixRuntime.getTable(conn, cdcName);
        assertEquals(nbuckets, cdcTable.getBucketNum());
        PTable indexTable = PhoenixRuntime.getTable(conn, CDCUtil.getCDCIndexName(cdcName));
        assertEquals(nbuckets, indexTable.getBucketNum());
    }

    protected Connection newConnection() throws SQLException {
        return newConnection(null);
    }

    protected Connection newConnection(String tenantId) throws SQLException {
        Properties props = new Properties();
        // FIXME: Uncomment these only while debugging.
        //props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        //props.put("hbase.client.scanner.timeout.period", "6000000");
        //props.put("phoenix.query.timeoutMs", "6000000");
        //props.put("zookeeper.session.timeout", "6000000");
        //props.put("hbase.rpc.timeout", "6000000");
        if (tenantId != null) {
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        return DriverManager.getConnection(getUrl(), props);
    }
}
