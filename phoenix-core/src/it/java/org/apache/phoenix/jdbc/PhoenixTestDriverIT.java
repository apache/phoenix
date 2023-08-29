package org.apache.phoenix.jdbc;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableRef;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixTestDriverIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    /**
     * Create 2 connections using URLs with different principals.
     * Create a table using one connection and verify that the other connection's cache
     * does not have this table's metadata.
     */
    @Test
    public void testMultipleCQSI() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        String url1 = QueryUtil.getConnectionUrl(props, config, "client1");
        String url2 = QueryUtil.getConnectionUrl(props, config, "client2");

        // create a table with url1
        String tableName = generateUniqueName();
        try (Connection conn1 = DriverManager.getConnection(url1)) {
            conn1.createStatement().execute("CREATE TABLE " + tableName
                    + "(k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER)");

            // this connection's cqsi cache should have the table metadata
            PMetaData cache = conn1.unwrap(PhoenixConnection.class).getQueryServices().getMetaDataCache();
            PTableRef pTableRef = cache.getTableRef(new PTableKey(null, tableName));
            Assert.assertNotNull(pTableRef);
        }
        catch (TableNotFoundException e) {
            Assert.fail("Table should have been found in CQSI cache.");
        }

        // table metadata should not be present in the other cqsi cache
        Connection conn2 = DriverManager.getConnection(url2);
        PMetaData cache = conn2.unwrap(PhoenixConnection.class).getQueryServices().getMetaDataCache();
        try {
            cache.getTableRef(new PTableKey(null, tableName));
            Assert.fail("Table should not have been found in CQSI cache.");
        }
        catch (TableNotFoundException e) {
            // expected since this connection was created using a different CQSI.
        }
    }
}
