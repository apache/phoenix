/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY;
import static org.apache.phoenix.query.QueryServices.PHOENIX_METADATA_CACHE_INVALIDATION_TIMEOUT_MS;
import static org.apache.phoenix.query.QueryServices.PHOENIX_METADATA_INVALIDATE_CACHE_ENABLED;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.fail;

@Category({NeedsOwnMiniClusterTest.class })
public class InvalidateMetadataCacheIT extends BaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(InvalidateMetadataCacheIT.class);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        NUM_SLAVES_BASE = 2;
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // to fail fast in case of exception.
        props.put("hbase.client.retries.number", String.valueOf(0));
        props.put(PHOENIX_METADATA_INVALIDATE_CACHE_ENABLED, "true");
        props.put(REGIONSERVER_COPROCESSOR_CONF_KEY,
                FailingPhoenixRegionServerEndpoint.class.getName());
        // Setting phoenix metadata cache invalidation timeout to a small number to fail fast.
        props.put(PHOENIX_METADATA_CACHE_INVALIDATION_TIMEOUT_MS, String.valueOf(2000));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    /**
     * Add FailingPhoenixRegionServerEndpoint as regionserver co-processor.
     * Make one of the regionserver sleep in invalidateServerMetadataCache method. This will trigger
     * TimeoutException in MetadataEndpointImpl#invalidateServerMetadataCacheWithRetries method.
     * Make sure that ALTER TABLE ADD COLUMN statement fails.
     */
    @Test
    public void testAddColumnWithTimeout() {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String dataTableFullName = generateUniqueName();
        String ddl = getCreateTableStmt(dataTableFullName);
        HRegionServer regionServerZero = utility.getMiniHBaseCluster().getRegionServer(0);
        FailingPhoenixRegionServerEndpoint coprocForRS0 =
                getFailingPhoenixRegionServerEndpoint(regionServerZero);
        coprocForRS0.sleep();
        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " ADD CF.col2 integer");
            fail("Shouldn't reach here");
        } catch (Exception e) {
            LOGGER.error("Exception while adding column", e);
            // This is expected
        }
    }

    /**
     * Add FailingPhoenixRegionServerEndpoint as regionserver co-processor.
     * Make one of the regionserver throw Exception in invalidateServerMetadataCache method.
     * Make sure that ALTER TABLE ADD COLUMN statement fails.
     */
    @Test
    public void testAddColumnWithOneRSFailing() {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String dataTableFullName = generateUniqueName();
        String ddl = getCreateTableStmt(dataTableFullName);
        HRegionServer regionServerZero = utility.getMiniHBaseCluster().getRegionServer(0);
        FailingPhoenixRegionServerEndpoint coprocForRS0 =
                getFailingPhoenixRegionServerEndpoint(regionServerZero);
        coprocForRS0.throwException();
        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " ADD CF.col2 integer");
            fail("Shouldn't reach here");
        } catch (Exception e) {
            LOGGER.error("Exception while adding column", e);
            // This is expected
        }
    }

    /**
     * Add FailingPhoenixRegionServerEndpoint as regionserver co-processor.
     * Make one of the regionserver throw Exception in the first attempt and succeed on retry.
     * Make sure that ALTER TABLE ADD COLUMN statement succeeds on retry.
     */
    @Test
    public void testAddColumnWithOneRSSucceedingOnRetry() {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String dataTableFullName = generateUniqueName();
        String ddl = getCreateTableStmt(dataTableFullName);
        HRegionServer regionServerZero = utility.getMiniHBaseCluster().getRegionServer(0);
        FailingPhoenixRegionServerEndpoint coprocForRS0 =
                getFailingPhoenixRegionServerEndpoint(regionServerZero);
        coprocForRS0.failFirstAndThenSucceed();
        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " ADD CF.col2 integer");
        } catch (Throwable e) {
            fail("Shouldn't reach here");
        }
    }

    /**
     * Add FailingPhoenixRegionServerEndpoint as regionserver co-processor.
     * Do not throw Exception or sleep in invalidateServerMetadataCache method for any regionservers
     * Make sure that ALTER TABLE ADD COLUMN statement succeeds.
     */
    @Test
    public void testAddColumnWithBothRSPassing() {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String dataTableFullName = generateUniqueName();
        String ddl = getCreateTableStmt(dataTableFullName);
        try(Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName +
                    " ADD CF.col2 integer");
        } catch (Throwable t) {
            fail("Shouldn't reach here");
        }
    }

    private String getCreateTableStmt(String tableName) {
        return   "CREATE TABLE " + tableName +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) ";
    }

    private FailingPhoenixRegionServerEndpoint getFailingPhoenixRegionServerEndpoint(
            HRegionServer regionServer) {
        FailingPhoenixRegionServerEndpoint coproc = regionServer
                .getRegionServerCoprocessorHost()
                .findCoprocessor(FailingPhoenixRegionServerEndpoint.class);
        return coproc;
    }
}