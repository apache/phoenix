/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


public class RenewLeaseIT extends BaseUniqueNamesOwnClusterIT {
    private static final long SCANNER_LEASE_TIMEOUT = 12000;
    private static volatile boolean SLEEP_NOW = false;
    private final static String TABLE_NAME = generateUniqueName();
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put("hbase.coprocessor.region.classes", SleepingRegionObserver.class.getName());
        Map<String,String> clientProps = Maps.newHashMapWithExpectedSize(1);
        // Must update config before starting server
        serverProps.put(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, Long.toString(SCANNER_LEASE_TIMEOUT));
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }
    
    @Test
    public void testLeaseDoesNotTimeout() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.createStatement().execute("create table " + TABLE_NAME + "(k VARCHAR PRIMARY KEY)");
        SLEEP_NOW = true;
        try {
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
        } finally {
            SLEEP_NOW = false;
        }
    }
    
    public static class SleepingRegionObserver extends SimpleRegionObserver {
        public SleepingRegionObserver() {}
        
        @Override
        public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
                final InternalScanner s, final List<Result> results,
                final int limit, final boolean hasMore) throws IOException {
            try {
                if (SLEEP_NOW && c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString().equals(TABLE_NAME)) {
                    Thread.sleep(2 * SCANNER_LEASE_TIMEOUT);
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            return super.preScannerNext(c, s, results, limit, hasMore);
        }
    }
}
