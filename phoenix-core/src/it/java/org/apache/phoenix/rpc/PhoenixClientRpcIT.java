/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.rpc;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.phoenix.end2end.BaseOwnClusterHBaseManagedTimeIT;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class PhoenixClientRpcIT extends BaseOwnClusterHBaseManagedTimeIT {

    private static final String SCHEMA_NAME = "S";
    private static final String INDEX_TABLE_NAME = "I";
    private static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, "T");

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Collections.singletonMap(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS, 
        		TestPhoenixIndexRpcSchedulerFactory.class.getName());
        NUM_SLAVES_BASE = 2;
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), ReadOnlyProps.EMPTY_PROPS);
    }
    
    @AfterClass
    public static void cleanUpAfterTestSuite() throws Exception {
        TestPhoenixIndexRpcSchedulerFactory.reset();
    }

    @Test
    public void testIndexQos() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = driver.connect(getUrl(), props);
        try {
            // create the table
            conn.createStatement().execute(
                    "CREATE TABLE " + DATA_TABLE_FULL_NAME
                            + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");

            // create the index
            conn.createStatement().execute(
                    "CREATE INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1) INCLUDE (v2)");

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?,?)");
            stmt.setString(1, "k1");
            stmt.setString(2, "v1");
            stmt.setString(3, "v2");
            stmt.execute();
            conn.commit();

            // run select query that should use the index
            String selectSql = "SELECT k, v2 from " + DATA_TABLE_FULL_NAME + " WHERE v1=?";
            stmt = conn.prepareStatement(selectSql);
            stmt.setString(1, "v1");

            // verify that the query does a range scan on the index table
            ResultSet rs = stmt.executeQuery("EXPLAIN " + selectSql);
            assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER S.I ['v1']", QueryUtil.getExplainPlan(rs));

            // verify that the correct results are returned
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("k1", rs.getString(1));
            assertEquals("v2", rs.getString(2));
            assertFalse(rs.next());

            // verify that index queue is not used (since the index writes originate from a client an not a region server)
            Mockito.verify(TestPhoenixIndexRpcSchedulerFactory.getIndexRpcExecutor(), Mockito.never()).dispatch(Mockito.any(CallRunner.class));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testMetadataQos() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = driver.connect(getUrl(), props);
        try {
            // create the table
            conn.createStatement().execute("CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, v VARCHAR)");
            // verify that that metadata queue is used at least once
            Mockito.verify(TestPhoenixIndexRpcSchedulerFactory.getMetadataRpcExecutor(), Mockito.atLeastOnce()).dispatch(Mockito.any(CallRunner.class));
        } finally {
            conn.close();
        }
    }

}
