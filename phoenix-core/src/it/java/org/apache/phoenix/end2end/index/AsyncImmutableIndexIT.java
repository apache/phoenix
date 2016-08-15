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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.end2end.BaseOwnClusterHBaseManagedTimeIT;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class AsyncImmutableIndexIT extends BaseOwnClusterHBaseManagedTimeIT {
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
            QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        setUpRealDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
            ReadOnlyProps.EMPTY_PROPS);
    }
    
    @Test
    public void testDeleteFromImmutable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE TEST_TABLE (\n" + 
                    "        pk1 VARCHAR NOT NULL,\n" + 
                    "        pk2 VARCHAR NOT NULL,\n" + 
                    "        pk3 VARCHAR\n" + 
                    "        CONSTRAINT PK PRIMARY KEY \n" + 
                    "        (\n" + 
                    "        pk1,\n" + 
                    "        pk2,\n" + 
                    "        pk3\n" + 
                    "        )\n" + 
                    "        ) IMMUTABLE_ROWS=true");
            conn.createStatement().execute("upsert into TEST_TABLE (pk1, pk2, pk3) values ('a', '1', '1')");
            conn.createStatement().execute("upsert into TEST_TABLE (pk1, pk2, pk3) values ('b', '2', '2')");
            conn.commit();
            conn.createStatement().execute("CREATE INDEX TEST_INDEX ON TEST_TABLE (pk3, pk2) ASYNC");
            
            // this delete will be issued at a timestamp later than the above timestamp of the index table
            conn.createStatement().execute("delete from TEST_TABLE where pk1 = 'a'");
            conn.commit();

            // run the index MR job
            final IndexTool indexingTool = new IndexTool();
            indexingTool.setConf(new Configuration(getUtility().getConfiguration()));
            final String[] cmdArgs =
                    IndexToolIT.getArgValues(null, "TEST_TABLE", "TEST_INDEX", true);
            int status = indexingTool.run(cmdArgs);
            assertEquals(0, status);

            // upsert two more rows
            conn.createStatement().execute(
                "upsert into TEST_TABLE (pk1, pk2, pk3) values ('a', '3', '3')");
            conn.createStatement().execute(
                "upsert into TEST_TABLE (pk1, pk2, pk3) values ('b', '4', '4')");
            conn.commit();

            // validate that delete markers were issued correctly and only ('a', '1', 'value1') was
            // deleted
            String query = "SELECT pk3 from TEST_TABLE ORDER BY pk3";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String expectedPlan =
                    "CLIENT 1-CHUNK PARALLEL 1-WAY FULL SCAN OVER TEST_INDEX\n"
                            + "    SERVER FILTER BY FIRST KEY ONLY";
            assertEquals("Wrong plan ", expectedPlan, QueryUtil.getExplainPlan(rs));
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("3", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("4", rs.getString(1));
            assertFalse(rs.next());
        }
    }

}
