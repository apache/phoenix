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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.MasterRegistry;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.rpc.TestPhoenixIndexRpcSchedulerFactory;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;



@Category(NeedsOwnMiniClusterTest.class)
public class RegistryIT extends ParallelStatsDisabledIT {

    @Test
    public void testReadOnlyUsingHRpc() throws Exception {
        String oldUrl = getUrl();

        /**
        Map<String, String> serverProps = Collections.singletonMap(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
                MasterRegistry.class.getName());
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(serverProps.entrySet().iterator()));
        **/
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tmpUrl = getUrl();
       // QueryUtil.getConnectionUrl(new Properties(),utility.getConfiguration());
       //  initAndRegisterTestDriver(tmpUrl, new ReadOnlyProps(Maps.newHashMap()));
       //  Connection connZk = DriverManager.getConnection(tmpUrl, props);

        /**
        String port = config.get(HConstants.MASTER_PORT);
        String masters = config.get(HConstants.MASTER_ADDRS_KEY);
        String[] splitz = masters.split(":");
        tmpUrl = QueryUtil.getHRpcUrl(splitz[0],Integer.parseInt(splitz[1]), null);

        tmpUrl = tmpUrl.replace(":+hrpc", "+hrpc");

        Map<String, String> prop = Maps.newHashMap();
        initAndRegisterTestDriver(tmpUrl, new ReadOnlyProps(prop));
        Connection conn = DriverManager.getConnection(tmpUrl, props);
        **/
        tmpUrl = tmpUrl.replace("phoenix", "phoenix+hrpc");

    Connection conn = DriverManager.getConnection(tmpUrl, props);
        String testTable = generateUniqueName();
      String ddl = "CREATE TABLE " + testTable + " " +
                        "  (r varchar not null, col1 integer" +
                        "  CONSTRAINT pk PRIMARY KEY (r))\n"; 
        createTestTable(getUrl(), ddl);

        String query = "UPSERT INTO " + testTable + "(r, col1) VALUES('row1', 777)";
        PreparedStatement statement = conn.prepareStatement(query);
        statement.executeUpdate();
        conn.commit();

	try{    
		conn.setReadOnly(true);
                assertTrue(conn.isReadOnly());
		ddl = "CREATE TABLE " + testTable + "2 " +
				"  (r varchar not null, col1 integer" +
				"  CONSTRAINT pk PRIMARY KEY (r))\n";
		statement = conn.prepareStatement(ddl);
        	statement.executeUpdate();
        	conn.commit();
		fail();
	} catch (SQLException e) {
              assertTrue(e.getMessage(), e.getMessage().contains("ERROR 518 (25502): Mutations are not permitted for a read-only connection."));
        }
	  
	try {
                query = "UPSERT INTO " + testTable + "(r, col1) VALUES('row1', 888)";
                statement = conn.prepareStatement(query);
                statement.executeUpdate();
                conn.commit();
                fail();
        } catch (SQLException e) {
              assertTrue(e.getMessage(), e.getMessage().contains("ERROR 518 (25502): Mutations are not permitted for a read-only connection."));
        }

	conn.setReadOnly(false);
        assertFalse(conn.isReadOnly());
        ddl = "ALTER TABLE " + testTable + " ADD col2 VARCHAR";
	statement = conn.prepareStatement(ddl);
        statement.executeUpdate();
	conn.commit();

        try {   
		conn.setReadOnly(true);
                ddl = "ALTER TABLE " + testTable + " ADD col3 VARCHAR";
		statement = conn.prepareStatement(ddl);
        	statement.executeUpdate();
                fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 518 (25502): Mutations are not permitted for a read-only connection."));
        }

  }
}
