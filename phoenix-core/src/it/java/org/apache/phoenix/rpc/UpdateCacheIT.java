/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.rpc;

import static org.apache.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.TRANSACTIONAL_DATA_TABLE;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Verifies the number of rpcs calls from {@link MetaDataClient} updateCache() 
 * for transactional and non-transactional tables.
 */
public class UpdateCacheIT extends ParallelStatsDisabledIT {
	
	public static final int NUM_MILLIS_IN_DAY = 86400000;

    private static void setupSystemTable(String fullTableName) throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(
            "create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA);
        }
    }
    
    @Test
    public void testUpdateCacheForTxnTable() throws Exception {
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + TRANSACTIONAL_DATA_TABLE;
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + "TRANSACTIONAL=true");
        helpTestUpdateCache(fullTableName, null, new int[] {1, 1});
    }
    
    @Test
    public void testUpdateCacheForNonTxnTable() throws Exception {
        String tableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + tableName;
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA);
        helpTestUpdateCache(fullTableName, null, new int[] {1, 3});
    }
	
    @Test
    public void testUpdateCacheForNonTxnSystemTable() throws Exception {
        String fullTableName = QueryConstants.SYSTEM_SCHEMA_NAME + QueryConstants.NAME_SEPARATOR + generateUniqueName();
        setupSystemTable(fullTableName);
        helpTestUpdateCache(fullTableName, null, new int[] {0, 0});
    }
    
    @Test
    public void testUpdateCacheForNeverUpdatedTable() throws Exception {
        String tableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA);
            conn.createStatement().execute(
            "alter table " + fullTableName + " SET UPDATE_CACHE_FREQUENCY=NEVER");
        }
        helpTestUpdateCache(fullTableName, null, new int[] {0, 0});
    }
    
    @Test
    public void testUpdateCacheForAlwaysUpdatedTable() throws Exception {
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + " UPDATE_CACHE_FREQUENCY=always");
        }
        helpTestUpdateCache(fullTableName, null, new int[] {1, 3});
    }
    
    @Test
    public void testUpdateCacheForTimeLimitedUpdateTable() throws Exception {
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + " UPDATE_CACHE_FREQUENCY=" + 10000);
        }
        helpTestUpdateCache(fullTableName, null, new int[] {0, 0});
        Thread.sleep(10000);
        helpTestUpdateCache(fullTableName, null, new int[] {1, 0});
    }
    
    @Test
    public void testUpdateCacheForChangingUpdateTable() throws Exception {
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + " UPDATE_CACHE_FREQUENCY=never");
        }
        helpTestUpdateCache(fullTableName, null, new int[] {0, 0});
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("ALTER TABLE " + fullTableName + " SET UPDATE_CACHE_FREQUENCY=ALWAYS");
        }
        helpTestUpdateCache(fullTableName, null, new int[] {1, 3});
    }
    
	public static void helpTestUpdateCache(String fullTableName, Long scn, int[] expectedRPCs) throws Exception {
	    String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);
	    String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
		String selectSql = "SELECT * FROM "+fullTableName;
		// use a spyed ConnectionQueryServices so we can verify calls to getTable
		ConnectionQueryServices connectionQueryServices = Mockito.spy(driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)));
		Properties props = new Properties();
		props.putAll(PhoenixEmbeddedDriver.DEFFAULT_PROPS.asMap());
		if (scn!=null) {
            props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn+10));
        }
		Connection conn = connectionQueryServices.connect(getUrl(), props);
		try {
			conn.setAutoCommit(false);
	        String upsert = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
	        PreparedStatement stmt = conn.prepareStatement(upsert);
			// upsert three rows
	        TestUtil.setRowKeyColumns(stmt, 1);
			stmt.execute();
			TestUtil.setRowKeyColumns(stmt, 2);
			stmt.execute();
			TestUtil.setRowKeyColumns(stmt, 3);
			stmt.execute();
			conn.commit();
            int numUpsertRpcs = expectedRPCs[0];
			// verify only 0 or 1 rpc to fetch table metadata, 
            verify(connectionQueryServices, times(numUpsertRpcs)).getTable((PName)isNull(), eq(PVarchar.INSTANCE.toBytes(schemaName)), eq(PVarchar.INSTANCE.toBytes(tableName)), anyLong(), anyLong());
            reset(connectionQueryServices);
            
            if (scn!=null) {
                // advance scn so that we can see the data we just upserted
                props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn+20));
                conn = connectionQueryServices.connect(getUrl(), props);
            }
			
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
			TestUtil.validateRowKeyColumns(rs, 1);
			TestUtil.validateRowKeyColumns(rs, 2);
			TestUtil.validateRowKeyColumns(rs, 3);
	        assertFalse(rs.next());
	        
	        rs = conn.createStatement().executeQuery(selectSql);
	        TestUtil.validateRowKeyColumns(rs, 1);
	        TestUtil.validateRowKeyColumns(rs, 2);
	        TestUtil.validateRowKeyColumns(rs, 3);
	        assertFalse(rs.next());
	        
	        rs = conn.createStatement().executeQuery(selectSql);
	        TestUtil.validateRowKeyColumns(rs, 1);
	        TestUtil.validateRowKeyColumns(rs, 2);
	        TestUtil.validateRowKeyColumns(rs, 3);
	        assertFalse(rs.next());
	        
	        // for non-transactional tables without a scn : verify one rpc to getTable occurs *per* query
            // for non-transactional tables with a scn : verify *only* one rpc occurs
            // for transactional tables : verify *only* one rpc occurs
	        // for non-transactional, system tables : verify no rpc occurs
            int numRpcs = expectedRPCs[1]; 
            verify(connectionQueryServices, times(numRpcs)).getTable((PName)isNull(), eq(PVarchar.INSTANCE.toBytes(schemaName)), eq(PVarchar.INSTANCE.toBytes(tableName)), anyLong(), anyLong());
		}
        finally {
        	conn.close();
        }
	}
}
