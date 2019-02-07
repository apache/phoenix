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
import static org.junit.Assert.assertTrue;
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
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Verifies the number of RPC calls from {@link MetaDataClient} updateCache() 
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
        for (TransactionFactory.Provider provider : TransactionFactory.Provider.values()) {
            if (provider.runTests()) {
                String tableName = generateUniqueName();
                String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + tableName;
                Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
                conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + "TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + provider + "'");
                helpTestUpdateCache(fullTableName, new int[] {1, 1}, false);
            }
        }
    }
    
    @Test
    public void testUpdateCacheForNonTxnTable() throws Exception {
        String tableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + tableName;
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        conn.createStatement().execute("create table " + fullTableName + TestUtil.TEST_TABLE_SCHEMA);
        helpTestUpdateCache(fullTableName, new int[] {1, 3}, false);
    }
	
    @Test
    public void testUpdateCacheForNonTxnSystemTable() throws Exception {
        String fullTableName = "\""+ QueryConstants.SYSTEM_SCHEMA_NAME + "\""+ QueryConstants.NAME_SEPARATOR + generateUniqueName();
        setupSystemTable(fullTableName);
        helpTestUpdateCache(fullTableName, new int[] {0, 0}, false);
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
        helpTestUpdateCache(fullTableName, new int[] {0, 0}, false);
    }
    
    @Test
    public void testUpdateCacheForAlwaysUpdatedTable() throws Exception {
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + " UPDATE_CACHE_FREQUENCY=always");
        }
        helpTestUpdateCache(fullTableName, new int[] {1, 3}, false);
    }
    
    @Test
    public void testUpdateCacheForTimeLimitedUpdateTable() throws Exception {
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + " UPDATE_CACHE_FREQUENCY=" + 10000);
        }
        helpTestUpdateCache(fullTableName, new int[] {0, 0}, false);
        Thread.sleep(10000);
        helpTestUpdateCache(fullTableName, new int[] {1, 0}, false);
    }
    
    @Test
    public void testUpdateCacheForChangingUpdateTable() throws Exception {
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + " UPDATE_CACHE_FREQUENCY=never");
        }
        helpTestUpdateCache(fullTableName, new int[] {0, 0}, false);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("ALTER TABLE " + fullTableName + " SET UPDATE_CACHE_FREQUENCY=ALWAYS");
        }
        helpTestUpdateCache(fullTableName, new int[] {1, 3}, false);
    }

    @Test
    public void testUpdateCacheFreqPropagatedToIndexes() throws Exception {
        String baseTableName = generateUniqueName();
        String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + baseTableName;
        String localIndex = "LOCAL_" + baseTableName;
        String globalIndex = "GLOBAL_" + baseTableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName +
                    TestUtil.TEST_TABLE_SCHEMA + " UPDATE_CACHE_FREQUENCY=never");

            // Create local and global indexes on the base table
            conn.createStatement().execute("CREATE LOCAL INDEX " + localIndex
                    + " on " + fullTableName + " (a.date1, b.varchar_col2)");
            conn.createStatement().execute("CREATE INDEX " + globalIndex + " on "
                    + fullTableName + " (a.int_col1, a.long_col1)");
        }

        // The indexes should have got the UPDATE_CACHE_FREQUENCY value of their base table
        helpTestUpdateCache(fullTableName, new int[] {0, 0}, false);
        helpTestUpdateCache(INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + localIndex,
                new int[] {0}, true);
        helpTestUpdateCache(INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + globalIndex,
                new int[] {0}, true);

        // Now alter the UPDATE_CACHE_FREQUENCY value of the base table
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement()
                    .execute("ALTER TABLE " + fullTableName + " SET UPDATE_CACHE_FREQUENCY=ALWAYS");
        }
        // Even the indexes should now have the modified value of UPDATE_CACHE_FREQUENCY
        // Note that when we query the base table, during query plan generation, we make 2 getTable
        // requests (to retrieve the base table) for each index of the base table
        helpTestUpdateCache(fullTableName, new int[] {1, 15}, false);
        helpTestUpdateCache(INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + localIndex,
                new int[] {3}, true);
        helpTestUpdateCache(INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + globalIndex,
                new int[] {3}, true);
    }
    
	private static void helpTestUpdateCache(String fullTableName, int[] expectedRPCs,
            boolean skipUpsertForIndexes) throws Exception {
	    String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);
	    String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
		String selectSql = "SELECT * FROM "+fullTableName;
		// use a spyed ConnectionQueryServices so we can verify calls to getTable
		ConnectionQueryServices connectionQueryServices = Mockito.spy(driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)));
		Properties props = new Properties();
		props.putAll(PhoenixEmbeddedDriver.DEFAULT_PROPS.asMap());
		Connection conn = connectionQueryServices.connect(getUrl(), props);
		try {
			conn.setAutoCommit(false);
            if (!skipUpsertForIndexes) {
                String upsert = "UPSERT INTO " + fullTableName + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) VALUES(?, ?, ?, ?, ?, ?)";
                PreparedStatement stmt = conn.prepareStatement(upsert);
                // upsert three rows
                for (int i=0; i<3; i++) {
                    TestUtil.setRowKeyColumns(stmt, i);
                    stmt.execute();
                }
                conn.commit();
                int numUpsertRpcs = expectedRPCs[0];
                // verify only 0 or 1 rpc to fetch table metadata,
                verify(connectionQueryServices, times(numUpsertRpcs)).getTable((PName) isNull(),
                        eq(PVarchar.INSTANCE.toBytes(schemaName)), eq(PVarchar.INSTANCE.toBytes(tableName)),
                        anyLong(), anyLong(), eq(false), eq(false), (PTable)isNull());
                reset(connectionQueryServices);
            }
            validateSelectRowKeyCols(conn, selectSql, skipUpsertForIndexes);
            validateSelectRowKeyCols(conn, selectSql, skipUpsertForIndexes);
            validateSelectRowKeyCols(conn, selectSql, skipUpsertForIndexes);

	        // for non-transactional tables without a scn : verify one rpc to getTable occurs *per* query
            // for non-transactional tables with a scn : verify *only* one rpc occurs
            // for transactional tables : verify *only* one rpc occurs
	        // for non-transactional, system tables : verify no rpc occurs
            int numRpcs = skipUpsertForIndexes ? expectedRPCs[0] : expectedRPCs[1];
            verify(connectionQueryServices, times(numRpcs)).getTable((PName) isNull(),
                eq(PVarchar.INSTANCE.toBytes(schemaName)), eq(PVarchar.INSTANCE.toBytes(tableName)),
                anyLong(), anyLong(), eq(false), eq(false), (PTable)isNull());
		}
        finally {
        	conn.close();
        }
	}

	private static void validateSelectRowKeyCols(Connection conn, String selectSql,
            boolean skipUpsertForIndexes) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(selectSql);
        if (skipUpsertForIndexes) {
            for (int i=0; i<3; i++) {
                assertTrue(rs.next());
            }
        } else {
            for (int i=0; i<3; i++) {
                TestUtil.validateRowKeyColumns(rs, i);
            }
        }
        assertFalse(rs.next());
    }
}
