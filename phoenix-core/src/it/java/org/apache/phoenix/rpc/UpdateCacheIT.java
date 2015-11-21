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
import static org.apache.phoenix.util.TestUtil.MUTABLE_INDEX_DATA_TABLE;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Verifies the number of rpcs calls from {@link MetaDataClient} updateCache() 
 * for transactional and non-transactional tables.
 */
public class UpdateCacheIT extends BaseHBaseManagedTimeIT {
	
	public static final int NUM_MILLIS_IN_DAY = 86400000;

    @Before
    public void setUp() throws SQLException {
        ensureTableCreated(getUrl(), MUTABLE_INDEX_DATA_TABLE);
        ensureTableCreated(getUrl(), TRANSACTIONAL_DATA_TABLE);
    }

    @Test
    public void testUpdateCacheForTxnTable() throws Exception {
        helpTestUpdateCache(true, null);
    }
    
    @Test
    public void testUpdateCacheForNonTxnTable() throws Exception {
        helpTestUpdateCache(false, null);
    }
	
	public static void helpTestUpdateCache(boolean isTransactional, Long scn) throws Exception {
	    String tableName = isTransactional ? TRANSACTIONAL_DATA_TABLE : MUTABLE_INDEX_DATA_TABLE;
	    String fullTableName = INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + tableName;
		String selectSql = "SELECT * FROM "+fullTableName;
		// use a spyed ConnectionQueryServices so we can verify calls to getTable
		ConnectionQueryServices connectionQueryServices = Mockito.spy(driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)));
		Properties props = new Properties();
		props.putAll(PhoenixEmbeddedDriver.DEFFAULT_PROPS.asMap());
		if (scn!=null) {
            props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn));
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
			// verify only one rpc to fetch table metadata, 
            verify(connectionQueryServices).getTable((PName)isNull(), eq(PVarchar.INSTANCE.toBytes(INDEX_DATA_SCHEMA)), eq(PVarchar.INSTANCE.toBytes(tableName)), anyLong(), anyLong());
            reset(connectionQueryServices);
            
            if (scn!=null) {
                // advance scn so that we can see the data we just upserted
                props.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(scn+2));
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
            int numRpcs = isTransactional || scn!=null ? 1 : 3; 
            verify(connectionQueryServices, times(numRpcs)).getTable((PName)isNull(), eq(PVarchar.INSTANCE.toBytes(INDEX_DATA_SCHEMA)), eq(PVarchar.INSTANCE.toBytes(tableName)), anyLong(), anyLong());
		}
        finally {
        	conn.close();
        }
	}
}
