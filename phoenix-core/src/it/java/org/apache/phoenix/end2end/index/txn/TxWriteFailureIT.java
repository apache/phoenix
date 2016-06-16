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
package org.apache.phoenix.end2end.index.txn;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseOwnClusterHBaseManagedTimeIT;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class TxWriteFailureIT extends BaseOwnClusterHBaseManagedTimeIT {
	
    private static final String SCHEMA_NAME = "S";
    private static final String DATA_TABLE_NAME = "T";
    private static final String INDEX_TABLE_NAME = "I";
    private static final String DATA_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, DATA_TABLE_NAME);
    private static final String INDEX_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, INDEX_TABLE_NAME);
    private static final String ROW_TO_FAIL = "fail";
    
    private final boolean localIndex;
	private final boolean mutable;

	public TxWriteFailureIT(boolean localIndex, boolean mutable) {
		this.localIndex = localIndex;
		this.mutable = mutable;
	}
	
	@BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(3);
        serverProps.put("hbase.coprocessor.region.classes", FailingRegionObserver.class.getName());
        serverProps.put("hbase.coprocessor.abortonerror", "false");
        serverProps.put(Indexer.CHECK_VERSION_CONF_KEY, "false");
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(10);
        clientProps.put(QueryServices.DEFAULT_TABLE_ISTRANSACTIONAL_ATTRIB, "true");
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, "true");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }
	
	@Parameters(name="localIndex = {0} , mutable = {1}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                 { false, false }, { false, true }, { true, false }, { true, true }
           });
    }
	
	@Test
    public void testIndexTableWriteFailure() throws Exception {
        helpTestWriteFailure(true);
	}
	
	@Test
    public void testDataTableWriteFailure() throws Exception {
        helpTestWriteFailure(false);
	}

	private void helpTestWriteFailure(boolean indexTableWriteFailure) throws SQLException {
		ResultSet rs;

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = driver.connect(url, props);
        conn.setAutoCommit(false);
        conn.createStatement().execute(
                "CREATE TABLE " + DATA_TABLE_FULL_NAME + " (k VARCHAR PRIMARY KEY, v1 VARCHAR)"+(!mutable? " IMMUTABLE_ROWS=true" : ""));
        conn.createStatement().execute(
                "CREATE "+(localIndex? "LOCAL " : "")+"INDEX " + INDEX_TABLE_NAME + " ON " + DATA_TABLE_FULL_NAME + " (v1)");
        
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + DATA_TABLE_FULL_NAME + " VALUES(?,?)");
        // to create a data table write failure set k as the ROW_TO_FAIL, to create an index table write failure set v1 as the ROW_TO_FAIL, 
        // FailingRegionObserver will throw an exception if the put contains ROW_TO_FAIL
        stmt.setString(1, !indexTableWriteFailure ? ROW_TO_FAIL : "k1");
        stmt.setString(2, indexTableWriteFailure ? ROW_TO_FAIL : "k2");
        stmt.execute();
        stmt.setString(1, "k2");
        stmt.setString(2, "v2");
        stmt.execute();
        try {
        	conn.commit();
        	fail();
        }
        catch (Exception e) {
        	conn.rollback();
        }
        stmt.setString(1, "k3");
        stmt.setString(2, "v3");
        stmt.execute();
        //this should pass
        conn.commit();
        
        // verify that only k3,v3 exists in the data table
        String dataSql = "SELECT k, v1 FROM " + DATA_TABLE_FULL_NAME + " order by k";
        rs = conn.createStatement().executeQuery("EXPLAIN "+dataSql);
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER S.T",
                QueryUtil.getExplainPlan(rs));
        rs = conn.createStatement().executeQuery(dataSql);
        assertTrue(rs.next());
        assertEquals("k3", rs.getString(1));
        assertEquals("v3", rs.getString(2));
        assertFalse(rs.next());

        // verify the only k3,v3  exists in the index table
        String indexSql = "SELECT k, v1 FROM " + DATA_TABLE_FULL_NAME + " order by v1";
        rs = conn.createStatement().executeQuery("EXPLAIN "+indexSql);
        if(localIndex) {
            assertEquals(
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + DATA_TABLE_FULL_NAME + " [1]\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "CLIENT MERGE SORT",
                QueryUtil.getExplainPlan(rs));
        } else {
	        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER " + INDEX_TABLE_FULL_NAME + "\n    SERVER FILTER BY FIRST KEY ONLY",
	                QueryUtil.getExplainPlan(rs));
        }
        rs = conn.createStatement().executeQuery(indexSql);
        assertTrue(rs.next());
        assertEquals("k3", rs.getString(1));
        assertEquals("v3", rs.getString(2));
        assertFalse(rs.next());
        
        conn.createStatement().execute("DROP TABLE " + DATA_TABLE_FULL_NAME);
	}
	
	
	public static class FailingRegionObserver extends SimpleRegionObserver {
        @Override
        public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
                final Durability durability) throws HBaseIOException {
            if (shouldFailUpsert(c, put)) {
                // throwing anything other than instances of IOException result
                // in this coprocessor being unloaded
                // DoNotRetryIOException tells HBase not to retry this mutation
                // multiple times
                throw new DoNotRetryIOException();
            }
        }
        
        private boolean shouldFailUpsert(ObserverContext<RegionCoprocessorEnvironment> c, Put put) {
            return Bytes.contains(put.getRow(), Bytes.toBytes(ROW_TO_FAIL));
        }
        
	}
	
}
