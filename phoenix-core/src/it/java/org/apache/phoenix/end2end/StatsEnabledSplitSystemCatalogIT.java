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

import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class StatsEnabledSplitSystemCatalogIT extends BaseTest {
	
	private String tableDDLOptions;
	private String transactionProvider;

	public StatsEnabledSplitSystemCatalogIT(String transactionProvider) {
	        StringBuilder optionBuilder = new StringBuilder();
	        this.transactionProvider = transactionProvider;
	        if (transactionProvider != null) {
	            optionBuilder.append(" TRANSACTIONAL=true, TRANSACTION_PROVIDER='" + transactionProvider + "'");
	        }
	        this.tableDDLOptions = optionBuilder.toString();
	    }

	@Parameters(name = "transactionProvider = {0}")
	public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { "OMID" }, 
            { null }});
	}
	
	@BeforeClass
    public static synchronized void doSetup() throws Exception {
        NUM_SLAVES_BASE = 3;
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

	/**
     * Salted tests must be in their own test file to ensure that the underlying
     * table is dropped. Otherwise, the splits may not be performed.
     * TODO: we should throw in that case
     * 
     * @throws Exception
     */
    @Test
    public void testSaltedUpdatableViewWithIndex() throws Exception {
        testUpdatableViewWithIndex(3, false);
    }

    @Test
    public void testSaltedUpdatableViewWithLocalIndex() throws Exception {
        if (transactionProvider == null ||
                !TransactionFactory.getTransactionProvider(
                        TransactionFactory.Provider.valueOf(transactionProvider)).isUnsupported(Feature.ALLOW_LOCAL_INDEX)) {
            testUpdatableViewWithIndex(3, true);
        }
    }
	
	@Test
    public void testNonSaltedUpdatableViewWithIndex() throws Exception {
        testUpdatableViewWithIndex(null, false);
    }
    
    @Test
    public void testNonSaltedUpdatableViewWithLocalIndex() throws Exception {
        if (transactionProvider == null ||
                !TransactionFactory.getTransactionProvider(
                        TransactionFactory.Provider.valueOf(transactionProvider)).isUnsupported(Feature.ALLOW_LOCAL_INDEX)) {
            testUpdatableViewWithIndex(null, true);
        }
    }
    
    @Test
    public void testUpdatableOnUpdatableView() throws Exception {
        String fullTableName = SchemaUtil.getTableName(generateUniqueName(), generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(generateUniqueName(), generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(generateUniqueName(), generateUniqueName());
        String ddl = "CREATE VIEW " + fullViewName2 + " AS SELECT * FROM " + fullViewName1 + " WHERE k3 = 2";
        ViewIT.testUpdatableView(fullTableName, fullViewName1, fullViewName2, ddl, null, tableDDLOptions);
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM " + fullViewName2);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(109, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());
        
        conn.createStatement().execute("UPSERT INTO " + fullViewName2 + "(k2) VALUES(122)");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM " + fullViewName2 + " WHERE k2 >= 120");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(122, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());
        
        try {
            conn.createStatement().execute("UPSERT INTO " + fullViewName2 + "(k2,k3) VALUES(123,3)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("UPSERT INTO " + fullViewName2 + "(k2,k3) select k2, 3 from " + fullViewName1);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN.getErrorCode(), e.getErrorCode());
        }
    }
    
    private void testUpdatableViewWithIndex(Integer saltBuckets, boolean localIndex) throws Exception {
        String schemaName = TestUtil.DEFAULT_SCHEMA_NAME + "_" + generateUniqueName();
        String tableName = "T_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String viewName = "V_" + generateUniqueName();
        ViewIT.testUpdatableView(fullTableName, viewName, null, null, saltBuckets, tableDDLOptions);
        Pair<String, Scan> pair = ViewIT.testUpdatableViewIndex(fullTableName, saltBuckets, localIndex, viewName);
        Scan scan = pair.getSecond();
        String physicalTableName = pair.getFirst();
        // Confirm that dropping the view also deletes the rows in the index
        if (saltBuckets == null) {
            try (Connection conn = DriverManager.getConnection(getUrl())) {
                Table htable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                        .getTable(Bytes.toBytes(physicalTableName));
                if (ScanUtil.isLocalIndex(scan)) {
                    ScanUtil.setLocalIndexAttributes(scan, 0, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
                            scan.getStartRow(), scan.getStopRow());
                }
                ResultScanner scanner = htable.getScanner(scan);
                Result result = scanner.next();
                // Confirm index has rows
                assertTrue(result != null && !result.isEmpty());

                conn.createStatement().execute("DROP VIEW " + viewName);

                // Confirm index has no rows after view is dropped
                scanner = htable.getScanner(scan);
                result = scanner.next();
                assertTrue(result == null || result.isEmpty());
            }
        }
    }
    
    @Test
    public void testReadOnlyOnReadOnlyView() throws Exception {
        Connection earlierCon = DriverManager.getConnection(getUrl());
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(generateUniqueName(), generateUniqueName());
        String fullParentViewName = SchemaUtil.getTableName(generateUniqueName(), generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(generateUniqueName(), generateUniqueName());
        
        String ddl = "CREATE TABLE " + fullTableName + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE) "+ tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullParentViewName + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullParentViewName + " WHERE k < 9";
        conn.createStatement().execute(ddl);
        
        try {
            conn.createStatement().execute("UPSERT INTO " + fullParentViewName + " VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + i + ")");
        }
        conn.commit();
        
        analyzeTable(conn, fullParentViewName, transactionProvider != null);
        
        List<KeyRange> splits = getAllSplits(conn, fullParentViewName);
        assertEquals(4, splits.size());
        
        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM " + fullTableName);
        while (rs.next()) {
            assertEquals(count++, rs.getInt(1));
        }
        assertEquals(10, count);
        
        count = 0;
        rs = conn.createStatement().executeQuery("SELECT k FROM " + fullParentViewName);
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
        count = 0;
        rs = earlierCon.createStatement().executeQuery("SELECT k FROM " + fullParentViewName);
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
        
        try {
            conn.createStatement().execute("UPSERT INTO " + fullViewName + " VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        } finally {
            conn.close();
        }

        conn = DriverManager.getConnection(getUrl());
        count = 0;
        rs = conn.createStatement().executeQuery("SELECT k FROM " + fullViewName);
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(3, count);
    }

}
