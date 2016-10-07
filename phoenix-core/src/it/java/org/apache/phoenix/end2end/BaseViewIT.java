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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class BaseViewIT extends ParallelStatsEnabledIT {
	
	protected String tableName;
    protected String schemaName;
	protected String fullTableName;
	protected String tableDDLOptions;
	protected boolean transactional;

    public BaseViewIT( boolean transactional) {
		StringBuilder optionBuilder = new StringBuilder();
		this.transactional = transactional;
		if (transactional) {
			optionBuilder.append(" TRANSACTIONAL=true ");
		}
		this.schemaName = TestUtil.DEFAULT_SCHEMA_NAME;
		this.tableDDLOptions = optionBuilder.toString();
		this.tableName = "T_" + generateUniqueName();
        this.fullTableName = SchemaUtil.getTableName(schemaName, tableName);
	}
    
    @Parameters(name="transactional = {0}")
    public static Collection<Boolean> data() {
        return Arrays.asList(new Boolean[] { false, true });
    }
    
    protected void testUpdatableViewWithIndex(Integer saltBuckets, boolean localIndex) throws Exception {
        String viewName = testUpdatableView(saltBuckets);
        Pair<String,Scan> pair = testUpdatableViewIndex(saltBuckets, localIndex, viewName);
        Scan scan = pair.getSecond();
        String tableName = pair.getFirst();
        // Confirm that dropping the view also deletes the rows in the index
        if (saltBuckets == null) {
            try (Connection conn = DriverManager.getConnection(getUrl())) {
                HTableInterface htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(tableName));
                if(ScanUtil.isLocalIndex(scan)) {
                    ScanUtil.setLocalIndexAttributes(scan, 0, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, scan.getStartRow(), scan.getStopRow());
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

    protected String testUpdatableView(Integer saltBuckets) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
		if (saltBuckets!=null) {
			if (tableDDLOptions.length()!=0)
				tableDDLOptions+=",";
			tableDDLOptions+=(" SALT_BUCKETS="+saltBuckets);
		}
		String viewName = "V_" + generateUniqueName();
        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, k3 DECIMAL, s VARCHAR CONSTRAINT pk PRIMARY KEY (k1, k2, k3))" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + viewName + " AS SELECT * FROM " + fullTableName + " WHERE k1 = 1";
        conn.createStatement().execute(ddl);
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + (i % 4) + "," + (i+100) + "," + (i > 5 ? 2 : 1) + ")");
        }
        conn.commit();
        ResultSet rs;
        
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName);
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + viewName);
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM " + viewName);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(101, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(105, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(109, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());

        conn.createStatement().execute("UPSERT INTO " + viewName + "(k2,S,k3) VALUES(120,'foo',50.0)");
        conn.createStatement().execute("UPSERT INTO " + viewName + "(k2,S,k3) VALUES(121,'bar',51.0)");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT k1, k2 FROM " + viewName + " WHERE k2 >= 120");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(120, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(121, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
        return viewName;
    }

    protected Pair<String,Scan> testUpdatableViewIndex(Integer saltBuckets, String viewName) throws Exception {
        return testUpdatableViewIndex(saltBuckets, false, viewName);
    }

    protected Pair<String,Scan> testUpdatableViewIndex(Integer saltBuckets, boolean localIndex, String viewName) throws Exception {
        ResultSet rs;
        Connection conn = DriverManager.getConnection(getUrl());
        String viewIndexName1 = "I_" + generateUniqueName();
        String viewIndexPhysicalName = MetaDataUtil.getViewIndexName(schemaName, tableName);
        if (localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + viewIndexName1 + " on " + viewName + "(k3)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + viewIndexName1 + " on " + viewName + "(k3) include (s)");
        }
        conn.createStatement().execute("UPSERT INTO " + viewName + "(k2,S,k3) VALUES(120,'foo',50.0)");
        conn.commit();

        analyzeTable(conn, viewName);        
        List<KeyRange> splits = getAllSplits(conn, viewIndexName1);
        // More guideposts with salted, since it's already pre-split at salt buckets
        assertEquals(saltBuckets == null ? 6 : 8, splits.size());
        
        String query = "SELECT k1, k2, k3, s FROM " + viewName + " WHERE k3 = 51.0";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(121, rs.getInt(2));
        assertTrue(BigDecimal.valueOf(51.0).compareTo(rs.getBigDecimal(3))==0);
        assertEquals("bar", rs.getString(4));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        String queryPlan = QueryUtil.getExplainPlan(rs);
        if (localIndex) {
            assertEquals("CLIENT PARALLEL "+ (saltBuckets == null ? 1 : saltBuckets)  +"-WAY RANGE SCAN OVER " + fullTableName +" [1,51]\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "CLIENT MERGE SORT",
                queryPlan);
        } else {
            assertEquals(saltBuckets == null
                    ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + viewIndexPhysicalName +" [" + Short.MIN_VALUE + ",51]"
                            : "CLIENT PARALLEL " + saltBuckets + "-WAY RANGE SCAN OVER " + viewIndexPhysicalName + " [0," + Short.MIN_VALUE + ",51] - ["+(saltBuckets.intValue()-1)+"," + Short.MIN_VALUE + ",51]\nCLIENT MERGE SORT",
                            queryPlan);
        }

        String viewIndexName2 = "I_" + generateUniqueName();
        if (localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + viewIndexName2 + " on " + viewName + "(s)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + viewIndexName2 + " on " + viewName + "(s)");
        }
        
        // new index hasn't been analyzed yet
        splits = getAllSplits(conn, viewIndexName2);
        assertEquals(saltBuckets == null ? 1 : 3, splits.size());
        
        // analyze table should analyze all view data
        analyzeTable(conn, fullTableName);        
        splits = getAllSplits(conn, viewIndexName2);
        assertEquals(saltBuckets == null ? 6 : 8, splits.size());

        
        query = "SELECT k1, k2, s FROM " + viewName + " WHERE s = 'foo'";
        Statement statement = conn.createStatement();
        rs = statement.executeQuery(query);
        Scan scan = statement.unwrap(PhoenixStatement.class).getQueryPlan().getContext().getScan();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(120, rs.getInt(2));
        assertEquals("foo", rs.getString(3));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        String physicalTableName;
        if (localIndex) {
            physicalTableName = tableName;
            assertEquals("CLIENT PARALLEL "+ (saltBuckets == null ? 1 : saltBuckets)  +"-WAY RANGE SCAN OVER " + fullTableName +" [" + (2) + ",'foo']\n"
                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "CLIENT MERGE SORT",QueryUtil.getExplainPlan(rs));
        } else {
            physicalTableName = viewIndexPhysicalName;
            assertEquals(saltBuckets == null
                    ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + viewIndexPhysicalName +" [" + (Short.MIN_VALUE+1) + ",'foo']\n"
                            + "    SERVER FILTER BY FIRST KEY ONLY"
                            : "CLIENT PARALLEL " + saltBuckets + "-WAY RANGE SCAN OVER " + viewIndexPhysicalName + " [0," + (Short.MIN_VALUE+1) + ",'foo'] - ["+(saltBuckets.intValue()-1)+"," + (Short.MIN_VALUE+1) + ",'foo']\n"
                                    + "    SERVER FILTER BY FIRST KEY ONLY\n"
                                    + "CLIENT MERGE SORT",
                            QueryUtil.getExplainPlan(rs));
        }
        conn.close();
        return new Pair<>(physicalTableName,scan);
    }
}
