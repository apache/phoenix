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

import static org.apache.phoenix.util.PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB;
import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.CUSTOM_ENTITY_DATA_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.PTSDB_NAME;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;


public class UpsertSelectIT extends BaseClientManagedTimeIT {
	
	
  @BeforeClass
  @Shadower(classBeingShadowed = BaseClientManagedTimeIT.class)
  public static void doSetup() throws Exception {
      Map<String,String> props = getDefaultProps();
      props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(500));
      props.put(QueryServices.THREAD_POOL_SIZE_ATTRIB, Integer.toString(64));

      // Must update config before starting server
      setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
  }
    
    @Test
    public void testUpsertSelectWithNoIndex() throws Exception {
        testUpsertSelect(false);
    }
    
    @Test
    public void testUpsertSelecWithIndex() throws Exception {
        testUpsertSelect(true);
    }
    
    private void testUpsertSelect(boolean createIndex) throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts-1);
        ensureTableCreated(getUrl(), CUSTOM_ENTITY_DATA_FULL_NAME, ts-1);
        String indexName = "IDX1";
        if (createIndex) {
            Properties props = new Properties();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts)); // Execute at timestamp 1
            Connection conn = DriverManager.getConnection(getUrl(), props);
            conn.createStatement().execute("CREATE INDEX IF NOT EXISTS " + indexName + " ON " + TestUtil.ATABLE_NAME + "(a_string)" );
            conn.close();
        }
        PreparedStatement upsertStmt;
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, Integer.toString(3)); // Trigger multiple batches
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        String upsert = "UPSERT INTO " + CUSTOM_ENTITY_DATA_FULL_NAME + "(custom_entity_data_id, key_prefix, organization_id, created_by) " +
            "SELECT substr(entity_id, 4), substr(entity_id, 1, 3), organization_id, a_string  FROM ATABLE WHERE ?=a_string";
        if (createIndex) { // Confirm index is used
            upsertStmt = conn.prepareStatement("EXPLAIN " + upsert);
            upsertStmt.setString(1, tenantId);
            ResultSet ers = upsertStmt.executeQuery();
            assertTrue(ers.next());
            String explainPlan = QueryUtil.getExplainPlan(ers);
            assertTrue(explainPlan.contains(" SCAN OVER " + indexName));
        }
        
        upsertStmt = conn.prepareStatement(upsert);
        upsertStmt.setString(1, A_VALUE);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(4, rowsInserted);
        conn.commit();
        conn.close();
        
        String query = "SELECT key_prefix, substr(custom_entity_data_id, 1, 1), created_by FROM " + CUSTOM_ENTITY_DATA_FULL_NAME + " WHERE organization_id = ? ";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 3)); // Execute at timestamp 3
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        ResultSet rs = statement.executeQuery();
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("2", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("3", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("4", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));

        assertFalse(rs.next());
        conn.close();

        // Test UPSERT through coprocessor
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        upsert = "UPSERT INTO " + CUSTOM_ENTITY_DATA_FULL_NAME + "(custom_entity_data_id, key_prefix, organization_id, last_update_by, division) " +
            "SELECT custom_entity_data_id, key_prefix, organization_id, created_by, 1.0  FROM " + CUSTOM_ENTITY_DATA_FULL_NAME + " WHERE organization_id = ? and created_by >= 'a'";
        
        upsertStmt = conn.prepareStatement(upsert);
        upsertStmt.setString(1, tenantId);
        assertEquals(4, upsertStmt.executeUpdate());
        conn.commit();

        query = "SELECT key_prefix, substr(custom_entity_data_id, 1, 1), created_by, last_update_by, division FROM " + CUSTOM_ENTITY_DATA_FULL_NAME + " WHERE organization_id = ?";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5)); 
        conn = DriverManager.getConnection(getUrl(), props);
        statement = conn.prepareStatement(query);
        statement.setString(1, tenantId);
        rs = statement.executeQuery();
       
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("1", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        assertEquals(A_VALUE, rs.getString(4));
        assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("2", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        assertEquals(A_VALUE, rs.getString(4));
        assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("3", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        assertEquals(A_VALUE, rs.getString(4));
        assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);
        
        assertTrue (rs.next());
        assertEquals("00A", rs.getString(1));
        assertEquals("4", rs.getString(2));
        assertEquals(A_VALUE, rs.getString(3));
        assertEquals(A_VALUE, rs.getString(4));
        assertTrue(BigDecimal.valueOf(1.0).compareTo(rs.getBigDecimal(5)) == 0);

        assertFalse(rs.next());
        conn.close();
    }

    // TODO: more tests - nullable fixed length last PK column
    @Test
    public void testUpsertSelectEmptyPKColumn() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts-1);
        ensureTableCreated(getUrl(), PTSDB_NAME, ts-1);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1)); // Execute at timestamp 1
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String upsert = "UPSERT INTO " + PTSDB_NAME + "(date, val, host) " +
            "SELECT current_date(), x_integer+2, entity_id FROM ATABLE WHERE a_integer >= ?";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        upsertStmt.setInt(1, 6);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(4, rowsInserted);
        conn.commit();
        conn.close();
        
        String query = "SELECT inst,host,date,val FROM " + PTSDB_NAME;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        
        Date now = new Date(System.currentTimeMillis());
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW6, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertEquals(null, rs.getBigDecimal(4));
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW7, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(7).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW8, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(6).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW9, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(5).compareTo(rs.getBigDecimal(4)) == 0);

        assertFalse(rs.next());
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 3));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        upsert = "UPSERT INTO " + PTSDB_NAME + "(date, val, inst) " +
            "SELECT date+1, val*10, host FROM " + PTSDB_NAME;
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(4, rowsInserted);
        conn.commit();
        conn.close();
        
        Date then = new Date(now.getTime() + QueryConstants.MILLIS_IN_DAY);
        query = "SELECT host,inst, date,val FROM " + PTSDB_NAME + " where inst is not null";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4)); // Execute at timestamp 2
        conn = DriverManager.getConnection(getUrl(), props);
        statement = conn.prepareStatement(query);
        
        rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW6, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertEquals(null, rs.getBigDecimal(4));
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW7, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(70).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW8, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(60).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW9, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(50).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertFalse(rs.next());
        conn.close();
        
        // Should just update all values with the same value, essentially just updating the timestamp
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        upsert = "UPSERT INTO " + PTSDB_NAME + " SELECT * FROM " + PTSDB_NAME;
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(8, rowsInserted);
        conn.commit();
        conn.close();
        
        query = "SELECT * FROM " + PTSDB_NAME ;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4)); // Execute at timestamp 2
        conn = DriverManager.getConnection(getUrl(), props);
        statement = conn.prepareStatement(query);
        
        rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW6, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertEquals(null, rs.getBigDecimal(4));
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW7, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(7).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW8, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(6).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW9, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(5).compareTo(rs.getBigDecimal(4)) == 0);

        assertTrue (rs.next());
        assertEquals(ROW6, rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertEquals(null, rs.getBigDecimal(4));
        
        assertTrue (rs.next());
        assertEquals(ROW7, rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(70).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(ROW8, rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(60).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(ROW9, rs.getString(1));
        assertEquals(null, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(50).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testUpsertSelectForAggAutoCommit() throws Exception {
        testUpsertSelectForAgg(true);
    }
    
    @Test
    public void testUpsertSelectForAgg() throws Exception {
        testUpsertSelectForAgg(false);
    }
    
    private void testUpsertSelectForAgg(boolean autoCommit) throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts-1);
        ensureTableCreated(getUrl(), PTSDB_NAME, ts-1);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1)); // Execute at timestamp 1
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(autoCommit);
        String upsert = "UPSERT INTO " + PTSDB_NAME + "(date, val, host) " +
            "SELECT current_date(), sum(a_integer), a_string FROM ATABLE GROUP BY a_string";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(3, rowsInserted);
        if (!autoCommit) {
            conn.commit();
        }
        conn.close();
        
        String query = "SELECT inst,host,date,val FROM " + PTSDB_NAME;
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement statement = conn.prepareStatement(query);
        ResultSet rs = statement.executeQuery();
        Date now = new Date(System.currentTimeMillis());
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(A_VALUE, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(10).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(B_VALUE, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(26).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(C_VALUE, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(9).compareTo(rs.getBigDecimal(4)) == 0);
        assertFalse(rs.next());
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 3));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        upsert = "UPSERT INTO " + PTSDB_NAME + "(date, val, host, inst) " +
            "SELECT current_date(), max(val), max(host), 'x' FROM " + PTSDB_NAME;
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        if (!autoCommit) {
            conn.commit();
        }
        conn.close();
        
        query = "SELECT inst,host,date,val FROM " + PTSDB_NAME + " WHERE inst='x'";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
        conn = DriverManager.getConnection(getUrl(), props);
        statement = conn.prepareStatement(query);
        rs = statement.executeQuery();
        now = new Date(System.currentTimeMillis());
        
        assertTrue (rs.next());
        assertEquals("x", rs.getString(1));
        assertEquals(C_VALUE, rs.getString(2));
        assertTrue(rs.getDate(3).before(now) );
        assertTrue(BigDecimal.valueOf(26).compareTo(rs.getBigDecimal(4)) == 0);
        assertFalse(rs.next());
        
    }

    @Test
    public void testUpsertSelectLongToInt() throws Exception {
        byte[][] splits = new byte[][] { PInteger.INSTANCE.toBytes(1), PInteger.INSTANCE.toBytes(2),
                PInteger.INSTANCE.toBytes(3), PInteger.INSTANCE.toBytes(4)};
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),"IntKeyTest",splits, ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO IntKeyTest VALUES(1)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        conn = DriverManager.getConnection(getUrl(), props);
        upsert = "UPSERT INTO IntKeyTest select i+1 from IntKeyTest";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        String select = "SELECT i FROM IntKeyTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testUpsertSelectRunOnServer() throws Exception {
        byte[][] splits = new byte[][] { PInteger.INSTANCE.toBytes(1), PInteger.INSTANCE.toBytes(2),
                PInteger.INSTANCE.toBytes(3), PInteger.INSTANCE.toBytes(4)};
        long ts = nextTimestamp();
        createTestTable(getUrl(), "create table IntKeyTest (i integer not null primary key desc, j integer)" ,splits, ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO IntKeyTest VALUES(1, 1)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 3));
        conn = DriverManager.getConnection(getUrl(), props);
        String select = "SELECT i,j+1 FROM IntKeyTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertEquals(2,rs.getInt(2));
        assertFalse(rs.next());
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true); // Force to run on server side.
        upsert = "UPSERT INTO IntKeyTest(i,j) select i, j+1 from IntKeyTest";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        select = "SELECT j FROM IntKeyTest";
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 15));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true); // Force to run on server side.
        upsert = "UPSERT INTO IntKeyTest(i,j) select i, i from IntKeyTest";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        select = "SELECT j FROM IntKeyTest";
        rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testUpsertSelectOnDescToAsc() throws Exception {
        byte[][] splits = new byte[][] { PInteger.INSTANCE.toBytes(1), PInteger.INSTANCE.toBytes(2),
                PInteger.INSTANCE.toBytes(3), PInteger.INSTANCE.toBytes(4)};
        long ts = nextTimestamp();
        createTestTable(getUrl(), "create table IntKeyTest (i integer not null primary key desc, j integer)" ,splits, ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO IntKeyTest VALUES(1, 1)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true); // Force to run on server side.
        upsert = "UPSERT INTO IntKeyTest(i,j) select i+1, j+1 from IntKeyTest";
        upsertStmt = conn.prepareStatement(upsert);
        rowsInserted = upsertStmt.executeUpdate();
        assertEquals(1, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        String select = "SELECT i,j FROM IntKeyTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(2,rs.getInt(1));
        assertEquals(2,rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertEquals(1,rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testUpsertSelectRowKeyMutationOnSplitedTable() throws Exception {
        byte[][] splits = new byte[][] { PInteger.INSTANCE.toBytes(1), PInteger.INSTANCE.toBytes(2),
                PInteger.INSTANCE.toBytes(3), PInteger.INSTANCE.toBytes(4)};
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(),"IntKeyTest",splits,ts-2);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String upsert = "UPSERT INTO IntKeyTest VALUES(?)";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
        upsertStmt.setInt(1, 1);
        upsertStmt.executeUpdate();
        upsertStmt.setInt(1, 3);
        upsertStmt.executeUpdate();
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 5));
        conn = DriverManager.getConnection(getUrl(), props);
        // Normally this would force a server side update. But since this changes the PK column, it would
        // for to run on the client side.
        conn.setAutoCommit(true);
        upsert = "UPSERT INTO IntKeyTest(i) SELECT i+1 from IntKeyTest";
        upsertStmt = conn.prepareStatement(upsert);
        int rowsInserted = upsertStmt.executeUpdate();
        assertEquals(2, rowsInserted);
        conn.commit();
        conn.close();
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        String select = "SELECT i FROM IntKeyTest";
        ResultSet rs = conn.createStatement().executeQuery(select);
        assertTrue(rs.next());
        assertEquals(1,rs.getInt(1));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertEquals(4,rs.getInt(1));
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testUpsertSelectWithLimit() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table phoenix_test (id varchar(10) not null primary key, val varchar(10), ts timestamp)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into phoenix_test values ('aaa', 'abc', current_date())");
        conn.createStatement().execute("upsert into phoenix_test values ('bbb', 'bcd', current_date())");
        conn.createStatement().execute("upsert into phoenix_test values ('ccc', 'cde', current_date())");
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select * from phoenix_test");
        
        assertTrue(rs.next());
        assertEquals("aaa",rs.getString(1));
        assertEquals("abc",rs.getString(2));
        assertNotNull(rs.getDate(3));
        
        assertTrue(rs.next());
        assertEquals("bbb",rs.getString(1));
        assertEquals("bcd",rs.getString(2));
        assertNotNull(rs.getDate(3));
        
        assertTrue(rs.next());
        assertEquals("ccc",rs.getString(1));
        assertEquals("cde",rs.getString(2));
        assertNotNull(rs.getDate(3));
        
        assertFalse(rs.next());
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into phoenix_test (id, ts) select id, null from phoenix_test where id <= 'bbb' limit 1");
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("select * from phoenix_test");
        
        assertTrue(rs.next());
        assertEquals("aaa",rs.getString(1));
        assertEquals("abc",rs.getString(2));
        assertNull(rs.getDate(3));
        
        assertTrue(rs.next());
        assertEquals("bbb",rs.getString(1));
        assertEquals("bcd",rs.getString(2));
        assertNotNull(rs.getDate(3));
        
        assertTrue(rs.next());
        assertEquals("ccc",rs.getString(1));
        assertEquals("cde",rs.getString(2));
        assertNotNull(rs.getDate(3));
        
        assertFalse(rs.next());
        conn.close();

    }
    
    @Test
    public void testUpsertSelectWithSequence() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("create table t1 (id bigint not null primary key, v varchar)");
        conn.createStatement().execute("create table t2 (k varchar primary key)");
        conn.createStatement().execute("create sequence s");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into t2 values ('a')");
        conn.createStatement().execute("upsert into t2 values ('b')");
        conn.createStatement().execute("upsert into t2 values ('c')");
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 15));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("upsert into t1 select next value for s, k from t2");
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select * from t1");
        
        assertTrue(rs.next());
        assertEquals(1,rs.getLong(1));
        assertEquals("a",rs.getString(2));
        
        assertTrue(rs.next());
        assertEquals(2,rs.getLong(1));
        assertEquals("b",rs.getString(2));
        
        assertTrue(rs.next());
        assertEquals(3,rs.getLong(1));
        assertEquals("c",rs.getString(2));
        
        assertFalse(rs.next());
        conn.close();
    }
    
    @Test
    public void testUpsertSelectWithSequenceAndOrderByWithSalting() throws Exception {

        int numOfRecords = 200;
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE IF NOT EXISTS DUMMY_CURSOR_STORAGE ("
                + "ORGANIZATION_ID CHAR(15) NOT NULL, QUERY_ID CHAR(15) NOT NULL, CURSOR_ORDER BIGINT NOT NULL, K1 INTEGER, V1 INTEGER "
                + "CONSTRAINT MAIN_PK PRIMARY KEY (ORGANIZATION_ID, QUERY_ID, CURSOR_ORDER) " + ") SALT_BUCKETS = 4";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute(
                "CREATE TABLE DUMMY_SEQ_TEST_DATA "
                        + "(ORGANIZATION_ID CHAR(15) NOT NULL, k1 integer NOT NULL, v1 integer NOT NULL "
                        + "CONSTRAINT PK PRIMARY KEY (ORGANIZATION_ID, k1, v1) ) VERSIONS=1, SALT_BUCKETS = 4");
        conn.createStatement().execute("create sequence s cache " + Integer.MAX_VALUE);
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        for (int i = 0; i < numOfRecords; i++) {
            conn.createStatement().execute(
                    "UPSERT INTO DUMMY_SEQ_TEST_DATA values ('00Dxx0000001gEH'," + i + "," + (i + 2) + ")");
        }
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 15));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        conn.createStatement().execute(
                    "UPSERT INTO DUMMY_CURSOR_STORAGE SELECT '00Dxx0000001gEH', 'MyQueryId', NEXT VALUE FOR S, k1, v1  FROM DUMMY_SEQ_TEST_DATA ORDER BY K1, V1");

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select count(*) from DUMMY_CURSOR_STORAGE");

        assertTrue(rs.next());
        assertEquals(numOfRecords, rs.getLong(1));
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 25));
        ResultSet rs2 = conn.createStatement().executeQuery(
                "select cursor_order, k1, v1 from DUMMY_CURSOR_STORAGE order by cursor_order");
        long seq = 1;
        while (rs2.next()) {
            assertEquals(seq, rs2.getLong("cursor_order"));
            // This value should be the sequence - 1 as we said order by k1 in the UPSERT...SELECT, but is not because
            // of sequence processing.
            assertEquals(seq - 1, rs2.getLong("k1"));
            seq++;
        }
        conn.close();

    }
}
