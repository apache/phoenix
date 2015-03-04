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

import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.C_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;


@RunWith(Parameterized.class)
public class DerivedTableIT extends BaseClientManagedTimeIT {
    private static final String tenantId = getOrganizationId();
    
    private long ts;
    private String[] indexDDL;
    private String[] plans;
    
    public DerivedTableIT(String[] indexDDL, String[] plans) {
        this.indexDDL = indexDDL;
        this.plans = plans;
    }
    
    @Before
    public void initTable() throws Exception {
         ts = nextTimestamp();
        initATableValues(tenantId, getDefaultSplits(tenantId), null, ts);
        if (indexDDL != null && indexDDL.length > 0) {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
            Connection conn = DriverManager.getConnection(getUrl(), props);
            for (String ddl : indexDDL) {
                conn.createStatement().execute(ddl);
            }
        }
    }
    
    @Parameters(name="{0}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        testCases.add(new String[][] {
                { 
                "CREATE INDEX ATABLE_DERIVED_IDX ON aTable (a_byte) INCLUDE (A_STRING, B_STRING)" 
                }, {
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE_DERIVED_IDX\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [\"A_STRING\", \"B_STRING\"]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT SORTED BY [\"B_STRING\"]\n" +
                "CLIENT SORTED BY [A]\n" +
                "CLIENT AGGREGATE INTO DISTINCT ROWS BY [A]\n" +
                "CLIENT SORTED BY [A DESC]",
                
                "CLIENT PARALLEL 1-WAY FULL SCAN OVER ATABLE_DERIVED_IDX\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [\"A_STRING\", \"B_STRING\"]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT AGGREGATE INTO DISTINCT ROWS BY [A]\n" +
                "CLIENT DISTINCT ON [COLLECTDISTINCT(B)]"}});
        testCases.add(new String[][] {
                {}, {
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT SORTED BY [B_STRING]\n" +
                "CLIENT SORTED BY [A]\n" +
                "CLIENT AGGREGATE INTO DISTINCT ROWS BY [A]\n" +
                "CLIENT SORTED BY [A DESC]",
                
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT AGGREGATE INTO DISTINCT ROWS BY [A]\n" +
                "CLIENT DISTINCT ON [COLLECTDISTINCT(B)]"}});
        return testCases;
    }

    @Test
    public void testDerivedTableWithWhere() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // (where)
            String query = "SELECT t.eid, t.x + 9 FROM (SELECT entity_id eid, b_string b, a_byte + 1 x FROM aTable WHERE a_byte + 1 < 9) AS t";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));
            assertEquals(11,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));
            assertEquals(12,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW3,rs.getString(1));
            assertEquals(13,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW4,rs.getString(1));
            assertEquals(14,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW5,rs.getString(1));
            assertEquals(15,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW6,rs.getString(1));
            assertEquals(16,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW7,rs.getString(1));
            assertEquals(17,rs.getInt(2));

            assertFalse(rs.next());
            
            // () where
            query = "SELECT t.eid, t.x + 9 FROM (SELECT entity_id eid, b_string b, a_byte + 1 x FROM aTable) AS t WHERE t.b = '" + C_VALUE + "'";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));
            assertEquals(12,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW5,rs.getString(1));
            assertEquals(15,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertEquals(18,rs.getInt(2));

            assertFalse(rs.next());
            
            // (where) where
            query = "SELECT t.eid, t.x + 9 FROM (SELECT entity_id eid, b_string b, a_byte + 1 x FROM aTable WHERE a_byte + 1 < 9) AS t WHERE t.b = '" + C_VALUE + "'";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));
            assertEquals(12,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW5,rs.getString(1));
            assertEquals(15,rs.getInt(2));

            assertFalse(rs.next());

            // (groupby where) where
            query = "SELECT t.a, t.c, t.m FROM (SELECT a_string a, count(*) c, max(a_byte) m FROM aTable WHERE a_byte != 8 GROUP BY a_string) AS t WHERE t.c > 1";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertEquals(4,rs.getInt(2));
            assertEquals(4,rs.getInt(3));
            assertTrue (rs.next());
            assertEquals(B_VALUE,rs.getString(1));
            assertEquals(3,rs.getInt(2));
            assertEquals(7,rs.getInt(3));

            assertFalse(rs.next());
            
            // (groupby having where) where
            query = "SELECT t.a, t.c, t.m FROM (SELECT a_string a, count(*) c, max(a_byte) m FROM aTable WHERE a_byte != 8 GROUP BY a_string HAVING count(*) >= 2) AS t WHERE t.a != '" + A_VALUE + "'";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(B_VALUE,rs.getString(1));
            assertEquals(3,rs.getInt(2));
            assertEquals(7,rs.getInt(3));

            assertFalse(rs.next());
            
            // (limit) where
            query = "SELECT t.eid FROM (SELECT entity_id eid, b_string b FROM aTable LIMIT 2) AS t WHERE t.b = '" + C_VALUE + "'";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));

            assertFalse(rs.next());

            // (count) where
            query = "SELECT t.c FROM (SELECT count(*) c FROM aTable) AS t WHERE t.c > 0";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(9,rs.getInt(1));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDerivedTableWithGroupBy() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // () groupby having
            String query = "SELECT t.a, count(*), max(t.s) FROM (SELECT a_string a, a_byte s FROM aTable WHERE a_byte != 8) AS t GROUP BY t.a HAVING count(*) > 1";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertEquals(4,rs.getInt(2));
            assertEquals(4,rs.getInt(3));
            assertTrue (rs.next());
            assertEquals(B_VALUE,rs.getString(1));
            assertEquals(3,rs.getInt(2));
            assertEquals(7,rs.getInt(3));

            assertFalse(rs.next());
            
            // (groupby) groupby
            query = "SELECT t.c, count(*) FROM (SELECT count(*) c FROM aTable GROUP BY a_string) AS t GROUP BY t.c";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(1,rs.getInt(1));
            assertEquals(1,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(4,rs.getInt(1));
            assertEquals(2,rs.getInt(2));

            assertFalse(rs.next());
            
            // (groupby) groupby orderby
            query = "SELECT t.c, count(*) FROM (SELECT count(*) c FROM aTable GROUP BY a_string) AS t GROUP BY t.c ORDER BY count(*) DESC";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(4,rs.getInt(1));
            assertEquals(2,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(1,rs.getInt(1));
            assertEquals(1,rs.getInt(2));

            assertFalse(rs.next());
            
            // (groupby a, b orderby b) groupby a orderby a
            query = "SELECT t.a, COLLECTDISTINCT(t.b) FROM (SELECT b_string b, a_string a FROM aTable GROUP BY a_string, b_string ORDER BY b_string) AS t GROUP BY t.a ORDER BY t.a DESC";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(C_VALUE,rs.getString(1));
            String[] b = new String[1];
            b[0] = E_VALUE;
            Array array = conn.createArrayOf("VARCHAR", b);
            assertEquals(array,rs.getArray(2));
            assertTrue (rs.next());
            assertEquals(B_VALUE,rs.getString(1));
            b = new String[3];
            b[0] = B_VALUE;
            b[1] = C_VALUE;
            b[2] = E_VALUE;
            array = conn.createArrayOf("VARCHAR", b);
            assertEquals(array,rs.getArray(2));
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertEquals(array,rs.getArray(2));

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(plans[0], QueryUtil.getExplainPlan(rs));
            
            // distinct b (groupby b, a) groupby a
            query = "SELECT DISTINCT COLLECTDISTINCT(t.b) FROM (SELECT b_string b, a_string a FROM aTable GROUP BY b_string, a_string) AS t GROUP BY t.a";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            b = new String[1];
            b[0] = E_VALUE;
            array = conn.createArrayOf("VARCHAR", b);
            assertEquals(array,rs.getArray(1));
            assertTrue (rs.next());
            b = new String[3];
            b[0] = B_VALUE;
            b[1] = C_VALUE;
            b[2] = E_VALUE;
            array = conn.createArrayOf("VARCHAR", b);
            assertEquals(array,rs.getArray(1));

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(plans[1], QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDerivedTableWithOrderBy() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // (orderby)
            String query = "SELECT t.eid FROM (SELECT entity_id eid, b_string b FROM aTable ORDER BY b, eid) AS t";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW4,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW7,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW5,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW3,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW6,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));

            assertFalse(rs.next());
            
            // () orderby
            query = "SELECT t.eid FROM (SELECT entity_id eid, b_string b FROM aTable) AS t ORDER BY t.b, t.eid";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW4,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW7,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW5,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW3,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW6,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));

            assertFalse(rs.next());
            
            // (orderby) orderby
            query = "SELECT t.eid FROM (SELECT entity_id eid, b_string b FROM aTable ORDER BY b, eid) AS t ORDER BY t.b DESC, t.eid DESC";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW6,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW3,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW5,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW7,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW4,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));

            assertFalse(rs.next());
            
            // (limit) orderby
            query = "SELECT t.eid FROM (SELECT entity_id eid, b_string b FROM aTable LIMIT 2) AS t ORDER BY t.b DESC, t.eid";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDerivedTableWithLimit() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // (limit)
            String query = "SELECT t.eid FROM (SELECT entity_id eid FROM aTable LIMIT 2) AS t";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));

            assertFalse(rs.next());
            
            // () limit
            query = "SELECT t.eid FROM (SELECT entity_id eid FROM aTable) AS t LIMIT 2";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));

            assertFalse(rs.next());
            
            // (limit 2) limit 4
            query = "SELECT t.eid FROM (SELECT entity_id eid FROM aTable LIMIT 2) AS t LIMIT 4";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));

            assertFalse(rs.next());
            
            // (limit 4) limit 2
            query = "SELECT t.eid FROM (SELECT entity_id eid FROM aTable LIMIT 4) AS t LIMIT 2";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));

            assertFalse(rs.next());                        
            
            // limit ? limit ?            
            query = "SELECT t.eid FROM (SELECT entity_id eid FROM aTable LIMIT ?) AS t LIMIT ?";
            statement = conn.prepareStatement(query);
            statement.setInt(1, 4);
            statement.setInt(2, 2);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));

            assertFalse(rs.next());
            
            // (groupby orderby) limit
            query = "SELECT a, s FROM (SELECT a_string a, sum(a_byte) s FROM aTable GROUP BY a_string ORDER BY sum(a_byte)) LIMIT 2";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(C_VALUE,rs.getString(1));
            assertEquals(9,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertEquals(10,rs.getInt(2));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDerivedTableWithDistinct() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // (distinct)
            String query = "SELECT * FROM (SELECT DISTINCT a_string, b_string FROM aTable) AS t WHERE t.b_string != '" + C_VALUE + "' ORDER BY t.b_string, t.a_string";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertEquals(B_VALUE,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(B_VALUE,rs.getString(1));
            assertEquals(B_VALUE,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertEquals(E_VALUE,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(B_VALUE,rs.getString(1));
            assertEquals(E_VALUE,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(C_VALUE,rs.getString(1));
            assertEquals(E_VALUE,rs.getString(2));

            assertFalse(rs.next());
            
            // distinct ()
            query = "SELECT DISTINCT t.a, t.b FROM (SELECT a_string a, b_string b FROM aTable) AS t WHERE t.b != '" + C_VALUE + "' ORDER BY t.b, t.a";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertEquals(B_VALUE,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(B_VALUE,rs.getString(1));
            assertEquals(B_VALUE,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertEquals(E_VALUE,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(B_VALUE,rs.getString(1));
            assertEquals(E_VALUE,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(C_VALUE,rs.getString(1));
            assertEquals(E_VALUE,rs.getString(2));

            assertFalse(rs.next());
            
            // distinct (distinct)
            query = "SELECT DISTINCT t.a FROM (SELECT DISTINCT a_string a, b_string b FROM aTable) AS t";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(B_VALUE,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(C_VALUE,rs.getString(1));

            assertFalse(rs.next());
            
            // distinct (groupby)
            query = "SELECT distinct t.c FROM (SELECT count(*) c FROM aTable GROUP BY a_string) AS t";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(1,rs.getInt(1));
            assertTrue (rs.next());
            assertEquals(4,rs.getInt(1));

            assertFalse(rs.next());
            
            // distinct (groupby) orderby
            query = "SELECT distinct t.c FROM (SELECT count(*) c FROM aTable GROUP BY a_string) AS t ORDER BY t.c DESC";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(4,rs.getInt(1));
            assertTrue (rs.next());
            assertEquals(1,rs.getInt(1));

            assertFalse(rs.next());
            
            // distinct (limit)
            query = "SELECT DISTINCT t.a, t.b FROM (SELECT a_string a, b_string b FROM aTable LIMIT 2) AS t";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertEquals(B_VALUE,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(A_VALUE,rs.getString(1));
            assertEquals(C_VALUE,rs.getString(2));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDerivedTableWithAggregate() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // (count)
            String query = "SELECT * FROM (SELECT count(*) FROM aTable WHERE a_byte != 8) AS t";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(8,rs.getInt(1));

            assertFalse(rs.next());
            
            // count ()
            query = "SELECT count(*) FROM (SELECT a_byte FROM aTable) AS t WHERE t.a_byte != 8";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(8,rs.getInt(1));

            assertFalse(rs.next());
            
            // count (distinct)
            query = "SELECT count(*) FROM (SELECT DISTINCT a_string FROM aTable) AS t";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(3,rs.getInt(1));

            assertFalse(rs.next());
            
            // count (groupby)
            query = "SELECT count(*) FROM (SELECT count(*) c FROM aTable GROUP BY a_string) AS t";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(3,rs.getInt(1));

            assertFalse(rs.next());
            
            // count (limit)
            query = "SELECT count(*) FROM (SELECT entity_id FROM aTable LIMIT 2) AS t";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(2,rs.getInt(1));

            assertFalse(rs.next());
            
            // count (subquery)
            query = "SELECT count(*) FROM (SELECT * FROM aTable WHERE (organization_id, entity_id) in (SELECT organization_id, entity_id FROM aTable WHERE a_byte != 8)) AS t";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(8,rs.getInt(1));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDerivedTableWithJoin() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // groupby (join)
            String query = "SELECT q.id1, count(q.id2) FROM (SELECT t1.entity_id id1, t2.entity_id id2, t2.a_byte b2" 
                        + " FROM aTable t1 JOIN aTable t2 ON t1.a_string = t2.b_string" 
                        + " WHERE t1.a_byte >= 8) AS q WHERE q.b2 != 5 GROUP BY q.id1";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertEquals(3,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));
            assertEquals(2,rs.getInt(2));

            assertFalse(rs.next());
            
            // distinct (join)
            query = "SELECT DISTINCT q.id1 FROM (SELECT t1.entity_id id1, t2.a_byte b2" 
                        + " FROM aTable t1 JOIN aTable t2 ON t1.a_string = t2.b_string" 
                        + " WHERE t1.a_byte >= 8) AS q WHERE q.b2 != 5";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));

            assertFalse(rs.next());

            // count (join)
            query = "SELECT COUNT(*) FROM (SELECT t2.a_byte b2" 
                        + " FROM aTable t1 JOIN aTable t2 ON t1.a_string = t2.b_string" 
                        + " WHERE t1.a_byte >= 8) AS q WHERE q.b2 != 5";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(5,rs.getInt(1));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNestedDerivedTable() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // select(select(select))
            String query = "SELECT q.id, q.x10 * 10 FROM (SELECT t.eid id, t.x + 9 x10, t.astr a, t.bstr b FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte + 1 x FROM aTable WHERE a_byte + 1 < ?) AS t ORDER BY b, id) AS q WHERE q.a = ? OR q.b = ? OR q.b = ?";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setInt(1, 9);
            statement.setString(2, A_VALUE);
            statement.setString(3, C_VALUE);
            statement.setString(4, E_VALUE);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1,rs.getString(1));
            assertEquals(110,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW4,rs.getString(1));
            assertEquals(140,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW2,rs.getString(1));
            assertEquals(120,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW5,rs.getString(1));
            assertEquals(150,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW3,rs.getString(1));
            assertEquals(130,rs.getInt(2));
            assertTrue (rs.next());
            assertEquals(ROW6,rs.getString(1));
            assertEquals(160,rs.getInt(2));

            assertFalse(rs.next());
            
            // select(select(select) join (select(select)))
            query = "SELECT q1.id, q2.id FROM (SELECT t.eid id, t.astr a, t.bstr b FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte abyte FROM aTable) AS t WHERE t.abyte >= ?) AS q1" 
                        + " JOIN (SELECT t.eid id, t.astr a, t.bstr b, t.abyte x FROM (SELECT entity_id eid, a_string astr, b_string bstr, a_byte abyte FROM aTable) AS t) AS q2 ON q1.a = q2.b" 
                        + " WHERE q2.x != ? ORDER BY q1.id, q2.id DESC";
            statement = conn.prepareStatement(query);
            statement.setInt(1, 8);
            statement.setInt(2, 5);
            rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertEquals(ROW7,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertEquals(ROW4,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(ROW8,rs.getString(1));
            assertEquals(ROW1,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));
            assertEquals(ROW8,rs.getString(2));
            assertTrue (rs.next());
            assertEquals(ROW9,rs.getString(1));
            assertEquals(ROW2,rs.getString(2));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}

