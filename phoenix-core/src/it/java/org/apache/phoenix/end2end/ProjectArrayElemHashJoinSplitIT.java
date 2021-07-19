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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProjectArrayElemHashJoinSplitIT extends BaseUniqueNamesOwnClusterIT {
	
    @BeforeClass
    public static void doSetup() throws Exception {
        NUM_SLAVES_BASE = 3;
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testUnsalted() throws Exception {

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {
            String table = createUnsalted(conn);
            testTable(conn, table);
        } finally {
            conn.close();
        }
    }

    private void testTable(Connection conn, String table) throws Exception {

        verifyExplain(conn, table, false, false);
        verifyExplain(conn, table, false, true);
        verifyExplain(conn, table, true, false);
        verifyExplain(conn, table, true, true);

        verifyResults(conn, table, false, false);
        verifyResults(conn, table, false, true);
        verifyResults(conn, table, true, false);
        verifyResults(conn, table, true, true);
    }

    private String createUnsalted(Connection conn) throws Exception {

        String table = "UNSALTED_" + generateUniqueName();
        String create = "CREATE TABLE " + table + " ("
            + " id INTEGER NOT NULL,"
            + " vals TINYINT[],"
            + " CONSTRAINT pk PRIMARY KEY (id)"
            + ") SPLIT ON (11, 21)";

        conn.createStatement().execute(create);

	TableName tableName = TableName.valueOf(null, table);

	// Dummy split points; the table is already split
	List<byte[]> splitPoints = new ArrayList<byte[]>(2);
	splitPoints.add(new byte[1]);
	splitPoints.add(new byte[1]);

	// Call this to move regions to servers
	splitTable(tableName, splitPoints);
        return table;
    }

    private String getQuery(String table, boolean fullArray, boolean hashJoin) {

        String query = "SELECT t.id AS id, t.vals[1] v1, t.vals[2] v2, t.vals[3] v3, t.vals[4] v4"
            + (fullArray ? ", t.vals AS vals" : "")
            + " FROM " + table + " t "
	    + (hashJoin ? " JOIN " + table + " t2 ON (t.id = t2.id) " : "")
	    + " ORDER BY t.id"
            ;

        return query;
    }

    private void verifyExplain(Connection conn, String table, boolean fullArray, boolean hashJoin)
        throws Exception {

        String query = "EXPLAIN " + getQuery(table, fullArray, hashJoin);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);

        try {
            String plan = QueryUtil.getExplainPlan(rs);
            assertTrue(plan != null);
            assertTrue(fullArray || plan.contains("SERVER ARRAY ELEMENT PROJECTION"));
            assertTrue(hashJoin == plan.contains("JOIN"));
        } finally {
            rs.close();
        }
    }

    private void verifyResults(Connection conn, String table, boolean fullArray, boolean hashJoin)
        throws Exception {

        String upsert = "UPSERT INTO " + table + "(id, vals)"
            + " VALUES(?, ARRAY[10, 20, 30, 40, 50])";
        PreparedStatement upsertStmt = conn.prepareStatement(upsert);
	for (int i = 1; i <= 30; i++) {
	    upsertStmt.setInt(1, i);
	    upsertStmt.execute();
	}
        conn.commit();

        String query = getQuery(table, fullArray, hashJoin);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query);

        try {
	    for (int i = 1; i <= 30; i++) {
		assertTrue(rs.next());
		assertEquals(i, rs.getInt("id"));
		assertEquals(10, rs.getInt("v1"));
		assertEquals(20, rs.getInt("v2"));
		assertEquals(30, rs.getInt("v3"));
		assertEquals(40, rs.getInt("v4"));

		if (fullArray) {
		    java.sql.Array array = rs.getArray("vals");
		    assertTrue(array != null);
		    Object obj = array.getArray();
		    assertTrue(obj != null);
		    assertTrue(obj.getClass().isArray());
		    assertEquals(5, java.lang.reflect.Array.getLength(obj));
		}
	    }

            assertFalse(rs.next());
        } finally {
            rs.close();
        }
    }

    private void dropTable(Connection conn, String table) throws Exception {

        String drop = "DROP TABLE " + table;
        Statement stmt = conn.createStatement();
        stmt.execute(drop);
        stmt.close();
    }
}
