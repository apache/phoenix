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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowValueConstructorOffsetNotCoercibleException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class RowValueConstructorOffsetIT extends ParallelStatsDisabledIT {

    private static final String SIMPLE_DDL = "CREATE TABLE %s (t_id VARCHAR NOT NULL,\n" + "k1 INTEGER NOT NULL,\n"
            + "k2 INTEGER NOT NULL,\n" + "v1 INTEGER,\n" + "v2 VARCHAR,\n"
            + "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2)) ";

    private static final String DATA_DDL = "CREATE TABLE %s (k1 TINYINT NOT NULL,\n" + "k2 TINYINT NOT NULL,\n"
            + "k3 TINYINT NOT NULL,\n" + "v1 INTEGER,\n" + "CONSTRAINT pk PRIMARY KEY (k1, k2, k3)) ";

    private static final String TABLE_NAME = "T_" + generateUniqueName();

    private static final String TABLE_ROW_KEY = "t_id, k1, k2";

    private static final String GOOD_TABLE_ROW_KEY_VALUE = "'a', 1, 2";

    private static final String DATA_TABLE_NAME = "T_" + generateUniqueName();

    private static final String DATA_ROW_KEY = "k1, k2, k3";

    private static final String GOOD_DATA_ROW_KEY_VALUE = "2, 3, 0";

    private static final String INDEX_NAME =  "INDEX_" + TABLE_NAME;

    private static final String DATA_INDEX_NAME =  "INDEX_" + DATA_TABLE_NAME;

    private static final String DATA_INDEX_ROW_KEY = "k2, k1, k3";

    private static Connection conn = null;

    @BeforeClass
    public static synchronized void init() throws SQLException {
        conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));

        String dataTableDDL = String.format(DATA_DDL, DATA_TABLE_NAME);

        try (Statement statement = conn.createStatement()) {
            statement.execute(dataTableDDL);
        }

        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format(SIMPLE_DDL, TABLE_NAME));
        }

        conn.commit();

        String upsertDML = String.format("UPSERT INTO %s VALUES(?,?,?,?)", DATA_TABLE_NAME);

        int nRows = 0;
        try (PreparedStatement ps = conn.prepareStatement(upsertDML)) {
            for (int k1 = 0; k1 < 4; k1++) {
                ps.setInt(1, k1);
                for (int k2 = 0; k2 < 4; k2++) {
                    ps.setInt(2, k2);
                    for (int k3 = 0; k3 < 4; k3++) {
                        ps.setInt(3, k3);
                        ps.setInt(4, nRows);
                        int result = ps.executeUpdate();
                        assertEquals(1, result);
                        nRows++;
                    }
                }
            }
            conn.commit();
        }

        String createIndex = "CREATE INDEX IF NOT EXISTS " + INDEX_NAME + " ON " + TABLE_NAME + " (k2 DESC,k1)";
        try (Statement statement = conn.createStatement()) {
            statement.execute(createIndex);
        }

        String createDataIndex = "CREATE INDEX IF NOT EXISTS " + DATA_INDEX_NAME + " ON " + DATA_TABLE_NAME
                + " (k2 DESC,k1)";
        try (Statement statement = conn.createStatement()) {
            statement.execute(createDataIndex);
        }
        conn.commit();
    }

    @AfterClass
    public static synchronized void cleanup() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Test RVC Offset columns must be coercible to a base table
    @Test
    public void testRVCOffsetNotCoercible() throws SQLException {
        //'ab' is not an integer so this fails
        String failureSql = String.format("SELECT %s FROM %s OFFSET (%s)=('a', 'ab', 2)",
                TABLE_ROW_KEY,TABLE_NAME,TABLE_ROW_KEY);
        try (Statement statement = conn.createStatement()){
            statement.execute(failureSql);
            fail("Should not allow non coercible values to PK in RVC Offset");
        } catch (RowValueConstructorOffsetNotCoercibleException e) {
            return;
        }
    }

    // Test Order By Not PK Order By Exception
    @Test
    public void testRVCOffsetNotAllowNonPKOrderBy() throws SQLException {
        String failureSql = String.format("SELECT %s, v1 FROM %s ORDER BY v1 OFFSET (%s)=(%s)",
                TABLE_ROW_KEY,TABLE_NAME,TABLE_ROW_KEY,GOOD_TABLE_ROW_KEY_VALUE);
        try (Statement statement = conn.createStatement()) {
            statement.execute(failureSql);
            fail("Should not allow no PK order by with RVC Offset");
        } catch (RowValueConstructorOffsetNotCoercibleException e) {
            return;
        }

    }

    // Test Order By Partial PK Order By Exception
    @Test
    public void testRVCOffsetNotAllowPartialPKOrderBy() throws SQLException {
        String failureSql = String.format("SELECT %s FROM %s ORDER BY k1 OFFSET (%s)=(%s)",
                TABLE_ROW_KEY,TABLE_NAME, TABLE_ROW_KEY, GOOD_TABLE_ROW_KEY_VALUE);
        try (Statement statement = conn.createStatement()){
            statement.execute(failureSql);
            fail("Should not allow partial PK order by with RVC Offset");
        } catch (RowValueConstructorOffsetNotCoercibleException e) {
            return;
        }
    }

    // Test Order By Different Sort PK Order By Exception
    @Test
    public void testRVCOffsetSamePKDifferentSortOrderBy() throws SQLException {
        String failureSql = String.format("SELECT %s FROM %s ORDER BY t_id DESC, k1, k2 OFFSET (%s)=(%s)",
                TABLE_ROW_KEY,TABLE_NAME, TABLE_ROW_KEY,GOOD_TABLE_ROW_KEY_VALUE);
        try (Statement statement = conn.createStatement()){
            statement.execute(failureSql);
            fail("Should not allow different PK order by with RVC Offset");
        } catch (RowValueConstructorOffsetNotCoercibleException e) {
            return;
        }
    }

    // Test Not allow joins
    @Test
    public void testRVCOffsetNotAllowedInJoins() throws SQLException {
        String tableName2 = "T_" + generateUniqueName();
        createTestTable(getUrl(), String.format(SIMPLE_DDL,tableName2));

        String failureSql = String.format("SELECT T1.k1,T2.k2 FROM %s AS T1, %s AS T2 WHERE T1.t_id=T2.t_id OFFSET (T1.t_id, T1.k1, T1.k2)=('a', 1, 2)",
                TABLE_NAME, tableName2);
        try (Statement statement = conn.createStatement()){
            statement.execute(failureSql);
            fail("Should not have JOIN in RVC Offset");
        } catch (SQLException e) {
            return;
        }
    }

    // Test Not allowed in subquery
    @Test
    public void testRVCOffsetNotAllowedInSubQuery() throws SQLException {
        String failureSql = String.format("SELECT B.k2 FROM (SELECT %s FROM %s OFFSET (%s)=(%s)) AS B",
                TABLE_ROW_KEY,TABLE_NAME,TABLE_ROW_KEY,GOOD_TABLE_ROW_KEY_VALUE);
        try (Statement statement = conn.createStatement()){
            statement.execute(failureSql);
            fail("Should not have subquery with RVC Offset");
        } catch (SQLException e) {
            return;
        }
    }

    // Test Not allowed on subquery
    @Test
    public void testRVCOffsetNotAllowedOnSubQuery() throws SQLException {
        //Note subselect often gets rewritten to a flat query, in this case offset is still viable, inner orderby should require failure
        String failureSql = String.format("SELECT * FROM (SELECT T_ID,K1,K2 AS COL3 FROM %s ORDER BY K1 LIMIT 2) AS B OFFSET (%s)=(%s)",
                TABLE_NAME,TABLE_ROW_KEY,GOOD_TABLE_ROW_KEY_VALUE);
        try (Statement statement = conn.createStatement()) {
            statement.execute(failureSql);
            fail("Should not have subquery with RVC Offset");
        } catch (SQLException e) {
            return;
        }
    }

    // Test RVC Offset must be a literal, cannot have column reference
    @Test
    public void testRVCOffsetLiteral() throws SQLException {
        // column doesn't work must be literal
        String failureSql = String.format("SELECT * FROM %s OFFSET (%s)=('a', 1, k2)",TABLE_NAME,TABLE_ROW_KEY);
        try (Statement statement = conn.createStatement()) {
            statement.execute(failureSql);
            fail("Should not have allowed column in RVC Offset");
        } catch (RowValueConstructorOffsetNotCoercibleException e) {
            return;
        }
    }

    // Test RVC Offset must be in non-aggregate
    @Test
    public void testRVCOffsetAggregate() {
        String failureSql = String.format("SELECT count(*) FROM %s  OFFSET (%s)=(%s)",TABLE_NAME,TABLE_ROW_KEY,GOOD_TABLE_ROW_KEY_VALUE);
        try (Statement statement = conn.createStatement()) {
            statement.execute(failureSql);
            fail("Should not have allowed aggregate with RVC Offset");
        } catch (SQLException e) {
            return;
        }
    }

    // Test if RVC Offset RHS has less expressions than the pk, then it fails
    @Test
    public void testRVCOffsetPartialKey() throws SQLException {
        String failureSql = String.format("SELECT * FROM %s  OFFSET (%s)=('a', 1)",TABLE_NAME,TABLE_ROW_KEY);
        try (Statement statement = conn.createStatement()) {
            statement.execute(failureSql);
            fail("Should not have allowed partial Key RVC Offset");
        } catch (RowValueConstructorOffsetNotCoercibleException e) {
            return;
        }
    }

    // Test if RVC Offset RHS has more expressions than the pk, then it fails
    @Test
    public void testRVCOffsetMoreThanKey() throws SQLException {
        String failureSql = String.format("SELECT * FROM %s OFFSET (%s)=('a', 1, 2, 3)",TABLE_NAME,TABLE_ROW_KEY);
        try (Statement statement = conn.createStatement()) {
            statement.execute(failureSql);
            fail("Should not have allowed more than pk columns in Key RVC Offset");
        } catch (RowValueConstructorOffsetNotCoercibleException e) {
            return;
        }
    }

    // Test RVC Offset doesn't match the rowkey
    @Test
    public void testRVCOffsetLHSDoesNotMatchTable() throws SQLException {
        String failureSql = String.format("SELECT * FROM %s LIMIT 2 OFFSET (k1,k2)=(%s)",TABLE_NAME,GOOD_TABLE_ROW_KEY_VALUE);
        try (Statement statement = conn.createStatement()) {
            statement.execute(failureSql);
            fail("Should not have allowed the LHS to not be the same as the pk");
        } catch (RowValueConstructorOffsetNotCoercibleException e) {
            return;
        }
    }

    // Test RVC Offset simple case, can we offset into the table and select the correct rows
    @Test
    public void testSimpleRVCOffsetLookup() throws SQLException {
        String sql = String.format("SELECT * FROM %s LIMIT 3 OFFSET (%s)=(%s)",DATA_TABLE_NAME,DATA_ROW_KEY,GOOD_DATA_ROW_KEY_VALUE);
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            assertTrue(rs.next());
            {
                int k1 = rs.getInt(1);
                int k2 = rs.getInt(2);
                int k3 = rs.getInt(3);
                assertEquals(2, k1);
                assertEquals(3, k2);
                assertEquals(1, k3);
            }
            assertTrue(rs.next());
            {
                int k1 = rs.getInt(1);
                int k2 = rs.getInt(2);
                int k3 = rs.getInt(3);
                assertEquals(2, k1);
                assertEquals(3, k2);
                assertEquals(2, k3);
            }
            assertTrue(rs.next());
            {
                int k1 = rs.getInt(1);
                int k2 = rs.getInt(2);
                int k3 = rs.getInt(3);
                assertEquals(2, k1);
                assertEquals(3, k2);
                assertEquals(3, k3);
            }
            assertFalse(rs.next());
        }
    }

    @Test
    public void testBindsRVCOffsetLookup() throws SQLException {
        String sql = String.format("SELECT * FROM %s LIMIT 2 OFFSET (%s)=(?, ?, ?)",DATA_TABLE_NAME,DATA_ROW_KEY);
        try(PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, 2);
            ps.setInt(2, 3);
            ps.setInt(3, 1);

            try (ResultSet rs = ps.executeQuery()) {

                assertTrue(rs.next());
                {
                    int k1 = rs.getInt(1);
                    int k2 = rs.getInt(2);
                    int k3 = rs.getInt(3);
                    assertEquals(2, k1);
                    assertEquals(3, k2);
                    assertEquals(2, k3);
                }
                assertTrue(rs.next());
                {
                    int k1 = rs.getInt(1);
                    int k2 = rs.getInt(2);
                    int k3 = rs.getInt(3);
                    assertEquals(2, k1);
                    assertEquals(3, k2);
                    assertEquals(3, k3);
                }
                assertFalse(rs.next());
            }
        }
    }



    // Test RVC Offset where clause
    @Test
    public void testWhereClauseRVCOffsetLookup() throws SQLException {
        //Offset should not overcome the where clause
        String sql = String.format("SELECT * FROM %s WHERE (k1,k2,k3)=(3,3,3) LIMIT 2 OFFSET (%s)=(%s)",DATA_TABLE_NAME,DATA_ROW_KEY,GOOD_DATA_ROW_KEY_VALUE);
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            assertTrue(rs.next());
            {
                int k1 = rs.getInt(1);
                int k2 = rs.getInt(2);
                int k3 = rs.getInt(3);
                assertEquals(3, k1);
                assertEquals(3, k2);
                assertEquals(3, k3);
            }
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSaltedTableRVCOffsetOrderBy() throws SQLException {
        //Make a salted table
        String saltedTableName = "T_" + generateUniqueName();

        String saltedDDL = String.format(DATA_DDL + "SALT_BUCKETS=4",saltedTableName);

        try(Statement statement = conn.createStatement()) {
            statement.execute(saltedDDL);
        }
        conn.commit();

        //If we attempt to order by the row key we should not fail
        String sql = "SELECT * FROM " + saltedTableName + " ORDER BY K1,K2,K3 LIMIT 2 OFFSET (k1,k2,k3)=(2, 3, 1)";
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            statement.executeQuery(sql);
        }

        //If we attempt to order by the not row key we should fail
        sql = "SELECT * FROM " + saltedTableName + " ORDER BY K2,K1,K3 LIMIT 2 OFFSET (k1,k2,k3)=(2, 3, 1)";
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            fail();
        } catch(RowValueConstructorOffsetNotCoercibleException e) {
            return;
        }
    }

    @Test
    public void testSaltedTableRVCOffset() throws SQLException {
        //Make a salted table
        String saltedTableName = "T_" + generateUniqueName();

        String saltedDDL = String.format(DATA_DDL + "SALT_BUCKETS=4",saltedTableName);

        try(Statement statement = conn.createStatement()) {
            statement.execute(saltedDDL);
            conn.commit();
        }

        String upsertDML = String.format("UPSERT INTO %s VALUES(?,?,?,?)", saltedTableName);

        int nRows = 0;
        try (PreparedStatement ps = conn.prepareStatement(upsertDML)) {
            for (int k1 = 0; k1 < 4; k1++) {
                ps.setInt(1, k1);
                for (int k2 = 0; k2 < 4; k2++) {
                    ps.setInt(2, k2);
                    for (int k3 = 0; k3 < 4; k3++) {
                        ps.setInt(3, k3);
                        ps.setInt(4, nRows);
                        int result = ps.executeUpdate();
                        assertEquals(1, result);
                        nRows++;
                    }
                }
            }
            conn.commit();
        }

        String sql = String.format("SELECT * FROM " + saltedTableName + " ORDER BY %s LIMIT 3 OFFSET (%s)=(%s)",DATA_ROW_KEY,DATA_ROW_KEY,GOOD_DATA_ROW_KEY_VALUE);

        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            assertTrue(rs.next());
            {
                int k1 = rs.getInt(1);
                int k2 = rs.getInt(2);
                int k3 = rs.getInt(3);
                assertEquals(2, k1);
                assertEquals(3, k2);
                assertEquals(1, k3);
            }
            assertTrue(rs.next());
            {
                int k1 = rs.getInt(1);
                int k2 = rs.getInt(2);
                int k3 = rs.getInt(3);
                assertEquals(2, k1);
                assertEquals(3, k2);
                assertEquals(2, k3);
            }
            assertTrue(rs.next());
            {
                int k1 = rs.getInt(1);
                int k2 = rs.getInt(2);
                int k3 = rs.getInt(3);
                assertEquals(2, k1);
                assertEquals(3, k2);
                assertEquals(3, k3);
            }
            assertFalse(rs.next());
        }
    }

    @Test
    public void testGlobalViewRVCOffset() throws SQLException {
        //Make a view
        String viewName1 = "V_" + generateUniqueName();

        //Simple View
        String viewDDL = "CREATE VIEW " + viewName1 + " AS SELECT * FROM " + DATA_TABLE_NAME;
        try(Statement statement = conn.createStatement()) {
            statement.execute(viewDDL);
            conn.commit();
        }
        String sql = "SELECT  k2,k1,k3 FROM " + viewName1 + " LIMIT 3 OFFSET (k2,k1,k3)=(3, 3, 1)";

        try(Statement statement = conn.createStatement() ; ResultSet rs = statement.executeQuery(sql)) {
            assertTrue(rs.next());
            {
                int k2 = rs.getInt(1);
                int k1 = rs.getInt(2);
                int k3 = rs.getInt(3);

                assertEquals(3, k2);
                assertEquals(3, k1);
                assertEquals(2, k3);
            }
            assertTrue(rs.next());
            {
                int k2 = rs.getInt(1);
                int k1 = rs.getInt(2);
                int k3 = rs.getInt(3);

                assertEquals(3, k2);
                assertEquals(3, k1);
                assertEquals(3, k3);
            }
            assertTrue(rs.next());
            {
                int k2 = rs.getInt(1);
                int k1 = rs.getInt(2);
                int k3 = rs.getInt(3);

                assertEquals(2, k2);
                assertEquals(0, k1);
                assertEquals(0, k3);
            }
            assertFalse(rs.next());
        }
    }

    private void parameterizedTenantTestCase(boolean isSalted) throws SQLException {
        String multiTenantDataTableName = "T_" + generateUniqueName();

        String multiTenantDDL = "CREATE TABLE %s (tenant_id VARCHAR NOT NULL, k1 TINYINT NOT NULL,\n" + "k2 TINYINT NOT NULL,\n"
                + "k3 TINYINT NOT NULL,\n" + "v1 INTEGER,\n" + "CONSTRAINT pk PRIMARY KEY (tenant_id, k1, k2, k3)) MULTI_TENANT=true";

        if(isSalted){
            multiTenantDDL = multiTenantDDL + ", SALT_BUCKETS=4";
        }

        try(Statement statement = conn.createStatement()) {
            statement.execute(String.format(multiTenantDDL, multiTenantDataTableName));
            conn.commit();
        }

        String tenantId2 = "tenant2";

        //tenant connection
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId2);

        try(Connection tenant2Connection = DriverManager.getConnection(getUrl(), props)) {

            //create tenant view with new pks
            String viewName = multiTenantDataTableName + "_" + tenantId2;
            try (Statement statement = tenant2Connection.createStatement()) {
                statement.execute("CREATE VIEW " + viewName + " ( vk1 INTEGER NOT NULL, vv1 INTEGER, CONSTRAINT PKVIEW PRIMARY KEY(vk1))  AS SELECT * FROM "
                        + multiTenantDataTableName);
            }
            //create tenant view index on tenant view
            String viewIndexName = viewName + "_Index1";
            try (Statement statement = tenant2Connection.createStatement()) {
                statement.execute("CREATE INDEX " + viewIndexName + " ON " + viewName + " ( vv1 ) ");
            }

            String upsertDML = String.format("UPSERT INTO %s VALUES(?,?,?,?,?,?)", viewName);
            int tenantRows = 0;
            try (PreparedStatement tps = tenant2Connection.prepareStatement(upsertDML)) {
                for (int k1 = 0; k1 < 4; k1++) {
                    tps.setInt(1, k1);
                    for (int k2 = 0; k2 < 4; k2++) {
                        tps.setInt(2, k2);
                        for (int k3 = 0; k3 < 4; k3++) {
                            tps.setInt(3, k3);
                            for (int vk1 = 0; vk1 < 4; vk1++) {
                                tps.setInt(4, tenantRows);
                                tps.setInt(5, vk1);
                                tps.setInt(6, -tenantRows); //vv1

                                int result = tps.executeUpdate();
                                assertEquals(1, result);
                                tenantRows++;
                            }
                        }
                    }
                }
                tenant2Connection.commit();
            }

            //tenant view
            {
                String sql = "SELECT k1,k2,k3,vk1 FROM " + viewName + "  LIMIT 2 OFFSET (k1,k2,k3,vk1)=(2, 3, 1, 2)";
                try (Statement statement = tenant2Connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
                    assertTrue(rs.next());
                    {
                        int k1 = rs.getInt(1);
                        int k2 = rs.getInt(2);
                        int k3 = rs.getInt(3);
                        int vk1 = rs.getInt(4);
                        assertEquals(2, k1);
                        assertEquals(3, k2);
                        assertEquals(1, k3);
                        assertEquals(3, vk1);
                    }
                    assertTrue(rs.next());
                    {
                        int k1 = rs.getInt(1);
                        int k2 = rs.getInt(2);
                        int k3 = rs.getInt(3);
                        int vk1 = rs.getInt(4);
                        assertEquals(2, k1);
                        assertEquals(3, k2);
                        assertEquals(2, k3);
                        assertEquals(0, vk1);
                    }
                    assertFalse(rs.next());
                }
            }

            //tenant index
            {
                String sql = "SELECT vv1,k1,k2,k3,vk1 FROM " + viewName + " ORDER BY vv1 LIMIT 2 OFFSET (vv1,k1,k2,k3,vk1)=(-184,2, 3, 2, 0)";
                try (Statement statement = tenant2Connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
                    assertTrue(rs.next());
                    {
                        int vv1 = rs.getInt(1);
                        int k1 = rs.getInt(2);
                        int k2 = rs.getInt(3);
                        int k3 = rs.getInt(4);
                        int vk1 = rs.getInt(5);
                        assertEquals(-183, vv1);
                        assertEquals(2, k1);
                        assertEquals(3, k2);
                        assertEquals(1, k3);
                        assertEquals(3, vk1);
                    }
                    assertTrue(rs.next());
                    {
                        int vv1 = rs.getInt(1);
                        int k1 = rs.getInt(2);
                        int k2 = rs.getInt(3);
                        int k3 = rs.getInt(4);
                        int vk1 = rs.getInt(5);
                        assertEquals(-182, vv1);
                        assertEquals(2, k1);
                        assertEquals(3, k2);
                        assertEquals(1, k3);
                        assertEquals(2, vk1);
                    }
                    assertFalse(rs.next());
                }
            }
        }
    }

    @Test
    public void testTenantRVCOffset() throws SQLException {
        parameterizedTenantTestCase(false);
    }

    @Test
    public void testSaltedViewIndexRVCOffset() throws SQLException {
        parameterizedTenantTestCase(true);
    }

    @Test
    public void testViewIndexRVCOffset() throws SQLException {
        String multiTenantDataTableName = "T_" + generateUniqueName();

        String multiTenantDDL = String.format("CREATE TABLE %s (tenant_id VARCHAR NOT NULL, k1 TINYINT NOT NULL,\n" + "k2 TINYINT NOT NULL,\n"
                + "k3 TINYINT NOT NULL,\n" + "v1 INTEGER,\n" + "CONSTRAINT pk PRIMARY KEY (tenant_id, k1, k2, k3)) MULTI_TENANT=true",multiTenantDataTableName);

        try(Statement statement = conn.createStatement()) {
            statement.execute(multiTenantDDL);
            conn.commit();
        }

        String tenantId2 = "tenant2";

        //tenant connection
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId2);
        try(Connection tenant2Connection = DriverManager.getConnection(getUrl(), props)) {
            //create tenant view with new pks
            String viewName = multiTenantDataTableName + "_" + tenantId2;
            try(Statement statement = tenant2Connection.createStatement()) {
                statement.execute("CREATE VIEW " + viewName + " ( vk1 INTEGER NOT NULL, vv1 INTEGER, CONSTRAINT PKVIEW PRIMARY KEY(vk1))  AS SELECT * FROM "
                        + multiTenantDataTableName);
            }

            //create tenant view index on tenant view
            String viewIndexName = viewName + "_Index1";
            try(Statement statement = tenant2Connection.createStatement()) {
                statement.execute("CREATE INDEX " + viewIndexName + " ON " + viewName + " ( vv1 ) ");
            }

            String upsertDML = String.format("UPSERT INTO %s VALUES(?,?,?,?,?,?)", viewName);
            int tenantRows = 0;
            try(PreparedStatement tps = tenant2Connection.prepareStatement(upsertDML)) {
                for (int k1 = 0; k1 < 4; k1++) {
                    tps.setInt(1, k1);
                    for (int k2 = 0; k2 < 4; k2++) {
                        tps.setInt(2, k2);
                        for (int k3 = 0; k3 < 4; k3++) {
                            tps.setInt(3, k3);
                            tps.setInt(4, tenantRows);
                            for (int vk1 = 0; vk1 < 4; vk1++) {
                                tps.setInt(5, vk1);
                                tps.setInt(6, -tenantRows); //vv1

                                int result = tps.executeUpdate();
                                assertEquals(1, result);
                                tenantRows++;
                            }
                        }
                    }
                }
                tenant2Connection.commit();
            }

            //View Index Queries
            String sql = "SELECT vv1,k1,k2,k3,vk1 FROM " + viewName + " ORDER BY vv1 LIMIT 3 OFFSET (vv1,k1,k2,k3,vk1)=(-196, 3,0,0,1)";
            try(Statement statement = tenant2Connection.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
                assertTrue(rs.next());
                {
                    int vv1 = rs.getInt(1);
                    int k1 = rs.getInt(2);
                    int k2 = rs.getInt(3);
                    int k3 = rs.getInt(4);
                    int vk1 = rs.getInt(5);

                    assertEquals(-196, vv1);
                    assertEquals(3, k1);
                    assertEquals(0, k2);
                    assertEquals(1, k3);
                    assertEquals(0, vk1);
                }
                assertTrue(rs.next());
                {
                    int vv1 = rs.getInt(1);
                    int k1 = rs.getInt(2);
                    int k2 = rs.getInt(3);
                    int k3 = rs.getInt(4);
                    int vk1 = rs.getInt(5);

                    assertEquals(-195, vv1);
                    assertEquals(3, k1);
                    assertEquals(0, k2);
                    assertEquals(0, k3);
                    assertEquals(3, vk1);
                }
                assertTrue(rs.next());
                {
                    int vv1 = rs.getInt(1);
                    int k1 = rs.getInt(2);
                    int k2 = rs.getInt(3);
                    int k3 = rs.getInt(4);
                    int vk1 = rs.getInt(5);

                    assertEquals(-194, vv1);
                    assertEquals(3, k1);
                    assertEquals(0, k2);
                    assertEquals(0, k3);
                    assertEquals(2, vk1);
                }
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testIndexRVCOffset() throws SQLException {
        String sql = String.format("SELECT %s FROM %s LIMIT 3 OFFSET (%s)=(3, 3, 1)",DATA_INDEX_ROW_KEY,DATA_TABLE_NAME,DATA_INDEX_ROW_KEY);
        try(Statement statement = conn.createStatement() ; ResultSet rs = statement.executeQuery(sql)) {
            assertTrue(rs.next());
            {
                int k2 = rs.getInt(1);
                int k1 = rs.getInt(2);
                int k3 = rs.getInt(3);

                assertEquals(3, k2);
                assertEquals(3, k1);
                assertEquals(2, k3);
            }
            assertTrue(rs.next());
            {
                int k2 = rs.getInt(1);
                int k1 = rs.getInt(2);
                int k3 = rs.getInt(3);

                assertEquals(3, k2);
                assertEquals(3, k1);
                assertEquals(3, k3);
            }
            assertTrue(rs.next());
            {
                int k2 = rs.getInt(1);
                int k1 = rs.getInt(2);
                int k3 = rs.getInt(3);

                assertEquals(2, k2);
                assertEquals(0, k1);
                assertEquals(0, k3);
            }
            assertFalse(rs.next());
        }
    }

    @Test
    public void testUncoveredIndexRVCOffsetFails() throws SQLException {
        //v1 is not in the index
        String sql = "SELECT  k2,k1,k3,v1 FROM " + DATA_TABLE_NAME + " LIMIT 3 OFFSET (k2,k1,k3)=(3, 3, 2)";
        try (Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)){
            fail("Should not have allowed uncovered index access with RVC Offset without hinting to index.");
        } catch (RowValueConstructorOffsetNotCoercibleException e) {
            return;
        }

    }

    @Test
    public void testIndexSaltedBaseTableRVCOffset() throws SQLException {
        String saltedTableName = "T_" + generateUniqueName();

        String saltedDDL = String.format(DATA_DDL + "SALT_BUCKETS=4",saltedTableName);

        try (Statement statement = conn.createStatement()) {
            statement.execute(saltedDDL);
            conn.commit();
        }

        String indexName = "I_" + generateUniqueName();

        String indexDDL = String.format("CREATE INDEX %s ON %s (v1,k2)",indexName,saltedTableName);

        try (Statement statement = conn.createStatement()) {
            statement.execute(indexDDL);
            conn.commit();
        }

        String upsertDML = String.format("UPSERT INTO %s VALUES(?,?,?,?)", saltedTableName);
        int nRows = 0;
        try(PreparedStatement ps = conn.prepareStatement(upsertDML)) {
            for (int k1 = 0; k1 < 4; k1++) {
                ps.setInt(1, k1);
                for (int k2 = 0; k2 < 4; k2++) {
                    ps.setInt(2, k2);
                    for (int k3 = 0; k3 < 4; k3++) {
                        ps.setInt(3, k3);
                        ps.setInt(4, nRows);
                        int result = ps.executeUpdate();
                        assertEquals(1, result);
                        nRows++;
                    }
                }
            }
            conn.commit();
        }

        //Note Today Salted Base Table forces salted index
        String sql = "SELECT v1,k2,k1,k3 FROM " + saltedTableName + " LIMIT 3 OFFSET (v1,k2,k1,k3)=(8, 2, 0, 0)";
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            assertTrue(rs.next());
            {
                int v1 = rs.getInt(1);
                int k2 = rs.getInt(2);
                int k1 = rs.getInt(3);
                int k3 = rs.getInt(4);
                assertEquals(9, v1);
                assertEquals(2, k2);
                assertEquals(0, k1);
                assertEquals(1, k3);
            }
            assertTrue(rs.next());
            {
                int v1 = rs.getInt(1);
                int k2 = rs.getInt(2);
                int k1 = rs.getInt(3);
                int k3 = rs.getInt(4);
                assertEquals(10, v1);
                assertEquals(2, k2);
                assertEquals(0, k1);
                assertEquals(2, k3);
            }
            assertTrue(rs.next());
            {
                int v1 = rs.getInt(1);
                int k2 = rs.getInt(2);
                int k1 = rs.getInt(3);
                int k3 = rs.getInt(4);
                assertEquals(11, v1);
                assertEquals(2, k2);
                assertEquals(0, k1);
                assertEquals(3, k3);
            }
            assertFalse(rs.next());
        }
    }

    @Test
    public void testIndexMultiColumnsMultiIndexesRVCOffset() throws SQLException {
        String ddlTemplate = "CREATE TABLE %s (k1 TINYINT NOT NULL,\n" +
                "k2 TINYINT NOT NULL,\n" +
                "k3 TINYINT NOT NULL,\n" +
                "k4 TINYINT NOT NULL,\n" +
                "k5 TINYINT NOT NULL,\n" +
                "k6 TINYINT NOT NULL,\n" +
                "v1 INTEGER,\n" +
                "v2 INTEGER,\n" +
                "v3 INTEGER,\n" +
                "v4 INTEGER,\n" +
                "CONSTRAINT pk PRIMARY KEY (k1, k2, k3, k4, k5, k6)) ";

        String longKeyTableName = "T_" + generateUniqueName();
        String longKeyIndex1Name =  "INDEX_1_" + longKeyTableName;
        String longKeyIndex2Name =  "INDEX_2_" + longKeyTableName;

        String ddl = String.format(ddlTemplate,longKeyTableName);
        try(Statement statement = conn.createStatement()) {
            statement.execute(ddl);
        }

        String createIndex1 = "CREATE INDEX IF NOT EXISTS " + longKeyIndex1Name + " ON " + longKeyTableName + " (k2 ,v1, k4)";
        String createIndex2 = "CREATE INDEX IF NOT EXISTS " + longKeyIndex2Name + " ON " + longKeyTableName + " (v1, v3)";

        try(Statement statement = conn.createStatement()) {
            statement.execute(createIndex1);
        }
        try(Statement statement = conn.createStatement()) {
            statement.execute(createIndex2);
        }

        String sql = "SELECT  k2,v1,k4 FROM " + longKeyTableName + " LIMIT 3 OFFSET (k2,v1,k4,k1,k3,k5,k6)=(2,-1,4,1,3,5,6)";
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
        }
        sql = "SELECT  v1,v3 FROM " + longKeyTableName + " LIMIT 3 OFFSET (v1,v3,k1,k2,k3,k4,k5,k6)=(-1,-3,1,2,3,4,5,6)";
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
        }
    }

    @Test
    public void testIndexMultiColumnsMultiIndexesVariableLengthNullLiteralsRVCOffset() throws SQLException {
        String ddlTemplate = "CREATE TABLE %s (k1 VARCHAR,\n" +
                "k2 VARCHAR,\n" +
                "k3 VARCHAR,\n" +
                "k4 VARCHAR,\n" +
                "k5 VARCHAR,\n" +
                "k6 VARCHAR,\n" +
                "v1 VARCHAR,\n" +
                "v2 VARCHAR,\n" +
                "v3 VARCHAR,\n" +
                "v4 VARCHAR,\n" +
                "CONSTRAINT pk PRIMARY KEY (k1, k2, k3, k4, k5, k6)) ";

        String longKeyTableName = "T_" + generateUniqueName();
        String longKeyIndex1Name =  "INDEX_1_" + longKeyTableName;

        String ddl = String.format(ddlTemplate,longKeyTableName);
        try(Statement statement = conn.createStatement()) {
            statement.execute(ddl);
        }

        String createIndex1 = "CREATE INDEX IF NOT EXISTS " + longKeyIndex1Name + " ON " + longKeyTableName + " (k2 ,v1, k4)";

        try(Statement statement = conn.createStatement()) {
            statement.execute(createIndex1);
        }

        String sql0 = "SELECT  v1,v3 FROM " + longKeyTableName + " LIMIT 3 OFFSET (k1 ,k2, k3, k4, k5, k6)=('0','1',null,null,null,'2')";
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql0)) {
            PhoenixResultSet phoenixResultSet = rs.unwrap(PhoenixResultSet.class);
            assertEquals(1,phoenixResultSet.getStatement().getQueryPlan().getScans().size());
            assertEquals(1,phoenixResultSet.getStatement().getQueryPlan().getScans().get(0).size());
            byte[] startRow = phoenixResultSet.getStatement().getQueryPlan().getScans().get(0).get(0).getStartRow();
            byte[] expectedRow = new byte[] {'0',0,'1',0,0,0,0,'2',1}; //note trailing 1 not 0 due to phoenix internal inconsistency
            assertArrayEquals(expectedRow,startRow);
        }

        String sql = "SELECT  k2,v1,k4 FROM " + longKeyTableName + " LIMIT 3 OFFSET (k2,v1,k4,k1,k3,k5,k6)=('2',null,'4','1','3','5','6')";
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            PhoenixResultSet phoenixResultSet = rs.unwrap(PhoenixResultSet.class);
            assertEquals(1,phoenixResultSet.getStatement().getQueryPlan().getScans().size());
            assertEquals(1,phoenixResultSet.getStatement().getQueryPlan().getScans().get(0).size());
            byte[] startRow = phoenixResultSet.getStatement().getQueryPlan().getScans().get(0).get(0).getStartRow();
            byte[] expectedRow = new byte[] {'2',0,0,'4',0,'1',0,'3',0,'5',0,'6',1}; //note trailing 1 not 0 due to phoenix internal inconsistency
            assertArrayEquals(expectedRow,startRow);
        }
    }

    @Test
    public void testIndexMultiColumnsIndexedFixedLengthNullLiteralsRVCOffset() throws SQLException {
        String ddlTemplate = "CREATE TABLE %s (k1 VARCHAR,\n" +
                "v1 TINYINT,\n" +
                "v2 TINYINT,\n" +
                "v3 TINYINT,\n" +
                "v4 TINYINT,\n" +
                "CONSTRAINT pk PRIMARY KEY (k1)) ";

        String longKeyTableName = "T_" + generateUniqueName();
        String longKeyIndex1Name =  "INDEX_1_" + longKeyTableName;

        String ddl = String.format(ddlTemplate,longKeyTableName);
        try(Statement statement = conn.createStatement()) {
            statement.execute(ddl);
        }

        String createIndex1 = "CREATE INDEX IF NOT EXISTS " + longKeyIndex1Name + " ON " + longKeyTableName + " (v1, k1, v2, v3)";

        try(Statement statement = conn.createStatement()) {
            statement.execute(createIndex1);
        }

        String sql0 = "SELECT  v1,v3 FROM " + longKeyTableName + " LIMIT 3 OFFSET (k1)=('-1')";
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql0)) {
            PhoenixResultSet phoenixResultSet = rs.unwrap(PhoenixResultSet.class);
            assertEquals(1,phoenixResultSet.getStatement().getQueryPlan().getScans().size());
            assertEquals(1,phoenixResultSet.getStatement().getQueryPlan().getScans().get(0).size());
            byte[] startRow = phoenixResultSet.getStatement().getQueryPlan().getScans().get(0).get(0).getStartRow();
            byte[] expectedRow = new byte[] {'-','1',1}; //note trailing 1 not 0 due to phoenix internal inconsistency
            assertArrayEquals(expectedRow,startRow);
        }

        String sql = "SELECT  v1,v3 FROM " + longKeyTableName + " LIMIT 3 OFFSET (v1, k1, v2, v3)=(null,'a',null,0)";
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            PhoenixResultSet phoenixResultSet = rs.unwrap(PhoenixResultSet.class);
            assertEquals(1,phoenixResultSet.getStatement().getQueryPlan().getScans().size());
            assertEquals(1,phoenixResultSet.getStatement().getQueryPlan().getScans().get(0).size());
            byte[] startRow = phoenixResultSet.getStatement().getQueryPlan().getScans().get(0).get(0).getStartRow();
            /* decimal is used for fixed with types
             * we represent fixed integer keys as decimal form in the index as they need to be variable length.
             * -128 normal two's compliment form would be 0b10000000
             * but we flip the leading edge to keep the rowkey sorted, yielding 0.
             */
            byte[] expectedRow = new byte[] {0,'a',0,0,-128,1}; //note trailing 1 not 0 due to phoenix internal inconsistency
            assertArrayEquals(expectedRow,startRow);
        }

    }

    @Test
    public void testOffsetExplain() throws SQLException {
        String sql = "EXPLAIN SELECT * FROM " + DATA_TABLE_NAME + "  LIMIT 2 OFFSET (k1,k2,k3)=(2, 3, 2)";
        try(Statement statement = conn.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            StringBuilder explainStringBuilder = new StringBuilder();
            while (rs.next()) {
                String explain = rs.getString(1);
                explainStringBuilder.append(explain);
            }
            assertTrue(explainStringBuilder.toString().contains("With RVC Offset"));
        }
    }

    @Test public void testGlobalIndexViewAccess() throws Exception {
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        String viewName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + "(\n"
                        + "    ORGANIZATION_ID CHAR(15) NOT NULL,\n"
                        + "    PARENT_KEY_PREFIX CHAR(3) NOT NULL,\n"
                        + "    PARENT_ID CHAR(15) NOT NULL,\n"
                        + "    CREATED_DATE DATE NOT NULL,\n"
                        + "    DATA VARCHAR   \n"
                        + "    CONSTRAINT PK PRIMARY KEY \n"
                        + "    (\n"
                        + "        ORGANIZATION_ID, \n"
                        + "        PARENT_KEY_PREFIX,\n"
                        + "        PARENT_ID,\n"
                        + "        CREATED_DATE\n"
                        + "    )\n"
                        + ") MULTI_TENANT=true";

        //Index reorders the pk only
        String indexSyncDDL = "CREATE INDEX IF NOT EXISTS " + indexName + "\n"
                        + "ON " + tableName +" (PARENT_KEY_PREFIX, CREATED_DATE, PARENT_ID)\n"
                        + "INCLUDE (DATA)";

        String viewDDL = "CREATE VIEW IF NOT EXISTS " + viewName + "\n"
                + "AS SELECT * FROM " + tableName;
        try (Statement statement = conn.createStatement()) {
            statement.execute(ddl);
            statement.execute(indexSyncDDL);
            conn.commit();
        }

        String tenantId2 = "tenant2";

        //tenant connection
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId2);

        try (Connection tenantConnection = DriverManager.getConnection(getUrl(), props)) {
            try (Statement statement = tenantConnection.createStatement()) {
                statement.execute(viewDDL);
            }
            String baseQuery = "SELECT PARENT_ID,PARENT_KEY_PREFIX,CREATED_DATE,PARENT_ID,DATA\n"
                            + "FROM " + viewName + " LIMIT 2\n";

            try (PreparedStatement statement = tenantConnection.prepareStatement(baseQuery)) {
                QueryPlan plan = PhoenixRuntime.getOptimizedQueryPlan(statement);
                assertEquals(PTableType.INDEX, plan.getTableRef().getTable().getType());
            }

            String query = "SELECT PARENT_ID,PARENT_KEY_PREFIX,CREATED_DATE,PARENT_ID,DATA\n"
                            + "FROM " + viewName + "\n"
                            + "LIMIT 2\n"
                            + "OFFSET (PARENT_KEY_PREFIX,CREATED_DATE,PARENT_ID) = (?,?,?)\n";

            try (PreparedStatement statement = tenantConnection.prepareStatement(query)) {
                statement.setString(1, "a");
                statement.setDate(2, new Date(0));
                statement.setString(3, "b");

                ResultSet rs = statement.executeQuery(query);

                QueryPlan plan = PhoenixRuntime.getOptimizedQueryPlan(statement);
                assertEquals(PTableType.INDEX, plan.getTableRef().getTable().getType());
            }
        }
    }

    @Test
    public void rvcOffsetTrailingVariableLengthKeyTest() throws Exception {

        String tableName = generateUniqueName();
        String ddl = String.format("CREATE TABLE IF NOT EXISTS %s (\n"
                + " ORGANIZATION_ID VARCHAR(15), \n" + " TEST_ID VARCHAR(15), \n"
                + " CREATED_DATE DATE, \n" + " LAST_UPDATE DATE\n"
                + " CONSTRAINT TEST_SCHEMA_PK PRIMARY KEY (ORGANIZATION_ID, TEST_ID) \n" + ")",tableName);

        try (Statement statement = conn.createStatement()) {
            statement.execute(ddl);
        }
        //setup
        String upsert = "UPSERT INTO %s(ORGANIZATION_ID,TEST_ID) VALUES (%s,%s)";
        List<String> upserts = new ArrayList<>();
        upserts.add(String.format(upsert,tableName,"'1'","'1'"));
        upserts.add(String.format(upsert,tableName,"'1'","'10'"));
        upserts.add(String.format(upsert,tableName,"'2'","'2'"));

        for(String sql : upserts) {
            try (Statement statement = conn.createStatement()) {
                statement.execute(sql);
            }
        }
        conn.commit();

        String query = String.format("SELECT * FROM %s OFFSET (ORGANIZATION_ID,TEST_ID) = ('1','1')",tableName);

        try (Statement statement = conn.createStatement() ; ResultSet rs2 = statement.executeQuery(query) ) {
            assertTrue(rs2.next());
            assertEquals("1",rs2.getString(1));
            assertEquals("10",rs2.getString(2));
            assertTrue(rs2.next());
            assertEquals("2",rs2.getString(1));
            assertEquals("2",rs2.getString(2));
            assertFalse(rs2.next());
        }
    }

    //Note that phoenix does not suport a rowkey with a null inserted so there is no single key version of this.
    @Test
    public void rvcOffsetTrailingNullKeyTest() throws Exception {
        String tableName = generateUniqueName();
        String ddl = String.format("CREATE TABLE IF NOT EXISTS %s (\n"
                + " ORGANIZATION_ID VARCHAR(15), \n" + " TEST_ID VARCHAR(15), \n"
                + " CREATED_DATE DATE, \n" + " LAST_UPDATE DATE\n"
                + " CONSTRAINT TEST_SCHEMA_PK PRIMARY KEY (ORGANIZATION_ID, TEST_ID) \n" + ")",tableName);

        try (Statement statement = conn.createStatement()) {
            statement.execute(ddl);
        }
        //setup
        String upsert = "UPSERT INTO %s(ORGANIZATION_ID,TEST_ID) VALUES (%s,%s)";
        List<String> upserts = new ArrayList<>();
        upserts.add(String.format(upsert,tableName,"'0'","null"));
        upserts.add(String.format(upsert,tableName,"'1'","null"));
        upserts.add(String.format(upsert,tableName,"'1'","'1'"));
        upserts.add(String.format(upsert,tableName,"'1'","'10'"));
        upserts.add(String.format(upsert,tableName,"'2'","null"));

        for(String sql : upserts) {
            try (Statement statement = conn.createStatement()) {
                statement.execute(sql);
            }
        }
        conn.commit();

        String query = String.format("SELECT * FROM %s OFFSET (ORGANIZATION_ID,TEST_ID) = ('1',null)",tableName);

        try (Statement statement = conn.createStatement() ; ResultSet rs2 = statement.executeQuery(query) ) {
            assertTrue(rs2.next());
            assertEquals("1",rs2.getString(1));
            assertEquals("1",rs2.getString(2));
            assertTrue(rs2.next());
            assertEquals("1",rs2.getString(1));
            assertEquals("10",rs2.getString(2));
            assertTrue(rs2.next());
            assertEquals("2",rs2.getString(1));
            assertNull(rs2.getString(2));
            assertFalse(rs2.next());
        }

        String preparedQuery = String.format("SELECT * FROM %s OFFSET (ORGANIZATION_ID,TEST_ID) = (?,?)",tableName);

        try (PreparedStatement statement = conn.prepareStatement(preparedQuery)  ) {

            statement.setString(1,"1");
            statement.setString(2,null);

            try(ResultSet rs2 = statement.executeQuery()) {
                assertTrue(rs2.next());
                assertEquals("1", rs2.getString(1));
                assertEquals("1", rs2.getString(2));
                assertTrue(rs2.next());
                assertEquals("1", rs2.getString(1));
                assertEquals("10", rs2.getString(2));
                assertTrue(rs2.next());
                assertEquals("2", rs2.getString(1));
                assertNull(rs2.getString(2));
                assertFalse(rs2.next());
            }
        }
    }

}
