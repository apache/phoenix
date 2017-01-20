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

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;

public class HashJoinMoreIT extends ParallelStatsDisabledIT {
    private final String[] plans = new String[] {
            /*
             * testJoinWithKeyRangeOptimization()
             *     SELECT lhs.col0, lhs.col1, lhs.col2, rhs.col0, rhs.col1, rhs.col2 
             *     FROM TEMP_TABLE_COMPOSITE_PK lhs 
             *     JOIN TEMP_TABLE_COMPOSITE_PK rhs ON lhs.col1 = rhs.col2
             */
            "CLIENT PARALLEL 4-WAY FULL SCAN OVER %s\n" +
            "CLIENT MERGE SORT\n" +
            "    PARALLEL INNER-JOIN TABLE 0\n" +
            "        CLIENT PARALLEL 4-WAY FULL SCAN OVER %s\n" +
            "        CLIENT MERGE SORT",
            /*
             * testJoinWithKeyRangeOptimization()
             *     SELECT lhs.col0, lhs.col1, lhs.col2, rhs.col0, rhs.col1, rhs.col2 
             *     FROM TEMP_TABLE_COMPOSITE_PK lhs 
             *     JOIN TEMP_TABLE_COMPOSITE_PK rhs ON lhs.col0 = rhs.col2
             */
            "CLIENT PARALLEL 4-WAY FULL SCAN OVER %s\n" +
            "CLIENT MERGE SORT\n" +
            "    PARALLEL INNER-JOIN TABLE 0\n" +
            "        CLIENT PARALLEL 4-WAY FULL SCAN OVER %s\n" +
            "        CLIENT MERGE SORT\n" +
            "    DYNAMIC SERVER FILTER BY LHS.COL0 IN (RHS.COL2)",
            /*
             * testJoinWithKeyRangeOptimization()
             *     SELECT lhs.col0, lhs.col1, lhs.col2, rhs.col0, rhs.col1, rhs.col2 
             *     FROM TEMP_TABLE_COMPOSITE_PK lhs 
             *     JOIN TEMP_TABLE_COMPOSITE_PK rhs ON lhs.col0 = rhs.col1 AND lhs.col1 = rhs.col2
             */
            "CLIENT PARALLEL 4-WAY FULL SCAN OVER %s\n" +
            "CLIENT MERGE SORT\n" +
            "    PARALLEL INNER-JOIN TABLE 0\n" +
            "        CLIENT PARALLEL 4-WAY FULL SCAN OVER %s\n" +
            "        CLIENT MERGE SORT\n" +
            "    DYNAMIC SERVER FILTER BY (LHS.COL0, LHS.COL1) IN ((RHS.COL1, RHS.COL2))",
            /*
             * testJoinWithKeyRangeOptimization()
             *     SELECT lhs.col0, lhs.col1, lhs.col2, rhs.col0, rhs.col1, rhs.col2 
             *     FROM TEMP_TABLE_COMPOSITE_PK lhs 
             *     JOIN TEMP_TABLE_COMPOSITE_PK rhs ON lhs.col0 = rhs.col1 AND lhs.col2 = rhs.col3 - 1 AND lhs.col1 = rhs.col2
             */
            "CLIENT PARALLEL 4-WAY FULL SCAN OVER %s\n" +
            "CLIENT MERGE SORT\n" +
            "    PARALLEL INNER-JOIN TABLE 0\n" +
            "        CLIENT PARALLEL 4-WAY FULL SCAN OVER %s\n" +
            "        CLIENT MERGE SORT\n" +
            "    DYNAMIC SERVER FILTER BY (LHS.COL0, LHS.COL1, LHS.COL2) IN ((RHS.COL1, RHS.COL2, TO_INTEGER((RHS.COL3 - 1))))",            
    };
    
    @Test
    public void testJoinOverSaltedTables() throws Exception {
        String tempTableNoSalting = generateUniqueName();
        String tempTableWithSalting = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute("CREATE TABLE " + tempTableNoSalting 
                    + "   (mypk INTEGER NOT NULL PRIMARY KEY, " 
                    + "    col1 INTEGER)");
            conn.createStatement().execute("CREATE TABLE " + tempTableWithSalting 
                    + "   (mypk INTEGER NOT NULL PRIMARY KEY, " 
                    + "    col1 INTEGER) SALT_BUCKETS=4");
            
            PreparedStatement upsertStmt = conn.prepareStatement(
                    "upsert into " + tempTableNoSalting + "(mypk, col1) " + "values (?, ?)");
            for (int i = 0; i < 3; i++) {
                upsertStmt.setInt(1, i + 1);
                upsertStmt.setInt(2, 3 - i);
                upsertStmt.execute();
            }
            conn.commit();
            
            upsertStmt = conn.prepareStatement(
                    "upsert into " + tempTableWithSalting + "(mypk, col1) " + "values (?, ?)");
            for (int i = 0; i < 6; i++) {
                upsertStmt.setInt(1, i + 1);
                upsertStmt.setInt(2, 3 - (i % 3));
                upsertStmt.execute();
            }
            conn.commit();
            
            // LHS=unsalted JOIN RHS=salted
            String query = "SELECT lhs.mypk, lhs.col1, rhs.mypk, rhs.col1 FROM " 
                    + tempTableNoSalting + " lhs JOIN "
                    + tempTableWithSalting + " rhs ON rhs.mypk = lhs.col1";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 3);
            assertEquals(rs.getInt(4), 1);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 2);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 1);
            assertEquals(rs.getInt(4), 3);

            assertFalse(rs.next());
            
            // LHS=salted JOIN RHS=salted
            query = "SELECT lhs.mypk, lhs.col1, rhs.mypk, rhs.col1 FROM " 
                    + tempTableWithSalting + " lhs JOIN "
                    + tempTableNoSalting + " rhs ON rhs.mypk = lhs.col1";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 3);
            assertEquals(rs.getInt(4), 1);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 2);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 1);
            assertEquals(rs.getInt(4), 3);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 4);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 3);
            assertEquals(rs.getInt(4), 1);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 5);
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 2);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 6);
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 1);
            assertEquals(rs.getInt(4), 3);

            assertFalse(rs.next());
            
            // LHS=salted JOIN RHS=salted
            query = "SELECT lhs.mypk, lhs.col1, rhs.mypk, rhs.col1 FROM " 
                    + tempTableWithSalting + " lhs JOIN "
                    + tempTableWithSalting + " rhs ON rhs.mypk = (lhs.col1 + 3)";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 6);
            assertEquals(rs.getInt(4), 1);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 5);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 4);
            assertEquals(rs.getInt(4), 3);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 4);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 6);
            assertEquals(rs.getInt(4), 1);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 5);
            assertEquals(rs.getInt(2), 2);
            assertEquals(rs.getInt(3), 5);
            assertEquals(rs.getInt(4), 2);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 6);
            assertEquals(rs.getInt(2), 1);
            assertEquals(rs.getInt(3), 4);
            assertEquals(rs.getInt(4), 3);

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testJoinOnDynamicColumns() throws Exception {
        String tableA = generateUniqueName();
        String tableB = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl(), props);
            String ddlA = "CREATE TABLE " + tableA + "   (pkA INTEGER NOT NULL, " + "    colA1 INTEGER, "
                    + "        colA2 VARCHAR " + "CONSTRAINT PK PRIMARY KEY" + "(pkA)" + ")";

            String ddlB = "CREATE TABLE " + tableB + "   (pkB INTEGER NOT NULL PRIMARY KEY, " + "    colB INTEGER)";
            conn.createStatement().execute(ddlA);
            conn.createStatement().execute(ddlB);

            String upsertA = "UPSERT INTO " + tableA + " (pkA, colA1, colA2) VALUES(?, ?, ?)";
            stmt = conn.prepareStatement(upsertA);
            int i = 0;
            for (i = 0; i < 5; i++) {
                stmt.setInt(1, i);
                stmt.setInt(2, i + 10);
                stmt.setString(3, "00" + i);
                stmt.executeUpdate();
            }
            conn.commit();
            stmt.close();

            String sequenceB = generateUniqueName();
            // upsert select dynamic columns in tableB
            conn.createStatement().execute("CREATE SEQUENCE " + sequenceB );
            String upsertBSelectA = "UPSERT INTO " + tableB + " (pkB, pkA INTEGER)"
                    + "SELECT NEXT VALUE FOR " + sequenceB + ", pkA FROM " + tableA ;
            stmt = conn.prepareStatement(upsertBSelectA);
            stmt.executeUpdate();
            stmt.close();
            conn.commit();
            conn.createStatement().execute("DROP SEQUENCE " + sequenceB );

            // perform a join between tableB and tableA by joining on the dynamic column that we upserted in
            // tableB. This join should return all the rows from table A.
            String joinSql = "SELECT A.pkA, A.COLA1, A.colA2 FROM " + tableB + " B(pkA INTEGER) JOIN " + tableA + " A ON a.pkA = b.pkA";
            stmt = conn.prepareStatement(joinSql);
            ResultSet rs = stmt.executeQuery();
            i = 0;
            while (rs.next()) {
                // check that we get back all the rows that we upserted for tableA above.
                assertEquals(rs.getInt(1), i);
                assertEquals(rs.getInt(2), i + 10);
                assertEquals(rs.getString(3), "00" + i);
                i++;
            }
            assertEquals(5,i);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }

        }

    }
    
    @Test
    public void testJoinWithKeyRangeOptimization() throws Exception {
        String tempTableWithCompositePK = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute("CREATE TABLE " + tempTableWithCompositePK 
                    + "   (col0 INTEGER NOT NULL, " 
                    + "    col1 INTEGER NOT NULL, " 
                    + "    col2 INTEGER NOT NULL, "
                    + "    col3 INTEGER "
                    + "   CONSTRAINT pk PRIMARY KEY (col0, col1, col2)) " 
                    + "   SALT_BUCKETS=4");
            
            PreparedStatement upsertStmt = conn.prepareStatement(
                    "upsert into " + tempTableWithCompositePK + "(col0, col1, col2, col3) " + "values (?, ?, ?, ?)");
            for (int i = 0; i < 3; i++) {
                upsertStmt.setInt(1, i + 1);
                upsertStmt.setInt(2, i + 2);
                upsertStmt.setInt(3, i + 3);
                upsertStmt.setInt(4, i + 5);
                upsertStmt.execute();
            }
            conn.commit();
            
            // No leading part of PK
            String query = "SELECT lhs.col0, lhs.col1, lhs.col2, lhs.col3, rhs.col0, rhs.col1, rhs.col2, rhs.col3 FROM " 
                    + tempTableWithCompositePK + " lhs JOIN "
                    + tempTableWithCompositePK + " rhs ON lhs.col1 = rhs.col2";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 4);
            assertEquals(rs.getInt(4), 6);            
            assertEquals(rs.getInt(5), 1);
            assertEquals(rs.getInt(6), 2);
            assertEquals(rs.getInt(7), 3);
            assertEquals(rs.getInt(8), 5);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 4);
            assertEquals(rs.getInt(3), 5);
            assertEquals(rs.getInt(4), 7);
            assertEquals(rs.getInt(5), 2);
            assertEquals(rs.getInt(6), 3);
            assertEquals(rs.getInt(7), 4);
            assertEquals(rs.getInt(8), 6);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(String.format(plans[0],tempTableWithCompositePK,tempTableWithCompositePK), QueryUtil.getExplainPlan(rs));
            
            // Two parts of PK but only one leading part
            query = "SELECT lhs.col0, lhs.col1, lhs.col2, lhs.col3, rhs.col0, rhs.col1, rhs.col2, rhs.col3 FROM " 
                    + tempTableWithCompositePK + " lhs JOIN "
                    + tempTableWithCompositePK + " rhs ON lhs.col2 = rhs.col3 AND lhs.col0 = rhs.col2";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 4);
            assertEquals(rs.getInt(3), 5);
            assertEquals(rs.getInt(4), 7);
            assertEquals(rs.getInt(5), 1);
            assertEquals(rs.getInt(6), 2);
            assertEquals(rs.getInt(7), 3);
            assertEquals(rs.getInt(8), 5);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(String.format(plans[1],tempTableWithCompositePK,tempTableWithCompositePK), QueryUtil.getExplainPlan(rs));
            
            // Two leading parts of PK
            query = "SELECT lhs.col0, lhs.col1, lhs.col2, lhs.col3, rhs.col0, rhs.col1, rhs.col2, rhs.col3 FROM " 
                    + tempTableWithCompositePK + " lhs JOIN "
                    + tempTableWithCompositePK + " rhs ON lhs.col1 = rhs.col2 AND lhs.col0 = rhs.col1";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 4);
            assertEquals(rs.getInt(4), 6);
            assertEquals(rs.getInt(5), 1);
            assertEquals(rs.getInt(6), 2);
            assertEquals(rs.getInt(7), 3);
            assertEquals(rs.getInt(8), 5);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 4);
            assertEquals(rs.getInt(3), 5);
            assertEquals(rs.getInt(4), 7);
            assertEquals(rs.getInt(5), 2);
            assertEquals(rs.getInt(6), 3);
            assertEquals(rs.getInt(7), 4);
            assertEquals(rs.getInt(8), 6);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(String.format(plans[2],tempTableWithCompositePK,tempTableWithCompositePK), QueryUtil.getExplainPlan(rs));
            
            // All parts of PK
            query = "SELECT lhs.col0, lhs.col1, lhs.col2, lhs.col3, rhs.col0, rhs.col1, rhs.col2, rhs.col3 FROM " 
                    + tempTableWithCompositePK + " lhs JOIN "
                    + tempTableWithCompositePK + " rhs ON lhs.col1 = rhs.col2 AND lhs.col2 = rhs.col3 - 1 AND lhs.col0 = rhs.col1";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getInt(2), 3);
            assertEquals(rs.getInt(3), 4);
            assertEquals(rs.getInt(4), 6);
            assertEquals(rs.getInt(5), 1);
            assertEquals(rs.getInt(6), 2);
            assertEquals(rs.getInt(7), 3);
            assertEquals(rs.getInt(8), 5);
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getInt(2), 4);
            assertEquals(rs.getInt(3), 5);
            assertEquals(rs.getInt(4), 7);
            assertEquals(rs.getInt(5), 2);
            assertEquals(rs.getInt(6), 3);
            assertEquals(rs.getInt(7), 4);
            assertEquals(rs.getInt(8), 6);

            assertFalse(rs.next());
            
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            assertEquals(String.format(plans[3],tempTableWithCompositePK,tempTableWithCompositePK), QueryUtil.getExplainPlan(rs));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSubqueryWithoutData() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String GRAMMAR_TABLE = "CREATE TABLE IF NOT EXISTS GRAMMAR_TABLE (ID INTEGER PRIMARY KEY, " +
                    "unsig_id UNSIGNED_INT, big_id BIGINT, unsig_long_id UNSIGNED_LONG, tiny_id TINYINT," +
                    "unsig_tiny_id UNSIGNED_TINYINT, small_id SMALLINT, unsig_small_id UNSIGNED_SMALLINT," + 
                    "float_id FLOAT, unsig_float_id UNSIGNED_FLOAT, double_id DOUBLE, unsig_double_id UNSIGNED_DOUBLE," + 
                    "decimal_id DECIMAL, boolean_id BOOLEAN, time_id TIME, date_id DATE, timestamp_id TIMESTAMP," + 
                    "unsig_time_id TIME, unsig_date_id DATE, unsig_timestamp_id TIMESTAMP, varchar_id VARCHAR (30)," + 
                    "char_id CHAR (30), binary_id BINARY (100), varbinary_id VARBINARY (100))";

            String LARGE_TABLE = "CREATE TABLE IF NOT EXISTS LARGE_TABLE (ID INTEGER PRIMARY KEY, " +
                    "unsig_id UNSIGNED_INT, big_id BIGINT, unsig_long_id UNSIGNED_LONG, tiny_id TINYINT," +
                    "unsig_tiny_id UNSIGNED_TINYINT, small_id SMALLINT, unsig_small_id UNSIGNED_SMALLINT," + 
                    "float_id FLOAT, unsig_float_id UNSIGNED_FLOAT, double_id DOUBLE, unsig_double_id UNSIGNED_DOUBLE," + 
                    "decimal_id DECIMAL, boolean_id BOOLEAN, time_id TIME, date_id DATE, timestamp_id TIMESTAMP," + 
                    "unsig_time_id TIME, unsig_date_id DATE, unsig_timestamp_id TIMESTAMP, varchar_id VARCHAR (30)," + 
                    "char_id CHAR (30), binary_id BINARY (100), varbinary_id VARBINARY (100))";

            String SECONDARY_LARGE_TABLE = "CREATE TABLE IF NOT EXISTS SECONDARY_LARGE_TABLE (SEC_ID INTEGER PRIMARY KEY," +
                    "sec_unsig_id UNSIGNED_INT, sec_big_id BIGINT, sec_usnig_long_id UNSIGNED_LONG, sec_tiny_id TINYINT," + 
                    "sec_unsig_tiny_id UNSIGNED_TINYINT, sec_small_id SMALLINT, sec_unsig_small_id UNSIGNED_SMALLINT," + 
                    "sec_float_id FLOAT, sec_unsig_float_id UNSIGNED_FLOAT, sec_double_id DOUBLE, sec_unsig_double_id UNSIGNED_DOUBLE," +
                    "sec_decimal_id DECIMAL, sec_boolean_id BOOLEAN, sec_time_id TIME, sec_date_id DATE," +
                    "sec_timestamp_id TIMESTAMP, sec_unsig_time_id TIME, sec_unsig_date_id DATE, sec_unsig_timestamp_id TIMESTAMP," +
                    "sec_varchar_id VARCHAR (30), sec_char_id CHAR (30), sec_binary_id BINARY (100), sec_varbinary_id VARBINARY (100))";
            createTestTable(getUrl(), GRAMMAR_TABLE);
            createTestTable(getUrl(), LARGE_TABLE);
            createTestTable(getUrl(), SECONDARY_LARGE_TABLE);

            String ddl = "SELECT * FROM (SELECT ID, BIG_ID, DATE_ID FROM LARGE_TABLE AS A WHERE (A.ID % 5) = 0) AS A " +
                    "INNER JOIN (SELECT SEC_ID, SEC_TINY_ID, SEC_UNSIG_FLOAT_ID FROM SECONDARY_LARGE_TABLE AS B WHERE (B.SEC_ID % 5) = 0) AS B " +     
                    "ON A.ID=B.SEC_ID WHERE A.DATE_ID > ALL (SELECT SEC_DATE_ID FROM SECONDARY_LARGE_TABLE LIMIT 100) " +      
                    "AND B.SEC_UNSIG_FLOAT_ID = ANY (SELECT sec_unsig_float_id FROM SECONDARY_LARGE_TABLE " +                                       
                    "WHERE SEC_ID > ALL (SELECT MIN (ID) FROM GRAMMAR_TABLE WHERE UNSIG_ID IS NULL) AND " +
                    "SEC_UNSIG_ID < ANY (SELECT DISTINCT(UNSIG_ID) FROM LARGE_TABLE WHERE UNSIG_ID<2500) LIMIT 1000) " +
                    "AND A.ID < 10000";
            ResultSet rs = conn.createStatement().executeQuery(ddl);
            assertFalse(rs.next());  
        } finally {
            Statement statement = conn.createStatement();
            String query = "drop table GRAMMAR_TABLE";
            statement.executeUpdate(query);
            query = "drop table LARGE_TABLE";
            statement.executeUpdate(query);
            query = "drop table SECONDARY_LARGE_TABLE";
            statement.executeUpdate(query);
            conn.close();
        }
    }
    
    // PHOENIX-2381
    @Test
    public void testJoinWithMultiTenancy() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute("CREATE TABLE INVENTORY (" +
                            " TENANTID UNSIGNED_INT NOT NULL" +
                            ",ID UNSIGNED_INT NOT NULL" +
                            ",FOO UNSIGNED_INT NOT NULL" +
                            ",\"TIMESTAMP\"  UNSIGNED_LONG NOT NULL" +
                            ",CODES INTEGER ARRAY[] NOT NULL" +
                            ",V UNSIGNED_LONG" +
                            " CONSTRAINT pk PRIMARY KEY (TENANTID, ID, FOO, \"TIMESTAMP\" , CODES))" +
                            " DEFAULT_COLUMN_FAMILY ='E'," +
                            " MULTI_TENANT=true");
            PreparedStatement upsertStmt = conn.prepareStatement(
                    "upsert into INVENTORY "
                    + "(tenantid, id, foo, \"TIMESTAMP\" , codes) "
                    + "values (?, ?, ?, ?, ?)");
            upsertStmt.setInt(1, 15);
            upsertStmt.setInt(2, 5);
            upsertStmt.setInt(3, 0);
            upsertStmt.setLong(4, 6);
            Array array = conn.createArrayOf("INTEGER", new Object[] {1, 2});
            upsertStmt.setArray(5, array);
            upsertStmt.executeUpdate();
            conn.commit();
            
            conn.createStatement().execute("CREATE TABLE PRODUCT_IDS (" +
                            " PRODUCT_ID UNSIGNED_INT NOT NULL" +
                            ",PRODUCT_NAME VARCHAR" +
                            " CONSTRAINT pk PRIMARY KEY (PRODUCT_ID))" +
                            " DEFAULT_COLUMN_FAMILY ='NAME'");
            upsertStmt = conn.prepareStatement(
                    "upsert into PRODUCT_IDS "
                    + "(product_id, product_name) "
                    + "values (?, ?)");
            upsertStmt.setInt(1, 5);
            upsertStmt.setString(2, "DUMMY");
            upsertStmt.executeUpdate();
            conn.commit();
            conn.close();            

            // Create a tenant-specific connection.
            props.setProperty("TenantId", "15");
            conn = DriverManager.getConnection(getUrl(), props);
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT * FROM INVENTORY INNER JOIN PRODUCT_IDS ON (PRODUCT_ID = INVENTORY.ID)");
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 5);
            assertFalse(rs.next());
            rs.close();
            rs = conn.createStatement().executeQuery(
                    "SELECT * FROM INVENTORY RIGHT JOIN PRODUCT_IDS ON (PRODUCT_ID = INVENTORY.ID)");
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 5);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testBug2480() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            conn.createStatement().execute(
                      "CREATE TABLE master_businessunit ("
                    + "code varchar(255) PRIMARY KEY, name varchar(255))");
            conn.createStatement().execute(
                      "CREATE TABLE master_company ("
                    + "code varchar(255) PRIMARY KEY, name varchar(255))");
            conn.createStatement().execute(
                      "CREATE TABLE master_costcenter ("
                    + "code varchar(255) PRIMARY KEY, name varchar(255))");
            conn.createStatement().execute(
                      "CREATE TABLE master_location ("
                    + "code varchar(255) PRIMARY KEY, name varchar(255))");
            conn.createStatement().execute(
                      "CREATE TABLE master_product ("
                    + "id integer PRIMARY KEY, product_name varchar(255))");
            conn.createStatement().execute(
                      "CREATE TABLE master_purchaseorder ("
                    + "purchaseOrderNumber varchar(255), "
                    + "companyCode varchar(255), "
                    + "businessUnitCode varchar(255), "
                    + "locationCode varchar(255), "
                    + "purchaseOrderId varchar(255) PRIMARY KEY, "
                    + "releasedOn date, name varchar(255))");
            conn.createStatement().execute(
                      "CREATE TABLE trans_purchaseorderitem ("
                    + "purchaseOrderItemId varchar(255) PRIMARY KEY, "
                    + "purchaseOrderId varchar(255), "
                    + "lineNo varchar(255), name varchar(255))");
            conn.createStatement().execute(
                      "CREATE TABLE trans_purchaseorderitem_costing ("
                    + "purchaseorderItem_costing_id varchar(255) primary key, "
                    + "purchaseorderItemId varchar(255), "
                    + "purchaseorderId varchar(255), "
                    + "costcenterCode varchar(255))");
            
            conn.createStatement().execute("upsert into master_businessunit(code,name) values ('1','BU1')");
            conn.createStatement().execute("upsert into master_businessunit(code,name) values ('2','BU2')");
            conn.createStatement().execute("upsert into master_company(code,name) values ('1','Company1')");
            conn.createStatement().execute("upsert into master_company(code,name) values ('2','Company2')");
            conn.createStatement().execute("upsert into master_costcenter(code,name) values ('1','CC1')");
            conn.createStatement().execute("upsert into master_costcenter(code,name) values ('2','CC2')");
            conn.createStatement().execute("upsert into master_location(code,name) values ('1','Location1')");
            conn.createStatement().execute("upsert into master_location(code,name) values ('2','Location2')");
            conn.createStatement().execute("upsert into master_product(id,product_name) values (1,'ProductName1')");
            conn.createStatement().execute("upsert into master_product(id,product_name) values (2,'Product2')");
            conn.createStatement().execute("upsert into master_purchaseorder(purchaseOrderNumber,companyCode,businessUnitCode,locationCode,purchaseOrderId,releasedOn,name) values ('1','1','1','1','1','2015-12-01','1')");
            conn.createStatement().execute("upsert into master_purchaseorder(purchaseOrderNumber,companyCode,businessUnitCode,locationCode,purchaseOrderId,releasedOn,name) values ('2','2','2','2','2','2015-12-02','2')");
            conn.createStatement().execute("upsert into trans_purchaseorderitem(purchaseOrderItemId,purchaseOrderId,lineNo,name) values ('1','1','1','1')");
            conn.createStatement().execute("upsert into trans_purchaseorderitem(purchaseOrderItemId,purchaseOrderId,lineNo,name) values ('2','2','2','2')");
            conn.createStatement().execute("upsert into trans_purchaseorderitem_costing(purchaseorderItem_costing_id,purchaseorderItemId,purchaseorderId,costcenterCode) values ('1','1','1','1')");
            conn.createStatement().execute("upsert into trans_purchaseorderitem_costing(purchaseorderItem_costing_id,purchaseorderItemId,purchaseorderId,costcenterCode) values ('2','2','2','2')");
            
            ResultSet rs = conn.createStatement().executeQuery(
                      "SELECT DISTINCT "
                    + "COALESCE( a1.name, 'N.A.'), "
                    + "COALESCE( a2.name, 'N.A.'), "
                    + "COALESCE( a3.name, 'N.A.'), "
                    + "COALESCE( a4.purchaseOrderNumber, 'N.A.'), "
                    + "COALESCE( a1.name, 'N.A.'), "
                    + "COALESCE( a4.name, 'N.A.'), "
                    + "COALESCE( a5.lineNo, 'N.A.'), "
                    + "COALESCE( a5.name, 'N.A.'), "
                    + "COALESCE( a7.name,'N.A.') "
                    + "FROM (master_purchaseorder a4 "
                    + "LEFT OUTER JOIN master_company a1 "
                    + "ON a4.companyCode = a1.code "
                    + "LEFT OUTER JOIN master_businessunit a2 "
                    + "ON a4.businessUnitCode = a2.code "
                    + "LEFT OUTER JOIN master_location a3 "
                    + "ON a4.locationCode = a3.code "
                    + "LEFT OUTER JOIN trans_purchaseorderitem a5 "
                    + "ON a5.purchaseOrderId = a4.purchaseOrderId "
                    + "LEFT OUTER JOIN trans_purchaseorderitem_costing a6 "
                    + "ON a6.purchaseOrderItemId = a5.purchaseOrderItemId "
                    + "AND a6.purchaseOrderId = a5.purchaseOrderId "
                    + "LEFT OUTER JOIN master_costcenter a7 "
                    + "ON a6.costCenterCode = a7.code)");
            
            assertTrue(rs.next());
            assertEquals("Company1", rs.getString(1));
            assertEquals("BU1", rs.getString(2));
            assertEquals("Location1", rs.getString(3));
            assertEquals("1", rs.getString(4));
            assertEquals("Company1", rs.getString(5));
            assertEquals("1", rs.getString(6));
            assertEquals("1", rs.getString(7));
            assertEquals("1", rs.getString(8));
            assertEquals("CC1", rs.getString(9));
            assertTrue(rs.next());
            assertEquals("Company2", rs.getString(1));
            assertEquals("BU2", rs.getString(2));
            assertEquals("Location2", rs.getString(3));
            assertEquals("2", rs.getString(4));
            assertEquals("Company2", rs.getString(5));
            assertEquals("2", rs.getString(6));
            assertEquals("2", rs.getString(7));
            assertEquals("2", rs.getString(8));
            assertEquals("CC2", rs.getString(9));
            
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testBug2894() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE IF NOT EXISTS EVENT_COUNT (\n" +
                    "        BUCKET VARCHAR,\n" +
                    "        TIMESTAMP_DATE TIMESTAMP,\n" +
                    "        \"TIMESTAMP\" UNSIGNED_LONG NOT NULL,\n" +
                    "        LOCATION VARCHAR,\n" +
                    "        A VARCHAR,\n" +
                    "        B VARCHAR,\n" +
                    "        C VARCHAR,\n" +
                    "        D UNSIGNED_LONG,\n" +
                    "        E FLOAT\n" +
                    "    CONSTRAINT pk PRIMARY KEY (BUCKET, \"TIMESTAMP\" DESC, LOCATION, A, B, C)\n" +
                    ") SALT_BUCKETS=2, COMPRESSION='GZ', TTL=31622400");
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO EVENT_COUNT(BUCKET, \"TIMESTAMP\", LOCATION, A, B, C) VALUES(?,?,?,?,?,?)");
            stmt.setString(1, "5SEC");
            stmt.setString(3, "Tr/Bal");
            stmt.setString(4, "A1");
            stmt.setString(5, "B1");
            stmt.setString(6, "C1");
            stmt.setLong(2, 1462993520000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993515000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993510000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993505000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993500000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993495000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993490000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993485000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993480000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993475000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993470000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993465000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993460000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993455000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993450000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993445000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993440000000000L);
            stmt.execute();
            stmt.setLong(2, 1462993430000000000L);
            stmt.execute();

            // We'll test the original version of the user table as well as a slightly modified
            // version, in order to verify that hash join works for columns both having DESC
            // sort order as well as one having ASC order and the other having DESC order.
            String[] t = new String[] {"EVENT_LATENCY", "EVENT_LATENCY_2"};
            for (int i = 0; i < 2; i++) {
                conn.createStatement().execute(
                        "CREATE TABLE IF NOT EXISTS " + t[i] + " (\n" +
                                "        BUCKET VARCHAR,\n" +
                                "        TIMESTAMP_DATE TIMESTAMP,\n" +
                                "        \"TIMESTAMP\" UNSIGNED_LONG NOT NULL,\n" +
                                "        SRC_LOCATION VARCHAR,\n" +
                                "        DST_LOCATION VARCHAR,\n" +
                                "        B VARCHAR,\n" +
                                "        C VARCHAR,\n" +
                                "        F UNSIGNED_LONG,\n" +
                                "        G UNSIGNED_LONG,\n" +
                                "        H UNSIGNED_LONG,\n" +
                                "        I UNSIGNED_LONG\n" +
                                "    CONSTRAINT pk PRIMARY KEY (BUCKET, \"TIMESTAMP\"" + (i == 0 ? " DESC" : "") + ", SRC_LOCATION, DST_LOCATION, B, C)\n" +
                        ") SALT_BUCKETS=2, COMPRESSION='GZ', TTL=31622400");
                stmt = conn.prepareStatement("UPSERT INTO " + t[i] + "(BUCKET, \"TIMESTAMP\", SRC_LOCATION, DST_LOCATION, B, C) VALUES(?,?,?,?,?,?)");
                stmt.setString(1, "5SEC");
                stmt.setString(3, "Tr/Bal");
                stmt.setString(4, "Tr/Bal");
                stmt.setString(5, "B1");
                stmt.setString(6, "C1");
                stmt.setLong(2, 1462993520000000000L);
                stmt.execute();
                stmt.setLong(2, 1462993515000000000L);
                stmt.execute();
                stmt.setLong(2, 1462993510000000000L);
                stmt.execute();
                stmt.setLong(2, 1462993505000000000L);
                stmt.execute();
                stmt.setLong(2, 1462993490000000000L);
                stmt.execute();
                stmt.setLong(2, 1462993485000000000L);
                stmt.execute();
                stmt.setLong(2, 1462993480000000000L);
                stmt.execute();
                stmt.setLong(2, 1462993475000000000L);
                stmt.execute();
                stmt.setLong(2, 1462993470000000000L);
                stmt.execute();
                stmt.setLong(2, 1462993430000000000L);
                stmt.execute();
                
                String q =
                        "SELECT C.BUCKET, C.\"TIMESTAMP\" FROM (\n" +
                        "     SELECT E.BUCKET as BUCKET, L.BUCKET as LBUCKET, E.\"TIMESTAMP\" as TIMESTAMP, L.\"TIMESTAMP\" as LTIMESTAMP FROM\n" +
                        "        (SELECT BUCKET, \"TIMESTAMP\"  FROM EVENT_COUNT\n" +
                        "             WHERE BUCKET = '5SEC' AND LOCATION = 'Tr/Bal'\n" +
                        "                 AND \"TIMESTAMP\"  <= 1462993520000000000 AND \"TIMESTAMP\"  > 1462993420000000000\n" +
                        "        ) E\n" +
                        "        JOIN\n" +
                        "         (SELECT BUCKET, \"TIMESTAMP\"  FROM "+ t[i] +"\n" +
                        "             WHERE BUCKET = '5SEC' AND SRC_LOCATION = 'Tr/Bal' AND SRC_LOCATION = DST_LOCATION\n" +
                        "                 AND \"TIMESTAMP\"  <= 1462993520000000000 AND \"TIMESTAMP\"  > 1462993420000000000\n" +
                        "         ) L\n" +
                        "     ON L.BUCKET = E.BUCKET AND L.\"TIMESTAMP\"  = E.\"TIMESTAMP\"\n" +
                        " ) C\n" +
                        " GROUP BY C.BUCKET, C.\"TIMESTAMP\"";
                    
                String p = i == 0 ?
                        "CLIENT PARALLEL 2-WAY SKIP SCAN ON 2 RANGES OVER EVENT_COUNT [0,'5SEC',~1462993520000000000,'Tr/Bal'] - [1,'5SEC',~1462993420000000000,'Tr/Bal']\n" +
                        "    SERVER FILTER BY FIRST KEY ONLY\n" +
                        "    SERVER AGGREGATE INTO DISTINCT ROWS BY [E.BUCKET, \"E.TIMESTAMP\"]\n" +
                        "CLIENT MERGE SORT\n" +
                        "    PARALLEL INNER-JOIN TABLE 0 (SKIP MERGE)\n" +
                        "        CLIENT PARALLEL 2-WAY SKIP SCAN ON 2 RANGES OVER " + t[i] + " [0,'5SEC',~1462993520000000000,'Tr/Bal'] - [1,'5SEC',~1462993420000000000,'Tr/Bal']\n" +
                        "            SERVER FILTER BY FIRST KEY ONLY AND SRC_LOCATION = DST_LOCATION\n" +
                        "        CLIENT MERGE SORT"
                        :
                        "CLIENT PARALLEL 2-WAY SKIP SCAN ON 2 RANGES OVER EVENT_COUNT [0,'5SEC',~1462993520000000000,'Tr/Bal'] - [1,'5SEC',~1462993420000000000,'Tr/Bal']\n" +
                        "    SERVER FILTER BY FIRST KEY ONLY\n" +
                        "    SERVER AGGREGATE INTO DISTINCT ROWS BY [E.BUCKET, \"E.TIMESTAMP\"]\n" +
                        "CLIENT MERGE SORT\n" +
                        "    PARALLEL INNER-JOIN TABLE 0 (SKIP MERGE)\n" +
                        "        CLIENT PARALLEL 2-WAY SKIP SCAN ON 2 RANGES OVER " + t[i] + " [0,'5SEC',1462993420000000001,'Tr/Bal'] - [1,'5SEC',1462993520000000000,'Tr/Bal']\n" +
                        "            SERVER FILTER BY FIRST KEY ONLY AND SRC_LOCATION = DST_LOCATION\n" +
                        "        CLIENT MERGE SORT";
                
                ResultSet rs = conn.createStatement().executeQuery("explain " + q);
                assertEquals(p, QueryUtil.getExplainPlan(rs));
                
                rs = conn.createStatement().executeQuery(q);
                assertTrue(rs.next());
                assertEquals("5SEC", rs.getString(1));
                assertEquals(1462993520000000000L, rs.getLong(2));
                assertTrue(rs.next());
                assertEquals("5SEC", rs.getString(1));
                assertEquals(1462993515000000000L, rs.getLong(2));
                assertTrue(rs.next());
                assertEquals("5SEC", rs.getString(1));
                assertEquals(1462993510000000000L, rs.getLong(2));
                assertTrue(rs.next());
                assertEquals("5SEC", rs.getString(1));
                assertEquals(1462993505000000000L, rs.getLong(2));
                assertTrue(rs.next());
                assertEquals("5SEC", rs.getString(1));
                assertEquals(1462993490000000000L, rs.getLong(2));
                assertTrue(rs.next());
                assertEquals("5SEC", rs.getString(1));
                assertEquals(1462993485000000000L, rs.getLong(2));
                assertTrue(rs.next());
                assertEquals("5SEC", rs.getString(1));
                assertEquals(1462993480000000000L, rs.getLong(2));
                assertTrue(rs.next());
                assertEquals("5SEC", rs.getString(1));
                assertEquals(1462993475000000000L, rs.getLong(2));
                assertTrue(rs.next());
                assertEquals("5SEC", rs.getString(1));
                assertEquals(1462993470000000000L, rs.getLong(2));
                assertTrue(rs.next());
                assertEquals("5SEC", rs.getString(1));
                assertEquals(1462993430000000000L, rs.getLong(2));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testBug2961() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            conn.createStatement().execute("CREATE TABLE test2961 (\n" + 
                    "ACCOUNT_ID VARCHAR NOT NULL,\n" + 
                    "BUCKET_ID VARCHAR NOT NULL,\n" + 
                    "OBJECT_ID VARCHAR NOT NULL,\n" + 
                    "OBJECT_VERSION VARCHAR NOT NULL,\n" + 
                    "LOC VARCHAR,\n" + 
                    "CONSTRAINT PK PRIMARY KEY (ACCOUNT_ID, BUCKET_ID, OBJECT_ID, OBJECT_VERSION DESC))");
            conn.createStatement().execute("UPSERT INTO test2961  (ACCOUNT_ID, BUCKET_ID, OBJECT_ID, OBJECT_VERSION, LOC) VALUES ('acct1', 'bucket1', 'obj1', '1111', 'loc1')");
            ResultSet rs = conn.createStatement().executeQuery("select ACCOUNT_ID, BUCKET_ID, OBJECT_VERSION  from test2961  WHERE ACCOUNT_ID = 'acct1' and BUCKET_ID = 'bucket1' and OBJECT_VERSION = '1111'");
            assertTrue(rs.next());
            rs = conn.createStatement().executeQuery("select ACCOUNT_ID, BUCKET_ID, OBJECT_VERSION  from test2961  WHERE ACCOUNT_ID = 'acct1' and BUCKET_ID = 'bucket1' and OBJECT_ID = 'obj1'");
            assertTrue(rs.next());
            rs = conn.createStatement().executeQuery("select ACCOUNT_ID, BUCKET_ID, OBJECT_VERSION  from test2961  WHERE ACCOUNT_ID = 'acct1' and BUCKET_ID = 'bucket1' and OBJECT_VERSION = '1111'  and OBJECT_ID = 'obj1'");
            assertTrue(rs.next());

            conn.createStatement().execute("UPSERT INTO test2961  (ACCOUNT_ID, BUCKET_ID, OBJECT_ID, OBJECT_VERSION, LOC) VALUES ('acct1', 'bucket1', 'obj1', '2222', 'loc1')");
            rs = conn.createStatement().executeQuery("SELECT  OBJ.ACCOUNT_ID, OBJ.BUCKET_ID, OBJ.OBJECT_ID, OBJ.OBJECT_VERSION, OBJ.LOC "
                    + "FROM ( SELECT ACCOUNT_ID, BUCKET_ID, OBJECT_ID, MAX(OBJECT_VERSION) AS MAXVER"
                    + "       FROM test2961 GROUP BY ACCOUNT_ID, BUCKET_ID, OBJECT_ID) AS X "
                    + "       INNER JOIN test2961 AS OBJ ON X.ACCOUNT_ID = OBJ.ACCOUNT_ID AND X.BUCKET_ID = OBJ.BUCKET_ID AND X.OBJECT_ID = OBJ.OBJECT_ID AND X.MAXVER = OBJ.OBJECT_VERSION");
            assertTrue(rs.next());
            assertEquals("2222", rs.getString(4));
            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("SELECT  OBJ.ACCOUNT_ID, OBJ.BUCKET_ID, OBJ.OBJECT_ID, OBJ.OBJECT_VERSION, OBJ.LOC "
                    + "FROM ( SELECT ACCOUNT_ID, BUCKET_ID, OBJECT_ID, MAX(OBJECT_VERSION) AS MAXVER "
                    + "       FROM test2961 GROUP BY ACCOUNT_ID, BUCKET_ID, OBJECT_ID) AS X "
                    + "       INNER JOIN test2961 AS OBJ ON X.ACCOUNT_ID = OBJ.ACCOUNT_ID AND X.OBJECT_ID = OBJ.OBJECT_ID AND X.MAXVER = OBJ.OBJECT_VERSION");
            assertTrue(rs.next());
            assertEquals("2222", rs.getString(4));
            assertFalse(rs.next());

            rs = conn.createStatement().executeQuery("SELECT  OBJ.ACCOUNT_ID, OBJ.BUCKET_ID, OBJ.OBJECT_ID, OBJ.OBJECT_VERSION, OBJ.LOC "
                    + "FROM ( SELECT ACCOUNT_ID, BUCKET_ID, OBJECT_ID, MAX(OBJECT_VERSION) AS MAXVER "
                    + "       FROM test2961 GROUP BY ACCOUNT_ID, BUCKET_ID, OBJECT_ID) AS X "
                    + "       INNER JOIN test2961 AS OBJ ON X.ACCOUNT_ID = OBJ.ACCOUNT_ID AND X.BUCKET_ID = OBJ.BUCKET_ID AND  X.MAXVER = OBJ.OBJECT_VERSION");
            assertTrue(rs.next());
            assertEquals("2222", rs.getString(4));
            assertFalse(rs.next());

            conn.createStatement().execute("UPSERT INTO test2961  (ACCOUNT_ID, BUCKET_ID, OBJECT_ID, OBJECT_VERSION, LOC) VALUES ('acct1', 'bucket1', 'obj2', '1111', 'loc1')");
            conn.createStatement().execute("UPSERT INTO test2961  (ACCOUNT_ID, BUCKET_ID, OBJECT_ID, OBJECT_VERSION, LOC) VALUES ('acct1', 'bucket1', 'obj3', '1111', 'loc1')");
            String q = "SELECT  OBJ.ACCOUNT_ID, OBJ.BUCKET_ID, OBJ.OBJECT_ID, OBJ.OBJECT_VERSION, OBJ.LOC "
                    + "FROM ( SELECT ACCOUNT_ID, BUCKET_ID, OBJECT_ID, MAX(OBJECT_VERSION) AS MAXVER "
                    + "       FROM test2961 GROUP BY ACCOUNT_ID, BUCKET_ID, OBJECT_ID) AS X "
                    + "       INNER JOIN test2961 AS OBJ ON X.ACCOUNT_ID = OBJ.ACCOUNT_ID AND X.BUCKET_ID = OBJ.BUCKET_ID AND X.OBJECT_ID = OBJ.OBJECT_ID AND  X.MAXVER = OBJ.OBJECT_VERSION";
            rs = conn.createStatement().executeQuery(q);
            assertTrue(rs.next());
            assertEquals("2222", rs.getString(4));
            assertTrue(rs.next());
            assertEquals("1111", rs.getString(4));
            assertTrue(rs.next());
            assertEquals("1111", rs.getString(4));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
