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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PFloat;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @since 1.2
 */


public class SortOrderIT extends ParallelStatsDisabledIT {
    private String baseTableName;
    
    @Before
    public void generateTableName() {
        baseTableName = generateUniqueName();
    }
    
    @Test
    public void noOrder() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (pk VARCHAR NOT NULL PRIMARY KEY)";
        runQueryTest(ddl, "pk", new Object[][]{{"a"}, {"b"}, {"c"}}, new Object[][]{{"a"}, {"b"}, {"c"}},
            table);
    }                                                           

    @Test
    public void noOrderCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid, code))";
        Object[][] rows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        runQueryTest(ddl, upsert("oid", "code"), rows, rows, table);
    }
    
    @Test
    public void ascOrderInlinePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (pk VARCHAR NOT NULL PRIMARY KEY ASC)";
        runQueryTest(ddl, "pk", new Object[][]{{"a"}, {"b"}, {"c"}}, new Object[][]{{"a"}, {"b"}, {"c"}},
            table);
    }
    
    @Test
    public void ascOrderCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid ASC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        Object[][] expectedRows = new Object[][]{{"o1", 3}, {"o1", 2}, {"o1", 1}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, table);
    }

    @Test
    public void descOrderInlinePK() throws Exception {
        String table = generateUniqueName();
        for (String type : new String[]{"CHAR(2)", "VARCHAR"}) {
            String ddl = "CREATE table " + table + " (pk ${type} NOT NULL PRIMARY KEY DESC)".replace("${type}", type);
            runQueryTest(ddl, "pk", new Object[][]{{"aa"}, {"bb"}, {"cc"}}, new Object[][]{{"cc"}, {"bb"}, {"aa"}},
                table);
        }
    }
    
    @Test
    public void descOrderCompositePK1() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        Object[][] expectedRows = new Object[][]{{"o3", 3}, {"o2", 2}, {"o1", 1}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, table);
    }
    
    @Test
    public void descOrderCompositePK2() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        Object[][] expectedRows = new Object[][]{{"o1", 3}, {"o1", 2}, {"o1", 1}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, table);
    }    

    @Test
    public void equalityDescInlinePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (pk VARCHAR NOT NULL PRIMARY KEY DESC)";
        runQueryTest(ddl, upsert("pk"), new Object[][]{{"a"}, {"b"}, {"c"}}, new Object[][]{{"b"}}, new WhereCondition("pk", "=", "'b'"),
            table);
    }
    
    @Test
    public void equalityDescCompositePK1() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, new Object[][]{{"o2", 2}}, new WhereCondition("oid", "=", "'o2'"),
            table);
    }
    
    @Test
    public void equalityDescCompositePK2() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, new Object[][]{{"o1", 2}}, new WhereCondition("code", "=", "2"),
            table);
    }
    
    @Test
    public void inDescCompositePK1() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, new Object[][]{{"o1", 2}}, new WhereCondition("code", "IN", "(2)"),
            table);
    }
    
    @Test
    public void inDescCompositePK2() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, new Object[][]{{"o2", 2}}, new WhereCondition("oid", "IN", "('o2')"),
            table);
    }
    
    @Test
    public void likeDescCompositePK1() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"a1", 1}, {"b2", 2}, {"c3", 3}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, new Object[][]{{"b2", 2}}, new WhereCondition("oid", "LIKE", "('b%')"),
            table);
    }
    
    @Test
    public void likeDescCompositePK2() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code CHAR(2) NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"a1", "11"}, {"b2", "22"}, {"c3", "33"}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, new Object[][]{{"b2", "22"}}, new WhereCondition("code", "LIKE", "('2%')"),
            table);
    }
    
    @Test
    public void greaterThanDescCompositePK3() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o1", 2}, {"o1", 3}};
        Object[][] expectedRows = new Object[][]{{"o1", 2}, {"o1", 1}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, new WhereCondition("code", "<", "3"),
            table);
    }
    
    @Test
    public void substrDescCompositePK1() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(3) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{"ao1", 1}, {"bo2", 2}, {"co3", 3}};
        Object[][] expectedRows = new Object[][]{{"co3", 3}, {"bo2", 2}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, new WhereCondition("SUBSTR(oid, 3, 1)", ">", "'1'"),
            table);
    }
        
    @Test
    public void substrDescCompositePK2() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(4) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{"aaaa", 1}, {"bbbb", 2}, {"cccd", 3}};
        Object[][] expectedRows = new Object[][]{{"cccd", 3}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, new WhereCondition("SUBSTR(oid, 4, 1)", "=", "'d'"),
            table);
    }    
    
    @Test
    public void substrFixedLengthDescPK1() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(3) PRIMARY KEY DESC)";
        Object[][] insertedRows = new Object[][]{{"a"}, {"ab"}};
        Object[][] expectedRows = new Object[][]{{"ab"}, {"a"} };
        runQueryTest(ddl, upsert("oid"), insertedRows, expectedRows, new WhereCondition("SUBSTR(oid, 1, 1)", "=", "'a'"),
            table);
    }
        
    @Test
    public void substrVarLengthDescPK1() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid VARCHAR PRIMARY KEY DESC)";
        Object[][] insertedRows = new Object[][]{{"a"}, {"ab"}};
        Object[][] expectedRows = new Object[][]{{"ab"}, {"a"} };
        runQueryTest(ddl, upsert("oid"), insertedRows, expectedRows, new WhereCondition("SUBSTR(oid, 1, 1)", "=", "'a'"),
            table);
    }
        
    @Test
    public void likeVarLengthDescPK1() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid VARCHAR PRIMARY KEY DESC)";
        Object[][] insertedRows = new Object[][]{{"a"}, {"ab"}};
        Object[][] expectedRows = new Object[][]{{"ab"}, {"a"} };
        runQueryTest(ddl, upsert("oid"), insertedRows, expectedRows, new WhereCondition("oid", "like", "'a%'"),
            table);
    }
        
    @Test
    public void likeFixedLengthDescPK1() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(3) PRIMARY KEY DESC)";
        Object[][] insertedRows = new Object[][]{{"a"}, {"ab"}};
        Object[][] expectedRows = new Object[][]{{"ab"}, {"a"} };
        runQueryTest(ddl, upsert("oid"), insertedRows, expectedRows, new WhereCondition("oid", "like", "'a%'"),
            table);
    }
        
    @Test
    public void decimalRangeDescPK1() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid DECIMAL PRIMARY KEY DESC)";
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES(4.99)");
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES(4.0)");
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES(5.0)");
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES(5.001)");
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES(5.999)");
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES(6.0)");
        conn.createStatement().execute("UPSERT INTO " + table + " VALUES(6.001)");
        conn.commit();
        
        String query = "SELECT * FROM " + table + " WHERE oid >= 5.0 AND oid < 6.0";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertTrue(new BigDecimal("5.999").compareTo(rs.getBigDecimal(1)) == 0);
        assertTrue(rs.next());
        assertTrue(new BigDecimal("5.001").compareTo(rs.getBigDecimal(1)) == 0);
        assertTrue(rs.next());
        assertTrue(new BigDecimal("5.0").compareTo(rs.getBigDecimal(1)) == 0);
        assertFalse(rs.next());
    }
        
    @Test
    public void lTrimDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid VARCHAR NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{" o1 ", 1}, {"  o2", 2}, {"  o3", 3}};
        Object[][] expectedRows = new Object[][]{{"  o2", 2}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, new WhereCondition("LTRIM(oid)", "=", "'o2'"),
            table);
    }
    
    @Test
    public void lPadDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid VARCHAR NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"aaaa", 1}, {"bbbb", 2}, {"cccc", 3}};
        Object[][] expectedRows = new Object[][]{{"bbbb", 2}};
        runQueryTest(ddl, upsert("oid", "code"), insertedRows, expectedRows, new WhereCondition("LPAD(oid, 8, '123')", "=", "'1231bbbb'"),
            table);
    }

    @Test
    public void countDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (oid CHAR(2) NOT NULL, code INTEGER NOT NULL constraint pk primary key (oid DESC, code ASC))";
        Object[][] insertedRows = new Object[][]{{"o1", 1}, {"o2", 2}, {"o3", 3}};
        Object[][] expectedRows = new Object[][]{{3l}};
        runQueryTest(ddl, upsert("oid", "code"), select("COUNT(oid)"), insertedRows, expectedRows,
            table);
    }
    
    @Test
    public void sumDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL " +
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC))";
        Object[][] insertedRows = new Object[][]{{10, bdec(10.2), 21l}, {20, bdec(20.2), 32l}, {30, bdec(30.2), 43l}};
        Object[][] expectedRows = new Object[][]{{60l, bdec(60.6), 96l}};
        runQueryTest(ddl, upsert("n1", "n2", "n3"), select("SUM(n1), SUM(n2), SUM(n3)"), insertedRows, expectedRows,
            table);
    }    
    
    @Test
    public void avgDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL " +
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC))";
        Object[][] insertedRows = new Object[][]{{10, bdec(10.2), 21l}, {20, bdec(20.2), 32l}, {30, bdec(30.2), 43l}};
        Object[][] expectedRows = new Object[][]{{new BigDecimal(bint(2), -1), bdec(20.2), BigDecimal.valueOf(32)}};
        runQueryTest(ddl, upsert("n1", "n2", "n3"), select("AVG(n1), AVG(n2), AVG(n3)"), insertedRows, expectedRows,
            table);
    }
    
    @Test
    public void minDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL " +
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC))";
        Object[][] insertedRows = new Object[][]{{10, bdec(10.2), 21l}, {20, bdec(20.2), 32l}, {30, bdec(30.2), 43l}};
        Object[][] expectedRows = new Object[][]{{10, bdec(10.2), 21l}};
        runQueryTest(ddl, upsert("n1", "n2", "n3"), select("MIN(n1), MIN(n2), MIN(n3)"), insertedRows, expectedRows,
            table);
    }
    
    @Test
    public void maxDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL " +
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC))";
        Object[][] insertedRows = new Object[][]{{10, bdec(10.2), 21l}, {20, bdec(20.2), 32l}, {30, bdec(30.2), 43l}};
        Object[][] expectedRows = new Object[][]{{30, bdec(30.2), 43l}};
        runQueryTest(ddl, upsert("n1", "n2", "n3"), select("MAX(n1), MAX(n2), MAX(n3)"), insertedRows, expectedRows,
            table);
    }
    
    @Test
    public void havingSumDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (name CHAR(1) NOT NULL, code INTEGER NOT NULL " +
            "constraint pk primary key (name DESC, code DESC))";
        Object[][] insertedRows = new Object[][]{{"a", 10}, {"a", 20}, {"b", 100}}; 
        Object[][] expectedRows = new Object[][]{{"a", 30l}};
        runQueryTest(ddl, upsert("name", "code"), select("name", "SUM(code)"), insertedRows, expectedRows, 
            new HavingCondition("name", "SUM(code) = 30"), table);
    }
    
    @Test
    public void queryDescDateWithExplicitOrderBy() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (c1 CHAR(1) NOT NULL, c2 CHAR(1) NOT NULL, d1 \"DATE\" NOT NULL, c3 CHAR(1) NOT NULL " +
            "constraint pk primary key (c1, c2, d1 DESC, c3))";
        Object[] row1 = {"1", "2", date(10, 11, 2001), "3"};
        Object[] row2 = {"1", "2", date(10, 11, 2003), "3"};
        Object[][] insertedRows = new Object[][]{row1, row2};
        runQueryTest(ddl, upsert("c1", "c2", "d1", "c3"), select("c1, c2, d1", "c3"), insertedRows, new Object[][]{row2, row1},
            null, null, new OrderBy("d1", OrderBy.Direction.DESC), table);
    }    
    
    @Test
    public void additionOnDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL, d1 DATE NOT NULL " +
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC, d1 DESC))";
        Object[][] insertedRows = new Object[][]{
            {10, bdec(10.2), 21l, date(1, 10, 2001)}, {20, bdec(20.2), 32l, date(2, 6, 2001)}, {30, bdec(30.2), 43l, date(3, 1, 2001)}};
        Object[][] expectedRows = new Object[][]{
            {31l, bdec(32.2), 46l, date(3, 5, 2001)}, {21l, bdec(22.2), 35l, date(2, 10, 2001)}, {11l, bdec(12.2), 24l, date(1, 14, 2001)}};
        runQueryTest(ddl, upsert("n1", "n2", "n3", "d1"), select("n1+1, n2+2, n3+3", "d1+4"), insertedRows, expectedRows,
            table);
    }
    
    @Test
    public void subtractionOnDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (n1 INTEGER NOT NULL, n2 DECIMAL(10, 2) NOT NULL, n3 BIGINT NOT NULL, d1 DATE NOT NULL " +
            "constraint pk primary key (n1 DESC, n2 DESC, n3 DESC, d1 DESC))";
        Object[][] insertedRows = new Object[][]{
            {10, bdec(10.2), 21l, date(1, 10, 2001)}, {20, bdec(20.2), 32l, date(2, 6, 2001)}, {30, bdec(30.2), 43l, date(3, 10, 2001)}};
        Object[][] expectedRows = new Object[][]{
            {29l, bdec(28.2), 40l, date(3, 6, 2001)}, {19l, bdec(18.2), 29l, date(2, 2, 2001)}, {9l, bdec(8.2), 18l, date(1, 6, 2001)}};
        runQueryTest(ddl, upsert("n1", "n2", "n3", "d1"), select("n1-1, n2-2, n3-3", "d1-4"), insertedRows, expectedRows,
            table);
    }
    
    @Test
    public void lessThanLeadingDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (id INTEGER NOT NULL, \"DATE\" DATE NOT NULL constraint pk primary key (id DESC, \"DATE\"))";
        Object[][] insertedRows = new Object[][]{{1, date(1, 1, 2012)}, {3, date(1, 1, 2013)}, {2, date(1, 1, 2011)}};
        Object[][] expectedRows = new Object[][]{{1, date(1, 1, 2012)}};
        runQueryTest(ddl, upsert("id", "date"), insertedRows, expectedRows, new WhereCondition("id", "<", "2"),
            table);
    }
    
    @Test
    public void lessThanTrailingDescCompositePK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (id INTEGER NOT NULL, \"DATE\" DATE NOT NULL constraint pk primary key (id DESC, \"DATE\"))";
        Object[][] insertedRows = new Object[][]{{1, date(1, 1, 2002)}, {3, date(1, 1, 2003)}, {2, date(1, 1, 2001)}};
        Object[][] expectedRows = new Object[][]{{2, date(1, 1, 2001)}};
        runQueryTest(ddl, upsert("id", "\"DATE\""), insertedRows, expectedRows, new WhereCondition("\"DATE\"", "<", "TO_DATE('02-02-2001','mm-dd-yyyy')"),
            table);
    }
    
    @Test
    public void descVarLengthPK() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (id VARCHAR PRIMARY KEY DESC)";
        Object[][] insertedRows = new Object[][]{{"a"}, {"ab"}, {"abc"}};
        Object[][] expectedRows = new Object[][]{{"abc"}, {"ab"}, {"a"}};
        runQueryTest(ddl, upsert("id"), select("id"), insertedRows, expectedRows,
                null, null, new OrderBy("id", OrderBy.Direction.DESC), table);
    }
    
    @Test
    public void descVarLengthAscPKGT() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (k1 INTEGER NOT NULL, k2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        Object[][] insertedRows = new Object[][]{{0, null}, {1, "a"}, {2, "b"}, {3, "ba"}, {4, "baa"}, {5, "c"}, {6, "d"}};
        Object[][] expectedRows = new Object[][]{{3}, {4}, {5}, {6}};
        runQueryTest(ddl, upsert("k1", "k2"), select("k1"), insertedRows, expectedRows,
                new WhereCondition("k2", ">", "'b'"), null, null, table);
    }
        
    @Test
    public void descVarLengthDescPKGT() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (k1 INTEGER NOT NULL, k2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1, k2 desc))";
        Object[][] insertedRows = new Object[][]{{0, null}, {1, "a"}, {2, "b"}, {3, "ba"}, {4, "baa"}, {5, "c"}, {6, "d"}};
        Object[][] expectedRows = new Object[][]{{3}, {4}, {5}, {6}};
        runQueryTest(ddl, upsert("k1", "k2"), select("k1"), insertedRows, expectedRows,
                new WhereCondition("k2", ">", "'b'"), null, null, table);
    }
        
    @Test
    public void descVarLengthDescPKLTE() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (k1 INTEGER NOT NULL, k2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1, k2 desc))";
        Object[][] insertedRows = new Object[][]{{0, null}, {1, "a"}, {2, "b"}, {3, "ba"}, {4, "bb"}, {5, "bc"}, {6, "bba"}, {7, "c"}};
        Object[][] expectedRows = new Object[][]{{1}, {2}, {3}, {4}};
        runQueryTest(ddl, upsert("k1", "k2"), select("k1"), insertedRows, expectedRows,
                new WhereCondition("k2", "<=", "'bb'"), null, null, table);
    }
        
    @Test
    public void descVarLengthAscPKLTE() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (k1 INTEGER NOT NULL, k2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        Object[][] insertedRows = new Object[][]{{0, null}, {1, "a"}, {2, "b"}, {3, "ba"}, {4, "bb"}, {5, "bc"}, {6, "bba"}, {7, "c"}};
        Object[][] expectedRows = new Object[][]{{1}, {2}, {3}, {4}};
        runQueryTest(ddl, upsert("k1", "k2"), select("k1"), insertedRows, expectedRows,
                new WhereCondition("k2", "<=", "'bb'"), null, null, table);
    }
        
    @Test
    public void varLengthAscLT() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        Object[][] insertedRows = new Object[][]{{"a", ""}, {"b",""}, {"b","a"}};
        Object[][] expectedRows = new Object[][]{{"a"}};
        runQueryTest(ddl, upsert("k1", "k2"), select("k1"), insertedRows, expectedRows,
                new WhereCondition("k1", "<", "'b'"), null, null, table);
    }
        
    @Test
    public void varLengthDescLT() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1 desc, k2))";
        Object[][] insertedRows = new Object[][]{{"a", ""}, {"b",""}, {"b","a"}};
        Object[][] expectedRows = new Object[][]{{"a"}};
        runQueryTest(ddl, upsert("k1", "k2"), select("k1"), insertedRows, expectedRows,
                new WhereCondition("k1", "<", "'b'"), null, null, table);
    }
        
    @Test
    public void varLengthDescGT() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE table " + table + " (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT pk PRIMARY KEY (k1 desc, k2))";
        Object[][] insertedRows = new Object[][]{{"a", ""}, {"b",""}, {"b","a"}, {"ba","a"}};
        Object[][] expectedRows = new Object[][]{{"ba"}};
        runQueryTest(ddl, upsert("k1", "k2"), select("k1"), insertedRows, expectedRows,
                new WhereCondition("k1", ">", "'b'"), null, null, table);
    }
        
   @Test
    public void testNonPKCompare() throws Exception {
        List<Integer> expectedResults = Lists.newArrayList(2,3,4);
        Integer[] saltBuckets = new Integer[] {null,3};
        PDataType[] dataTypes = new PDataType[] {PDecimal.INSTANCE, PDouble.INSTANCE, PFloat.INSTANCE};
        for (Integer saltBucket : saltBuckets) {
            for (PDataType dataType : dataTypes) {
                for (SortOrder sortOrder : SortOrder.values()) {
                    testCompareCompositeKey(saltBucket, dataType, sortOrder, "", expectedResults, "");
                }
            }
        }
    }

    @Test
    public void testSkipScanCompare() throws Exception {
        List<Integer> expectedResults = Lists.newArrayList(2,4);
        List<Integer> rExpectedResults = new ArrayList<>(expectedResults);
        Collections.reverse(rExpectedResults);
        Integer[] saltBuckets = new Integer[] {null,3};
        PDataType[] dataTypes = new PDataType[] {PDecimal.INSTANCE, PDouble.INSTANCE, PFloat.INSTANCE};
        for (Integer saltBucket : saltBuckets) {
            for (PDataType dataType : dataTypes) {
                for (SortOrder sortOrder : SortOrder.values()) {
                    testCompareCompositeKey(saltBucket, dataType, sortOrder, "k1 in (2,4)", expectedResults, "");
                    testCompareCompositeKey(saltBucket, dataType, sortOrder, "k1 in (2,4)", rExpectedResults, "ORDER BY k1 DESC");
                }
            }
        }
    }

    private void testCompareCompositeKey(Integer saltBuckets, PDataType dataType, SortOrder sortOrder, String whereClause, List<Integer> expectedResults, String orderBy) throws SQLException {
        String tableName = "t_" + saltBuckets + "_" + dataType + "_" + sortOrder + "_" + baseTableName;
        String ddl = "create table if not exists " + tableName + " (k1 bigint not null, k2 " + dataType.getSqlTypeName() + (dataType.isFixedWidth() ? " not null" : "") + ", constraint pk primary key (k1,k2 " + sortOrder + "))" + (saltBuckets == null ? "" : (" SALT_BUCKETS= " + saltBuckets));
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        conn.createStatement().execute(ddl);
        if (!dataType.isFixedWidth()) {
            conn.createStatement().execute("upsert into " + tableName + " values (0, null)");
        }
        conn.createStatement().execute("upsert into "  + tableName + " values (1, 0.99)");
        conn.createStatement().execute("upsert into " + tableName + " values (2, 1.01)");
        conn.createStatement().execute("upsert into "  + tableName + " values (3, 2.0)");
        conn.createStatement().execute("upsert into " + tableName + " values (4, 1.001)");
        conn.commit();

        String query = "select k1 from " + tableName + " where " + (whereClause.length() > 0 ? (whereClause + " AND ") : "") + " k2>1.0 " + (orderBy.length() == 0 ? "" : orderBy);
        try {
            ResultSet rs = conn.createStatement().executeQuery(query);

            for (int k : expectedResults) {
                assertTrue (tableName, rs.next());
                assertEquals(tableName, k,rs.getInt(1));
            }

            assertFalse(tableName, rs.next());
        } finally {
            conn.close();
        }
    }

    private void runQueryTest(String ddl, String columnName, Object[][] rows,
        Object[][] expectedRows, String table) throws Exception {
        runQueryTest(ddl, new String[]{columnName}, rows, expectedRows, null, table);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, Object[][] rows,
        Object[][] expectedRows, String table) throws Exception {
        runQueryTest(ddl, columnNames, rows, expectedRows, null, table);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, Object[][] rows, Object[][] expectedRows, WhereCondition condition,
        String table) throws Exception {
        runQueryTest(ddl, columnNames, columnNames, rows, expectedRows, condition, null, null,
            table);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, String[] projections, Object[][] rows, Object[][] expectedRows,
        String table) throws Exception {
        runQueryTest(ddl, columnNames, projections, rows, expectedRows, null, null, null, table);
    }
    
    private void runQueryTest(String ddl, String[] columnNames, String[] projections, Object[][] rows, Object[][] expectedRows, HavingCondition havingCondition,
        String table) throws Exception {
        runQueryTest(ddl, columnNames, projections, rows, expectedRows, null, havingCondition, null,
            table);
    }
    

    private void runQueryTest(String ddl, String[] columnNames, String[] projections, Object[][] rows, Object[][] expectedRows,
        WhereCondition whereCondition, HavingCondition havingCondition, OrderBy orderBy,
        String table)
        throws Exception 
    {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        try {

            conn.setAutoCommit(false);

            createTestTable(getUrl(), ddl);

            String columns = appendColumns(columnNames);
            String placeholders = appendPlaceholders(columnNames);
            String dml = "UPSERT INTO " + table + " (" + columns + ") VALUES(" + placeholders +")";
            PreparedStatement stmt = conn.prepareStatement(dml);

            for (int row = 0; row < rows.length; row++) {
                for (int col = 0; col < rows[row].length; col++) {
                    Object value = rows[row][col];
                    stmt.setObject(col + 1, value);
                }
                stmt.execute();
            }
            conn.commit();
            
            String selectClause = "SELECT " + appendColumns(projections) + " FROM " + table;
            
            for (WhereCondition whereConditionClause : new WhereCondition[]{whereCondition, WhereCondition.reverse(whereCondition)}) {
                String query = WhereCondition.appendWhere(whereConditionClause, selectClause);
                query = HavingCondition.appendHaving(havingCondition, query);
                query = OrderBy.appendOrderBy(orderBy, query);
                runQuery(conn, query, expectedRows);
            }
            
            if (orderBy != null) {
                orderBy = OrderBy.reverse(orderBy);
                String query = WhereCondition.appendWhere(whereCondition, selectClause);
                query = HavingCondition.appendHaving(havingCondition, query);
                query = OrderBy.appendOrderBy(orderBy, query);
                runQuery(conn, query, reverse(expectedRows));
            }
            
        } finally {
            conn.close();
        }
    }
    
    private String appendColumns(String[] columnNames) {
        String appendedColumns = "";
        for (int i = 0; i < columnNames.length; i++) {                
            appendedColumns += columnNames[i];
            if (i < columnNames.length - 1) {
                appendedColumns += ",";
            }
        }
        return appendedColumns;
    }
    
    private String appendPlaceholders(String[] columnNames) {
        String placeholderList = "";
        for (int i = 0; i < columnNames.length; i++) {                
            placeholderList += "?";
            if (i < columnNames.length - 1) {
                placeholderList += ",";
            }
        }
        return placeholderList;        
    }
    
    private static void runQuery(Connection connection, String query, Object[][] expectedValues) throws Exception {
        PreparedStatement stmt = connection.prepareStatement(query);

        ResultSet rs = stmt.executeQuery();
        int rowCounter = 0;
        while (rs.next()) {
            if (rowCounter == expectedValues.length) {
                Assert.assertEquals("Exceeded number of expected rows for query" + query, expectedValues.length, rowCounter+1);
            }
            Object[] cols = new Object[expectedValues[rowCounter].length];
            for (int colCounter = 0; colCounter < expectedValues[rowCounter].length; colCounter++) {
                cols[colCounter] = rs.getObject(colCounter+1);
            }
            Assert.assertArrayEquals("Unexpected result for query " + query, expectedValues[rowCounter], cols);
            rowCounter++;
        }
        Assert.assertEquals("Unexpected number of rows for query " + query, expectedValues.length, rowCounter);
    }
    
    private static Object[][] reverse(Object[][] rows) {
        Object[][] reversedArray = new Object[rows.length][];
        System.arraycopy(rows, 0, reversedArray, 0, rows.length);
        ArrayUtils.reverse(reversedArray);
        return reversedArray;
    }
    
    private static Date date(int month, int day, int year) {
        Calendar cal = new GregorianCalendar();
        cal.set(Calendar.MONTH, month-1);
        cal.set(Calendar.DAY_OF_MONTH, day);
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.HOUR_OF_DAY, 10);
        cal.set(Calendar.MINUTE, 2);
        cal.set(Calendar.SECOND, 5);
        cal.set(Calendar.MILLISECOND, 101);
        Date d = new Date(cal.getTimeInMillis()); 
        return d;
    }
        
    private static String[] upsert(String...args) {
        return args;
    }
    
    private static String[] select(String...args) {
        return args;
    }
    
    private static BigDecimal bdec(double d) {
        return BigDecimal.valueOf(d);
    }
    
    private static BigInteger bint(long l) {
        return BigInteger.valueOf(l);
    }    
    
    private static class WhereCondition {
        final String lhs;
        final String operator;
        final String rhs;
    
        WhereCondition(String lhs, String operator, String rhs) {
            this.lhs = lhs;
            this.operator = operator;
            this.rhs = rhs;
        }
        
        static WhereCondition reverse(WhereCondition whereCondition) {
            
            if (whereCondition == null) {
                return null; 
            }
            
            if (whereCondition.operator.equalsIgnoreCase("IN") || whereCondition.operator.equalsIgnoreCase("LIKE")) {
                return whereCondition;
            } else {
                return new WhereCondition(whereCondition.rhs, whereCondition.getReversedOperator(), whereCondition.lhs);
            }
        }
        
         static String appendWhere(WhereCondition whereCondition, String query) {
             if (whereCondition == null) {
                 return query;
             }
            return query + " WHERE " + whereCondition.lhs + " " + whereCondition.operator + " " + whereCondition.rhs;
        }
        
        private String getReversedOperator() {
            if (operator.equals("<")) {
                return ">";
            } else if (operator.equals(">")) {
                return "<";
            } else if (operator.equals(">=")) {
                return "<=";
            } else if (operator.equals("<=")) {
                return ">=";
            } else {
                return operator;
            }
        }
    }
    
    private static class HavingCondition {
        
        private String groupby;
        private String having;
        
        HavingCondition(String groupby, String having) {
            this.groupby = groupby;
            this.having = having;
        }
        
        static String appendHaving(HavingCondition havingCondition, String query) {
            if (havingCondition == null) {
                return query;
            }
            return query + " GROUP BY " + havingCondition.groupby + " HAVING " + havingCondition.having + " ";
        }
    }
    
    private static class OrderBy {
        
        enum Direction {
            
            ASC, DESC;
            
            Direction reverse() {
                if (this == ASC) {
                    return DESC;
                }
                return ASC;
            }
        }
        
        private List<String> columnNames = Lists.newArrayList();
        private List<Direction> directions = Lists.newArrayList();
        
        OrderBy() {            
        }
        
        OrderBy(String columnName, Direction orderBy) {
            add(columnName, orderBy);
        }
        
        void add(String columnName, Direction direction) {
            columnNames.add(columnName);
            directions.add(direction);
        }
        
        static OrderBy reverse(OrderBy orderBy) {
            
            if (orderBy == null) {
                return null;
            }
            
            List<Direction> reversedDirections = Lists.newArrayList();
            for (Direction dir : orderBy.directions) {
                reversedDirections.add(dir.reverse());
            }
            OrderBy reversedOrderBy = new OrderBy();
            reversedOrderBy.columnNames = orderBy.columnNames;
            reversedOrderBy.directions = reversedDirections;
            return reversedOrderBy;
        }
        
        static String appendOrderBy(OrderBy orderBy, String query) {
            if (orderBy == null || orderBy.columnNames.isEmpty()) {
                return query;
            }
            query += " ORDER BY ";
            for (int i = 0; i < orderBy.columnNames.size(); i++) {
                query += orderBy.columnNames.get(i) + " " + orderBy.directions.get(i).toString() + " ";
            }
            
            query += " LIMIT 1000 ";
            
            return query;
        }        
    }
}
