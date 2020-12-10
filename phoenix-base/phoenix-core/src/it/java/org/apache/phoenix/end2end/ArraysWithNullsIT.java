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

import static org.junit.Assert.assertEquals;

import java.sql.*;

import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Test;

public class ArraysWithNullsIT extends ParallelStatsDisabledIT {

    @Test
    public void testArrayUpsertIntWithNulls() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t1 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t1 + " ( k VARCHAR PRIMARY KEY, a INTEGER[])");

        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + t1 + " VALUES('a',ARRAY[null,3,null])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("Select a from " + t1 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("INTEGER",new Object[]{null,3,null});

        assertEquals(rs.getArray(1),array);
        conn.close();

    }



    @Test
    public void testArrayUpsertVarcharWithNulls() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t2 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t2 + " ( k VARCHAR PRIMARY KEY, a VARCHAR[])");

        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + t2 + " VALUES('a',ARRAY['10',null])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("Select a from " + t2 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("VARCHAR",new Object[]{"10",null});

        assertEquals(rs.getArray(1),array);
        conn.close();

    }

    @Test
    public void testArrayUpsertBigIntWithNulls() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t3 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t3 + " ( k VARCHAR PRIMARY KEY, a BIGINT[])");

        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + t3 + " VALUES('a',ARRAY[2,null,32335,4])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("Select a from " + t3 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("BIGINT",new Object[]{(long)2,null,(long)32335,(long)4});

        assertEquals(rs.getArray(1),array);
        conn.close();

    }

    @Test
    public void testArrayUpsertFloatWithNulls() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t4 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t4 + " ( k VARCHAR PRIMARY KEY, a FLOAT[])");

        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + t4 + " VALUES('a',ARRAY[1.1,2.2,null,3.4])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("Select a from " + t4 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("FLOAT",new Object[]{(float)1.1,(float)2.2,null,(float)3.4});

        assertEquals(rs.getArray(1),array);
        conn.close();

    }

    @Test
    public void testArrayUpsertSmallIntWithNulls() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t5 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t5 + " ( k VARCHAR PRIMARY KEY, a SMALLINT[])");

        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + t5 + " VALUES('a',ARRAY[123,456,null,456])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("Select a from " + t5 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("SMALLINT",new Object[]{(short)123,(short)456,null,(short)456});

        assertEquals(rs.getArray(1),array);
        conn.close();

    }

    @Test
    public void testArrayUpsertTinyIntWithNulls() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t6 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t6 + " ( k VARCHAR PRIMARY KEY, a TINYINT[])");

        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + t6 + " VALUES('a',ARRAY[123,45,null,45])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("Select a from " + t6 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("TINYINT",new Object[]{(byte)123,(byte)45,null,(byte)45});

        assertEquals(rs.getArray(1),array);
        conn.close();

    }

    @Test
    public void testArrayUpsertBooleanWithNulls() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t7 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t7 + " ( k VARCHAR PRIMARY KEY, a BOOLEAN[])");

        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + t7 + " VALUES('a',ARRAY[true,false,null,true])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("Select a from " + t7 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("BOOLEAN",new Object[]{true,false,null,true});

        assertEquals(rs.getArray(1),array);
        conn.close();

    }

    @Test
    public void testArrayUpsertDoubleWithNulls() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t8 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t8 + " ( k VARCHAR PRIMARY KEY, a DOUBLE[])");

        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + t8 + " VALUES('a',ARRAY[1.2,2.3,null,3.4])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("Select a from " + t8 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("DOUBLE",new Object[]{1.2,2.3,null,3.4});

        assertEquals(rs.getArray(1),array);
        conn.close();

    }

    @Test
    public void testArrayUpsertDateWithNulls1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t9 = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + t9 + " ( k VARCHAR PRIMARY KEY, a DATE[])");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + t9
            + " VALUES('a',ARRAY[TO_DATE('2015-05-20 06:12:14.184'),null,TO_DATE('2015-05-20 06:12:14.184'),null])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("Select a from " + t9 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("DATE",new Date[]{new Date(1432102334184l),new Date(0l),new Date(1432102334184l),new Date(0l)});

        assertEquals(rs.getArray(1),array);
        conn.close();
    }

    @Test
    public void testArrayUpsertDateWithNulls2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t10 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t10 + " ( k VARCHAR PRIMARY KEY, a DATE[])");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + t10
            + " VALUES('a',ARRAY[TO_DATE('1970-01-01 00:00:00.000'), TO_DATE('2015-05-20 06:12:14.184'),TO_DATE('2015-05-20 06:12:14.184')])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "Select a from " + t10 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("DATE",new Date[]{new Date(0l), new Date(1432102334184l), new Date(1432102334184l)});

        assertEquals(rs.getArray(1),array);
        conn.close();
    }

    @Test
    public void testArrayUpsertTimeWithNulls1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t11 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t11 + " ( k VARCHAR PRIMARY KEY, a TIME[])");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + t11
            + " VALUES('a',ARRAY[TO_TIME('2015-05-20 06:12:14.184'),null,TO_TIME('2015-05-20 06:12:14.184'),null])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "Select a from " + t11 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("TIME",new Time[]{new Time(1432102334184l),new Time(0l),new Time(1432102334184l),new Time(0l)});

        assertEquals(rs.getArray(1),array);
        conn.close();
    }

    @Test
    public void testArrayUpsertTimeWithNulls2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t12 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t12 + " ( k VARCHAR PRIMARY KEY, a TIME[])");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + t12
            + " VALUES('a',ARRAY[TO_TIME('1970-01-01 00:00:00.000'), TO_TIME('2015-05-20 06:12:14.184'),null,TO_TIME('2015-05-20 06:12:14.184'),null])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "Select a from " + t12 + " where k = 'a'");
        rs.next();
        Array array = conn.createArrayOf("TIME",new Time[]{new Time(0l),new Time(1432102334184l),new Time(0l),new Time(1432102334184l),new Time(0l)});

        assertEquals(rs.getArray(1),array);
        conn.close();
    }

    @Test
    public void testArrayUpsertTimeStampWithNulls1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t13 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t13 + " ( k VARCHAR PRIMARY KEY, a TIMESTAMP[])");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + t13
            + " VALUES('a',ARRAY[TO_TIMESTAMP('2015-05-20 06:12:14.184'),null,TO_TIMESTAMP('2015-05-20 06:12:14.184'),TO_TIMESTAMP('1970-01-01 00:00:00.000')])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "Select a from " + t13 + " where k = 'a'");
        rs.next();

        assertEquals(rs.getArray(1),conn.createArrayOf("TIMESTAMP",new Timestamp[]{new Timestamp(1432102334184l),new Timestamp(0l),new Timestamp(1432102334184l),new Timestamp(0l)}));
        conn.close();
    }

    @Test
    public void testArrayUpsertTimeStampWithNulls2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t14 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t14 + " ( k VARCHAR PRIMARY KEY, a TIMESTAMP[])");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + t14
            + " VALUES('a',ARRAY[TO_TIMESTAMP('1970-01-01 00:00:00.000'),TO_TIMESTAMP('2015-05-20 06:12:14.184'),TO_TIMESTAMP('1970-01-01 00:00:00.000'),TO_TIMESTAMP('2015-05-20 06:12:14.184'),TO_TIMESTAMP('1970-01-01 00:00:00.000')])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "Select a from " + t14 + " where k = 'a'");
        rs.next();

        assertEquals(rs.getArray(1),conn.createArrayOf("TIMESTAMP",new Timestamp[]{new Timestamp(0l),new Timestamp(1432102334184l),new Timestamp(0l),new Timestamp(1432102334184l),new Timestamp(0l)}));
        conn.close();
    }

    @Test
    public void testArrayUpsertCharWithNulls1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t15 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t15 + " ( k VARCHAR PRIMARY KEY, a CHAR(15)[])");
        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + t15 + " VALUES('a',ARRAY['foo',null,'fo','foo'])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "Select a from " + t15 + " where k = 'a'");
        rs.next();

        assertEquals(rs.getArray(1),conn.createArrayOf("CHAR",new String[]{"foo","","fo","foo"}));
        conn.close();
    }

    @Test
    public void testArrayUpsertCharWithNulls2() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String t16 = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + t16 + " ( k VARCHAR PRIMARY KEY, a CHAR(15)[])");
        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + t16 + " VALUES('a',ARRAY[null,'foo',null,'fo','foo'])");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery(
            "Select a from " + t16 + " where k = 'a'");
        rs.next();

        assertEquals(rs.getArray(1),conn.createArrayOf("CHAR",new String[]{"","foo","","fo","foo"}));
        conn.close();
    }
}
