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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Basic tests for Phoenix dynamic upserting
 * 
 * 
 * @since 1.3
 */


public class DynamicUpsertIT extends BaseClientManagedTimeIT {

    private static final String TABLE = "DynamicUpserts";
    //private static final byte[] TABLE_BYTES = Bytes.toBytes(TABLE);

    @BeforeClass
    public static void doBeforeTestSetup() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "create table if not exists  " + TABLE + "   (entry varchar not null primary key,"
                + "    a.dummy varchar," + "    b.dummy varchar)";
        conn.createStatement().execute(ddl);
        conn.close();
    }

    /**
     * Test a simple upsert with a dynamic Column
     */
    @Test
    public void testUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE
                + " (entry, a.DynCol VARCHAR,a.dummy) VALUES ('dynEntry','DynValue','DynColValue')";
        String selectquery = "SELECT DynCol FROM " + TABLE + " (a.DynCol VARCHAR) where entry='dynEntry'";
        // String selectquery = "SELECT * FROM "+TABLE;

        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            int rowsInserted = statement.executeUpdate();
            assertEquals(1, rowsInserted);

            // since the upsert does not alter the schema check with a dynamicolumn
            PreparedStatement selectStatement = conn.prepareStatement(selectquery);
            ResultSet rs = selectStatement.executeQuery();
            assertTrue(rs.next());
            assertEquals("DynValue", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of multiple dynamic Columns
     */
    @Test
    public void testMultiUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE
                + " (entry, a.DynColA VARCHAR,b.DynColB varchar) VALUES('dynEntry','DynColValuea','DynColValueb')";
        String selectquery = "SELECT DynColA,entry,DynColB FROM " + TABLE
                + " (a.DynColA VARCHAR,b.DynColB VARCHAR) where entry='dynEntry'";

        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            int rowsInserted = statement.executeUpdate();
            assertEquals(1, rowsInserted);

            // since the upsert does not alter the schema check with a dynamicolumn
            statement = conn.prepareStatement(selectquery);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("DynColValuea", rs.getString(1));
            assertEquals("dynEntry", rs.getString(2));
            assertEquals("DynColValueb", rs.getString(3));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of a full row with dynamic Columns
     */
    @Test
    public void testFullUpsert() throws Exception {
        String upsertquery = "UPSERT INTO "
                + TABLE
                + " (a.DynColA VARCHAR,b.DynColB varchar) VALUES('dynEntry','aValue','bValue','DynColValuea','DynColValueb')";
        String selectquery = "SELECT entry,DynColA,a.dummy,DynColB,b.dummy FROM " + TABLE
                + " (a.DynColA VARCHAR,b.DynColB VARCHAR) where entry='dynEntry'";

        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            int rowsInserted = statement.executeUpdate();
            assertEquals(1, rowsInserted);

            // since the upsert does not alter the schema check with a dynamicolumn
            statement = conn.prepareStatement(selectquery);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("dynEntry", rs.getString(1));
            assertEquals("DynColValuea", rs.getString(2));
            assertEquals("aValue", rs.getString(3));
            assertEquals("DynColValueb", rs.getString(4));
            assertEquals("bValue", rs.getString(5));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of a full row with dynamic Columns and unbalanced number of values
     */
    @Test
    public void testFullUnbalancedUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE
                + " (a.DynCol VARCHAR,b.DynCol varchar) VALUES('dynEntry','aValue','bValue','dyncola')";

        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.execute();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.UPSERT_COLUMN_NUMBERS_MISMATCH.getErrorCode(),e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of prexisting schema defined columns and dynamic ones with different datatypes
     */
    @Test(expected = ColumnAlreadyExistsException.class)
    public void testAmbiguousStaticUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE + " (a.dummy INTEGER,b.dummy INTEGER) VALUES(1,2)";
        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.execute();
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of two conflicting dynamic columns
     */
    @Test(expected = ColumnAlreadyExistsException.class)
    public void testAmbiguousDynamicUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE + " (a.DynCol VARCHAR,a.DynCol INTEGER) VALUES('dynCol',1)";
        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.execute();
        } finally {
            conn.close();
        }
    }

    /**
     * Test an upsert of an undefined ColumnFamily dynamic columns
     */
    @Test(expected = ColumnFamilyNotFoundException.class)
    public void testFakeCFDynamicUpsert() throws Exception {
        String upsertquery = "UPSERT INTO " + TABLE + " (fakecf.DynCol VARCHAR) VALUES('dynCol')";
        String url = getUrl() + ";";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(upsertquery);
            statement.execute();
        } finally {
            conn.close();
        }
    }
}
