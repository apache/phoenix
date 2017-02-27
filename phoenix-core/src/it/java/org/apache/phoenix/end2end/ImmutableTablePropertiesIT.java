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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

public class ImmutableTablePropertiesIT extends ParallelStatsDisabledIT {

    @Test
    public void testImmutableKeyword() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String immutableDataTableFullName = SchemaUtil.getTableName("", generateUniqueName());
        String mutableDataTableFullName = SchemaUtil.getTableName("", generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            Statement stmt = conn.createStatement();
            // create table with immutable keyword
            String ddl = "CREATE IMMUTABLE TABLE  " + immutableDataTableFullName +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string)) STORE_NULLS=true";
            stmt.execute(ddl);
            
            // create table without immutable keyword
            ddl = "CREATE TABLE  " + mutableDataTableFullName +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string)) STORE_NULLS=true";
            stmt.execute(ddl);
            
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            PTable immutableTable = phxConn.getTable(new PTableKey(null, immutableDataTableFullName));
            assertTrue("IMMUTABLE_ROWS should be set to true", immutableTable.isImmutableRows());
            PTable mutableTable = phxConn.getTable(new PTableKey(null, mutableDataTableFullName));
            assertFalse("IMMUTABLE_ROWS should be set to false", mutableTable.isImmutableRows());
        } 
    }
    
    @Test
    public void testImmutableProperty() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String immutableDataTableFullName = SchemaUtil.getTableName("", generateUniqueName());
        String mutableDataTableFullName = SchemaUtil.getTableName("", generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            Statement stmt = conn.createStatement();
            // create table with immutable table property set to true
            String ddl = "CREATE TABLE  " + immutableDataTableFullName +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string)) IMMUTABLE_ROWS=true";
            stmt.execute(ddl);
            
            // create table with immutable table property set to false
            ddl = "CREATE TABLE  " + mutableDataTableFullName +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string))  IMMUTABLE_ROWS=false";
            stmt.execute(ddl);
            
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            PTable immutableTable = phxConn.getTable(new PTableKey(null, immutableDataTableFullName));
            assertTrue("IMMUTABLE_ROWS should be set to true", immutableTable.isImmutableRows());
            PTable mutableTable = phxConn.getTable(new PTableKey(null, mutableDataTableFullName));
            assertFalse("IMMUTABLE_ROWS should be set to false", mutableTable.isImmutableRows());
        } 
    }
    
    @Test
    public void testImmutableKeywordAndProperty() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String immutableDataTableFullName = SchemaUtil.getTableName("", generateUniqueName());
        String mutableDataTableFullName = SchemaUtil.getTableName("", generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            Statement stmt = conn.createStatement();
            try {
                // create immutable table with immutable table property set to true 
                String ddl = "CREATE IMMUTABLE TABLE  " + immutableDataTableFullName +
                        "  (a_string varchar not null, col1 integer" +
                        "  CONSTRAINT pk PRIMARY KEY (a_string)) IMMUTABLE_ROWS=true";
                stmt.execute(ddl);
                fail();
            }
            catch (SQLException e) {
                assertEquals(SQLExceptionCode.IMMUTABLE_TABLE_PROPERTY_INVALID.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // create immutable table with immutable table property set to false
                String ddl = "CREATE IMMUTABLE TABLE  " + mutableDataTableFullName +
                        "  (a_string varchar not null, col1 integer" +
                        "  CONSTRAINT pk PRIMARY KEY (a_string))  IMMUTABLE_ROWS=false";
                stmt.execute(ddl);
                fail();
            }
            catch (SQLException e) {
                assertEquals(SQLExceptionCode.IMMUTABLE_TABLE_PROPERTY_INVALID.getErrorCode(), e.getErrorCode());
            }
            
        } 
    }
    
    @Test
    public void testImmutableTableWithStorageSchemeAndColumnEncodingProps() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String immutableDataTableFullName = SchemaUtil.getTableName("", generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            Statement stmt = conn.createStatement();
            try {
                // create immutable table with immutable table property set to true 
                String ddl = "CREATE IMMUTABLE TABLE  " + immutableDataTableFullName +
                        "  (a_string varchar not null, col1 integer" +
                        "  CONSTRAINT pk PRIMARY KEY (a_string)) COLUMN_ENCODED_BYTES=0, IMMUTABLE_STORAGE_SCHEME="
                        + PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
                stmt.execute(ddl);
                fail();
            }
            catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_AND_COLUMN_QUALIFIER_BYTES.getErrorCode(), e.getErrorCode());
            }
        } 
    }
    
    @Test
    public void testAlterImmutableStorageSchemeProp() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String immutableDataTableFullName1 = SchemaUtil.getTableName("", generateUniqueName());
        String immutableDataTableFullName2 = SchemaUtil.getTableName("", generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            Statement stmt = conn.createStatement();
            // create an immutable table with  ONE_CELL_PER_COLUMN storage scheme
            String ddl = "CREATE IMMUTABLE TABLE  " + immutableDataTableFullName1 +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string)) COLUMN_ENCODED_BYTES=0, IMMUTABLE_STORAGE_SCHEME="
                    + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN;
            stmt.execute(ddl);
            // create an immutable table with  SINGLE_CELL_ARRAY_WITH_OFFSETS storage scheme
            ddl = "CREATE IMMUTABLE TABLE  " + immutableDataTableFullName2 +
                    "  (a_string varchar not null, col1 integer" +
                    "  CONSTRAINT pk PRIMARY KEY (a_string)) COLUMN_ENCODED_BYTES=4, IMMUTABLE_STORAGE_SCHEME="
                    + PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
            stmt.execute(ddl);
            
            // changing the storage scheme from/to ONCE_CELL_PER_COLUMN should fail
            try {
                stmt.execute("ALTER TABLE " + immutableDataTableFullName1 + " SET IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS);
                fail();
            }
            catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_CHANGE.getErrorCode(), e.getErrorCode());
            }
            try {
                stmt.execute("ALTER TABLE " + immutableDataTableFullName2 + " SET IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
                fail();
            }
            catch (SQLException e) {
                assertEquals(SQLExceptionCode.INVALID_IMMUTABLE_STORAGE_SCHEME_CHANGE.getErrorCode(), e.getErrorCode());
            }
        } 
    }
    
}
