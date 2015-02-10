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

package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PDataType;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class PhoenixRuntimeTest extends BaseConnectionlessQueryTest {
    @Test
    public void testParseArguments_MinimalCase() {
        PhoenixRuntime.ExecutionCommand execCmd = PhoenixRuntime.ExecutionCommand.parseArgs(
                new String[] { "localhost", "test.csv" });


        assertEquals(
                "localhost",
                execCmd.getConnectionString());

        assertEquals(
                ImmutableList.of("test.csv"),
                execCmd.getInputFiles());

        assertEquals(',', execCmd.getFieldDelimiter());
        assertEquals('"', execCmd.getQuoteCharacter());
        assertNull(execCmd.getEscapeCharacter());

        assertNull(execCmd.getTableName());

        assertNull(execCmd.getColumns());

        assertFalse(execCmd.isStrict());

        assertEquals(
                CSVCommonsLoader.DEFAULT_ARRAY_ELEMENT_SEPARATOR,
                execCmd.getArrayElementSeparator());
    }

    @Test
    public void testParseArguments_FullOption() {
        PhoenixRuntime.ExecutionCommand execCmd = PhoenixRuntime.ExecutionCommand.parseArgs(
                new String[] { "-t", "mytable", "myzkhost:2181",  "--strict", "file1.sql",
                        "test.csv", "file2.sql", "--header", "one, two,three", "-a", "!", "-d",
                        ":", "-q", "3", "-e", "4" });

        assertEquals("myzkhost:2181", execCmd.getConnectionString());

        assertEquals(ImmutableList.of("file1.sql", "test.csv", "file2.sql"),
                execCmd.getInputFiles());

        assertEquals(':', execCmd.getFieldDelimiter());
        assertEquals('3', execCmd.getQuoteCharacter());
        assertEquals(Character.valueOf('4'), execCmd.getEscapeCharacter());

        assertEquals("mytable", execCmd.getTableName());

        assertEquals(ImmutableList.of("one", "two", "three"), execCmd.getColumns());
        assertTrue(execCmd.isStrict());
        assertEquals("!", execCmd.getArrayElementSeparator());
    }
    
    @Test
    public void testGetPkColsDataTypes() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl(), new Properties());
        int i = 0;
        PDataType[] pTypes = PDataType.values();
        int size = pTypes.length;
        StringBuilder sb = null;
        try {
            for (i = 0 ; i < size; i++) {
                PDataType pType = pTypes[i];
                String sqlTypeName = pType.getSqlTypeName();
                if (sqlTypeName.equalsIgnoreCase("VARBINARY ARRAY")) {
                    // we don't support VARBINARY ARRAYS yet
                    // JIRA - https://issues.apache.org/jira/browse/PHOENIX-1329
                    continue;
                }
                if (pType.isArrayType() && PDataType.arrayBaseType(pType).isFixedWidth() && PDataType.arrayBaseType(pType).getByteSize() == null) {
                    // Need to treat array type whose base type is of fixed width whose byte size is not known as a special case. 
                    // Cannot just use the sql type name returned by PDataType.getSqlTypeName().
                    String baseTypeName = PDataType.arrayBaseType(pType).getSqlTypeName();
                    sqlTypeName = baseTypeName + "(15)" + " " + PDataType.ARRAY_TYPE_SUFFIX;
                } else if (pType.isFixedWidth() && pType.getByteSize() == null) {
                    sqlTypeName = sqlTypeName + "(15)";
                }
                String columnName = "col" + i;
                String tableName = "t" + i;
                
                sb = new StringBuilder(100);
                
                // create a table by using the type name as returned by PDataType
                sb.append("CREATE TABLE " + tableName + " (");
                sb.append(columnName + " " + sqlTypeName + " NOT NULL PRIMARY KEY, V1 VARCHAR)");
                conn.createStatement().execute(sb.toString());

                // generate the optimized query plan by going through the pk of the table.
                PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + tableName + " WHERE " + columnName  + " = ?");
                Integer maxLength = pType.isFixedWidth() && pType.getByteSize() == null ? 15 : null;
                stmt.setObject(1, pType.getSampleValue(maxLength));
                QueryPlan plan = PhoenixRuntime.getOptimizedQueryPlan(stmt);

                // now go through the utility method, get column name and type name and
                // try creating another table with the returned info. Use the query plan generated above.
                // If table can be created with the returned sql type name, then great!
                // It would mean "Roundtrip" of column data type name works.
                List<Pair<String, String>> pkCols = new ArrayList<Pair<String, String>>();
                List<String> dataTypes = new ArrayList<String>();
                PhoenixRuntime.getPkColsDataTypesForSql(pkCols, dataTypes, plan, conn, true);

                tableName = "newt" + i;
                columnName = "newCol" + i;
                String roundTripSqlTypeName = dataTypes.get(0);

                // create a table by using the type name as returned by the utility method
                sb = new StringBuilder(100);
                sb.append("CREATE TABLE " + tableName + " (");
                sb.append(columnName + " " + roundTripSqlTypeName + " NOT NULL PRIMARY KEY)");
                conn.createStatement().execute(sb.toString());
            }
        } catch (Exception e) {
            fail("Failed sql: " + sb.toString() + ExceptionUtils.getStackTrace(e));
        }
    }
    
    @Test
    public void testGetTenantIdExpression() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        Expression e1 = PhoenixRuntime.getTenantIdExpression(conn, PhoenixDatabaseMetaData.SYSTEM_STATS_NAME);
        assertNull(e1);
        Expression e2 = PhoenixRuntime.getTenantIdExpression(conn, PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME);
        assertNotNull(e2);

        Expression e3 = PhoenixRuntime.getTenantIdExpression(conn, PhoenixDatabaseMetaData.SEQUENCE_FULLNAME);
        assertNotNull(e3);
        
        conn.createStatement().execute("CREATE TABLE FOO (k VARCHAR PRIMARY KEY)");
        Expression e4 = PhoenixRuntime.getTenantIdExpression(conn, "FOO");
        assertNull(e4);
        
        conn.createStatement().execute("CREATE TABLE A.BAR (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT PK PRIMARY KEY(K1,K2)) MULTI_TENANT=true");
        Expression e5 = PhoenixRuntime.getTenantIdExpression(conn, "A.BAR");
        assertNotNull(e5);

        conn.createStatement().execute("CREATE INDEX I1 ON A.BAR (K2)");
        Expression e5A = PhoenixRuntime.getTenantIdExpression(conn, "A.I1");
        assertNotNull(e5A);
        
        conn.createStatement().execute("CREATE TABLE BAS (k1 VARCHAR NOT NULL, k2 VARCHAR, CONSTRAINT PK PRIMARY KEY(K1,K2)) MULTI_TENANT=true, SALT_BUCKETS=3");
        Expression e6 = PhoenixRuntime.getTenantIdExpression(conn, "BAS");
        assertNotNull(e6);
        
        conn.createStatement().execute("CREATE INDEX I2 ON BAS (K2)");
        Expression e6A = PhoenixRuntime.getTenantIdExpression(conn, "I2");
        assertNotNull(e6A);
        
        try {
            PhoenixRuntime.getTenantIdExpression(conn, "NOT.ATABLE");
            fail();
        } catch (TableNotFoundException e) {
            // Expected
        }
        
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "t1");
        Connection tsconn = DriverManager.getConnection(getUrl(), props);
        tsconn.createStatement().execute("CREATE VIEW V(V1 VARCHAR) AS SELECT * FROM BAS");
        Expression e7 = PhoenixRuntime.getTenantIdExpression(tsconn, "V");
        assertNotNull(e7);
        tsconn.createStatement().execute("CREATE LOCAL INDEX I3 ON V (V1)");
        try {
            PhoenixRuntime.getTenantIdExpression(tsconn, "I3");
            fail();
        } catch (SQLFeatureNotSupportedException e) {
            // Expected
        }
    }
}
