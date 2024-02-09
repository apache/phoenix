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
package org.apache.phoenix.schema.tool;

import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.end2end.ParallelStatsEnabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.ParseException;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

@Category(ParallelStatsEnabledTest.class)
public class SchemaToolExtractionIT extends ParallelStatsEnabledIT {

    @BeforeClass
    public static synchronized void setup() throws Exception {
        Map<String, String> props = Collections.emptyMap();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testCreateTableStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStmt = "CREATE TABLE "+ pTableFullName + "(K VARCHAR NOT NULL PRIMARY KEY, "
                + "V1 VARCHAR, V2 VARCHAR) TTL=2592000, IMMUTABLE_ROWS=TRUE, DISABLE_WAL=TRUE";
        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStmt);
        String result = runSchemaExtractionTool(schemaName, tableName, null, queries);
        Assert.assertEquals(createTableStmt, result.toUpperCase());
    }

    @Test
    public void testCreateTableStatementLowerCase() throws Exception {
        String tableName = "lowecasetbl1";
        String schemaName = "lowecaseschemaname1";
        String pTableFullName = SchemaUtil.getEscapedTableName(schemaName, tableName);
        String createTableStmt = "CREATE TABLE "+ pTableFullName + "(\"smallK\" VARCHAR NOT NULL PRIMARY KEY, "
                + "\"asd\".V1 VARCHAR, \"foo\".\"bar\" VARCHAR) TTL=2592000, IMMUTABLE_ROWS=true, DISABLE_WAL=true";
        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStmt);
        String result = runSchemaExtractionTool("\"" + schemaName + "\"", "\"" + tableName + "\"", null, queries);
        Assert.assertEquals(createTableStmt, result);
    }

    @Test
    public void testCreateIndexStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        String indexName1 = generateUniqueName();
        String indexName2 = generateUniqueName();
        String indexName3 = generateUniqueName();
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStatement = "CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, \"v1\" VARCHAR, v2 VARCHAR)"
                + properties;
        //FIXME never verified
        String createIndexStatement = "CREATE INDEX "+indexName + " ON "+pTableFullName+"(\"v1\" DESC) INCLUDE (v2)";
        //FIXME never verified
        String createIndexStatement1 = "CREATE INDEX "+indexName1 + " ON "+pTableFullName+"(v2 DESC) INCLUDE (\"v1\")";
        String createIndexStatement2 = "CREATE INDEX "+indexName2 + " ON "+pTableFullName+"(k)";
        String createIndexStatement3 ="CREATE INDEX " + indexName3 + " ON " + pTableFullName +
                "('QUOTED' || \"v1\" || V2 DESC, \"v1\" DESC, K) INCLUDE (V2)";
        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStatement);
        queries.add(createIndexStatement);
        queries.add(createIndexStatement1);
        queries.add(createIndexStatement2);

        String result = runSchemaExtractionTool(schemaName, indexName2, null, queries);
        Assert.assertEquals(createIndexStatement2.toUpperCase(), result.toUpperCase());

        List<String> queries3 = new ArrayList<String>(){};
        queries3.add(createIndexStatement3);

        String result3 = runSchemaExtractionTool(schemaName, indexName3, null, queries3);
        Assert.assertEquals(createIndexStatement3, result3);
    }

    @Test
    public void testDDLsWithDefaults() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        String properties = "COLUMN_ENCODED_BYTES=4";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String pIndexFullName = SchemaUtil.getQualifiedTableName(schemaName, indexName);
        String createTableStatement = "CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)";
        String createIndexStatement = "CREATE INDEX "+indexName + " ON "+pTableFullName+"(v1 DESC) INCLUDE (v2)" + properties;

        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStatement);
        queries.add(createIndexStatement);
        try (PhoenixConnection conn = DriverManager.getConnection(getUrl(), props)
                .unwrap(PhoenixConnection.class)) {
            executeCreateStatements(conn, queries);
            PTable pData = conn.getTable(pTableFullName);
            PTable pIndex = conn.getTable(pIndexFullName);
            SchemaExtractionProcessor schemaExtractionProcessor = new SchemaExtractionProcessor(null, config, pData, true);
            String tableDDL = schemaExtractionProcessor.process();
            assertTrue(tableDDL.contains("IMMUTABLE_STORAGE_SCHEME"));
            SchemaExtractionProcessor schemaExtractionProcessorIndex = new SchemaExtractionProcessor(null, config, pIndex, true);
            String indexDDL = schemaExtractionProcessorIndex.process();
            assertTrue(indexDDL.contains("IMMUTABLE_STORAGE_SCHEME"));
            assertTrue(indexDDL.contains("ENCODING_SCHEME='FOUR_BYTE_QUALIFIERS'"));
        }
    }

    @Test
    public void testCreateLocalIndexStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        String indexName2 = generateUniqueName();
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStatement = "CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                + properties;
        String createIndexStatement = "CREATE LOCAL INDEX "+indexName + " ON "+pTableFullName+"(v1 DESC, k) INCLUDE (v2)";
        String createIndexStatement2 = "CREATE LOCAL INDEX "+indexName2 + " ON "+pTableFullName+"( LPAD(v1,10) DESC, k) INCLUDE (v2)";

        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStatement);
        queries.add(createIndexStatement);

        String result = runSchemaExtractionTool(schemaName, indexName, null, queries);
        Assert.assertEquals(createIndexStatement.toUpperCase(), result.toUpperCase());

        List<String> queries2 = new ArrayList<String>(){};
        queries2.add(createIndexStatement2);

        String result2 = runSchemaExtractionTool(schemaName, indexName2, null, queries2);
        Assert.assertEquals(createIndexStatement2.toUpperCase(), result2.toUpperCase());
    }

    @Test
    public void testCreateLocalIndexStatementLowerCase() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStatement = "CREATE TABLE "+pTableFullName + "(K VARCHAR NOT NULL PRIMARY KEY, \"v1\" VARCHAR, V2 VARCHAR)"
                + properties;
        String createIndexStatement = "CREATE LOCAL INDEX "+indexName + " ON "+pTableFullName+"( LPAD(\"v1\",10) DESC, K) INCLUDE (V2)";

        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStatement);
        queries.add(createIndexStatement);

        String result = runSchemaExtractionTool(schemaName, indexName, null, queries);
        Assert.assertEquals(createIndexStatement, result);
    }

    @Test
    public void testCreateIndexStatementLowerCase() throws Exception {
        String tableName = "lowercase" + generateUniqueName();
        String schemaName = "lowercase" + generateUniqueName();
        String indexName = "\"lowercaseIND" + generateUniqueName() + "\"";
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";
        String pTableFullName = SchemaUtil.getEscapedTableName(schemaName, tableName);
        String createTableStatement = "CREATE TABLE " + pTableFullName + "(\"k\" VARCHAR NOT NULL PRIMARY KEY, \"a\".V1 VARCHAR, \"v2\" VARCHAR)"
                + properties;
        String createIndexStatement = "CREATE INDEX " + indexName + " ON "+ pTableFullName + "(\"a\".V1 DESC, \"k\") INCLUDE (\"v2\")";
        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStatement);
        queries.add(createIndexStatement);

        String result = runSchemaExtractionTool("\"" + schemaName + "\"",  indexName, null, queries);
        Assert.assertEquals(createIndexStatement, result);
    }

    @Test
    public void testCreateIndexStatementLowerCaseCombined() throws Exception {
        String tableName = "lowercase" + generateUniqueName();
        String schemaName = "lowercase" + generateUniqueName();
        String indexName = "\"lowercaseIND" + generateUniqueName() + "\"";
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";
        String pTableFullName = SchemaUtil.getEscapedTableName(schemaName, tableName);
        String createTableStatement = "CREATE TABLE " + pTableFullName + "(ID varchar primary key, \"number\" integer, \"currency\" decimal(6,2), lista varchar[])"
                + properties;
        String createIndexStatement = "CREATE INDEX " + indexName + " ON "+ pTableFullName + "(\"number\" * \"currency\", ID) INCLUDE (LISTA)";
        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStatement);
        queries.add(createIndexStatement);

        String result = runSchemaExtractionTool("\"" + schemaName + "\"",  indexName, null, queries);
        Assert.assertEquals(createIndexStatement, result);
    }

    @Test
    public void testCreateViewStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String viewName = generateUniqueName();
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";

        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStmt = "CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, "
                + "v1 VARCHAR, v2 VARCHAR)"
                + properties;
        String viewFullName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
        String createView = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, "
                + "id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) "
                + "AS SELECT * FROM "+pTableFullName;

        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStmt);
        queries.add(createView);
        String result = runSchemaExtractionTool(schemaName, viewName, null, queries);
        Assert.assertEquals(createView.toUpperCase(), result.toUpperCase());

    }

    @Test
    public void testCreateViewStatementLowerCase() throws Exception {
        String tableName = "lowercase" + generateUniqueName();
        String schemaName = "lowercase" + generateUniqueName();
        String viewName = "lowercase" + generateUniqueName();
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";

        String pTableFullName = SchemaUtil.getEscapedTableName(schemaName, tableName);
        String createTableStmt = "CREATE TABLE "+pTableFullName + "(\"k\" BIGINT NOT NULL PRIMARY KEY, "
                + "\"a\".V1 VARCHAR, v2 VARCHAR)"
                + properties;
        String viewFullName = SchemaUtil.getEscapedTableName(schemaName, viewName);
        String createView = "CREATE VIEW "+viewFullName + "(ID1 BIGINT, \"id2\" BIGINT NOT NULL, "
                + "ID3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (\"id2\", ID3 DESC)) "
                + "AS SELECT * FROM " + pTableFullName + " WHERE \"k\" > 3";

        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStmt);
        queries.add(createView);
        String result = runSchemaExtractionTool("\"" + schemaName + "\"", "\"" + viewName + "\"", null, queries);
        Assert.assertEquals(createView, result);
    }

    @Test
    public void testCreateViewStatement_customName() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String viewName = generateUniqueName()+"@@";
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";

        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStmt = "CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, "
                + "v1 VARCHAR, v2 VARCHAR)"
                + properties;
        String viewFullName = SchemaUtil.getPTableFullNameWithQuotes(schemaName, viewName);

        String createView = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, "
                + "id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) "
                + "AS SELECT * FROM "+pTableFullName;

        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStmt);
        queries.add(createView);
        String result = runSchemaExtractionTool(schemaName, viewName, null, queries);
        Assert.assertEquals(createView.toUpperCase(), result.toUpperCase());

    }

    @Test
    public void testCreateViewIndexStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String viewName = generateUniqueName();
        String childView = generateUniqueName();
        String indexName = generateUniqueName();
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStmt = "CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, "
                + "v1 VARCHAR, v2 VARCHAR)";
        String viewFullName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
        String childviewName = SchemaUtil.getQualifiedTableName(schemaName, childView);
        String createView = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, "
                + "id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) "
                + "AS SELECT * FROM "+pTableFullName;
        String createView1 = "CREATE VIEW "+childviewName + " AS SELECT * FROM "+viewFullName;
        String createIndexStatement = "CREATE INDEX "+indexName + " ON "+childviewName+"(id2, id1) INCLUDE (v1)";
        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStmt);
        queries.add(createView);
        queries.add(createView1);
        queries.add(createIndexStatement);
        String expected = "CREATE INDEX %s ON " +childviewName +"(ID2, ID1, K, ID3 DESC) INCLUDE (V1)";
        String result = runSchemaExtractionTool(schemaName, indexName, null, queries);
        Assert.assertEquals(String.format(expected, indexName).toUpperCase(), result.toUpperCase());
        queries.clear();
        String newIndex =indexName+"_NEW";
        queries.add(String.format(expected, newIndex));
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            executeCreateStatements(conn, queries);
        }
        compareOrdinalPositions(indexName, newIndex);
    }

    private void compareOrdinalPositions(String table, String newTable) throws SQLException {
        String ordinalQuery = "SELECT COLUMN_NAME, "
                + "ORDINAL_POSITION FROM SYSTEM.CATALOG"
                + " WHERE TABLE_NAME='%s' AND ORDINAL_POSITION IS NOT NULL ORDER BY COLUMN_NAME";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Map<String, Integer> ordinalMap = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            ResultSet rs = conn.createStatement().executeQuery(String.format(ordinalQuery, table));
            while(rs.next()) {
                ordinalMap.put(rs.getString(1), rs.getInt(2));
            }
            rs = conn.createStatement().executeQuery(String.format(ordinalQuery, newTable));
            while(rs.next()) {
                Assert.assertEquals(ordinalMap.get(rs.getString(1)).intValue(),
                        rs.getInt(2));
            }
        }
    }

    @Test
    public void testCreateViewStatement_tenant() throws Exception {
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();
        String schemaName = generateUniqueName();
        String tenantId = "abc";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStmt = "CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, "
                + "v1 VARCHAR, v2 VARCHAR)";
        String viewFullName = SchemaUtil.getPTableFullNameWithQuotes(schemaName, viewName);
        String createViewStmt = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, "
                + "id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) "
                + "AS SELECT * FROM "+pTableFullName;

        List<String> queries1 = new ArrayList<String>(){};
        queries1.add(createTableStmt);
        runSchemaExtractionTool(schemaName, tableName, null, queries1);
        List<String> queries2 = new ArrayList<String>();
        queries2.add(createViewStmt);
        String result2 = runSchemaExtractionTool(schemaName, viewName, tenantId, queries2);
        Assert.assertEquals(createViewStmt.toUpperCase(), result2.toUpperCase());
    }

    @Test
    public void testSaltedTableStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String query = "create table " + pTableFullName +
                "(a_integer integer not null CONSTRAINT pk PRIMARY KEY (a_integer)) SALT_BUCKETS=16";
        List<String> queries = new ArrayList<String>(){};
        queries.add(query);
        String result = runSchemaExtractionTool(schemaName, tableName, null, queries);
        Assert.assertTrue(getProperties(result).contains("SALT_BUCKETS=16"));
    }

    @Test
    public void testCreateTableWithPKConstraint() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String query = "create table " + pTableFullName +
                "(a_char CHAR(15) NOT NULL, " +
                "b_char CHAR(15) NOT NULL, " +
                "c_bigint BIGINT NOT NULL CONSTRAINT PK PRIMARY KEY (a_char, b_char, c_bigint)) IMMUTABLE_ROWS=TRUE";
        List<String> queries = new ArrayList<String>(){};
        queries.add(query);
        String result = runSchemaExtractionTool(schemaName, tableName, null, queries);
        Assert.assertEquals(query.toUpperCase(), result.toUpperCase());
    }

    @Test
    public void testCreateTableWithArrayColumn() throws Exception {
        String tableName = generateUniqueName();
        String pTableFullName = tableName;
        String query = "create table " + pTableFullName +
                "(a_char CHAR(15) NOT NULL, " +
                "b_char CHAR(10) NOT NULL, " +
                "c_var_array VARCHAR ARRAY, " +
                "d_char_array CHAR(15) ARRAY[3] " +
                "CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " +
                "TTL=2592000, IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN', REPLICATION_SCOPE=1";
        List<String> queries = new ArrayList<String>(){};
        queries.add(query);
        String result = runSchemaExtractionTool("", tableName, null, queries);
        Assert.assertEquals(query.toUpperCase(), result.toUpperCase());
    }

    @Test
    public void testCreateTableWithDefaultCFProperties() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String properties = "KEEP_DELETED_CELLS=TRUE, TTL=1209600, IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN', "
                + "REPLICATION_SCOPE=1, DEFAULT_COLUMN_FAMILY='cv', SALT_BUCKETS=16, MULTI_TENANT=true, TIME_TEST='72HOURS'";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String query = "create table " + pTableFullName +
                "(a_char CHAR(15) NOT NULL, " +
                "b_char CHAR(10) NOT NULL, " +
                "\"av\".\"_\" CHAR(1), " +
                "\"bv\".\"_\" CHAR(1), " +
                "\"cv\".\"_\" CHAR(1), " +
                "\"dv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " + properties;
        List<String> queries = new ArrayList<String>(){};
        queries.add(query);
        String result = runSchemaExtractionTool(schemaName, tableName, null, queries);
        Assert.assertTrue(compareProperties(properties, getProperties(result)));
    }

    @Test
    public void testCreateTableWithCFProperties() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String properties = "\"av\".VERSIONS=2, \"bv\".VERSIONS=2, " +
                "DATA_BLOCK_ENCODING='DIFF', " +
                "IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN', SALT_BUCKETS=16, MULTI_TENANT=true";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String query = "create table " + pTableFullName +
                "(a_char CHAR(15) NOT NULL, " +
                "b_char CHAR(10) NOT NULL, " +
                "\"av\".\"_\" CHAR(1), " +
                "\"bv\".\"_\" CHAR(1), " +
                "\"cv\".\"_\" CHAR(1) " +
                "CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " + properties;
        List<String> queries = new ArrayList<String>(){};
        queries.add(query);
        String result = runSchemaExtractionTool(schemaName, tableName, null, queries);
        Assert.assertTrue(compareProperties(properties, getProperties(result)));
    }

    @Test
    public void testCreateTableWithMultipleCF() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String properties = "\"av\".VERSIONS=2, \"bv\".VERSIONS=3, " +
                "\"cv\".VERSIONS=4, DATA_BLOCK_ENCODING='DIFF', " +
                "IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN', SALT_BUCKETS=16, MULTI_TENANT=true";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        final String query = "create table " + pTableFullName +
                "(a_char CHAR(15) NOT NULL, " +
                "b_char CHAR(10) NOT NULL, " +
                "\"av\".\"_\" CHAR(1), " +
                "\"bv\".\"_\" CHAR(1), " +
                "\"cv\".\"_\" CHAR(1), " +
                "\"dv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " + properties;
        List<String> queries = new ArrayList<String>(){};
        queries.add(query);
        String result = runSchemaExtractionTool(schemaName, tableName, null, queries);
        Assert.assertTrue(compareProperties(properties, getProperties(result)));
    }

    @Test
    public void testCreateTableWithMultipleCFProperties() throws Exception {
        String tableName = "07"+generateUniqueName();
        String schemaName = generateUniqueName();
        String properties = "\"av\".DATA_BLOCK_ENCODING='DIFF', \"bv\".DATA_BLOCK_ENCODING='DIFF', "
                + "\"cv\".DATA_BLOCK_ENCODING='DIFF', " +
                "IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN', "
                + "SALT_BUCKETS=16, MULTI_TENANT=true, BLOOMFITER='ROW'";
        String simplifiedProperties = "DATA_BLOCK_ENCODING='DIFF', "
                + "IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN', "
                + "SALT_BUCKETS=16, MULTI_TENANT=true, BLOOMFITER='ROW'";
        String query = "create table " + schemaName+".\""+tableName+"\"" +
                "(a_char CHAR(15) NOT NULL, " +
                "b_char CHAR(10) NOT NULL, " +
                "\"av\".\"_\" CHAR(1), " +
                "\"bv\".\"_\" CHAR(1), " +
                "\"cv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " + properties;
        List<String> queries = new ArrayList<String>(){};
        queries.add(query);
        String result = runSchemaExtractionTool(schemaName, tableName, null, queries);
        try {
            new SQLParser(result).parseStatement();
        } catch (ParseException pe) {
            fail("This should not happen!");
        }
        Assert.assertTrue(compareProperties(simplifiedProperties, getProperties(result)));
    }

    @Test
    public void testColumnAndPKOrdering() throws Exception {
        String table = "CREATE TABLE IF NOT EXISTS MY_SCHEMA.MY_DATA_TABLE (\n"
                + "    ORGANIZATION_ID CHAR(15) NOT NULL, \n"
                + "    KEY_PREFIX CHAR(3) NOT NULL,\n"
                + "    CREATED_DATE DATE,\n"
                + "    CREATED_BY CHAR(15) \n"
                + "    CONSTRAINT PK PRIMARY KEY (\n"
                + "        ORGANIZATION_ID, \n"
                + "        KEY_PREFIX\n" + "    )\n"
                + ") VERSIONS=1, IMMUTABLE_ROWS=true, MULTI_TENANT=true, REPLICATION_SCOPE=1";

        String view = "CREATE VIEW IF NOT EXISTS MY_SCHEMA.MY_DATA_VIEW  (\n"
                + "    DATE_TIME1 DATE NOT NULL,\n"
                + "    TEXT1 VARCHAR NOT NULL,\n"
                + "    INT1 BIGINT NOT NULL,\n"
                + "    DOUBLE1 DECIMAL(12, 3),\n"
                + "    DOUBLE2 DECIMAL(12, 3),\n"
                + "    DOUBLE3 DECIMAL(12, 3),\n"
                + "    CONSTRAINT PKVIEW PRIMARY KEY\n" + "    (\n"
                + "        DATE_TIME1, TEXT1, INT1\n" + "    )\n" + ")\n"
                + "AS SELECT * FROM MY_SCHEMA.MY_DATA_TABLE WHERE KEY_PREFIX = '9Yj'";

        String index = "CREATE INDEX IF NOT EXISTS MY_VIEW_INDEX\n"
                + "ON MY_SCHEMA.MY_DATA_VIEW (TEXT1, DATE_TIME1 DESC, DOUBLE1)\n"
                + "INCLUDE (CREATED_BY, CREATED_DATE)";
        List<String> queries = new ArrayList<String>(){};

        queries.add(table);
        queries.add(view);
        queries.add(index);

        String expectedIndex = "CREATE INDEX MY_VIEW_INDEX "
                + "ON MY_SCHEMA.MY_DATA_VIEW(TEXT1, DATE_TIME1 DESC, DOUBLE1, INT1)"
                + " INCLUDE (CREATED_BY, CREATED_DATE)";
        String result = runSchemaExtractionTool("MY_SCHEMA", "MY_VIEW_INDEX", null, queries);
        Assert.assertEquals(expectedIndex.toUpperCase(), result.toUpperCase());

        String expectedView = "CREATE VIEW MY_SCHEMA.MY_DATA_VIEW(DATE_TIME1 DATE NOT NULL, "
                + "TEXT1 VARCHAR NOT NULL, INT1 BIGINT NOT NULL, DOUBLE1 DECIMAL(12,3), "
                + "DOUBLE2 DECIMAL(12,3), DOUBLE3 DECIMAL(12,3)"
                + " CONSTRAINT PKVIEW PRIMARY KEY (DATE_TIME1, TEXT1, INT1))"
                + " AS SELECT * FROM MY_SCHEMA.MY_DATA_TABLE WHERE KEY_PREFIX = '9YJ'";
        result = runSchemaExtractionTool("MY_SCHEMA", "MY_DATA_VIEW", null, new ArrayList<String>());
        Assert.assertEquals(expectedView.toUpperCase(), result.toUpperCase());
    }

    @Test
    public void testColumnAndPKOrdering_nonView() throws Exception {
        String indexName = "MY_DATA_TABLE_INDEX";
        String table = "CREATE TABLE MY_SCHEMA.MY_SAMPLE_DATA_TABLE("
                + "ORGANIZATION_ID CHAR(15) NOT NULL,"
                + " SOME_ID_COLUMN CHAR(3) NOT NULL,"
                + " SOME_ID_COLUMN_2 CHAR(15) NOT NULL,"
                + " CREATED_DATE DATE NOT NULL,"
                + " SOME_ID_COLUMN_3 CHAR(15) NOT NULL,"
                + " SOME_ID_COLUMN_4 CHAR(15),"
                + " CREATED_BY_ID VARCHAR,"
                + " VALUE_FIELD VARCHAR"
                + " CONSTRAINT PK PRIMARY KEY (ORGANIZATION_ID, SOME_ID_COLUMN, SOME_ID_COLUMN_2,"
                + " CREATED_DATE DESC, SOME_ID_COLUMN_3))"
                + " IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME='ONE_CELL_PER_COLUMN',"
                + " MULTI_TENANT=true, REPLICATION_SCOPE=1\n";

        String index = "CREATE INDEX IF NOT EXISTS MY_DATA_TABLE_INDEX\n"
                + " ON MY_SCHEMA.MY_SAMPLE_DATA_TABLE (SOME_ID_COLUMN, CREATED_DATE DESC,"
                + " SOME_ID_COLUMN_2, SOME_ID_COLUMN_3)\n"
                + " INCLUDE\n"
                + "(SOME_ID_COLUMN_4, CREATED_BY_ID, VALUE_FIELD)\n";
        List<String> queries = new ArrayList<String>(){};

        queries.add(table);
        queries.add(index);
        String result = runSchemaExtractionTool("MY_SCHEMA",
                "MY_DATA_TABLE_INDEX", null, queries);
        String expected = "CREATE INDEX %s ON MY_SCHEMA.MY_SAMPLE_DATA_TABLE"
                + "(SOME_ID_COLUMN, CREATED_DATE DESC, SOME_ID_COLUMN_2, SOME_ID_COLUMN_3) "
                + "INCLUDE (SOME_ID_COLUMN_4, CREATED_BY_ID, VALUE_FIELD)";

        Assert.assertEquals(String.format(expected, indexName).toUpperCase(), result.toUpperCase());
        queries.clear();
        String newIndex = indexName+"_NEW";
        queries.add(String.format(expected, newIndex));
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            executeCreateStatements(conn, queries);
        }
        compareOrdinalPositions(indexName, newIndex);
    }

    @Test
    public void testCreateIndexStatementWithColumnFamily() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStmt = "CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, "
                + "\"av\".\"_\" CHAR(1), v2 VARCHAR)";
        String createIndexStmt = "CREATE INDEX "+ indexName + " ON "+pTableFullName+ "(\"av\".\"_\")";
        List<String> queries = new ArrayList<String>() {};
        queries.add(createTableStmt);
        queries.add(createIndexStmt);
        //by the principle of having maximal columns in pk
        String expected = "CREATE INDEX %s ON "+pTableFullName+ "(\"av\".\"_\", K)";
        String result =  runSchemaExtractionTool(schemaName, indexName, null, queries);
        Assert.assertEquals(String.format(expected, indexName).toUpperCase(), result.toUpperCase());
        queries.clear();
        String newIndex = indexName+"_NEW";
        queries.add(String.format(expected, newIndex));
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            executeCreateStatements(conn, queries);
        }
        compareOrdinalPositions(indexName, newIndex);
    }

    private Connection getTenantConnection(String url, String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(url, props);
    }

    private String runSchemaExtractionTool(String schemaName, String tableName, String tenantId,
            List<String> queries) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String output;
        if (tenantId == null) {
            try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
                executeCreateStatements(conn, queries);
                String [] args = {"-m", "EXTRACT", "-tb", tableName, "-s", schemaName};
                output = runSchemaTool(conn, args);
            }
        } else {
            try (Connection conn = getTenantConnection(getUrl(), tenantId)) {
                executeCreateStatements(conn, queries);
                String [] args = {"-m", "EXTRACT","-tb", tableName, "-s", schemaName, "-t", tenantId};
                output = runSchemaTool(conn, args);
            }
        }
        return output;
    }

    private void executeCreateStatements(Connection conn, List<String> queries) throws SQLException {
        for (String query: queries){
            conn.createStatement().execute(query);
        }
        conn.commit();
    }

    public static String runSchemaTool(Connection conn, String [] args) throws Exception {
        SchemaTool set = new SchemaTool();
        if(conn!=null) {
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
        }
        set.run(args);
        return set.getOutput();
    }

    private String getProperties(String query){
        return query.substring(query.lastIndexOf(")")+1);
    }

    private boolean compareProperties(String prop1, String prop2){
        String[] propArray1 = prop1.toUpperCase().replaceAll("\\s+","").split(",");
        String[] propArray2 = prop2.toUpperCase().replaceAll("\\s+","").split(",");

        Set<String> set1 = new HashSet<>(Arrays.asList(propArray1));
        Set<String> set2 = new HashSet<>(Arrays.asList(propArray2));

        return set1.equals(set2);
    }
}
