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
package org.apache.phoenix.schema;

import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

public class SchemaExtractionToolIT extends ParallelStatsEnabledIT {

    @BeforeClass
    public static void setup() throws Exception {
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
    public void testCreateIndexStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        String indexName1 = generateUniqueName();
        String indexName2 = generateUniqueName();
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStatement = "CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                + properties;
        String createIndexStatement = "CREATE INDEX "+indexName + " ON "+pTableFullName+"(v1 DESC) INCLUDE (v2)";
        String createIndexStatement1 = "CREATE INDEX "+indexName1 + " ON "+pTableFullName+"(v2 DESC) INCLUDE (v1)";
        String createIndexStatement2 = "CREATE INDEX "+indexName2 + " ON "+pTableFullName+"(k)";
        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStatement);
        queries.add(createIndexStatement);
        queries.add(createIndexStatement1);
        queries.add(createIndexStatement2);

        String result = runSchemaExtractionTool(schemaName, indexName2, null, queries);
        Assert.assertEquals(createIndexStatement2.toUpperCase(), result.toUpperCase());
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
        String viewFullName1 = SchemaUtil.getQualifiedTableName(schemaName, viewName+"1");
        String createView = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, "
                + "id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) "
                + "AS SELECT * FROM "+pTableFullName;
        String createView1 = "CREATE VIEW "+viewFullName1 + "(id1 BIGINT, id2 BIGINT NOT NULL, "
                + "id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) "
                + "AS SELECT * FROM "+pTableFullName;

        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStmt);
        queries.add(createView);
        queries.add(createView1);
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
        String createIndexStatement = "CREATE INDEX "+indexName + " ON "+childviewName+"(id1) INCLUDE (v1)";
        List<String> queries = new ArrayList<String>(){};
        queries.add(createTableStmt);
        queries.add(createView);
        queries.add(createView1);
        queries.add(createIndexStatement);
        String result = runSchemaExtractionTool(schemaName, indexName, null, queries);
        Assert.assertEquals(createIndexStatement.toUpperCase(), result.toUpperCase());
    }

    @Test
    public void testCreateTableStatement_tenant() throws Exception {
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();
        String schemaName = generateUniqueName();
        String tenantId = "abc";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStmt = "CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, "
                + "v1 VARCHAR, v2 VARCHAR)";
        String viewFullName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
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
        String schemaName = generateUniqueName();
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String query = "create table " + pTableFullName +
                "(a_char CHAR(15) NOT NULL, " +
                "b_char CHAR(10) NOT NULL, " +
                "c_var_array VARCHAR ARRAY, " +
                "d_char_array CHAR(15) ARRAY[3] CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " +
                "TTL=2592000, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, REPLICATION_SCOPE=1";
        List<String> queries = new ArrayList<String>(){};
        queries.add(query);
        String result = runSchemaExtractionTool(schemaName, tableName, null, queries);
        Assert.assertEquals(query.toUpperCase(), result.toUpperCase());
    }

    @Test
    public void testCreateTableWithDefaultCFProperties() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String properties = "KEEP_DELETED_CELLS=TRUE, TTL=1209600, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, REPLICATION_SCOPE=1, DEFAULT_COLUMN_FAMILY=cv, SALT_BUCKETS=16, MULTI_TENANT=true";
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
                "DATA_BLOCK_ENCODING=DIFF, " +
                "IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, SALT_BUCKETS=16, MULTI_TENANT=true";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String query = "create table " + pTableFullName +
                "(a_char CHAR(15) NOT NULL, " +
                "b_char CHAR(10) NOT NULL, " +
                "\"av\".\"_\" CHAR(1), " +
                "\"bv\".\"_\" CHAR(1), " +
                "\"cv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " + properties;
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
                "\"cv\".VERSIONS=4, DATA_BLOCK_ENCODING=DIFF, " +
                "IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, SALT_BUCKETS=16, MULTI_TENANT=true";
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
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String properties = "\"av\".DATA_BLOCK_ENCODING=DIFF, \"bv\".DATA_BLOCK_ENCODING=DIFF, \"cv\".DATA_BLOCK_ENCODING=DIFF, " +
                "IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, SALT_BUCKETS=16, MULTI_TENANT=true";
        String simplifiedProperties = "DATA_BLOCK_ENCODING=DIFF, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, SALT_BUCKETS=16, MULTI_TENANT=true";
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String query = "create table " + pTableFullName +
                "(a_char CHAR(15) NOT NULL, " +
                "b_char CHAR(10) NOT NULL, " +
                "\"av\".\"_\" CHAR(1), " +
                "\"bv\".\"_\" CHAR(1), " +
                "\"cv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " + properties;
        List<String> queries = new ArrayList<String>(){};
        queries.add(query);
        String result = runSchemaExtractionTool(schemaName, tableName, null, queries);
        Assert.assertTrue(compareProperties(simplifiedProperties, getProperties(result)));
    }

    @Test
    public void testCreateIndexStatementWithColumnFamily() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTableStmt = "CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, \"av\".\"_\" CHAR(1), v2 VARCHAR)";
        String createIndexStmt = "CREATE INDEX "+ indexName + " ON "+pTableFullName+ "(\"av\".\"_\")";
        List<String> queries = new ArrayList<String>() {};
        queries.add(createTableStmt);
        queries.add(createIndexStmt);
        String result =  runSchemaExtractionTool(schemaName, indexName, null, queries);
        Assert.assertEquals(createIndexStmt.toUpperCase(), result.toUpperCase());
    }

    private Connection getTenantConnection(String url, String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(url, props);
    }

    private String runSchemaExtractionTool(String schemaName, String tableName, String tenantId, List<String> queries) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String output;
        if (tenantId == null){
            try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
                executeCreateStatements(conn, queries);
                String [] args = {"-tb", tableName, "-s", schemaName};
                output = extractSchema(conn, args);
            }
        } else {
            try (Connection conn = getTenantConnection(getUrl(), tenantId)) {
                executeCreateStatements(conn, queries);
                String [] args = {"-tb", tableName, "-s", schemaName, "-t", tenantId};
                output = extractSchema(conn, args);
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

    private String extractSchema(Connection conn, String [] args) throws Exception {
        SchemaExtractionTool set = new SchemaExtractionTool();
        set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
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
