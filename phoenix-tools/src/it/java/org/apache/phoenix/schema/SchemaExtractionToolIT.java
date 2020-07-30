package org.apache.phoenix.schema;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
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
import java.util.*;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

public class SchemaExtractionToolIT extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        Map<String, String> props = Collections.emptyMap();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testCreateTableStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String createTable = "CREATE TABLE "+ pTableFullName + "(K VARCHAR NOT NULL PRIMARY KEY, "
                + "V1 VARCHAR, V2 VARCHAR) TTL=2592000, IMMUTABLE_ROWS=TRUE, DISABLE_WAL=TRUE";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTable);
            conn.commit();
            String [] args = {"-tb", tableName, "-s", schemaName};

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            Assert.assertEquals(createTable, set.getOutput().toUpperCase());
        }
    }

    @Test
    public void testCreateTableStatement_tenant() throws Exception {
        String tableName = generateUniqueName();
        String viewName = generateUniqueName();
        String schemaName = generateUniqueName();
        String tenantId = "abc";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";
        SchemaExtractionTool set;
        String viewFullName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
        String createView = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, "
                + "id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) "
                + "AS SELECT * FROM "+pTableFullName;

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, "
                    + "v1 VARCHAR, v2 VARCHAR)"
                    + properties);
            set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            conn.commit();
        }
        try (Connection conn = getTenantConnection(getUrl(), tenantId)) {
            conn.createStatement().execute(createView);
            conn.commit();
        }
        String [] args = {"-tb", viewName, "-s", schemaName, "-t", tenantId};
        set.run(args);
        Assert.assertEquals(createView.toUpperCase(), set.getOutput().toUpperCase());
    }

    private Connection getTenantConnection(String url, String tenantId) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(url, props);
    }

    @Test
    public void testCreateIndexStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        String indexName1 = generateUniqueName();
        String indexName2 = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)"
                    + properties);

            String createIndexStatement = "CREATE INDEX "+indexName + " ON "+pTableFullName+"(v1 DESC) INCLUDE (v2)";

            String createIndexStatement1 = "CREATE INDEX "+indexName1 + " ON "+pTableFullName+"(v2 DESC) INCLUDE (v1)";

            String createIndexStatement2 = "CREATE INDEX "+indexName2 + " ON "+pTableFullName+"(k)";

            conn.createStatement().execute(createIndexStatement);
            conn.createStatement().execute(createIndexStatement1);
            conn.createStatement().execute(createIndexStatement2);
            conn.commit();
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());

            String [] args = {"-tb", indexName, "-s", schemaName};
            set.run(args);
            Assert.assertEquals(createIndexStatement.toUpperCase(), set.getOutput().toUpperCase());

            String [] args1 = {"-tb", indexName1, "-s", schemaName};
            set.run(args1);
            Assert.assertEquals(createIndexStatement1.toUpperCase(), set.getOutput().toUpperCase());

            String [] args2 = {"-tb", indexName2, "-s", schemaName};
            set.run(args2);
            Assert.assertEquals(createIndexStatement2.toUpperCase(), set.getOutput().toUpperCase());
        }
    }

    @Test
    public void testCreateViewStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String viewName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, "
                    + "v1 VARCHAR, v2 VARCHAR)"
                    + properties);
            String viewFullName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
            String viewFullName1 = SchemaUtil.getQualifiedTableName(schemaName, viewName+"1");


            String createView = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, "
                    + "id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) "
                    + "AS SELECT * FROM "+pTableFullName;
            String createView1 = "CREATE VIEW "+viewFullName1 + "(id1 BIGINT, id2 BIGINT NOT NULL, "
                    + "id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) "
                    + "AS SELECT * FROM "+pTableFullName;

            conn.createStatement().execute(createView);
            conn.createStatement().execute(createView1);
            conn.commit();
            String [] args = {"-tb", viewName, "-s", schemaName};

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            Assert.assertEquals(createView.toUpperCase(), set.getOutput().toUpperCase());
        }
    }

    @Test
    public void testCreateViewIndexStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String viewName = generateUniqueName();
        String childView = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k BIGINT NOT NULL PRIMARY KEY, "
                    + "v1 VARCHAR, v2 VARCHAR)"
                    + properties);
            String viewFullName = SchemaUtil.getQualifiedTableName(schemaName, viewName);
            String childviewName = SchemaUtil.getQualifiedTableName(schemaName, childView);

            String createView = "CREATE VIEW "+viewFullName + "(id1 BIGINT, id2 BIGINT NOT NULL, "
                    + "id3 VARCHAR NOT NULL CONSTRAINT PKVIEW PRIMARY KEY (id2, id3 DESC)) "
                    + "AS SELECT * FROM "+pTableFullName;

            String createView1 = "CREATE VIEW "+childviewName + " AS SELECT * FROM "+viewFullName;

            String createIndexStatement = "CREATE INDEX "+indexName + " ON "+childviewName+"(id1) INCLUDE (v1)";

            conn.createStatement().execute(createView);
            conn.createStatement().execute(createView1);
            conn.createStatement().execute(createIndexStatement);
            conn.commit();
            String [] args = {"-tb", indexName, "-s", schemaName};

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            Assert.assertEquals(createIndexStatement.toUpperCase(), set.getOutput().toUpperCase());
        }
    }

    @Test
    public void testSaltedTableStatement() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_integer integer not null CONSTRAINT pk PRIMARY KEY (a_integer)) SALT_BUCKETS=16";
            conn.createStatement().execute(query);
            conn.commit();
            String [] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            String actualProperties = set.getOutput().substring(set.getOutput().lastIndexOf(")")+1);
            Assert.assertEquals(true, actualProperties.contains("SALT_BUCKETS=16"));
        }
    }

    @Test
    public void testCreateTableWithPKConstraint() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_char CHAR(15) NOT NULL, " +
                    "b_char CHAR(15) NOT NULL, " +
                    "c_bigint BIGINT NOT NULL CONSTRAINT PK PRIMARY KEY (a_char, b_char, c_bigint)) IMMUTABLE_ROWS=TRUE";
            conn.createStatement().execute(query);
            conn.commit();
            String [] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);

            Assert.assertEquals(query.toUpperCase(), set.getOutput().toUpperCase());
        }
    }

    @Test
    public void testCreateTableWithArrayColumn() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_char CHAR(15) NOT NULL, " +
                    "b_char CHAR(10) NOT NULL, " +
                    "c_var_array VARCHAR ARRAY, " +
                    "d_char_array CHAR(15) ARRAY[3] CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " +
                    "TTL=2592000, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, DISABLE_TABLE_SOR=true, REPLICATION_SCOPE=1";
            conn.createStatement().execute(query);
            conn.commit();
            String[] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);

            Assert.assertEquals(query.toUpperCase(), set.getOutput().toUpperCase());
        }
    }

    @Test
    public void testCreateTableWithNonDefaultColumnFamily() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_char CHAR(15) NOT NULL, " +
                    "b_char CHAR(10) NOT NULL, " +
                    "\"av\".\"_\" CHAR(1), " +
                    "\"bv\".\"_\" CHAR(1), " +
                    "\"cv\".\"_\" CHAR(1), " +
                    "\"dv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " +
                    "TTL=1209600, IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, SALT_BUCKETS=16, DISABLE_TABLE_SOR=true, MULTI_TENANT=true";
            conn.createStatement().execute(query);
            conn.commit();
            String[] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);

            Assert.assertEquals(query.toUpperCase(), set.getOutput().toUpperCase());
        }
    }

    @Test
    public void testCreateTableWithUniversalCFProperties() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "KEEP_DELETED_CELLS=TRUE, TTL=1209600, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, REPLICATION_SCOPE=1";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_char CHAR(15) NOT NULL, " +
                    "b_char CHAR(10) NOT NULL, " +
                    "\"av\".\"_\" CHAR(1), " +
                    "\"bv\".\"_\" CHAR(1), " +
                    "\"cv\".\"_\" CHAR(1), " +
                    "\"dv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " + properties;
            conn.createStatement().execute(query);
            conn.commit();
            String[] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);

            Assert.assertEquals(query.toUpperCase(), set.getOutput().toUpperCase());
        }
    }

    @Test
    public void testCreateTableWithDefaultCFProperties() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "KEEP_DELETED_CELLS=TRUE, TTL=1209600, IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, REPLICATION_SCOPE=1, DEFAULT_COLUMN_FAMILY=cv";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_char CHAR(15) NOT NULL, " +
                    "b_char CHAR(10) NOT NULL, " +
                    "\"av\".\"_\" CHAR(1), " +
                    "\"bv\".\"_\" CHAR(1), " +
                    "\"cv\".\"_\" CHAR(1), " +
                    "\"dv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " + properties;
            conn.createStatement().execute(query);
            conn.commit();
            String[] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);

            Assert.assertTrue(compareProperties(properties, set.getOutput().substring(set.getOutput().lastIndexOf(")")+1)));
        }
    }

    @Test
    public void testCreateTableWithMultipleCFProperties() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "\"av\".VERSIONS=2, \"bv\".VERSIONS=3, " +
                "\"cv\".VERSIONS=4, DATA_BLOCK_ENCODING=DIFF, " +
                "IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, SALT_BUCKETS=16, DISABLE_TABLE_SOR=true, MULTI_TENANT=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_char CHAR(15) NOT NULL, " +
                    "b_char CHAR(10) NOT NULL, " +
                    "\"av\".\"_\" CHAR(1), " +
                    "\"bv\".\"_\" CHAR(1), " +
                    "\"cv\".\"_\" CHAR(1), " +
                    "\"dv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " + properties;
            conn.createStatement().execute(query);
            conn.commit();
            String[] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);

            Assert.assertTrue(compareProperties(properties, set.getOutput().substring(set.getOutput().lastIndexOf(")")+1)));
        }
    }

    @Test
    public void testCreateTableWithMultipleCFProperties2() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String properties = "\"av\".VERSIONS=2, \"bv\".VERSIONS=2, " +
                "DATA_BLOCK_ENCODING=DIFF, " +
                "IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, SALT_BUCKETS=16, DISABLE_TABLE_SOR=true, MULTI_TENANT=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            String query = "create table " + pTableFullName +
                    "(a_char CHAR(15) NOT NULL, " +
                    "b_char CHAR(10) NOT NULL, " +
                    "\"av\".\"_\" CHAR(1), " +
                    "\"bv\".\"_\" CHAR(1), " +
                    "\"cv\".\"_\" CHAR(1) CONSTRAINT PK PRIMARY KEY (a_char, b_char)) " + properties;
            conn.createStatement().execute(query);
            conn.commit();
            String[] args = {"-tb", tableName, "-s", schemaName};
            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);

            Assert.assertTrue(compareProperties(properties, set.getOutput().substring(set.getOutput().lastIndexOf(")")+1)));
        }
    }

    @Test
    public void testCreateIndexStatementWithColumnFamily() throws Exception {
        String tableName = generateUniqueName();
        String schemaName = generateUniqueName();
        String indexName = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, \"av\".\"_\" CHAR(1), v2 VARCHAR)");
            String createIndexStatement = "CREATE INDEX "+ indexName + " ON "+pTableFullName+ "(\"av\".\"_\")";
            conn.createStatement().execute(createIndexStatement);
            conn.commit();

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            String [] args2 = {"-tb", indexName, "-s", schemaName};
            set.run(args2);
            Assert.assertEquals(createIndexStatement.toUpperCase(), set.getOutput().toUpperCase());
        }
    }

    private boolean compareProperties(String prop1, String prop2){
        String[] propArray1 = prop1.toUpperCase().replaceAll("\\s+","").split(",");
        String[] propArray2 = prop2.toUpperCase().replaceAll("\\s+","").split(",");

        Set<String> set1 = new HashSet<>(Arrays.asList(propArray1));
        Set<String> set2 = new HashSet<>(Arrays.asList(propArray2));

        return set1.equals(set2);
    }
}
