package org.apache.phoenix.schema;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;

import java.util.Map;
import java.util.Properties;

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
        String properties = "TTL=2592000,IMMUTABLE_ROWS=true,DISABLE_WAL=true";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {

            String pTableFullName = SchemaUtil.getQualifiedTableName(schemaName, tableName);
            conn.createStatement().execute("CREATE TABLE "+ pTableFullName + "(k VARCHAR NOT NULL PRIMARY KEY, "
                    + "v1 VARCHAR, v2 VARCHAR)"
                    + properties);
            conn.commit();
            String [] args = {"-tb", tableName, "-s", schemaName};

            SchemaExtractionTool set = new SchemaExtractionTool();
            set.setConf(conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration());
            set.run(args);
            String actualProperties = set.getOutput().substring(set.getOutput().lastIndexOf(")")+1).replace(" ","");
            Assert.assertEquals(3, actualProperties.split(",").length);
        }
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
}
