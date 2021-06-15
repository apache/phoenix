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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;

import static org.apache.phoenix.schema.tool.SchemaSynthesisProcessor.ENTITY_NAME_IN_BASE_AND_ALTER_DDL_DON_T_MATCH;
import static org.apache.phoenix.schema.tool.SchemaSynthesisProcessor.UNSUPPORTED_DDL_EXCEPTION;
import static org.apache.phoenix.schema.tool.SchemaToolExtractionIT.runSchemaTool;

public class SchemaToolSynthesisIT {

    private static final String SYNTHESIS_DIR = "synthesis/";
    URL fileUrl = SchemaToolSynthesisIT.class.getClassLoader().getResource(
            SYNTHESIS_DIR);
    String filePath = new File(fileUrl.getFile()).getAbsolutePath();


    @Test
    // Adding new column RELATED_COMMAND
    public void testCreateTableStatement_addColumn() throws Exception {
        String expected = "CREATE TABLE IF NOT EXISTS TEST.SAMPLE_TABLE\n"
                + "(ORG_ID CHAR(15) NOT NULL,\n" + "SOME_ANOTHER_ID BIGINT NOT NULL,\n" + "SECOND_ID BIGINT NOT NULL,\n"
                + "TYPE VARCHAR,\n" + "STATUS VARCHAR,\n" + "START_TIMESTAMP BIGINT,\n"
                + "END_TIMESTAMP BIGINT,\n" + "PARAMS VARCHAR,\n" + "RESULT VARCHAR,\n"
                + "RELATED_COMMAND BIGINT DEFAULT 100\n"
                + "CONSTRAINT PK PRIMARY KEY (ORG_ID, SOME_ANOTHER_ID, SECOND_ID))\n"
                + "VERSIONS=1,MULTI_TENANT=false,REPLICATION_SCOPE=1,TTL=31536000";
        String baseDDL = filePath+"/alter_table_add.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // Dropping TYPE column
    public void testCreateTableStatement_dropColumn() throws Exception {
        String expected = "CREATE TABLE IF NOT EXISTS TEST.SAMPLE_TABLE\n"
                + "(ORG_ID CHAR(15) NOT NULL,\n" + "SOME_ANOTHER_ID BIGINT NOT NULL,\n" + "SECOND_ID BIGINT NOT NULL,\n"
                + "STATUS VARCHAR,\n" + "START_TIMESTAMP BIGINT,\n" + "END_TIMESTAMP BIGINT,\n"
                + "PARAMS VARCHAR,\n" + "RESULT VARCHAR\n"
                + "CONSTRAINT PK PRIMARY KEY (ORG_ID, SOME_ANOTHER_ID, SECOND_ID))\n"
                + "VERSIONS=1,MULTI_TENANT=false,REPLICATION_SCOPE=1,TTL=31536000";
        String baseDDL = filePath+"/alter_table_drop.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // Changing REPLICATION SCOPE from 1 to 0
    public void testCreateTableStatement_changeProperty() throws Exception {
        String expected = "CREATE TABLE IF NOT EXISTS TEST.SAMPLE_TABLE\n"
                + "(ORG_ID CHAR(15) NOT NULL,\n" + "SOME_ANOTHER_ID BIGINT NOT NULL,\n" + "SECOND_ID BIGINT NOT NULL,\n"
                + "TYPE VARCHAR,\n" + "STATUS VARCHAR,\n" + "START_TIMESTAMP BIGINT,\n"
                + "END_TIMESTAMP BIGINT,\n" + "PARAMS VARCHAR,\n" + "RESULT VARCHAR\n"
                + "CONSTRAINT PK PRIMARY KEY (ORG_ID, SOME_ANOTHER_ID, SECOND_ID))\n"
                + "MULTI_TENANT=false,REPLICATION_SCOPE=0,TTL=31536000,VERSIONS=1";
        String baseDDL = filePath+"/alter_change_property.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // Adding DISABLE_MIGRATION property
    public void testCreateTableStatement_addProperty() throws Exception {
        String expected = "CREATE TABLE IF NOT EXISTS TEST.SAMPLE_TABLE\n"
                + "(ORG_ID CHAR(15) NOT NULL,\n" + "SOME_ANOTHER_ID BIGINT NOT NULL,\n" + "SECOND_ID BIGINT NOT NULL,\n"
                + "TYPE VARCHAR,\n" + "STATUS VARCHAR,\n" + "START_TIMESTAMP BIGINT,\n"
                + "END_TIMESTAMP BIGINT,\n" + "PARAMS VARCHAR,\n" + "RESULT VARCHAR\n"
                + "CONSTRAINT PK PRIMARY KEY (ORG_ID, SOME_ANOTHER_ID, SECOND_ID))\n"
                + "DISABLE_MIGRATION=true,MULTI_TENANT=false,REPLICATION_SCOPE=1,TTL=31536000,VERSIONS=1";
        String baseDDL = filePath+"/alter_add_property.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // Add NEW_COLUMN to the view
    public void testCreateViewStatement_addColumn() throws Exception {
        String expected = "CREATE VIEW IF NOT EXISTS TEST.SAMPLE_VIEW\n"
                + "(DATE_TIME1 DATE NOT NULL,\n" + "INT1 BIGINT NOT NULL,\n"
                + "SOME_ID CHAR(15) NOT NULL,\n" + "DOUBLE1 DECIMAL(12,3),\n"
                + "IS_BOOLEAN BOOLEAN,\n" + "RELATE CHAR(15),\n" + "TEXT1 VARCHAR,\n"
                + "TEXT_READ_ONLY VARCHAR,\n" + "NEW_COLUMN VARCHAR(20)\n"
                + "CONSTRAINT PKVIEW PRIMARY KEY (DATE_TIME1 DESC, INT1, SOME_ID))\n"
                + "AS SELECT * FROM TEST.SAMPLE_TABLE_VIEW WHERE FILTER_PREFIX = 'abc'";
        String baseDDL = filePath+"/alter_view_add.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // Add SOME_ID VARCHAR NULL to the PK of the table
    public void testCreateTableStatement_addColumn_pk() throws Exception {
        String expected = "CREATE TABLE IF NOT EXISTS TEST.TABLE_1\n" + "(STATE CHAR(1) NOT NULL,\n"
                + "SOME_ID VARCHAR\n" + "CONSTRAINT PK PRIMARY KEY (STATE, SOME_ID))";
        String baseDDL = filePath+"/alter_table_add_pk.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // Dropping DOUBLE1 column from the view
    public void testCreateViewStatement_dropColumn() throws Exception {
        String expected = "CREATE VIEW IF NOT EXISTS TEST.SAMPLE_VIEW\n"
                + "(DATE_TIME1 DATE NOT NULL,\n" + "INT1 BIGINT NOT NULL,\n"
                + "SOME_ID CHAR(15) NOT NULL,\n" + "IS_BOOLEAN BOOLEAN,\n" + "RELATE CHAR(15),\n"
                + "TEXT1 VARCHAR,\n" + "TEXT_READ_ONLY VARCHAR\n"
                + "CONSTRAINT PKVIEW PRIMARY KEY (DATE_TIME1 DESC, INT1, SOME_ID))\n"
                + "AS SELECT * FROM TEST.SAMPLE_TABLE_VIEW WHERE FILTER_PREFIX = 'abc'";
        String baseDDL = filePath+"/alter_view_drop.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // changing TTL
    public void testCreateIndexStatement_changeProperty() throws Exception {
        String expected = "CREATE INDEX IF NOT EXISTS ANOTHER_INDEX_ON_SOME_TABLE\n"
                + "ON TEST.SOME_TABLE_WITH_INDEX(SOME_VALUE_COL_1, SOME_VALUE_COL)\n"
                + "INCLUDE (TEXT_VALUE) ASYNC\n" + "TTL=5000";
        String baseDDL = filePath+"/alter_index_change_property.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // Adding REPLICATION_SCOPE to 1
    public void testCreateIndexStatement_addProperty() throws Exception {
        String expected = "CREATE INDEX IF NOT EXISTS ANOTHER_INDEX_ON_SOME_TABLE\n"
                + "ON TEST.SOME_TABLE_WITH_INDEX(SOME_VALUE_COL_1, SOME_VALUE_COL)\n"
                + "INCLUDE (TEXT_VALUE) ASYNC\n" + "TTL=123000,REPLICATION_SCOPE=1";
        String baseDDL = filePath+"/alter_index_add_property.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // drop table
    public void testCreateTableStatement_dropTable() throws Exception {
        String expected = "";
        String baseDDL = filePath+"/drop_table.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // drop table
    public void testCreateTableStatement_dropRecreateTable() throws Exception {
        String expected = "CREATE TABLE IF NOT EXISTS TEST.SAMPLE_TABLE (\n"
                + "   ORG_ID CHAR(15) NOT NULL,\n" + "   SOME_ANOTHER_ID BIGINT NOT NULL,\n"
                + "   TYPE VARCHAR,\n" + "   STATUS VARCHAR,\n" + "   START_TIMESTAMP BIGINT,\n"
                + "   END_TIMESTAMP BIGINT,\n" + "   PARAMS VARCHAR,   RESULT VARCHAR\n"
                + "   CONSTRAINT PK PRIMARY KEY (ORG_ID, SOME_ANOTHER_ID)\n"
                + ") VERSIONS=1,MULTI_TENANT=FALSE,REPLICATION_SCOPE=1,TTL=31536000";
        String baseDDL = filePath+"/drop_create_table.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // drop table
    public void testCreateTableStatement_add_pk() throws Exception {
        String expected = "CREATE TABLE IF NOT EXISTS TEST.TABLE_1\n" + "(STATE CHAR(1) NOT NULL,\n"
                + "SOME_ID VARCHAR\n" + "CONSTRAINT PK PRIMARY KEY (STATE, SOME_ID))";
        String baseDDL = filePath+"/alter_table_add_pk.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // drop table
    public void testCreateIndexStatement_dropIndex() throws Exception {
        String expected = "";
        String baseDDL = filePath+"/drop_index.sql";
        runAndVerify(expected, baseDDL);
    }

    private void runAndVerify(String expected, String baseDDL) throws Exception {
        String[] arg = { "-m", "SYNTH", "-d", baseDDL };
        String result = runSchemaTool(null, arg);
        Assert.assertEquals(expected, result);
    }

    @Test
    // Alter DDL file can have multiple alter statements
    public void testMultipleAlterDDL() throws Exception {
        String expected = "CREATE TABLE IF NOT EXISTS TEST.SAMPLE_TABLE\n"
                + "(ORG_ID CHAR(15) NOT NULL,\n" + "SOME_ANOTHER_ID BIGINT NOT NULL,\n" + "SECOND_ID BIGINT NOT NULL,\n"
                + "TYPE VARCHAR,\n" + "STATUS VARCHAR,\n" + "START_TIMESTAMP BIGINT,\n"
                + "END_TIMESTAMP BIGINT,\n" + "PARAMS VARCHAR,\n" + "RESULT VARCHAR,\n"
                + "SOME_NEW_COLUMN BIGINT\n"
                + "CONSTRAINT PK PRIMARY KEY (ORG_ID, SOME_ANOTHER_ID, SECOND_ID))\n"
                + "MULTI_TENANT=false,REPLICATION_SCOPE=1,TTL=2000,VERSIONS=1";
        String baseDDL = filePath+"/alter_table_multiple.sql";
        runAndVerify(expected, baseDDL);
    }

    @Test
    // create DDL and alter DDL should be for the same entity
    public void testMismatchedEntityNames() throws Exception {
        String baseDDL = filePath+"/mismatched_entity_name.sql";
        String [] arg = {"-m", "SYNTH", "-d", baseDDL};
        try {
            runSchemaTool(null, arg);
        } catch (Exception e) {
            e.getMessage().equalsIgnoreCase(ENTITY_NAME_IN_BASE_AND_ALTER_DDL_DON_T_MATCH);
        }
    }

    @Test
    public void testUnsupportedStatements() {
        String baseDDL = filePath+"/create_function.sql";
        String [] arg = {"-m", "SYNTH", "-d", baseDDL};
        try {
            runSchemaTool(null, arg);
        } catch (Exception e) {
            e.getMessage().equalsIgnoreCase(UNSUPPORTED_DDL_EXCEPTION);
        }
    }
}
