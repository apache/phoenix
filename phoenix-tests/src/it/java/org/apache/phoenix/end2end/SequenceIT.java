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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SEQUENCE;
import static org.apache.phoenix.query.QueryServicesTestImpl.DEFAULT_SEQUENCE_CACHE_SIZE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesTestImpl;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.schema.SequenceAlreadyExistsException;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.SequenceUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;


@Category(ParallelStatsDisabledTest.class)
public class SequenceIT extends ParallelStatsDisabledIT {
    private static final String SELECT_NEXT_VALUE_SQL = "SELECT NEXT VALUE FOR %s";
    private static final String SCHEMA_NAME = "S";

    private Connection conn;

    private static String generateTableNameWithSchema() {
        return SchemaUtil.getTableName(SCHEMA_NAME, generateUniqueName());
    }

    private static String generateSequenceNameWithSchema() {
        return SchemaUtil.getTableName(SCHEMA_NAME, generateUniqueSequenceName());
    }

    @Before
    public void init() throws Exception {
        createConnection();
    }

    @After
    public void tearDown() throws Exception {
        // close any open connection between tests, so that connections are not leaked
        if (conn != null) {
            boolean refCountLeaked = isAnyStoreRefCountLeaked();
            conn.close();
            assertFalse("refCount leaked", refCountLeaked);
        }
    }

    @Test
    public void testSystemTable() throws Exception {
        conn.createStatement().execute("CREATE SEQUENCE " + generateSequenceNameWithSchema());
        String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM \"SYSTEM\".\"SEQUENCE\"";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
    }

    private static class MyClock extends EnvironmentEdge {
        public volatile long time;

        public MyClock(long time) {
            this.time = time;
        }

        @Override
        public long currentTime() {
            return time;
        }
    }

    @Test
    public void testDuplicateSequences() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();


        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4\n");

        try {
            conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4\n");
            Assert.fail("Duplicate sequences");
        } catch (SequenceAlreadyExistsException e) {

        }
    }

    @Test
    public void testDuplicateSequencesAtSameTimestamp() throws Exception {
        final MyClock clock = new MyClock(1000);
        EnvironmentEdgeManager.injectEdge(clock);
        try {
            String sequenceName = generateSequenceNameWithSchema();

            conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4\n");

            try {
                conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4\n");
                Assert.fail("Duplicate sequences");
            } catch (SequenceAlreadyExistsException e) {

            }
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test
    public void testSequenceNotFound() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        String query = "SELECT NEXT value FOR " + sequenceName;
        try {
            conn.prepareStatement(query).executeQuery();
            fail("Sequence not found");
        } catch (SequenceNotFoundException e) {

        }
    }

    @Test
    public void testCreateSequenceWhenNamespaceEnabled() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection nsConn = DriverManager.getConnection(getUrl(), props);

        String sequenceName = generateSequenceNameWithSchema();
        String sequenceSchemaName = getSchemaName(sequenceName);

        try {
            nsConn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4");
            fail();
        } catch (SchemaNotFoundException e) {
            // expected
        }

        nsConn.createStatement().execute("CREATE SCHEMA " + sequenceSchemaName);
        nsConn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4");
        sequenceSchemaName = "TEST_SEQ_SCHEMA";
        sequenceName = "M_SEQ";
        nsConn.createStatement().execute("CREATE SCHEMA " + sequenceSchemaName);
        nsConn.createStatement().execute("USE " + sequenceSchemaName);
        nsConn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4");
        String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM \"SYSTEM\".\"SEQUENCE\" WHERE sequence_name='"
                + sequenceName + "'";
        ResultSet rs = nsConn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(sequenceSchemaName, rs.getString("sequence_schema"));
        assertEquals(sequenceName, rs.getString("sequence_name"));
        assertEquals(2, rs.getInt("current_value"));
        assertEquals(4, rs.getInt("increment_by"));
        assertFalse(rs.next());
        try {
            nsConn.createStatement().execute(
                    "CREATE SEQUENCE " + sequenceSchemaName + "." + sequenceName + " START WITH 2 INCREMENT BY 4");
            fail();
        } catch (SequenceAlreadyExistsException e) {

        }
    }

    @Test
    public void testCreateSequenceWhenNamespaceEnabledAndIsLowerCase() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection nsConn = DriverManager.getConnection(getUrl(), props);

        String sequenceSchemaName = "\"test_seq_schema\"";
        String sequenceName = "\"m_seq\"";
        nsConn.createStatement().execute("CREATE SCHEMA " + sequenceSchemaName);
        nsConn.createStatement().execute("USE " + sequenceSchemaName);
        nsConn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4");
        String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM \"SYSTEM\".\"SEQUENCE\" WHERE sequence_name='"
                + SchemaUtil.normalizeIdentifier(sequenceName) + "'";
        ResultSet rs = nsConn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(SchemaUtil.normalizeIdentifier(sequenceSchemaName), rs.getString("sequence_schema"));
        assertEquals(SchemaUtil.normalizeIdentifier(sequenceName), rs.getString("sequence_name"));
        assertEquals(2, rs.getInt("current_value"));
        assertEquals(4, rs.getInt("increment_by"));
        assertFalse(rs.next());
        try {
            nsConn.createStatement().execute(
                    "CREATE SEQUENCE " + sequenceSchemaName + "." + sequenceName + " START WITH 2 INCREMENT BY 4");
            fail();
        } catch (SequenceAlreadyExistsException e) {

        }
    }

    @Test
    public void testCreateSequence() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String sequenceNameWithoutSchema = getNameWithoutSchema(sequenceName);
        String schemaName = getSchemaName(sequenceName);

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4");
        int bucketNum = PhoenixRuntime.getTableNoCache(conn, SYSTEM_CATALOG_SCHEMA + "." + TYPE_SEQUENCE).getBucketNum();
        assertEquals("Salt bucket for SYSTEM.SEQUENCE should be test default", bucketNum, QueryServicesTestImpl.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
        String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM \"SYSTEM\".\"SEQUENCE\" WHERE sequence_name='" + sequenceNameWithoutSchema + "'";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(schemaName, rs.getString("sequence_schema"));
        assertEquals(sequenceNameWithoutSchema, rs.getString("sequence_name"));
        assertEquals(2, rs.getInt("current_value"));
        assertEquals(4, rs.getInt("increment_by"));
        assertFalse(rs.next());
    }

    @Test
    public void testCurrentValueFor() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        ResultSet rs;

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4");

        try {
            rs = conn.createStatement().executeQuery("SELECT CURRENT VALUE FOR " + sequenceName);
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_CALL_CURRENT_BEFORE_NEXT_VALUE.getErrorCode(), e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        rs = conn.createStatement().executeQuery("SELECT NEXT VALUE FOR " + sequenceName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT CURRENT VALUE FOR " + sequenceName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
    }

    @Test
    public void testDropSequence() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String sequenceNameWithoutSchema = getNameWithoutSchema(sequenceName);
        String schemaName = getSchemaName(sequenceName);

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 4");
        String query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM \"SYSTEM\".\"SEQUENCE\" WHERE sequence_name='" + sequenceNameWithoutSchema + "'";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(schemaName, rs.getString("sequence_schema"));
        assertEquals(sequenceNameWithoutSchema, rs.getString("sequence_name"));
        assertEquals(2, rs.getInt("current_value"));
        assertEquals(4, rs.getInt("increment_by"));
        assertFalse(rs.next());

        conn.createStatement().execute("DROP SEQUENCE " + sequenceName);
        query = "SELECT sequence_schema, sequence_name, current_value, increment_by FROM \"SYSTEM\".\"SEQUENCE\" WHERE sequence_name='" + sequenceNameWithoutSchema + "'";
        rs = conn.prepareStatement(query).executeQuery();
        assertFalse(rs.next());

        try {
            conn.createStatement().execute("DROP SEQUENCE " + sequenceName);
            fail();
        } catch (SequenceNotFoundException ignore) {
        }
    }

    @Test
    public void testSelectNextValueFor() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY 2");
        assertSequenceValuesForSingleRow(sequenceName, 3, 5, 7);
    }

    @Test
    public void testInsertNextValueFor() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String tableName = generateTableNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 1");
        conn.createStatement().execute("CREATE TABLE " + tableName + " ( id INTEGER NOT NULL PRIMARY KEY)");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (id) VALUES (NEXT VALUE FOR " + sequenceName + ")");
        conn.createStatement().execute("UPSERT INTO " + tableName + " (id) VALUES (NEXT VALUE FOR " + sequenceName + ")");
        conn.commit();
        String query = "SELECT id FROM " + tableName;
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
    }

    @Test
    public void testSequenceCreation() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String sequenceNameWithoutSchema = getNameWithoutSchema(sequenceName);
        String schemaName = getSchemaName(sequenceName);

        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 3 MINVALUE 0 MAXVALUE 10 CYCLE CACHE 5");

        ResultSet rs =
                conn.createStatement()
                        .executeQuery(
                                "SELECT start_with, current_value, increment_by, cache_size, min_value, max_value, cycle_flag, sequence_schema, sequence_name FROM \"SYSTEM\".\"SEQUENCE\" WHERE SEQUENCE_SCHEMA='" + schemaName + "' AND SEQUENCE_NAME='" + sequenceNameWithoutSchema + "'");
        assertTrue(rs.next());
        assertEquals(2, rs.getLong("start_with"));
        assertEquals(2, rs.getInt("current_value"));
        assertEquals(3, rs.getLong("increment_by"));
        assertEquals(5, rs.getLong("cache_size"));
        assertEquals(0, rs.getLong("min_value"));
        assertEquals(10, rs.getLong("max_value"));
        assertEquals(true, rs.getBoolean("cycle_flag"));
        assertEquals(schemaName, rs.getString("sequence_schema"));
        assertEquals(sequenceNameWithoutSchema, rs.getString("sequence_name"));
        assertFalse(rs.next());
        rs =
                conn.createStatement()
                        .executeQuery(
                                "SELECT NEXT VALUE FOR " + sequenceName + ", CURRENT VALUE FOR " + sequenceName);
        assertTrue(rs.next());
        assertEquals(2, rs.getLong(1));
        assertEquals(2, rs.getLong(2));

        assertFalse(rs.next());
        rs =
                conn.createStatement()
                        .executeQuery(
                                "SELECT CURRENT VALUE FOR " + sequenceName + ", NEXT VALUE FOR " + sequenceName);
        assertTrue(rs.next());
        assertEquals(5, rs.getLong(1));
        assertEquals(5, rs.getLong(2));
        assertFalse(rs.next());
    }

    @Test
    public void testSameMultipleSequenceValues() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 4 INCREMENT BY 7");
        String query = "SELECT NEXT VALUE FOR " + sequenceName + ", NEXT VALUE FOR " + sequenceName;
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(4, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testMultipleSequenceValues() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String alternateSequenceName = generateSequenceNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 4 INCREMENT BY 7");
        conn.createStatement().execute("CREATE SEQUENCE " + alternateSequenceName + " START WITH 9 INCREMENT BY 2");

        String query = "SELECT NEXT VALUE FOR " + sequenceName + ", NEXT VALUE FOR " + alternateSequenceName + " FROM " + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE + " LIMIT 2";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4 + 7, rs.getInt(1));
        assertEquals(9 + 2, rs.getInt(2));
        assertFalse(rs.next());

        // Test that sequences don't have gaps (if no other client request the same sequence before we close it)
        Connection conn2 = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        rs = conn2.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4 + 7 * 2, rs.getInt(1));
        assertEquals(9 + 2 * 2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4 + 7 * 3, rs.getInt(1));
        assertEquals(9 + 2 * 3, rs.getInt(2));
        assertFalse(rs.next());
        conn2.close();
    }

    @Test
    public void testMultipleSequencesNoCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String sequenceNameWithoutSchema = getNameWithoutSchema(sequenceName);
        String schemaName = getSchemaName(sequenceName);
        String alternateSequenceName = sequenceName + "_ALT";
        String alternatesequenceNameWithoutSchema = getNameWithoutSchema(alternateSequenceName);

        conn.createStatement().execute(
                "CREATE SEQUENCE " + sequenceName + " START WITH 4 INCREMENT BY 7 MAXVALUE 24");
        conn.createStatement().execute(
                "CREATE SEQUENCE " + alternateSequenceName + " START WITH 9 INCREMENT BY -2 MINVALUE 5");
        String query =
                "SELECT NEXT VALUE FOR " + sequenceName + ", NEXT VALUE FOR " + alternateSequenceName + " FROM " + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE + " LIMIT 2";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4 + 7, rs.getInt(1));
        assertEquals(9 - 2, rs.getInt(2));
        assertFalse(rs.next());

        rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4 + 7 * 2, rs.getInt(1));
        assertEquals(9 - 2 * 2, rs.getInt(2));
        try {
            rs.next();
            fail();
        } catch (SQLException e) {
            SQLException sqlEx1 =
                    SequenceUtil.getException(schemaName, sequenceNameWithoutSchema,
                            SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE);
            SQLException sqlEx2 =
                    SequenceUtil.getException(schemaName, alternatesequenceNameWithoutSchema,
                            SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE);
            verifyExceptions(e, Lists.newArrayList(sqlEx1.getMessage(), sqlEx2.getMessage()));
        }
        conn.close();
    }

    @Test
    public void testMultipleSequencesCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String alternateSequenceName = generateSequenceNameWithSchema();

        conn.createStatement().execute(
                "CREATE SEQUENCE " + sequenceName + " START WITH 4 INCREMENT BY 7 MINVALUE 4 MAXVALUE 19 CYCLE");
        conn.createStatement().execute(
                "CREATE SEQUENCE " + alternateSequenceName + " START WITH 9 INCREMENT BY -2 MINVALUE 5 MAXVALUE 9 CYCLE");

        String query =
                "SELECT NEXT VALUE FOR " + sequenceName + ", NEXT VALUE FOR " + alternateSequenceName + " FROM " + PhoenixDatabaseMetaData.SYSTEM_SEQUENCE + " LIMIT 2";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4 + 7, rs.getInt(1));
        assertEquals(9 - 2, rs.getInt(2));
        assertFalse(rs.next());

        rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4 + 7 * 2, rs.getInt(1));
        assertEquals(9 - 2 * 2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
    }

    @Test
    public void testCompilerOptimization() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String tableName = generateTableNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY 2");
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) IMMUTABLE_ROWS=true");

        conn.createStatement().execute("CREATE INDEX " + generateUniqueName() + " ON " + tableName + "(v1) INCLUDE (v2)");

        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        stmt.optimizeQuery("SELECT k, NEXT VALUE FOR " + sequenceName + " FROM " + tableName + " WHERE v1 = 'bar'");
    }

    @Test
    public void testSelectRowAndSequence() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String tableName = generateTableNameWithSchema();
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 1 INCREMENT BY 4");
        conn.createStatement().execute("CREATE TABLE " + tableName + " ( id INTEGER NOT NULL PRIMARY KEY)");

        conn.createStatement().execute("UPSERT INTO " + tableName + " (id) VALUES (NEXT VALUE FOR " + sequenceName + ")");
        conn.commit();

        String query = "SELECT NEXT VALUE FOR " + sequenceName + ", id FROM " + tableName;
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertFalse(rs.next());
    }

    @Test
    public void testSelectNextValueForOverMultipleBatches() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String tableName = generateTableNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName);
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k BIGINT NOT NULL PRIMARY KEY)");

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(NEXT VALUE FOR " + sequenceName + ")");
        for (int i = 0; i < DEFAULT_SEQUENCE_CACHE_SIZE * 2 + 1; i++) {
            stmt.execute();
        }
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT count(*),max(k) FROM " + tableName);
        assertTrue(rs.next());
        assertEquals(DEFAULT_SEQUENCE_CACHE_SIZE * 2 + 1, rs.getInt(1));
        assertEquals(DEFAULT_SEQUENCE_CACHE_SIZE * 2 + 1, rs.getInt(2));
    }

    @Test
    public void testSelectNextValueForGroupBy() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String tableName1 = generateTableNameWithSchema();
        String tableName2 = generateTableNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName);
        conn.createStatement().execute("CREATE TABLE " + tableName1 + " (k BIGINT NOT NULL PRIMARY KEY, v VARCHAR)");
        conn.createStatement().execute("CREATE TABLE " + tableName2 + " (k BIGINT NOT NULL PRIMARY KEY, v VARCHAR)");

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName1 + " VALUES(NEXT VALUE FOR " + sequenceName + ", ?)");
        stmt.setString(1, "a");
        stmt.execute();
        stmt.setString(1, "a");
        stmt.execute();
        stmt.setString(1, "b");
        stmt.execute();
        stmt.setString(1, "b");
        stmt.execute();
        stmt.setString(1, "c");
        stmt.execute();
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k from " + tableName1);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
        assertFalse(rs.next());


        conn.setAutoCommit(true);
        ;
        conn.createStatement().execute("UPSERT INTO " + tableName2 + " SELECT NEXT VALUE FOR " + sequenceName + ",v FROM " + tableName1 + " GROUP BY v");

        rs = conn.createStatement().executeQuery("SELECT * from " + tableName2);
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));
        assertEquals("a", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
        assertEquals("b", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(8, rs.getInt(1));
        assertEquals("c", rs.getString(2));
        assertFalse(rs.next());
    }

    @Test
    public void testSelectNextValueForMultipleConn() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String tableName = generateTableNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName);
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k BIGINT NOT NULL PRIMARY KEY)");

        Connection conn1 = conn;
        PreparedStatement stmt1 = conn1.prepareStatement("UPSERT INTO " + tableName + " VALUES(NEXT VALUE FOR " + sequenceName + ")");
        for (int i = 0; i < DEFAULT_SEQUENCE_CACHE_SIZE + 1; i++) {
            stmt1.execute();
        }
        conn1.commit();

        Connection conn2 = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO " + tableName + " VALUES(NEXT VALUE FOR " + sequenceName + ")");
        stmt2.execute();
        stmt1.close(); // Should still continue with next value, even on separate connection
        for (int i = 0; i < DEFAULT_SEQUENCE_CACHE_SIZE; i++) {
            stmt2.execute();
        }
        conn2.commit();
        conn2.close();

        // No gaps exist even when sequences were generated from different connections
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM " + tableName);
        for (int i = 0; i < (DEFAULT_SEQUENCE_CACHE_SIZE + 1) * 2; i++) {
            assertTrue(rs.next());
            assertEquals(i + 1, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testSelectNextValueForMultipleConnWithStmtClose() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String tableName = generateTableNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName);
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k BIGINT NOT NULL PRIMARY KEY)");
        PreparedStatement stmt1 = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(NEXT VALUE FOR  " + sequenceName + " )");
        for (int i = 0; i < DEFAULT_SEQUENCE_CACHE_SIZE + 1; i++) {
            stmt1.execute();
        }
        conn.commit();
        stmt1.close();

        Connection conn2 = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO " + tableName + " VALUES(NEXT VALUE FOR  " + sequenceName + " )");
        for (int i = 0; i < DEFAULT_SEQUENCE_CACHE_SIZE + 1; i++) {
            stmt2.execute();
        }
        conn2.commit();
        conn2.close();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM " + tableName);
        for (int i = 0; i < 2 * (DEFAULT_SEQUENCE_CACHE_SIZE + 1); i++) {
            assertTrue(rs.next());
            assertEquals(i + 1, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testSelectNextValueForMultipleConnWithConnClose() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String tableName = generateTableNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName);
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k BIGINT NOT NULL PRIMARY KEY)");

        PreparedStatement stmt1 = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(NEXT VALUE FOR  " + sequenceName + " )");
        for (int i = 0; i < DEFAULT_SEQUENCE_CACHE_SIZE + 1; i++) {
            stmt1.execute();
        }
        conn.commit();

        Connection conn2 = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        ;
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO " + tableName + " VALUES(NEXT VALUE FOR  " + sequenceName + " )");
        for (int i = 0; i < DEFAULT_SEQUENCE_CACHE_SIZE + 1; i++) {
            stmt2.execute();
        }
        conn2.commit();
        conn2.close();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM " + tableName);
        for (int i = 0; i < 2 * (DEFAULT_SEQUENCE_CACHE_SIZE + 1); i++) {
            assertTrue(rs.next());
            assertEquals(i + 1, rs.getInt(1));
        }
        assertFalse(rs.next());
    }

    @Test
    public void testDropCachedSeq1() throws Exception {
        testDropCachedSeq(false);
    }

    @Test
    public void testDropCachedSeq2() throws Exception {
        testDropCachedSeq(true);
    }

    private void testDropCachedSeq(boolean detectDeleteSeqInEval) throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String alternateSequenceName = generateSequenceNameWithSchema();
        String tableName = generateTableNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName);
        conn.createStatement().execute("CREATE SEQUENCE " + alternateSequenceName + " START WITH 101");
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k BIGINT NOT NULL PRIMARY KEY)");

        String stmtStr1a = "UPSERT INTO " + tableName + " VALUES(NEXT VALUE FOR  " + sequenceName + " )";
        PreparedStatement stmt1a = conn.prepareStatement(stmtStr1a);
        stmt1a.execute();
        stmt1a.execute();
        String stmtStr1b = "UPSERT INTO " + tableName + " VALUES(NEXT VALUE FOR " + alternateSequenceName + ")";
        PreparedStatement stmt1b = conn.prepareStatement(stmtStr1b);
        stmt1b.execute();
        stmt1b.execute();
        stmt1b.execute();
        conn.commit();

        Connection conn2 = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        ;
        PreparedStatement stmt2 = conn2.prepareStatement("UPSERT INTO " + tableName + " VALUES(NEXT VALUE FOR " + alternateSequenceName + ")");
        stmt2.execute();
        conn2.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM " + tableName + "");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(101, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(102, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(103, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(104, rs.getInt(1));
        assertFalse(rs.next());

        conn.createStatement().execute("DROP SEQUENCE " + alternateSequenceName);

        stmt1a = conn.prepareStatement(stmtStr1a);
        stmt1a.execute();
        if (!detectDeleteSeqInEval) {
            stmt1a.execute(); // Will allocate new batch for " + sequenceName + " and get error for bar.bas, but ignore it
        }

        stmt1b = conn.prepareStatement(stmtStr1b);
        try {
            stmt1b.execute(); // Will try to get new batch, but fail b/c sequence has been dropped
            fail();
        } catch (SequenceNotFoundException e) {
        }
        conn2.close();
    }

    @Test
    public void testExplainPlanValidatesSequences() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String sequenceNameWithoutSchema = getNameWithoutSchema(sequenceName);
        String tableName = generateTableNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName);
        conn.createStatement().execute("CREATE TABLE " + tableName + " (k BIGINT NOT NULL PRIMARY KEY)");

        Connection conn2 = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        String query = "SELECT NEXT VALUE FOR " + sequenceName + " FROM " + tableName;
        ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
        assertEquals(1,
                explainPlanAttributes.getClientSequenceCount().intValue());

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT sequence_name, current_value FROM \"SYSTEM\".\"SEQUENCE\" WHERE sequence_name='"
                        + sequenceNameWithoutSchema + "'");
        assertTrue(rs.next());
        assertEquals(sequenceNameWithoutSchema, rs.getString(1));
        assertEquals(1, rs.getInt(2));
        conn2.close();

        try {
            conn.createStatement().executeQuery("EXPLAIN SELECT NEXT VALUE FOR zzz FROM " + tableName);
            fail();
        } catch (SequenceNotFoundException e) {
            // expected
        }
        conn.close();
    }

    @Test
    public void testSelectNextValueAsInput() throws Exception {

        String sequenceName = generateSequenceName();
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY 2");
        String query = "SELECT LPAD(ENCODE(NEXT VALUE FOR  " + sequenceName + " ,'base62'),5,'0') FROM \"SYSTEM\".\"SEQUENCE\"";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals("00003", rs.getString(1));
    }

    private String generateSequenceName() {
        return generateUniqueSequenceName();
    }

    @Test
    public void testSelectNextValueInArithmetic() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY 2");

        String query = "SELECT NEXT VALUE FOR  " + sequenceName + " +1";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
    }

    private void createConnection() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
    }

    @Test
    public void testSequenceDefault() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName);

        assertSequenceValuesForSingleRow(sequenceName, 1, 2, 3);
        conn.createStatement().execute("DROP SEQUENCE " + sequenceName);

        sequenceName = generateSequenceNameWithSchema();
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " INCREMENT BY -1");

        assertSequenceValuesForSingleRow(sequenceName, 1, 0, -1);
        conn.createStatement().execute("DROP SEQUENCE " + sequenceName);

        sequenceName = generateSequenceNameWithSchema();
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " MINVALUE 10");

        assertSequenceValuesForSingleRow(sequenceName, 10, 11, 12);
        conn.createStatement().execute("DROP SEQUENCE " + sequenceName);

        sequenceName = generateSequenceNameWithSchema();
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " INCREMENT BY -1 MINVALUE 10 ");

        assertSequenceValuesForSingleRow(sequenceName, Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE - 2);
        conn.createStatement().execute("DROP SEQUENCE " + sequenceName);

        sequenceName = generateSequenceNameWithSchema();
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " MAXVALUE 0");

        assertSequenceValuesForSingleRow(sequenceName, Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 2);
        conn.createStatement().execute("DROP SEQUENCE " + sequenceName);

        sequenceName = generateSequenceNameWithSchema();
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " INCREMENT BY -1 MAXVALUE 0");

        assertSequenceValuesForSingleRow(sequenceName, 0, -1, -2);
    }

    @Test
    public void testSequenceValidateStartValue() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String alternateSequenceName = generateSequenceNameWithSchema();

        try {
            conn.createStatement().execute(
                    "CREATE SEQUENCE " + sequenceName + " START WITH 1 INCREMENT BY 1 MINVALUE 2 MAXVALUE 3");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.STARTS_WITH_MUST_BE_BETWEEN_MIN_MAX_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        try {
            conn.createStatement().execute(
                    "CREATE SEQUENCE " + alternateSequenceName + " START WITH 4 INCREMENT BY 1 MINVALUE 2 MAXVALUE 3");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.STARTS_WITH_MUST_BE_BETWEEN_MIN_MAX_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceValidateMinValue() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        try {
            conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " MINVALUE abc");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MINVALUE_MUST_BE_CONSTANT.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceValidateMaxValue() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        try {
            conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " MAXVALUE null");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.MAXVALUE_MUST_BE_CONSTANT.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceValidateMinValueLessThanOrEqualToMaxValue() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();


        try {
            conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " MINVALUE 2 MAXVALUE 1");
            fail();
        } catch (SQLException e) {
            assertEquals(
                    SQLExceptionCode.MINVALUE_MUST_BE_LESS_THAN_OR_EQUAL_TO_MAXVALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceValidateIncrementConstant() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        try {
            conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " INCREMENT null");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INCREMENT_BY_MUST_BE_CONSTANT.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceValidateIncrementNotEqualToZero() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        try {
            conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " INCREMENT 0");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.INCREMENT_BY_MUST_NOT_BE_ZERO.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceStartWithMinMaxSameValueIncreasingCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY 1 MINVALUE 3 MAXVALUE 3 CYCLE CACHE 1");

        assertSequenceValuesForSingleRow(sequenceName, 3, 3, 3);
    }

    @Test
    public void testSequenceStartWithMinMaxSameValueDecreasingCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY -1 MINVALUE 3 MAXVALUE 3 CYCLE CACHE 2");

        assertSequenceValuesForSingleRow(sequenceName, 3, 3, 3);
    }

    @Test
    public void testSequenceStartWithMinMaxSameValueIncreasingNoCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();


        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY 1 MINVALUE 3 MAXVALUE 3 CACHE 1");

        assertSequenceValuesForSingleRow(sequenceName, 3);
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceStartWithMinMaxSameValueDecreasingNoCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY -1 MINVALUE 3 MAXVALUE 3 CACHE 2");

        assertSequenceValuesForSingleRow(sequenceName, 3);
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceIncreasingCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 3 MINVALUE 1 MAXVALUE 10 CYCLE CACHE 2");
        assertSequenceValuesForSingleRow(sequenceName, 2, 5, 8, 1, 4, 7, 10, 1, 4);
    }

    @Test
    public void testSequenceDecreasingCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY -2 MINVALUE 1 MAXVALUE 10 CYCLE CACHE 2");
        assertSequenceValuesForSingleRow(sequenceName, 3, 1, 10, 8, 6, 4, 2, 10, 8);
    }

    @Test
    public void testSequenceIncreasingNoCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();


        // client throws exception
        conn.createStatement().execute(
                "CREATE SEQUENCE " + sequenceName + " START WITH 2 INCREMENT BY 3 MINVALUE 1 MAXVALUE 10 CACHE 100");
        assertSequenceValuesForSingleRow(sequenceName, 2, 5, 8);
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceIncreasingUsingMaxValueNoCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        // server throws exception
        conn.createStatement().execute(
                "CREATE SEQUENCE " + sequenceName + " START WITH 8 INCREMENT BY 2 MINVALUE 1 MAXVALUE 10 CACHE 2");
        assertSequenceValuesForSingleRow(sequenceName, 8, 10);
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceDecreasingNoCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        // client will throw exception
        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH 4 INCREMENT BY -2 MINVALUE 1 MAXVALUE 10 CACHE 100");
        assertSequenceValuesForSingleRow(sequenceName, 4, 2);
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceDecreasingUsingMinValueNoCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        // server will throw exception
        conn.createStatement().execute(
                "CREATE SEQUENCE " + sequenceName + " START WITH 3 INCREMENT BY -2 MINVALUE 1 MAXVALUE 10 CACHE 2");
        assertSequenceValuesForSingleRow(sequenceName, 3, 1);
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceIncreasingOverflowNoCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        // start with Long.MAX_VALUE
        conn.createStatement().execute(
                "CREATE SEQUENCE " + sequenceName + " START WITH 9223372036854775807 INCREMENT BY 1 CACHE 10");

        assertSequenceValuesForSingleRow(sequenceName, Long.MAX_VALUE);
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceIncreasingOverflowCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        // start with Long.MAX_VALUE
        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH 9223372036854775807 INCREMENT BY 9223372036854775807 CYCLE CACHE 10");
        assertSequenceValuesForSingleRow(sequenceName, Long.MAX_VALUE, Long.MIN_VALUE, -1, Long.MAX_VALUE - 1,
                Long.MIN_VALUE, -1);
    }

    @Test
    public void testSequenceDecreasingOverflowNoCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        // start with Long.MIN_VALUE + 1
        conn.createStatement().execute(
                "CREATE SEQUENCE " + sequenceName + " START WITH -9223372036854775807 INCREMENT BY -1 CACHE 10");
        assertSequenceValuesForSingleRow(sequenceName, Long.MIN_VALUE + 1, Long.MIN_VALUE);
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
            rs.next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testSequenceDecreasingOverflowCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();

        // start with Long.MIN_VALUE + 1
        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH -9223372036854775807 INCREMENT BY -9223372036854775807 CYCLE CACHE 10");
        assertSequenceValuesForSingleRow(sequenceName, Long.MIN_VALUE + 1, Long.MAX_VALUE, 0, Long.MIN_VALUE + 1,
                Long.MAX_VALUE, 0);
    }

    @Test
    public void testMultipleSequenceValuesNoCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String alternateSequenceName = generateSequenceNameWithSchema();

        conn.createStatement().execute(
                "CREATE SEQUENCE " + sequenceName + " START WITH 1 INCREMENT BY 2 MINVALUE 1 MAXVALUE 10 CACHE 2");
        conn.createStatement().execute("CREATE SEQUENCE " + alternateSequenceName);
        assertSequenceValuesMultipleSeq(sequenceName, 1, 3);
        assertSequenceValuesMultipleSeq(sequenceName, 5, 7);

        PreparedStatement stmt = conn.prepareStatement(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(9, rs.getInt(1));
        assertFalse(rs.next());
        try {
            stmt.executeQuery().next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        try {
            stmt.executeQuery().next();
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testMultipleSequenceValuesCycle() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String alternateSequenceName = generateSequenceNameWithSchema();
        conn.createStatement()
                .execute(
                        "CREATE SEQUENCE " + sequenceName + " START WITH 1 INCREMENT BY 2 MINVALUE 1 MAXVALUE 10 CYCLE CACHE 2");
        conn.createStatement().execute("CREATE SEQUENCE " + alternateSequenceName);
        assertSequenceValuesMultipleSeq(sequenceName, 1, 3);
        assertSequenceValuesMultipleSeq(sequenceName, 5, 7);
        assertSequenceValuesMultipleSeq(sequenceName, 9, 1);
        assertSequenceValuesMultipleSeq(sequenceName, 3, 5);
        assertSequenceValuesMultipleSeq(sequenceName, 7, 9);
        assertSequenceValuesMultipleSeq(sequenceName, 1, 3);
        assertSequenceValuesMultipleSeq(sequenceName, 5, 7);
    }

    @Test
    public void testUpsertSelectGroupByWithSequence() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String tableName1 = generateTableNameWithSchema();
        String tableName2 = generateTableNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName);

        conn.createStatement()
                .execute(
                        "CREATE TABLE " + tableName1 + "(event_id BIGINT NOT NULL PRIMARY KEY, user_id char(15), val BIGINT )");
        conn.createStatement()
                .execute(
                        "CREATE TABLE " + tableName2 + " (metric_id char(15) NOT NULL PRIMARY KEY, agg_id char(15), metric_val INTEGER )");


        // 2 rows for user1, 3 rows for user2 and 1 row for user3
        insertEvent(tableName1, 1, "user1", 1);
        insertEvent(tableName1, 2, "user2", 1);
        insertEvent(tableName1, 3, "user1", 1);
        insertEvent(tableName1, 4, "user2", 1);
        insertEvent(tableName1, 5, "user2", 1);
        insertEvent(tableName1, 6, "user3", 1);
        conn.commit();

        conn.createStatement()
                .execute(
                        "UPSERT INTO " + tableName2 + " SELECT 'METRIC_'||(LPAD(ENCODE(NEXT VALUE FOR " + sequenceName + ",'base62'),5,'0')), user_id, sum(val) FROM " + tableName1 + " GROUP BY user_id ORDER BY user_id");
        conn.commit();

        PreparedStatement stmt =
                conn.prepareStatement("SELECT metric_id, agg_id, metric_val FROM " + tableName2);
        ResultSet rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals("METRIC_00001", rs.getString("metric_id"));
        assertEquals("user1", rs.getString("agg_id"));
        assertEquals(2, rs.getLong("metric_val"));
        assertTrue(rs.next());
        assertEquals("METRIC_00002", rs.getString("metric_id"));
        assertEquals("user2", rs.getString("agg_id"));
        assertEquals(3, rs.getLong("metric_val"));
        assertTrue(rs.next());
        assertEquals("METRIC_00003", rs.getString("metric_id"));
        assertEquals("user3", rs.getString("agg_id"));
        assertEquals(1, rs.getLong("metric_val"));
        assertFalse(rs.next());
    }

    @Test
    /**
     * Test to validate that the bug discovered in PHOENIX-2149 has been fixed. There was an issue
     * whereby, when closing connections and returning sequences we were not setting the limit
     * reached flag correctly and this was causing the max value to be ignored as the LIMIT_REACHED_FLAG
     * value was being unset from true to false.
     */
    public void testNextValuesForSequenceClosingConnections() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        // Create Sequence
        conn.createStatement().execute("CREATE SEQUENCE " + sequenceName + " START WITH 4990 MINVALUE 4990 MAXVALUE 5000 CACHE 10");

        // Call NEXT VALUE FOR 1 time more than available values in the Sequence. We expected the final time
        // to throw an error as we will have reached the max value
        try {
            long val = 0L;
            for (int i = 0; i <= 11; i++) {
                ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
                rs.next();
                val = rs.getLong(1);

            }
            fail("Expect to fail as we have arrived at the max sequence value " + val);
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                    e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    private void insertEvent(String tableName, long id, String userId, long val) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        stmt.setLong(1, id);
        stmt.setString(2, userId);
        stmt.setLong(3, val);
        stmt.execute();
    }

    /**
     * Helper to verify the sequence values returned in multiple ResultSets each containing one row
     *
     * @param seqVals expected sequence values (one per ResultSet)
     */
    private void assertSequenceValuesForSingleRow(String sequenceName, long... seqVals)
            throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
        for (long seqVal : seqVals) {
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(seqVal, rs.getLong(1));
            assertFalse(rs.next());
            rs.close();
        }
        stmt.close();
    }

    /**
     * Helper to verify the sequence values returned in a single ResultSet containing multiple row
     *
     * @param seqVals expected sequence values (from one ResultSet)
     */
    private void assertSequenceValuesMultipleSeq(String sequenceName, long... seqVals) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
        for (long seqVal : seqVals) {
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(seqVal, rs.getLong(1));
            assertFalse(rs.next());
        }
        stmt.close();
    }

    private void verifyExceptions(SQLException sqlE, List<String> expectedExceptions) {
        List<String> missingExceptions = Lists.newArrayList(expectedExceptions);
        List<String> unexpectedExceptions = Lists.newArrayList();
        do {
            if (!expectedExceptions.contains(sqlE.getMessage())) {
                unexpectedExceptions.add(sqlE.getMessage());
            }
            missingExceptions.remove(sqlE.getMessage());
        } while ((sqlE = sqlE.getNextException()) != null);
        if (unexpectedExceptions.size() != 0 && missingExceptions.size() != 0) {
            fail("Actual exceptions does not match expected exceptions. Unexpected exceptions : "
                    + unexpectedExceptions + " missing exceptions : " + missingExceptions);
        }
    }

    @Test
    public void testValidateBeforeReserve() throws Exception {

        String tableName = generateTableNameWithSchema();
        String seqName = generateSequenceNameWithSchema();

        conn.createStatement().execute(
                "CREATE TABLE " + tableName + " (k VARCHAR PRIMARY KEY, l BIGINT)");
        conn.createStatement().execute(
                "CREATE SEQUENCE " + seqName);


        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT NEXT VALUE FOR " + seqName + " FROM " + tableName);
        assertTrue(rs.next());
        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " VALUES ('a', NEXT VALUE FOR " + seqName + ")");
        conn.createStatement().execute(
                "UPSERT INTO " + tableName + " VALUES ('b', NEXT VALUE FOR " + seqName + ")");
        conn.commit();

        rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals(1, rs.getLong(2));
        assertTrue(rs.next());
        assertEquals("b", rs.getString(1));
        assertEquals(2, rs.getLong(2));
        assertFalse(rs.next());


        PreparedStatement stmt = conn.prepareStatement("SELECT NEXT VALUE FOR " + seqName + " FROM " + tableName);
        ParameterMetaData md = stmt.getParameterMetaData();
        assertEquals(0, md.getParameterCount());
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(3, rs.getLong(1));
        assertTrue(rs.next());
        assertEquals(4, rs.getLong(1));
        assertFalse(rs.next());
    }

    @Test
    public void testNoFromClause() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String alternateSequenceName = generateSequenceNameWithSchema();

        ResultSet rs;

        String seqName = sequenceName;
        String secondSeqName = alternateSequenceName;
        conn.createStatement().execute("CREATE SEQUENCE " + seqName + " START WITH 1 INCREMENT BY 1");
        conn.createStatement().execute("CREATE SEQUENCE " + secondSeqName + " START WITH 2 INCREMENT BY 3");

        String query = "SELECT NEXT VALUE FOR " + seqName;
        ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
        assertEquals(new Integer(1),
                explainPlanAttributes.getClientSequenceCount());

        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        query = "SELECT CURRENT VALUE FOR " + seqName;
        plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
        explainPlanAttributes = plan.getPlanStepsAsAttributes();
        assertEquals(new Integer(1),
                explainPlanAttributes.getClientSequenceCount());

        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT NEXT VALUE FOR " + seqName + ", NEXT VALUE FOR " + secondSeqName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        rs = conn.createStatement().executeQuery("SELECT CURRENT VALUE FOR " + seqName + ", NEXT VALUE FOR " + secondSeqName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(5, rs.getInt(2));
    }

    @Test
    public void testBug6574() throws Exception {
        String sequenceName = generateSequenceNameWithSchema();
        String tableName = generateTableNameWithSchema();

        try(Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SEQUENCE " + sequenceName);
            stmt.execute("CREATE TABLE " + tableName + " (id integer primary key)");

            String query = "SELECT * FROM SYSTEM.\"SEQUENCE\" where SEQUENCE_NAME = '" + getNameWithoutSchema(sequenceName) + "'";
            ResultSet rs = stmt.executeQuery(query);
            assertTrue(rs.next());
            rs.close();

            stmt.execute("DROP TABLE " + tableName);
            rs = stmt.executeQuery(query);
            assertTrue(rs.next());
            rs.close();
        }
    }

    private static String getSchemaName(String tableName) {
        return tableName.substring(0, tableName.indexOf("."));
    }

    private static String getNameWithoutSchema(String tableName) {
        return tableName.substring(tableName.indexOf(".") + 1);
    }

}