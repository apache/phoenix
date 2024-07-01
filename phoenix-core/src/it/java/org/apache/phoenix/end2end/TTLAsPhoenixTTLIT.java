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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TTLExpression;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX;
import static org.apache.phoenix.exception.SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY;
import static org.apache.phoenix.exception.SQLExceptionCode.TTL_SUPPORTED_FOR_TABLES_AND_VIEWS_ONLY;
import static org.apache.phoenix.schema.TTLExpression.TTL_EXPRESSION_NOT_DEFINED;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class TTLAsPhoenixTTLIT extends ParallelStatsDisabledIT{

    private static final String DDL_TEMPLATE = "CREATE TABLE IF NOT EXISTS %s "
            + "("
            + " ID INTEGER NOT NULL,"
            + " COL1 INTEGER NOT NULL,"
            + " COL2 bigint NOT NULL,"
            + " CREATED_DATE DATE,"
            + " CREATION_TIME TIME,"
            + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
            + ")";

    private static final String DEFAULT_DDL_OPTIONS = "MULTI_TENANT=true";

    private static final int DEFAULT_TTL_FOR_TEST = 86400;
    private static final int DEFAULT_TTL_FOR_CHILD = 10000;
    private static final int DEFAULT_TTL_FOR_ALTER = 7000;
    private static final String DEFAULT_TTL_EXPRESSION = "CURRENT_TIME() - cREATION_TIME > 500"; // case-insensitive comparison
    private static final String DEFAULT_TTL_EXPRESSION_FOR_ALTER =
            "CURRENT_TIME() - PHOENIX_ROW_TIMESTAMP() > 100";

    private boolean useExpression;
    private TTLExpression defaultTTL;
    private String defaultTTLDDLOption;
    private TTLExpression alterTTL;
    private String alterTTLDDLOption;

    public TTLAsPhoenixTTLIT(boolean useExpression) {
        this.useExpression = useExpression;
        this.defaultTTL = useExpression ?
                TTLExpression.create(DEFAULT_TTL_EXPRESSION) :
                TTLExpression.create(DEFAULT_TTL_FOR_TEST);
        this.defaultTTLDDLOption = useExpression ?
                String.format("'%s'", DEFAULT_TTL_EXPRESSION) :
                String.valueOf(DEFAULT_TTL_FOR_TEST);
        this.alterTTL = useExpression ?
                TTLExpression.create(DEFAULT_TTL_EXPRESSION_FOR_ALTER) :
                TTLExpression.create(DEFAULT_TTL_FOR_ALTER);
        this.alterTTLDDLOption = useExpression ?
                String.format("'%s'", DEFAULT_TTL_EXPRESSION_FOR_ALTER) :
                String.valueOf(DEFAULT_TTL_FOR_ALTER);
    }

    @Parameterized.Parameters(name = "useExpression={0}")
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][]{
                {false}, {true}
        });
    }

    /**
     * test TTL is being set as PhoenixTTL when PhoenixTTL is enabled.
     */
    @Test
    public void testCreateTableWithTTL() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());) {
            PTable table = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null,
                    createTableWithOrWithOutTTLAsItsProperty(conn, true)));
            assertTTLValue(table, defaultTTL);
            assertTrue("RowKeyMatcher should be Null",
                    (Bytes.compareTo(HConstants.EMPTY_BYTE_ARRAY, table.getRowKeyMatcher()) == 0));
        }
    }

    @Test
    public void testCreateTableWithNoTTL() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());) {
            PTable table = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null,
                    createTableWithOrWithOutTTLAsItsProperty(conn, false)));
            assertTTLValue(table, TTL_EXPRESSION_NOT_DEFINED);
        }
    }

    @Test
    public void testSwitchingTTLFromCondToValue() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = createTableWithOrWithOutTTLAsItsProperty(conn, true);
            PTable table = conn.unwrap(PhoenixConnection.class).
                    getTable(new PTableKey(null, tableName));
            assertTTLValue(table, defaultTTL);
            // Switch from cond ttl to value or vice versa
            String alterTTL = useExpression ? String.valueOf(DEFAULT_TTL_FOR_ALTER) :
                    String.format("'%s'", DEFAULT_TTL_EXPRESSION_FOR_ALTER);
            String alterDDL = "ALTER TABLE " + tableName + " SET TTL = " + alterTTL;
            conn.createStatement().execute(alterDDL);
            TTLExpression expected = useExpression ?
                    TTLExpression.create(DEFAULT_TTL_FOR_ALTER) :
                    TTLExpression.create(DEFAULT_TTL_EXPRESSION_FOR_ALTER);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), expected, tableName);
        }
    }

    /**
     * Tests that when: 1) DDL has both pk as well as key value columns 2) Key value columns have
     *      * both default and explicit column family names 3) TTL specifier doesn't have column family
     *      * name. Then it should not affect TTL being set at Phoenix Level.
     */
    @Test
    public void  testCreateTableWithTTLWithDifferentColumnFamilies() throws  Exception {
        String tableName = generateUniqueName();
        String ttlExpr = "id = 'x' AND b.col2 = 7";
        String quotedTTLExpr = "id = ''x'' AND b.col2 = 7"; // to retain single quotes in DDL
        int ttlValue = 86400;
        String ttl = (useExpression ?
                String.format("'%s'", quotedTTLExpr) :
                String.valueOf(ttlValue));
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " b.col2 bigint," + " col3 bigint, "
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                        + " ) TTL=" + ttl;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            TTLExpression expected = useExpression ?
                    TTLExpression.create(ttlExpr) : TTLExpression.create(ttlValue);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), expected, tableName);
        }
        //Setting TTL should not be stored as CF Descriptor properties when
        //phoenix.table.ttl.enabled is true
        Admin admin = driver.getConnectionQueryServices(getUrl(), new Properties()).getAdmin();
        ColumnFamilyDescriptor[] columnFamilies =
                admin.getDescriptor(TableName.valueOf(tableName)).getColumnFamilies();
        assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_TTL, columnFamilies[0].getTimeToLive());
    }

    @Test
    public void testCreateAndAlterTableDDLWithForeverAndNoneTTLValues() throws Exception {
        if (useExpression) {
            return;
        }
        String tableName = generateUniqueName();
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " b.col2 bigint," + " col3 bigint, "
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                        + " ) TTL=FOREVER";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            assertTTLValue(conn.unwrap(PhoenixConnection.class),
                    TTLExpression.TTL_EXPRESSION_FORVER, tableName);

            ddl = "ALTER TABLE  " + tableName + " SET TTL=NONE";
            conn.createStatement().execute(ddl);
            assertTTLValue(conn.unwrap(PhoenixConnection.class),
                    TTL_EXPRESSION_NOT_DEFINED, tableName);
            //Setting TTL should not be stored as CF Descriptor properties when
            //phoenix.table.ttl.enabled is true
            Admin admin = driver.getConnectionQueryServices(getUrl(), new Properties()).getAdmin();
            ColumnFamilyDescriptor[] columnFamilies =
                    admin.getDescriptor(TableName.valueOf(tableName)).getColumnFamilies();
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_TTL, columnFamilies[0].getTimeToLive());

            tableName = generateUniqueName();
            ddl =
                    "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                            + " col1 integer NOT NULL," + " b.col2 bigint," + " col3 bigint, "
                            + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                            + " ) TTL=NONE";
            conn.createStatement().execute(ddl);
            assertTTLValue(conn.unwrap(PhoenixConnection.class),
                    TTL_EXPRESSION_NOT_DEFINED, tableName);

            ddl = "ALTER TABLE  " + tableName + " SET TTL=FOREVER";
            conn.createStatement().execute(ddl);
            assertTTLValue(conn.unwrap(PhoenixConnection.class),
                    TTLExpression.TTL_EXPRESSION_FORVER, tableName);
            //Setting TTL should not be stored as CF Descriptor properties when
            //phoenix.table.ttl.enabled is true
            columnFamilies =
                    admin.getDescriptor(TableName.valueOf(tableName)).getColumnFamilies();
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_TTL, columnFamilies[0].getTimeToLive());
        }
    }

    @Test
    public void testSettingTTLAsAlterTableCommand() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), new Properties());
             PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);){
            String tableName = createTableWithOrWithOutTTLAsItsProperty(conn, false);
            //Checking Default TTL in case of PhoenixTTLEnabled
            assertTTLValue(conn.unwrap(PhoenixConnection.class), TTL_EXPRESSION_NOT_DEFINED, tableName);

            String ddl = "ALTER TABLE  " + tableName + " SET TTL = " + this.alterTTLDDLOption;
            conn.createStatement().execute(ddl);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), this.alterTTL, tableName);
            //Asserting TTL should not be stored as CF Descriptor properties when
            //phoenix.table.ttl.enabled is true
            Admin admin = driver.getConnectionQueryServices(getUrl(), new Properties()).getAdmin();
            ColumnFamilyDescriptor[] columnFamilies =
                    admin.getDescriptor(TableName.valueOf(tableName)).getColumnFamilies();
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_TTL, columnFamilies[0].getTimeToLive());
        }
    }

    @Test
    public void testSettingTTLForIndexes() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = createTableWithOrWithOutTTLAsItsProperty(conn, true);

            //By default, Indexes should set TTL what Base Table has
            createIndexOnTableOrViewProvidedWithTTL(conn, tableName, PTable.IndexType.LOCAL, false);
            createIndexOnTableOrViewProvidedWithTTL(conn, tableName, PTable.IndexType.GLOBAL, false);
            List<PTable> indexes = PhoenixRuntime.getTable(conn, tableName).getIndexes();
            for (PTable index : indexes) {
                assertTTLValue(index, defaultTTL);
            }

            tableName = createTableWithOrWithOutTTLAsItsProperty(conn, false);

            String localIndexName = createIndexOnTableOrViewProvidedWithTTL(conn, tableName, PTable.IndexType.LOCAL, false);
            String globalIndexName = createIndexOnTableOrViewProvidedWithTTL(conn, tableName, PTable.IndexType.GLOBAL, false);
            indexes = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, tableName)).getIndexes();
            for (PTable index : indexes) {
                assertTTLValue(index, TTL_EXPRESSION_NOT_DEFINED);
                assertTrue(Bytes.compareTo(
                        index.getRowKeyMatcher(), HConstants.EMPTY_BYTE_ARRAY) == 0
                );
            }

            //Test setting TTL as index property not allowed while creating them or setting them explicitly.
            String ttl = (useExpression ?
                    String.format("'%s'",DEFAULT_TTL_EXPRESSION) :
                    String.valueOf(1000));
            try {
                conn.createStatement().execute("ALTER TABLE " + localIndexName + " SET TTL = " + ttl);
                fail();
            } catch (SQLException sqe) {
                assertEquals("Should fail with cannot set or alter property for index",
                        CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(), sqe.getErrorCode());
            }

            try {
                conn.createStatement().execute("ALTER TABLE " + globalIndexName + " SET TTL = " + ttl);
                fail();
            } catch (SQLException sqe) {
                assertEquals("Should fail with cannot set or alter property for index",
                        CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(), sqe.getErrorCode());
            }

            try {
                createIndexOnTableOrViewProvidedWithTTL(conn, tableName, PTable.IndexType.LOCAL, true);
                fail();
            } catch (SQLException sqe) {
                assertEquals("Should fail with cannot set or alter property for index",
                        CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(), sqe.getErrorCode());
            }

            try {
                createIndexOnTableOrViewProvidedWithTTL(conn, tableName, PTable.IndexType.GLOBAL, true);
                fail();
            } catch (SQLException sqe) {
                assertEquals("Should fail with cannot set or alter property for index",
                        CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode(), sqe.getErrorCode());
            }
        }
    }

    @Test
    public void testConditionalTTLDDL() throws Exception {
        if (!useExpression) {
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            String ddl = "CREATE TABLE %s (ID1 VARCHAR NOT NULL, ID2 INTEGER NOT NULL, COL1 VARCHAR, G.COL2 DATE " +
                    "CONSTRAINT PK PRIMARY KEY(ID1, ID2)) TTL = '%s'";
            try {
                conn.createStatement().execute(String.format(ddl, tableName, "ID2 = 12 OR UNKNOWN_COLUMN = 67"));
                fail("Should have thrown ColumnNotFoundException");
            } catch (ColumnNotFoundException e) {
            }
            String ttl = "ID2 = 34 AND G.COL2 > CURRENT_DATE() + 1000";
            conn.createStatement().execute(String.format(ddl, tableName, ttl));
            TTLExpression expected = TTLExpression.create(ttl);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), expected, tableName);

            conn.createStatement().execute(String.format("ALTER TABLE %s SET TTL=NONE", tableName));
            assertTTLValue(conn.unwrap(PhoenixConnection.class), TTL_EXPRESSION_NOT_DEFINED, tableName);

            try {
                conn.createStatement().execute(String.format("ALTER TABLE %s SET TTL='%s'",
                        tableName, "UNKNOWN_COLUMN=67"));
                fail("Alter table should have thrown ColumnNotFoundException");
            } catch (ColumnNotFoundException e) {

            }

            String viewName = generateUniqueName();
            ddl = "CREATE VIEW %s ( VINT SMALLINT) AS SELECT * FROM %s TTL='%s'";
            ttl = "F.ID2 = 124";
            try {
                conn.createStatement().execute(String.format(ddl, viewName, tableName, ttl));
                fail("Should have thrown ColumnFamilyNotFoundException");
            } catch (ColumnFamilyNotFoundException e) {

            }
            ttl = "G.COL2 > CURRENT_DATE() + 200 AND VINT > 123";
            conn.createStatement().execute(String.format(ddl, viewName, tableName, ttl));
            expected = TTLExpression.create(ttl);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), expected, viewName);

            ttl = "G.COL2 > CURRENT_DATE() + 500 AND VINT > 123";
            conn.createStatement().execute(String.format("ALTER VIEW %s SET TTL='%s'",
                    viewName, ttl));
            expected = TTLExpression.create(ttl);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), expected, viewName);
        }
    }

    @Test
    public void testSettingTTLForViewsOnTableWithTTL() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tenantID = generateUniqueName().substring(1);
            String tenantID1 = generateUniqueName().substring(1);

            Properties props = new Properties();
            props.setProperty(TENANT_ID_ATTRIB, tenantID);
            Connection tenantConn = DriverManager.getConnection(getUrl(), props);

            Properties props1 = new Properties();
            props1.setProperty(TENANT_ID_ATTRIB, tenantID1);
            Connection tenantConn1 = DriverManager.getConnection(getUrl(), props1);

            String tableName = createTableWithOrWithOutTTLAsItsProperty(conn, true);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), defaultTTL, tableName);

            //Setting TTL on views is not allowed if Table already has TTL
            try {
                createUpdatableViewOnTableWithTTL(conn, tableName, true);
                fail();
            } catch (SQLException sqe) {
                assertEquals("Should fail with TTL already defined in hierarchy",
                        TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode(), sqe.getErrorCode());
            }

            //TTL is only supported for Table and Updatable Views
            try {
                createReadOnlyViewOnTableWithTTL(conn, tableName, true);
                fail();
            } catch (SQLException sqe) {
                assertEquals("Should have failed with TTL supported on Table and Updatable" +
                        "View only", TTL_SUPPORTED_FOR_TABLES_AND_VIEWS_ONLY.getErrorCode(), sqe.getErrorCode());
            }

            //View should have gotten TTL from parent table.
            String viewName = createUpdatableViewOnTableWithTTL(conn, tableName, false);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), defaultTTL, viewName);

            //Child View's PTable gets TTL from parent View's PTable which gets from Table.
            String childView = createViewOnViewWithTTL(tenantConn, viewName, false);
            assertTTLValue(tenantConn.unwrap(PhoenixConnection.class), defaultTTL, childView);

            String childView1 = createViewOnViewWithTTL(tenantConn1, viewName, false);
            assertTTLValue(tenantConn1.unwrap(PhoenixConnection.class), defaultTTL, childView1);

            createIndexOnTableOrViewProvidedWithTTL(conn, viewName, PTable.IndexType.GLOBAL,
                    false);
            createIndexOnTableOrViewProvidedWithTTL(conn, viewName, PTable.IndexType.LOCAL,
                    false);

            List<PTable> indexes = PhoenixRuntime.getTable(
                    conn.unwrap(PhoenixConnection.class), viewName).getIndexes();

            for (PTable index : indexes) {
                assertTTLValue(index, defaultTTL);
            }

            createIndexOnTableOrViewProvidedWithTTL(conn, tableName, PTable.IndexType.GLOBAL, false);
            List<PTable> tIndexes = PhoenixRuntime.getTable(
                    conn.unwrap(PhoenixConnection.class), tableName).getIndexes();

            for (PTable index : tIndexes) {
                assertTTLValue(index, defaultTTL);
            }
        }
    }

    @Test
    public void testAlteringTTLToNONEAndThenSettingAtAnotherLevel() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tenantID = generateUniqueName().substring(1);

            Properties props = new Properties();
            props.setProperty(TENANT_ID_ATTRIB, tenantID);
            Connection tenantConn = DriverManager.getConnection(getUrl(), props);

            String tableName = createTableWithOrWithOutTTLAsItsProperty(conn, true);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), defaultTTL, tableName);

            //Setting TTL on views is not allowed if Table already has TTL
            try {
                createUpdatableViewOnTableWithTTL(conn, tableName, true);
                fail();
            } catch (SQLException sqe) {
                assertEquals("Should fail with TTL already defined in hierarchy",
                        TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode(), sqe.getErrorCode());
            }

            String ddl = "ALTER TABLE " + tableName + " SET TTL=NONE";
            conn.createStatement().execute(ddl);

            assertTTLValue(conn.unwrap(PhoenixConnection.class),
                    TTL_EXPRESSION_NOT_DEFINED, tableName);

            String viewName = createUpdatableViewOnTableWithTTL(conn, tableName, true);
            TTLExpression expectedChildTTl = useExpression ?
                    TTLExpression.create(DEFAULT_TTL_EXPRESSION) :
                    TTLExpression.create(DEFAULT_TTL_FOR_CHILD);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), expectedChildTTl, viewName);

            try {
                createViewOnViewWithTTL(tenantConn, viewName, true);
                fail();
            } catch (SQLException sqe) {
                assertEquals("Should fail with TTL already defined in hierarchy",
                        TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode(), sqe.getErrorCode());
            }

            String ttlAlter = (useExpression ?
                    String.format("'%s'", DEFAULT_TTL_EXPRESSION_FOR_ALTER) :
                    String.valueOf(DEFAULT_TTL_FOR_ALTER));
            try {
                ddl = "ALTER TABLE " + tableName + " SET TTL=" + ttlAlter;
                conn.createStatement().execute(ddl);
            } catch (SQLException sqe) {
                assertEquals("Should fail with TTL already defined in hierarchy",
                        TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode(), sqe.getErrorCode());
            }

            ddl = "ALTER VIEW " + viewName + " SET TTL=NONE";
            conn.createStatement().execute(ddl);

            String childView = createViewOnViewWithTTL(tenantConn, viewName, true);
            assertTTLValue(tenantConn.unwrap(PhoenixConnection.class), expectedChildTTl, childView);

            ddl = "ALTER VIEW " + childView + " SET TTL=NONE";
            tenantConn.createStatement().execute(ddl);

            assertTTLValue(tenantConn.unwrap(PhoenixConnection.class),
                    TTL_EXPRESSION_NOT_DEFINED, childView);

            ddl = "ALTER VIEW " + viewName + " SET TTL=" + ttlAlter;
            conn.createStatement().execute(ddl);
            TTLExpression expectedAlterTTl = useExpression ?
                    TTLExpression.create(DEFAULT_TTL_EXPRESSION_FOR_ALTER) :
                    TTLExpression.create(DEFAULT_TTL_FOR_ALTER);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), expectedAlterTTl, viewName);
        }
    }

    @Test
    public void testAlteringTTLAtOneLevelAndCheckingAtAnotherLevel() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tenantID = generateUniqueName().substring(1);
            String tenantID1 = generateUniqueName().substring(1);

            Properties props = new Properties();
            props.setProperty(TENANT_ID_ATTRIB, tenantID);
            Connection tenantConn = DriverManager.getConnection(getUrl(), props);

            Properties props1 = new Properties();
            props1.setProperty(TENANT_ID_ATTRIB, tenantID1);
            Connection tenantConn1 = DriverManager.getConnection(getUrl(), props1);

            String tableName = createTableWithOrWithOutTTLAsItsProperty(conn, true);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), defaultTTL, tableName);

            //View should have gotten TTL from parent table.
            String viewName = createUpdatableViewOnTableWithTTL(conn, tableName, false);
            assertTTLValue(conn.unwrap(PhoenixConnection.class), defaultTTL, viewName);

            //Child View's PTable gets TTL from parent View's PTable which gets from Table.
            String childView = createViewOnViewWithTTL(tenantConn, viewName, false);
            assertTTLValue(tenantConn.unwrap(PhoenixConnection.class), defaultTTL, childView);

            String childView1 = createViewOnViewWithTTL(tenantConn1, viewName, false);
            assertTTLValue(tenantConn1.unwrap(PhoenixConnection.class), defaultTTL, childView1);

            String ttlAlter = (useExpression ?
                    String.format("'%s'", DEFAULT_TTL_EXPRESSION_FOR_ALTER) :
                    String.valueOf(DEFAULT_TTL_FOR_ALTER));
            String alter = "ALTER TABLE " + tableName + " SET TTL = " + ttlAlter;
            conn.createStatement().execute(alter);

            //Clear Cache for all Tables to reflect Alter TTL commands in hierarchy
            clearCache(conn, null, tableName);
            clearCache(conn, null, viewName);
            clearCache(tenantConn, null, childView);
            clearCache(tenantConn1, null, childView1);

            TTLExpression expectedAlterTTl = useExpression ?
                    TTLExpression.create(DEFAULT_TTL_EXPRESSION_FOR_ALTER) :
                    TTLExpression.create(DEFAULT_TTL_FOR_ALTER);
            //Assert TTL for each entity again with altered value
            assertTTLValue(conn.unwrap(PhoenixConnection.class), expectedAlterTTl, viewName);
            assertTTLValue(tenantConn.unwrap(PhoenixConnection.class), expectedAlterTTl, childView);
            assertTTLValue(tenantConn1.unwrap(PhoenixConnection.class), expectedAlterTTl, childView1);
        }
    }

    private void assertTTLValue(PhoenixConnection conn, TTLExpression expected, String name) throws SQLException {
        assertEquals("TTL value did not match :-", expected,
                PhoenixRuntime.getTableNoCache(conn, name).getTTL());
    }

    private void assertTTLValue(PTable table, TTLExpression expected) {
        assertEquals("TTL value did not match :-", expected, table.getTTL());
    }

    private String createTableWithOrWithOutTTLAsItsProperty(Connection conn, boolean withTTL) throws SQLException {
        String tableName = generateUniqueName();
        StringBuilder ddl = new StringBuilder();
        ddl.append(String.format(DDL_TEMPLATE, tableName));
        ddl.append(DEFAULT_DDL_OPTIONS);
        if (withTTL) {
            ddl.append(", TTL = " + this.defaultTTLDDLOption);
        }
        conn.createStatement().execute(ddl.toString());
        return tableName;
    }

    private String createIndexOnTableOrViewProvidedWithTTL(Connection conn, String baseTableOrViewName, PTable.IndexType indexType,
                                                           boolean withTTL) throws SQLException {
        String ttl = (useExpression ?
                String.format("'%s'", DEFAULT_TTL_EXPRESSION) :
                String.valueOf(DEFAULT_TTL_FOR_CHILD));
        switch (indexType) {
            case LOCAL:
                String localIndexName = baseTableOrViewName + "_Local_" + generateUniqueName();
                conn.createStatement().execute("CREATE LOCAL INDEX " + localIndexName + " ON " +
                        baseTableOrViewName + " (COL2) " + (withTTL ? "TTL = " + ttl : ""));
                return localIndexName;

            case GLOBAL:
                String globalIndexName = baseTableOrViewName + "_Global_" + generateUniqueName();
                conn.createStatement().execute("CREATE INDEX " + globalIndexName + " ON " +
                        baseTableOrViewName + " (COL2) " + (withTTL ? "TTL = " + ttl : ""));
                return globalIndexName;

            default:
                return baseTableOrViewName;
        }
    }

    private String createReadOnlyViewOnTableWithTTL(Connection conn, String baseTableName,
                                            boolean withTTL) throws SQLException {
        String ttl = (useExpression ?
                String.format("'%s'", DEFAULT_TTL_EXPRESSION) :
                String.valueOf(DEFAULT_TTL_FOR_CHILD));
        String viewName = "VIEW_" + baseTableName + "_" + generateUniqueName();
        conn.createStatement().execute("CREATE VIEW " + viewName
                + " (" + generateUniqueName() + " SMALLINT) as select * from "
                + baseTableName + " where COL1 > 1 "
                + (withTTL ? "TTL = " + ttl  : "") );
        return viewName;
    }

    private String createUpdatableViewOnTableWithTTL(Connection conn, String baseTableName,
                                            boolean withTTL) throws SQLException {
        String ttl = (useExpression ?
                String.format("'%s'", DEFAULT_TTL_EXPRESSION) :
                String.valueOf(DEFAULT_TTL_FOR_CHILD));
        String viewName = "VIEW_" + baseTableName + "_" + generateUniqueName();
        conn.createStatement().execute("CREATE VIEW " + viewName
                + " (" + generateUniqueName() + " SMALLINT) as select * from "
                + baseTableName + " where COL1 = 1 "
                + (withTTL ? "TTL = " + ttl : "") );
        return viewName;
    }

    private String createViewOnViewWithTTL(Connection conn, String parentViewName,
                                           boolean withTTL) throws SQLException {
        String ttl = (useExpression ?
                String.format("'%s'", DEFAULT_TTL_EXPRESSION) :
                String.valueOf(DEFAULT_TTL_FOR_CHILD));
        String childView = parentViewName + "_" + generateUniqueName();
        conn.createStatement().execute("CREATE VIEW " + childView +
                " (E BIGINT, F BIGINT) AS SELECT * FROM " + parentViewName +
                (withTTL ? " TTL = " + ttl : ""));
        return childView;
    }

    /**
     * TODO :- We are externally calling clearCache for Alter Table scenario, remove this after
     * https://issues.apache.org/jira/browse/PHOENIX-7135 is completed.
     * @throws SQLException
     */

    public static void clearCache(Connection tenantConnection, String schemaName, String tableName) throws SQLException {

        PhoenixConnection currentConnection = tenantConnection.unwrap(PhoenixConnection.class);
        PName tenantIdName = currentConnection.getTenantId();
        String tenantId = tenantIdName == null ? "" : tenantIdName.getString();

        // Clear server side cache
        currentConnection.unwrap(PhoenixConnection.class).getQueryServices().clearTableFromCache(
                Bytes.toBytes(tenantId), schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY :
                        Bytes.toBytes(schemaName), Bytes.toBytes(tableName), 0);

        // Clear connection cache
        currentConnection.getMetaDataCache().removeTable(currentConnection.getTenantId(),
                String.format("%s.%s", schemaName, tableName), null, 0);
    }
}
