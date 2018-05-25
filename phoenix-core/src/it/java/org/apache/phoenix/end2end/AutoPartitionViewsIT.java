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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AutoPartitionViewsIT extends ParallelStatsDisabledIT {

    private String tableDDLOptions;
    private boolean isMultiTenant;
    private final String TENANT_SPECIFIC_URL1 = getUrl() + ';' + PhoenixRuntime.TENANT_ID_ATTRIB
            + "=tenant1";
    private final String TENANT_SPECIFIC_URL2 = getUrl() + ';' + PhoenixRuntime.TENANT_ID_ATTRIB
            + "=tenant2";

    @Parameters(name = "AutoPartitionViewsIT_salted={0},multi-tenant={1}") // name is used by failsafe as file name in reports
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] { { false, false }, { false, true }, { true, false },
                { true, true } });
    }

    public AutoPartitionViewsIT(boolean salted, boolean isMultiTenant) {
        this.isMultiTenant = isMultiTenant;
        StringBuilder optionBuilder = new StringBuilder(" AUTO_PARTITION_SEQ=\"%s\"");
        if (salted) optionBuilder.append(", SALTED=4 ");
        if (isMultiTenant) optionBuilder.append(", MULTI_TENANT=true ");
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Test
    public void testValidateAttributes() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn1 =
                        isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1)
                                : DriverManager.getConnection(getUrl());
                Connection viewConn2 =
                        isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1)
                                : DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            String autoSeqName = generateUniqueName();

            try {

                String ddl =
                        String.format(
                            "CREATE TABLE " + tableName + " (%s metricId VARCHAR, val1 DOUBLE, val2 DOUBLE CONSTRAINT PK PRIMARY KEY( %s metricId)) %s",
                                isMultiTenant ? "tenantId VARCHAR, " : "", 
                                isMultiTenant ? "tenantId, ": "", String.format(tableDDLOptions, autoSeqName)
                                );
                conn.createStatement().execute(ddl);
                fail("Sequence value must be castable to the auto partition id column data type");
            } catch (SQLException e) {
                assertEquals(
                    SQLExceptionCode.SEQUENCE_NOT_CASTABLE_TO_AUTO_PARTITION_ID_COLUMN
                            .getErrorCode(),
                    e.getErrorCode());
            }
            String ddl =
                    String.format(
                        "CREATE TABLE " + tableName + " (%s metricId INTEGER NOT NULL, val1 DOUBLE, val2 DOUBLE CONSTRAINT PK PRIMARY KEY( %s metricId)) %s",
                            isMultiTenant ? "tenantId VARCHAR NOT NULL, " : "", 
                            isMultiTenant ? "tenantId, ": "",
                            String.format(tableDDLOptions, autoSeqName));
            conn.createStatement().execute(ddl);


            String baseViewName = generateUniqueName();
            String metricView1 = baseViewName + "_VIEW1";
            String metricView2 = baseViewName + "_VIEW2";
            String metricView3 = baseViewName + "_VIEW3";
            String metricView4 = baseViewName + "_VIEW4";
            try {
                viewConn1.createStatement().execute(
                    "CREATE VIEW " + metricView1 + "  AS SELECT * FROM " + tableName);
                fail("Auto-partition sequence must be created before view is created");
            } catch (SequenceNotFoundException e) {
            }

            conn.createStatement().execute(
                "CREATE SEQUENCE " + autoSeqName + " start with " + (Integer.MAX_VALUE-2) + " cache 1");
            viewConn1.createStatement().execute(
                "CREATE VIEW " + metricView1 + " AS SELECT * FROM " + tableName + " WHERE val2=1.2");
            // create a view without a where clause
            viewConn1.createStatement().execute(
                    "CREATE VIEW " + metricView2 + " AS SELECT * FROM " + tableName);
            // create a view with a complex where clause
            viewConn1.createStatement().execute(
                "CREATE VIEW " + metricView3 + " AS SELECT * FROM " + tableName + " WHERE val1=1.0 OR val2=2.0");

            try {
                viewConn1.createStatement().execute(
                    "CREATE VIEW " + metricView4 + " AS SELECT * FROM " + tableName);
                fail("Creating a view with a partition id that is too large should fail");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_COERCE_AUTO_PARTITION_ID.getErrorCode(),
                    e.getErrorCode());
            }

            if (isMultiTenant) {
                // load tables into cache
                viewConn1.createStatement().execute("SELECT * FROM " + metricView1);
                viewConn1.createStatement().execute("SELECT * FROM " + metricView2);
                viewConn1.createStatement().execute("SELECT * FROM " + metricView3);
            }
            PhoenixConnection pconn = viewConn1.unwrap(PhoenixConnection.class);
            PTable view1 = pconn.getTable(new PTableKey(pconn.getTenantId(), metricView1));
            PTable view2 = pconn.getTable(new PTableKey(pconn.getTenantId(), metricView2));
            PTable view3 = pconn.getTable(new PTableKey(pconn.getTenantId(), metricView3));
            
            // verify the view statement was set correctly 
            String expectedViewStatement1 =
                    "SELECT * FROM \"" + tableName + "\" WHERE VAL2 = 1.2 AND METRICID = "
                            + (Integer.MAX_VALUE - 2);
            String expectedViewStatement2 =
                    "SELECT * FROM \"" + tableName + "\" WHERE METRICID = " + (Integer.MAX_VALUE - 1);
            String expectedViewStatement3 =
                    "SELECT * FROM \"" + tableName + "\" WHERE (VAL1 = 1.0 OR VAL2 = 2.0) AND METRICID = " + Integer.MAX_VALUE;
            assertEquals("Unexpected view statement", expectedViewStatement1,
                view1.getViewStatement());
            assertEquals("Unexpected view statement", expectedViewStatement2,
                view2.getViewStatement());
            assertEquals("Unexpected view statement", expectedViewStatement3,
                view3.getViewStatement());
            // verify isViewReferenced was set correctly
            int expectedParitionColIndex = isMultiTenant ? 1 : 0;
            PColumn partitionCol1 = view1.getColumns().get(expectedParitionColIndex);
            PColumn partitionCol2 = view2.getColumns().get(expectedParitionColIndex);
            PColumn partitionCol3 = view3.getColumns().get(expectedParitionColIndex);
            assertTrue("Partition column view referenced attribute should be true ",
                partitionCol1.isViewReferenced());
            assertTrue("Partition column view referenced attribute should be true ",
                partitionCol2.isViewReferenced());
            assertTrue("Partition column view referenced attribute should be true ",
                partitionCol3.isViewReferenced());
            // verify viewConstant was set correctly
            byte[] expectedPartition1 = new byte[Bytes.SIZEOF_INT + 1];
            PInteger.INSTANCE.toBytes(Integer.MAX_VALUE - 2, expectedPartition1, 0);
            byte[] expectedPartition2 = new byte[Bytes.SIZEOF_INT + 1];
            PInteger.INSTANCE.toBytes(Integer.MAX_VALUE - 1, expectedPartition2, 0);
            byte[] expectedPartition3 = new byte[Bytes.SIZEOF_INT + 1];
            PInteger.INSTANCE.toBytes(Integer.MAX_VALUE, expectedPartition3, 0);
            assertArrayEquals("Unexpected Partition column view constant attribute",
                expectedPartition1, partitionCol1.getViewConstant());
            assertArrayEquals("Unexpected Partition column view constant attribute",
                expectedPartition2, partitionCol2.getViewConstant());
            assertArrayEquals("Unexpected Partition column view constant attribute",
                expectedPartition3, partitionCol3.getViewConstant());

            // verify that the table was created correctly on the server
            viewConn2.createStatement().execute("SELECT * FROM " + metricView1);
            viewConn2.createStatement().execute("SELECT * FROM " + metricView2 );
            viewConn2.createStatement().execute("SELECT * FROM " + metricView3);
            pconn = viewConn2.unwrap(PhoenixConnection.class);
            view1 = pconn.getTable(new PTableKey(pconn.getTenantId(), metricView1));
            view2 = pconn.getTable(new PTableKey(pconn.getTenantId(), metricView2));
            view3 = pconn.getTable(new PTableKey(pconn.getTenantId(), metricView3));
            
            // verify the view statement was set correctly 
            assertEquals("Unexpected view statement", expectedViewStatement1,
                view1.getViewStatement());
            assertEquals("Unexpected view statement", expectedViewStatement2,
                view2.getViewStatement());
            assertEquals("Unexpected view statement", expectedViewStatement3,
                view3.getViewStatement());
            // verify isViewReferenced was set correctly
            partitionCol1 = view1.getColumns().get(expectedParitionColIndex);
            partitionCol2 = view2.getColumns().get(expectedParitionColIndex);
            partitionCol3 = view3.getColumns().get(expectedParitionColIndex);
            assertTrue("Partition column view referenced attribute should be true ",
                partitionCol1.isViewReferenced());
            assertTrue("Partition column view referenced attribute should be true ",
                partitionCol2.isViewReferenced());
            assertTrue("Partition column view referenced attribute should be true ",
                partitionCol3.isViewReferenced());
            // verify viewConstant was set correctly
            assertArrayEquals("Unexpected Partition column view constant attribute",
                expectedPartition1, partitionCol1.getViewConstant());
            assertArrayEquals("Unexpected Partition column view constant attribute",
                expectedPartition2, partitionCol2.getViewConstant());
            assertArrayEquals("Unexpected Partition column view constant attribute",
                expectedPartition3, partitionCol3.getViewConstant());
        }
    }

    @Test
    public void testViewCreationFailure() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn1 =
                        isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1)
                                : DriverManager.getConnection(getUrl());
                Connection viewConn2 =
                        isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL2)
                                : DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            String autoSeqName = generateUniqueName();

            String ddl =
                    String.format(
                        "CREATE TABLE " + tableName + " (%s metricId INTEGER NOT NULL, val1 DOUBLE, val2 DOUBLE CONSTRAINT PK PRIMARY KEY( %s metricId)) %s",
                            isMultiTenant ? "tenantId VARCHAR NOT NULL, " : "", 
                            isMultiTenant ? "tenantId, ": "", 
                            String.format(tableDDLOptions, autoSeqName));
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("CREATE SEQUENCE " + autoSeqName + " CACHE 1");

            String baseViewName = generateUniqueName();
            String metricView1 = baseViewName + "_VIEW1";
            String metricView2 = baseViewName + "_VIEW2";
            // create a view
            viewConn1.createStatement().execute(
                "CREATE VIEW " + metricView1 + " AS SELECT * FROM " + tableName + " WHERE val2=1.2");
            try {
                // create the same view which should fail
                viewConn1.createStatement()
                        .execute("CREATE VIEW " + metricView1 + " AS SELECT * FROM " + tableName);
                fail("view should already exist");
            } catch (TableAlreadyExistsException e) {
            }

            // create a second view (without a where clause)
            viewConn2.createStatement().execute(
                "CREATE VIEW " + metricView2 + " AS SELECT * FROM " +  tableName);

            // upsert a row into each view
            viewConn1.createStatement().execute("UPSERT INTO " + metricView1 + "(val1) VALUES(1.1)");
            viewConn1.commit();
            viewConn2.createStatement().execute("UPSERT INTO " + metricView2 + "(val1,val2) VALUES(2.1,2.2)");
            viewConn2.commit();

            // query the base table
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            int offset = 0;
            if (isMultiTenant) {
                assertEquals("tenant1", rs.getString(1));
                offset = 1;
            }
            assertEquals(1, rs.getInt(1+offset));
            assertEquals(1.1, rs.getDouble(2+offset), 1e-6);
            assertEquals(1.2, rs.getDouble(3+offset), 1e-6);
            assertTrue(rs.next());
            // validate that the auto partition sequence was not incremented even though view creation failed
            if (isMultiTenant) {
                assertEquals("tenant2", rs.getString(1));
            }
            assertEquals(2, rs.getInt(1+offset));
            assertEquals(2.1, rs.getDouble(2+offset), 1e-6);
            assertEquals(2.2, rs.getDouble(3+offset), 1e-6);
            assertFalse(rs.next());

            // query the first view
            rs = viewConn1.createStatement().executeQuery("SELECT * FROM " + metricView1);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1.1, rs.getDouble(2), 1e-6);
            assertEquals(1.2, rs.getDouble(3), 1e-6);
            assertFalse(rs.next());

            // query the second view
            rs = viewConn2.createStatement().executeQuery("SELECT * FROM " + metricView2);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(2.1, rs.getDouble(2), 1e-6);
            assertEquals(2.2, rs.getDouble(3), 1e-6);
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testAddDropColumns() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn1 =
                        isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1)
                                : DriverManager.getConnection(getUrl())) {
            String tableName = generateUniqueName();
            String autoSeqName = generateUniqueName();

            String ddl =
                    String.format(
                        "CREATE TABLE " + tableName + " (%s metricId INTEGER NOT NULL, val1 DOUBLE, CONSTRAINT PK PRIMARY KEY( %s metricId)) %s",
                            isMultiTenant ? "tenantId VARCHAR NOT NULL, " : "", 
                            isMultiTenant ? "tenantId, ": "", 
                            String.format(tableDDLOptions, autoSeqName));
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("CREATE SEQUENCE " + autoSeqName + " CACHE 1");

            String metricView = generateUniqueName() + "_VIEW";
            // create a view
            viewConn1.createStatement().execute(
                "CREATE VIEW " + metricView + " AS SELECT * FROM " + tableName);
            
            // add a column to the base table
            conn.createStatement().execute(
                    "ALTER TABLE " + tableName + " add val2 DOUBLE");
            
            // add a column to the view
            viewConn1.createStatement().execute(
                    "ALTER VIEW " + metricView + " add val3 DOUBLE");

            // upsert a row into the view
            viewConn1.createStatement().execute("UPSERT INTO " + metricView + "(val1,val2,val3) VALUES(1.1,1.2,1.3)");
            viewConn1.commit();

            // query the base table
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            int offset = 0;
            if (isMultiTenant) {
                assertEquals("tenant1", rs.getString(1));
                offset = 1;
            }
            assertEquals(1, rs.getInt(1+offset));
            assertEquals(1.1, rs.getDouble(2+offset), 1e-6);
            assertEquals(1.2, rs.getDouble(3+offset), 1e-6);
            assertFalse(rs.next());
            
            // query the view
            rs = viewConn1.createStatement().executeQuery("SELECT * FROM " + metricView);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1.1, rs.getDouble(2), 1e-6);
            assertEquals(1.2, rs.getDouble(3), 1e-6);
            assertEquals(1.3, rs.getDouble(4), 1e-6);
            assertFalse(rs.next());

            // drop a column from the base table
            conn.createStatement().execute(
                    "ALTER TABLE " + tableName + " DROP COLUMN val2");
            
            // add a column to the view
            viewConn1.createStatement().execute(
                    "ALTER VIEW " + metricView + " DROP COLUMN val3");
            
            // verify columns don't exist
            try {
                viewConn1.createStatement().executeQuery("SELECT val2 FROM " + metricView);
                fail("column should have been dropped");
            }
            catch (ColumnNotFoundException e) {
            }
            try {
                viewConn1.createStatement().executeQuery("SELECT val3 FROM " + metricView);
                fail("column should have been dropped");
            }
            catch (ColumnNotFoundException e) {
            }
        }
    }
}
