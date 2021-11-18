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
package org.apache.phoenix.end2end.transform;

import org.apache.phoenix.compat.hbase.coprocessor.CompatBaseScannerRegionObserver;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.end2end.index.SingleCellIndexIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.transform.SystemTransformRecord;
import org.apache.phoenix.schema.transform.Transform;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.end2end.transform.TransformMonitorIT.waitForTransformToGetToState;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class TransformMonitorExtendedIT extends BaseTest {
    private static RegionCoprocessorEnvironment taskRegionEnvironment;
    protected String dataTableDdl = "";
    private Properties propsNamespace = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = com.google.common.collect.Maps.newHashMapWithExpectedSize(2);
        serverProps.put(CompatBaseScannerRegionObserver.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(60*60)); // An hour
        serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());

        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
        clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,  Boolean.TRUE.toString());

        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    public TransformMonitorExtendedIT() throws IOException, InterruptedException {
        StringBuilder optionBuilder = new StringBuilder();
        optionBuilder.append(" COLUMN_ENCODED_BYTES=0,IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN");
        this.dataTableDdl = optionBuilder.toString();
        propsNamespace.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));

        TableName taskTable = TableName.valueOf(SYSTEM_CATALOG_SCHEMA + QueryConstants.NAMESPACE_SEPARATOR + SYSTEM_TASK_TABLE);
        taskRegionEnvironment = (RegionCoprocessorEnvironment) getUtility()
                .getRSForFirstRegionInTable(taskTable)
                .getRegions(taskTable)
                .get(0).getCoprocessorHost()
                .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
    }

    @Before
    public void setupTest() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            conn.createStatement().execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_TRANSFORM_NAME);
            conn.createStatement().execute("DELETE FROM " + PhoenixDatabaseMetaData.SYSTEM_TASK_NAME);
        }
    }

    @Test
    public void testTransformIndexWithNamespaceEnabled() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String fullDataTableName = SchemaUtil.getTableName(schemaName , dataTableName);
        String indexName = "IDX_" + generateUniqueName();
        String fullIndexName = SchemaUtil.getTableName(schemaName , indexName);
        String createIndexStmt = "CREATE INDEX %s ON " + fullDataTableName + " (NAME) INCLUDE (ZIP) ";
        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            TransformToolIT.createTableAndUpsertRows(conn, fullDataTableName, numOfRows, dataTableDdl);
            conn.createStatement().execute(String.format(createIndexStmt, indexName));
            assertEquals(numOfRows, TransformMonitorIT.countRows(conn, fullIndexName));

            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, fullIndexName);

            conn.createStatement().execute("ALTER INDEX " + indexName + " ON " + fullDataTableName +
                    " ACTIVE IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
            SystemTransformRecord record = Transform.getTransformRecord(schemaName, indexName, fullDataTableName, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());

            TransformToolIT.upsertRows(conn, fullDataTableName, 2, 1);

            ResultSet rs = conn.createStatement().executeQuery("SELECT \":ID\", \"0:ZIP\" FROM " + fullIndexName);
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals( 95051, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertEquals( 95052, rs.getInt(2));
            assertFalse(rs.next());
        }
    }
    @Test
    public void testTransformTableWithNamespaceEnabled() throws Exception {
        String schemaName = "S_" + generateUniqueName();
        String dataTableName = "TBL_" + generateUniqueName();
        String fullDataTableName = SchemaUtil.getTableName(schemaName, dataTableName);
        try (Connection conn = DriverManager.getConnection(getUrl(), propsNamespace)) {
            conn.setAutoCommit(true);
            int numOfRows = 1;
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            TransformToolIT.createTableAndUpsertRows(conn, fullDataTableName, numOfRows, dataTableDdl);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN, PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, fullDataTableName);

            conn.createStatement().execute("ALTER TABLE " + fullDataTableName +
                    " SET IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");

            SystemTransformRecord record = Transform.getTransformRecord(schemaName, dataTableName, null, null, conn.unwrap(PhoenixConnection.class));
            assertNotNull(record);
            waitForTransformToGetToState(conn.unwrap(PhoenixConnection.class), record, PTable.TransformStatus.COMPLETED);
            SingleCellIndexIT.assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS, PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, record.getNewPhysicalTableName());
            TransformToolIT.upsertRows(conn, fullDataTableName, 2, 1);
            assertEquals(numOfRows+1, TransformMonitorIT.countRows(conn, fullDataTableName));

            ResultSet rs = conn.createStatement().executeQuery("SELECT ID, ZIP FROM " + fullDataTableName);
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1));
            assertEquals( 95051, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1));
            assertEquals( 95052, rs.getInt(2));
            assertFalse(rs.next());
        }
    }
}
