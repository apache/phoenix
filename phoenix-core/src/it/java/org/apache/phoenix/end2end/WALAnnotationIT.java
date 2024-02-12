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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessor;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.CHANGE_DETECTION_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class WALAnnotationIT extends BaseTest {
    private final boolean isImmutable;
    private final boolean isMultiTenant;

    // name is used by failsafe as file name in reports
    @Parameterized.Parameters(name = "WALAnnotationIT_isImmutable={0}_isMultiTenant={1}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[]{true, true}, new Object[]{true, false},
            new Object[]{false, true}, new Object[]{false, false});
    }

    public WALAnnotationIT(boolean isImmutable, boolean isMultiTenant) {
        this.isImmutable = isImmutable;
        this.isMultiTenant = isMultiTenant;
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = new HashMap<>(2);
        props.put("hbase.coprocessor.wal.classes",
            AnnotatedWALObserver.class.getName());
        props.put(IndexRegionObserver.PHOENIX_APPEND_METADATA_TO_WAL, "true");
        props.put(QueryServices.ENABLE_SERVER_UPSERT_SELECT, "true");
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testSimpleUpsertAndDelete() throws Exception {
        SchemaBuilder builder = new SchemaBuilder(getUrl());
        boolean createGlobalIndex = false;
        String externalSchemaId = upsertAndDeleteHelper(builder, createGlobalIndex);
        assertAnnotation(2, builder.getPhysicalTableName(false), externalSchemaId);
    }

    @Test
    public void testNoAnnotationsIfChangeDetectionDisabled() throws Exception {
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl())) {
            conn.setAutoCommit(true);
            SchemaBuilder builder = new SchemaBuilder(getUrl());
            SchemaBuilder.TableOptions tableOptions = getTableOptions();
            tableOptions.setChangeDetectionEnabled(false);
            builder.withTableOptions(tableOptions).build();
            PTable table = conn.getTableNoCache(builder.getEntityTableName());
            assertFalse("Change detection is enabled when it shouldn't be!",
                table.isChangeDetectionEnabled());
            String upsertSql = "UPSERT INTO " + builder.getEntityTableName() + " VALUES" +
                " ('a', 'b', '2', 'bc', '3')";
            conn.createStatement().execute(upsertSql);
            List<Map<String, byte[]>> entries =
                getEntriesForTable(TableName.valueOf(builder.getPhysicalTableName(false)));
            assertTrue("WAL annotations should not contain EXTERNAL_SCHEMA_ID", entries.size() == 0 ||
                    !entries.get(0).containsKey(MutationState.MutationMetadataType.EXTERNAL_SCHEMA_ID.toString()));
            //now flip to TRUE so we can test disabling it
            String enableSql =
                "ALTER TABLE " + builder.getEntityTableName() +
                    " SET " + CHANGE_DETECTION_ENABLED + "=TRUE";
            conn.createStatement().execute(enableSql);
            table = conn.getTableNoCache(builder.getEntityTableName());
            assertTrue("Change detection is disabled when it should be enabled!",
                table.isChangeDetectionEnabled());
            //set to FALSE
            String disableSql =
                "ALTER TABLE " + builder.getEntityTableName() +
                    " SET " + CHANGE_DETECTION_ENABLED + "=FALSE";
            conn.createStatement().execute(disableSql);
            table = conn.getTableNoCache(builder.getEntityTableName());
            assertFalse("Change detection is enabled when it should be disabled!",
                table.isChangeDetectionEnabled());
            //now upsert again
            conn.createStatement().execute(upsertSql);
            //check that we still didn't annotate anything
            entries = getEntriesForTable(TableName.valueOf(builder.getPhysicalTableName(false)));
            assertTrue("WAL annotations should not contain EXTERNAL_SCHEMA_ID", entries.size() == 0 ||
                    !entries.get(0).containsKey(MutationState.MutationMetadataType.EXTERNAL_SCHEMA_ID.toString()));
        }
    }

    @Test
    public void testCantSetChangeDetectionOnIndex() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            SchemaBuilder builder = new SchemaBuilder(getUrl());
            builder.withTableDefaults().build();
            try {
                String badIndexSql =
                    "CREATE INDEX IDX_SHOULD_FAIL"  + " ON " + builder.getEntityTableName() +
                        "(COL1) "
                        + CHANGE_DETECTION_ENABLED + "=TRUE";
                conn.createStatement().execute(badIndexSql);
                fail("Didn't throw a SQLException for setting change detection on an " +
                    "index at create time!");
            } catch (SQLException se) {
                TestUtil.assertSqlExceptionCode(
                    SQLExceptionCode.CHANGE_DETECTION_SUPPORTED_FOR_TABLES_AND_VIEWS_ONLY, se);
            }
        }
    }

    @Test
    public void testUpsertAndDeleteWithGlobalIndex() throws Exception {
        SchemaBuilder builder = new SchemaBuilder(getUrl());
        boolean createGlobalIndex = true;
        String externalSchemaId = upsertAndDeleteHelper(builder, createGlobalIndex);
        assertAnnotation(2, builder.getPhysicalTableName(false), externalSchemaId);
        assertAnnotation(0, builder.getPhysicalTableIndexName(false),
            externalSchemaId);
    }

    // Note that local secondary indexes aren't supported because they go in the same WALEdit as the
    // "base" table data they index.

    private String upsertAndDeleteHelper(SchemaBuilder builder, boolean createGlobalIndex) throws Exception {
        try (PhoenixConnection conn = getConnection()) {
            SchemaBuilder.TableOptions tableOptions = getTableOptions();

            if (createGlobalIndex) {
                builder.withTableOptions(tableOptions).withTableIndexDefaults().build();
            } else {
                builder.withTableOptions(tableOptions).build();
            }

            String upsertSql = "UPSERT INTO " + builder.getEntityTableName() + " VALUES" +
                " ('a', 'b', 'c')";
            conn.createStatement().execute(upsertSql);
            conn.commit();
            PTable table = conn.getTableNoCache(builder.getEntityTableName());
            assertTrue("Change Detection Enabled is false!", table.isChangeDetectionEnabled());
            // Deleting by entire PK gets executed as more like an UPSERT VALUES than an UPSERT
            // SELECT (i.e, it generates the Mutations and then pushes them to server, rather than
            // running a select query and deleting the mutations returned)
            String deleteSql = "DELETE FROM " + builder.getEntityTableName() + " " +
                "WHERE OID = 'a' AND KP = 'b'";
            conn.createStatement().execute(deleteSql);
            conn.commit();
            // DDL timestamp is the timestamp at which a table or view was created, or when it
            // last had columns added or removed. It is NOT the timestamp of a particular mutation
            // We need it in the annotation to match up with schema object in an external schema
            // repo.
            return table.getExternalSchemaId();
        }
    }

    private SchemaBuilder.TableOptions getTableOptions() {
        SchemaBuilder.TableOptions tableOptions =
            SchemaBuilder.TableOptions.withDefaults();
        tableOptions.setImmutable(isImmutable);
        tableOptions.setMultiTenant(isMultiTenant);
        tableOptions.setChangeDetectionEnabled(true);
        return tableOptions;
    }

    @Test
    public void testUpsertSelectClientSide() throws Exception {
        try (PhoenixConnection conn = getConnection()) {
            SchemaBuilder baseBuilder = new SchemaBuilder(getUrl());
            SchemaBuilder targetBuilder = new SchemaBuilder(getUrl());
            //upsert selecting from a different table will force processing to be client-side
            baseBuilder.withTableOptions(getTableOptions()).build();
            conn.createStatement().execute("UPSERT INTO " + baseBuilder.getEntityTableName() + " " +
                "VALUES" +
                " ('a', 'b', '2', 'bc', '3')");
            conn.commit();
            targetBuilder.withTableOptions(getTableOptions()).build();
            String sql = "UPSERT INTO " + targetBuilder.getEntityTableName() +
                " (OID, KP, COL1, COL2, COL3) SELECT * FROM " + baseBuilder.getEntityTableName();
            conn.createStatement().execute(sql);
            conn.commit();
            int expectedAnnotations = 1;
            verifyBaseAndTargetAnnotations(conn, baseBuilder, targetBuilder, expectedAnnotations);
        }
    }

    private void verifyBaseAndTargetAnnotations(PhoenixConnection conn, SchemaBuilder baseBuilder,
                                                SchemaBuilder targetBuilder,
                                                int expectedAnnotations) throws SQLException, IOException {
        PTable baseTable = conn.getTableNoCache(baseBuilder.getEntityTableName());
        assertAnnotation(expectedAnnotations, baseBuilder.getPhysicalTableName(false), baseTable.getExternalSchemaId());
        PTable targetTable = conn.getTableNoCache(targetBuilder.getEntityTableName());
        assertAnnotation(expectedAnnotations, targetBuilder.getPhysicalTableName(false), targetTable.getExternalSchemaId());
    }

    @Test
    public void testUpsertSelectServerSide() throws Exception {
        Assume.assumeFalse(isImmutable); //only mutable tables can be processed server-side
        SchemaBuilder targetBuilder = new SchemaBuilder(getUrl());
        try (PhoenixConnection conn = getConnection()) {
            targetBuilder.withTableOptions(getTableOptions()).build();
            conn.createStatement().execute("UPSERT INTO " + targetBuilder.getEntityTableName() + " " +
                "VALUES" +
                " ('a', 'b', '2', 'bc', '3')");
            conn.commit();
            conn.setAutoCommit(true); //required for server side execution
            clearAnnotations(TableName.valueOf(targetBuilder.getPhysicalTableName(false)));
            String sql = "UPSERT INTO " + targetBuilder.getEntityTableName() +
                " (OID, KP, COL1, COL2, COL3) SELECT * FROM " + targetBuilder.getEntityTableName();
            conn.createStatement().execute(sql);
            PTable table = conn.getTableNoCache(targetBuilder.getEntityTableName());
            assertAnnotation(1, targetBuilder.getPhysicalTableName(false),
                table.getExternalSchemaId());
        }

    }

    @Test
    public void testGroupedUpsertSelect() throws Exception {
        // because we're inserting to a different table than we're selecting from, this should be
        // processed client-side
        SchemaBuilder baseBuilder = new SchemaBuilder(getUrl());
        SchemaBuilder targetBuilder = new SchemaBuilder(getUrl());
        try (PhoenixConnection conn = getConnection()) {
            baseBuilder.withTableOptions(getTableOptions()).build();
            targetBuilder.withTableOptions(getTableOptions()).build();
            conn.createStatement().execute("UPSERT INTO " + baseBuilder.getEntityTableName() + " VALUES" +
                " ('a', 'b', '2', 'bc', '3')");
            conn.commit();
            String aggSql = "UPSERT INTO " + targetBuilder.getEntityTableName() +
                " SELECT OID, KP, MAX(COL1), MIN(COL2), MAX(COL3) FROM " + baseBuilder.getEntityTableName() +
                " GROUP BY OID, KP";
            conn.createStatement().execute(aggSql);
            conn.commit();
            int expectedAnnotations = 1;
            verifyBaseAndTargetAnnotations(conn, baseBuilder, targetBuilder, expectedAnnotations);
        }
    }

    @Test
    public void testRangeDeleteServerSide() throws Exception {
        boolean isClientSide = false;
        testRangeDeleteHelper(isClientSide);
    }

    private void testRangeDeleteHelper(boolean isClientSide) throws Exception {
        SchemaBuilder builder = new SchemaBuilder(getUrl());
        builder.withTableOptions(getTableOptions()).build();
        try (PhoenixConnection conn = getConnection()) {
            conn.createStatement().execute("UPSERT INTO " + builder.getEntityTableName() +
                " VALUES ('a', 'b', '2', 'bc', '3')");
            conn.commit();
            // Deleting by a partial PK to so that it executes a SELECT and then deletes the
            // returned mutations
            String sql = "DELETE FROM " + builder.getEntityTableName() + " " +
                "WHERE OID = 'a' AND KP = 'b'";

            if (isClientSide) {
                sql += " LIMIT 1";
            }
            conn.setAutoCommit(!isClientSide);
            conn.createStatement().execute(sql);
            conn.commit();
            PTable table = conn.getTableNoCache(builder.getEntityTableName());
            assertAnnotation(2, table.getPhysicalName().getString(), table.getExternalSchemaId());
        }

    }

    @Test
    public void testRangeDeleteClientSide() throws Exception {
        boolean isClientSide = true;
        testRangeDeleteHelper(isClientSide);
    }

    @Test
    public void testGlobalViewUpsert() throws Exception {
        SchemaBuilder builder = new SchemaBuilder(getUrl());
        try (PhoenixConnection conn = getConnection()) {
            createGlobalViewHelper(builder, conn);
            conn.createStatement().execute("UPSERT INTO " + builder.getEntityGlobalViewName()
                + " VALUES" + " ('a', '" + PhoenixTestBuilder.DDLDefaults.DEFAULT_KP +
                "', '2', 'bc', '3', 'c')");
            conn.commit();
            String deleteSql = "DELETE FROM " + builder.getEntityGlobalViewName() + " " +
                "WHERE OID = 'a' AND KP = '" + PhoenixTestBuilder.DDLDefaults.DEFAULT_KP + "' " +
                "and ID = 'c'";
            conn.createStatement().execute(deleteSql);
            conn.commit();
            PTable view = conn.getTableNoCache(builder.getEntityGlobalViewName());
            assertAnnotation(2, view.getPhysicalName().getString(), view.getExternalSchemaId());
        }

    }

    private void createGlobalViewHelper(SchemaBuilder builder, PhoenixConnection conn) throws Exception {
        builder.withTableOptions(getTableOptions()).
            withGlobalViewOptions(getGlobalViewOptions(builder)).build();
        PTable view = conn.getTableNoCache(builder.getEntityGlobalViewName());
        assertTrue("View does not have change detection enabled!",
            view.isChangeDetectionEnabled());
    }

    private SchemaBuilder.GlobalViewOptions getGlobalViewOptions(SchemaBuilder builder) {
        SchemaBuilder.GlobalViewOptions options = SchemaBuilder.GlobalViewOptions.withDefaults();
        options.setChangeDetectionEnabled(true);
        return options;
    }

    @Test
    public void testTenantViewUpsert() throws Exception {
        Assume.assumeTrue(isMultiTenant);
        boolean createIndex = false;
        tenantViewHelper(createIndex);
    }

    private void tenantViewHelper(boolean createIndex) throws Exception {
        // create a base table, global view, and child tenant view, then insert / delete into the
        // child tenant view. Make sure that the annotations use the tenant view name
        String tenant = generateUniqueName();
        SchemaBuilder builder = new SchemaBuilder(getUrl());
        try (PhoenixConnection conn = getConnection()) {
            createGlobalViewHelper(builder, conn);
        }
        try (PhoenixConnection conn = (PhoenixConnection) getTenantConnection(tenant)) {
            SchemaBuilder.DataOptions dataOptions = builder.getDataOptions();
            dataOptions.setTenantId(tenant);
            if (createIndex) {
                builder.withTenantViewOptions(getTenantViewOptions(builder)).
                    withDataOptions(dataOptions).withTenantViewIndexDefaults().build();
            } else {
                builder.withTenantViewOptions(getTenantViewOptions(builder)).
                    withDataOptions(dataOptions).build();
            }
            builder.withTenantViewOptions(getTenantViewOptions(builder)).
                withDataOptions(dataOptions).withTenantViewIndexDefaults().build();
            conn.createStatement().execute("UPSERT INTO " + builder.getEntityTenantViewName()
                + " VALUES" + " ('" + PhoenixTestBuilder.DDLDefaults.DEFAULT_KP + "', '2', 'bc', " +
                "'3', 'c', " + "'col4', 'col5', 'col6', 'd')");
            conn.commit();
            String deleteSql = "DELETE FROM " + builder.getEntityTenantViewName() + " " +
                "WHERE KP = '"+ PhoenixTestBuilder.DDLDefaults.DEFAULT_KP +
                "' and COL1 = '2' AND ID = 'c' AND ZID = 'd'";
            conn.createStatement().execute(deleteSql);
            conn.commit();
            PTable view = conn.getTableNoCache(builder.getEntityTenantViewName());
            assertAnnotation(2, view.getPhysicalName().getString(), view.getExternalSchemaId());
            if (createIndex) {
                assertAnnotation(0,
                    MetaDataUtil.getViewIndexPhysicalName(builder.getEntityTableName()),
                    view.getExternalSchemaId());
            }
        }

    }

    private SchemaBuilder.TenantViewOptions getTenantViewOptions(SchemaBuilder builder) {
        SchemaBuilder.TenantViewOptions options = SchemaBuilder.TenantViewOptions.withDefaults();
        options.setChangeDetectionEnabled(true);
        return options;
    }

    @Test
    public void testTenantViewUpsertWithIndex() throws Exception {
        Assume.assumeTrue(isMultiTenant);
        tenantViewHelper(true);
    }

    @Test
    public void testOnDuplicateUpsertWithIndex() throws Exception {
        Assume.assumeFalse(this.isImmutable); // on duplicate is not supported for immutable tables
        SchemaBuilder builder = new SchemaBuilder(getUrl());
        try (PhoenixConnection conn = getConnection()) {
            SchemaBuilder.TableOptions tableOptions = getTableOptions();
            builder.withTableOptions(tableOptions).withTableIndexDefaults().build();
            PTable table = conn.getTableNoCache(builder.getEntityTableName());
            assertTrue("Change Detection Enabled is false!", table.isChangeDetectionEnabled());
            Long ddlTimestamp = table.getLastDDLTimestamp();
            String upsertSql = "UPSERT INTO " + builder.getEntityTableName() + " VALUES" +
                " ('a', 'b', 'c', 'd')";
            conn.createStatement().execute(upsertSql);
            conn.commit();
            List<String> columns = builder.getTableOptions().getTableColumns();
            assertTrue(columns.size() >= 2);
            String col1 = columns.get(0);
            String col2 = columns.get(1);
            // col1 = col1 || col1, col2 = null
            String onDupClause = String.format("%s = %s || %s, %s = null", col1, col1, col1, col2);
            // this will result in one Put and one Delete (because of null) mutation
            upsertSql = "UPSERT INTO " + builder.getEntityTableName() + " VALUES" +
                " ('a', 'b', 'c', 'd') ON DUPLICATE KEY UPDATE " + onDupClause;
            conn.createStatement().execute(upsertSql);
            conn.commit();
            assertAnnotation(2, builder.getPhysicalTableName(false), table.getExternalSchemaId());
            assertAnnotation(0, builder.getPhysicalTableIndexName(false),
                table.getExternalSchemaId());
        }
    }

    private List<Map<String, byte[]>> getEntriesForTable(TableName tableName) throws IOException {
        AnnotatedWALObserver c = getTestCoprocessor(tableName);
        List<Map<String, byte[]>> entries = c.getWalAnnotationsByTable(tableName);
        return entries != null ? entries : new ArrayList<Map<String, byte[]>>();
    }

    private AnnotatedWALObserver getTestCoprocessor(TableName tableName) throws IOException {
        RegionInfo info = getUtility().getHBaseCluster().getRegions(tableName).get(0).getRegionInfo();
        WAL wal = getUtility().getHBaseCluster().getRegionServer(0).getWAL(info);
        WALCoprocessorHost host = wal.getCoprocessorHost();
        return (AnnotatedWALObserver) host.findCoprocessor(AnnotatedWALObserver.class.getName());
    }

    private void clearAnnotations(TableName tableName) throws IOException {
        AnnotatedWALObserver observer = getTestCoprocessor(tableName);
        observer.clearAnnotations();
    }

    private void assertAnnotation(int numOccurrences, String physicalTableName,
        String externalSchemaId) throws IOException {
        int foundCount = 0;
        int notFoundCount = 0;
        List<Map<String, byte[]>> entries =
            getEntriesForTable(TableName.valueOf(physicalTableName));
        for (Map<String, byte[]> m : entries) {
            byte[] externalSchemaIdBytes = m.get(MutationState.MutationMetadataType.EXTERNAL_SCHEMA_ID.toString());
            assertNotNull(externalSchemaIdBytes);
            if (Objects.equals(externalSchemaId, Bytes.toString(externalSchemaIdBytes))) {
                foundCount++;
            } else {
                notFoundCount++;
            }
        }
        assertEquals(numOccurrences, foundCount);
        assertEquals(0, notFoundCount);
    }

    private PhoenixConnection getConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(false));
        return (PhoenixConnection) DriverManager.getConnection(getUrl(), props);
    }

    private Connection getTenantConnection(String tenant) throws SQLException {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenant);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(false));
        return DriverManager.getConnection(getUrl(), props);
    }

    public static class AnnotatedWALObserver implements WALCoprocessor, WALObserver {
        Map<TableName, List<Map<String, byte[]>>> walAnnotations = new HashMap<>();

        public Map<TableName, List<Map<String, byte[]>>> getWalAnnotations() {
            return walAnnotations;
        }

        public List<Map<String, byte[]>> getWalAnnotationsByTable(TableName tableName) {
            return walAnnotations.get(tableName);
        }

        public void clearAnnotations() {
            walAnnotations.clear();
        }

        @Override
        public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
                                 RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
            TableName tableName = logKey.getTableName();
            Map<String, byte[]> annotationMap =
                IndexRegionObserver.getAttributeValuesFromWALKey(logKey);
            if (annotationMap.size() > 0) {
                if (!walAnnotations.containsKey(tableName)) {
                    walAnnotations.put(tableName, new ArrayList<Map<String, byte[]>>());
                }
                walAnnotations.get(logKey.getTableName()).add(annotationMap);
            }
        }

        @Override
        public Optional<WALObserver> getWALObserver() {
            return Optional.of((WALObserver)this);
        }
    }
}
