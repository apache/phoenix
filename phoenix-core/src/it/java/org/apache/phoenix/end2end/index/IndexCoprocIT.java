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

package org.apache.phoenix.end2end.index;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static org.apache.phoenix.end2end.index.IndexTestUtil.assertCoprocConfig;
import static org.apache.phoenix.end2end.index.IndexTestUtil.assertCoprocsContains;
import static org.apache.phoenix.end2end.index.IndexTestUtil.assertCoprocsNotContains;
import static org.apache.phoenix.end2end.index.IndexTestUtil.assertUsingOldCoprocs;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class IndexCoprocIT extends ParallelStatsDisabledIT {
    private boolean isNamespaceMapped = false;
    private boolean isMultiTenant = false;
    public static final String GLOBAL_INDEX_CHECKER_CONFIG =
        "|org.apache.phoenix.index.GlobalIndexChecker|805306365|";
    public static final String INDEX_REGION_OBSERVER_CONFIG =
        "|org.apache.phoenix.hbase.index.IndexRegionObserver|805306366|" +
            "index.builder=org.apache.phoenix.index.PhoenixIndexBuilder," +
            "org.apache.hadoop.hbase.index.codec.class=org.apache.phoenix.index.PhoenixIndexCodec";
    public static final String INDEXER_CONFIG =
        "|org.apache.phoenix.hbase.index.Indexer|805306366|" +
            "index.builder=org.apache.phoenix.index.PhoenixIndexBuilder," +
            "org.apache.hadoop.hbase.index.codec.class=org.apache.phoenix.index.PhoenixIndexCodec";

    public IndexCoprocIT(boolean isMultiTenant){
        this.isMultiTenant = isMultiTenant;
    }
    @Parameterized.Parameters(name ="CreateIndexCoprocIT_mulitTenant={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{true}, {false}});
    }

    @Test
    public void testCreateCoprocs() throws Exception {
        String schemaName = "S" + generateUniqueName();
        String tableName = "T_" + generateUniqueName();
        String indexName = "I_" + generateUniqueName();
        String physicalTableName = SchemaUtil.getPhysicalHBaseTableName(schemaName, tableName,
            isNamespaceMapped).getString();
        String physicalIndexName = SchemaUtil.getPhysicalHBaseTableName(schemaName,
            indexName, isNamespaceMapped).getString();
        Admin admin = getConnection().getQueryServices().getAdmin();

        createBaseTable(schemaName, tableName, isMultiTenant, 0, null);
        createIndexTable(schemaName, tableName, indexName);

        IndexTestUtil.downgradeCoprocs(physicalTableName, physicalIndexName, admin);
        createBaseTable(schemaName, tableName, true, 0, null);
        TableDescriptor baseDescriptor = admin.getDescriptor(TableName.valueOf(physicalTableName));
        TableDescriptor indexDescriptor = admin.getDescriptor(TableName.valueOf(physicalIndexName));
        assertUsingOldCoprocs(baseDescriptor, indexDescriptor);
    }

    @Test
    public void testCreateOnExistingHBaseTable() throws Exception {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String indexName = generateUniqueName();
        byte[] cf = Bytes.toBytes("f");
        try (PhoenixConnection conn = getConnection()){
            TableName table = TableName.valueOf(SchemaUtil.getPhysicalHBaseTableName(schemaName,
                tableName, isNamespaceMapped).getString());
            TableDescriptorBuilder originalDescBuilder = TableDescriptorBuilder.newBuilder(table);
            ColumnFamilyDescriptorBuilder familyDescBuilder =
                ColumnFamilyDescriptorBuilder.newBuilder(cf);
            originalDescBuilder.setColumnFamily(familyDescBuilder.build());
            Admin admin = conn.getQueryServices().getAdmin();
            admin.createTable(originalDescBuilder.build());
            createBaseTable(schemaName, tableName, isMultiTenant, 0, null);
            TableDescriptor baseDescriptor = admin.getDescriptor(table);
            assertUsingNewCoprocs(baseDescriptor);
            createIndexTable(schemaName, tableName, indexName);
            baseDescriptor = admin.getDescriptor(table);
            TableName indexTable = TableName.valueOf(SchemaUtil.getPhysicalHBaseTableName(schemaName,
                indexName, isNamespaceMapped).getString());
            TableDescriptor indexDescriptor = admin.getDescriptor(indexTable);
            assertUsingNewCoprocs(baseDescriptor, indexDescriptor);
        }
    }

    @Test
    public void testAlterDoesntChangeCoprocs() throws Exception {
        String schemaName = "S" + generateUniqueName();
        String tableName = "T_" + generateUniqueName();
        String indexName = "I_" + generateUniqueName();
        String physicalTableName = SchemaUtil.getPhysicalHBaseTableName(schemaName, tableName,
            isNamespaceMapped).getString();
        String physicalIndexName = SchemaUtil.getPhysicalHBaseTableName(schemaName,
            indexName, isNamespaceMapped).getString();
        Admin admin = getConnection().getQueryServices().getAdmin();

        createBaseTable(schemaName, tableName, isMultiTenant, 0, null);
        createIndexTable(schemaName, tableName, indexName);
        TableDescriptor baseDescriptor = admin.getDescriptor(TableName.valueOf(physicalTableName));
        TableDescriptor indexDescriptor = admin.getDescriptor(TableName.valueOf(physicalIndexName));

        assertCoprocsContains(IndexRegionObserver.class, baseDescriptor);
        assertCoprocsContains(GlobalIndexChecker.class, indexDescriptor);
        String columnName = "foo";
        addColumnToBaseTable(schemaName, tableName, columnName);
        assertCoprocsContains(IndexRegionObserver.class, baseDescriptor);
        assertCoprocsContains(GlobalIndexChecker.class, indexDescriptor);
        dropColumnToBaseTable(schemaName, tableName, columnName);
        assertCoprocsContains(IndexRegionObserver.class, baseDescriptor);
        assertCoprocsContains(GlobalIndexChecker.class, indexDescriptor);
    }

    private void assertUsingNewCoprocs(TableDescriptor baseDescriptor) {
        assertCoprocsContains(IndexRegionObserver.class, baseDescriptor);
        assertCoprocsNotContains(Indexer.class, baseDescriptor);
    }

    private void assertUsingNewCoprocs(TableDescriptor baseDescriptor,
                                       TableDescriptor indexDescriptor) {
        assertCoprocsContains(IndexRegionObserver.class, baseDescriptor);
        assertCoprocConfig(baseDescriptor, IndexRegionObserver.class.getName(),
            INDEX_REGION_OBSERVER_CONFIG);
        assertCoprocsNotContains(Indexer.class, baseDescriptor);
        assertCoprocsNotContains(Indexer.class, indexDescriptor);
        assertCoprocsContains(GlobalIndexChecker.class, indexDescriptor);
        assertCoprocConfig(indexDescriptor, GlobalIndexChecker.class.getName(),
            GLOBAL_INDEX_CHECKER_CONFIG);
    }

    private void createIndexTable(String schemaName, String tableName, String indexName)
        throws SQLException {
        Connection conn = getConnection();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + fullTableName + "(v1)");
    }

    private void addColumnToBaseTable(String schemaName, String tableName, String columnName) throws Exception{
        Connection conn = getConnection();
        String ddl = "ALTER TABLE " + SchemaUtil.getTableName(schemaName, tableName) + " " +
            " ADD " + columnName + " varchar(512)";
        conn.createStatement().execute(ddl);
    }

    private void dropColumnToBaseTable(String schemaName, String tableName, String columnName) throws Exception{
        Connection conn = getConnection();
        String ddl = "ALTER TABLE " + SchemaUtil.getTableName(schemaName, tableName) + " " +
            " DROP COLUMN " + columnName;
        conn.createStatement().execute(ddl);
    }

    private void createBaseTable(String schemaName, String tableName, boolean multiTenant, Integer saltBuckets, String splits)
        throws SQLException {
        Connection conn = getConnection();
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        String ddl = "CREATE TABLE IF NOT EXISTS "
            + SchemaUtil.getTableName(schemaName, tableName) + " (t_id VARCHAR NOT NULL,\n" +
            "k1 VARCHAR NOT NULL,\n" +
            "k2 INTEGER NOT NULL,\n" +
            "v1 VARCHAR,\n" +
            "v2 INTEGER,\n" +
            "CONSTRAINT pk PRIMARY KEY (t_id, k1, k2))\n";
        String ddlOptions = multiTenant ? "MULTI_TENANT=true" : "";
        if (saltBuckets != null) {
            ddlOptions = ddlOptions
                + (ddlOptions.isEmpty() ? "" : ",")
                + "salt_buckets=" + saltBuckets;
        }
        if (splits != null) {
            ddlOptions = ddlOptions
                + (ddlOptions.isEmpty() ? "" : ",")
                + "splits=" + splits;
        }
        conn.createStatement().execute(ddl + ddlOptions);
        conn.close();
    }

    private PhoenixConnection getConnection() throws SQLException{
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        return (PhoenixConnection) DriverManager.getConnection(getUrl(),props);
    }
}