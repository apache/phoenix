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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
/*
 * since 4.8
 */
@Category(ParallelStatsDisabledTest.class)
public class NamespaceSchemaMappingIT extends ParallelStatsDisabledIT {
    /**
     * Tests that when: There is a table created with older version of phoenix and a table created with newer version
     * having {@code QueryServices#IS_NAMESPACE_MAPPING_ENABLED} true, then there is only a flag
     * {@code PhoenixDatabaseMetaData#IS_NAMESPACE_MAPPED} differentiates that whether schema of the table is mapped to
     * namespace or not
     */
    @Test
    public void testBackWardCompatibility() throws Exception {

        String namespace = generateUniqueName();
        String schemaName = namespace;
        String tableName = generateUniqueName();

        String phoenixFullTableName = schemaName + "." + tableName;
        String hbaseFullTableName = schemaName + ":" + tableName;
        Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName))
                .addColumnFamily(ColumnFamilyDescriptorBuilder.of(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES)).build());
        admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(phoenixFullTableName))
                .addColumnFamily(ColumnFamilyDescriptorBuilder.of(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES)).build());

        Put put = new Put(PVarchar.INSTANCE.toBytes(phoenixFullTableName));
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
        Table phoenixSchematable = admin.getConnection().getTable(TableName.valueOf(phoenixFullTableName));
        phoenixSchematable.put(put);
        put = new Put(PVarchar.INSTANCE.toBytes(hbaseFullTableName));
        put.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES,
                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
        Table namespaceMappedtable = admin.getConnection().getTable(TableName.valueOf(hbaseFullTableName));
        namespaceMappedtable.put(put);
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "create table " + phoenixFullTableName + "(tableName varchar primary key) COLUMN_ENCODED_BYTES=NONE";
        conn.createStatement().execute(ddl);
        String query = "select tableName from " + phoenixFullTableName;

        ResultSet rs = conn.createStatement().executeQuery(query);
        TestUtil.dumpTable(namespaceMappedtable);
        TestUtil.dumpTable(phoenixSchematable);
        assertTrue(rs.next());
        assertEquals(phoenixFullTableName, rs.getString(1));
        namespaceMappedtable.close();
        phoenixSchematable.close();

        Table metatable = admin.getConnection().getTable(
                SchemaUtil.getPhysicalName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES,
                        (conn.unwrap(PhoenixConnection.class).getQueryServices().getProps())));
        Put p = new Put(SchemaUtil.getTableKey(null, schemaName, tableName));
        p.addColumn(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED_BYTES,
                PBoolean.INSTANCE.toBytes(true));
        metatable.put(p);
        metatable.close();

        PhoenixConnection phxConn = (conn.unwrap(PhoenixConnection.class));
        phxConn.getQueryServices().clearCache();
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(hbaseFullTableName, rs.getString(1));
        admin.close();
        conn.close();
    }
}
