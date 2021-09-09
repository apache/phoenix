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

import static org.apache.phoenix.util.TestUtil.HBASE_NATIVE_SCHEMA_NAME;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class DropMetadataIT extends ParallelStatsDisabledIT {
    private static final String PRINCIPAL = "dropMetaData";
    private static final byte[] FAMILY_NAME = Bytes.toBytes(SchemaUtil.normalizeIdentifier("1"));
    public static final String SCHEMA_NAME = "";

    private Connection getConnection() throws Exception {
        return getConnection(PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
    }
    
    private Connection getConnection(Properties props) throws Exception {
        props.setProperty(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        // Force real driver to be used as the test one doesn't handle creating
        // more than one ConnectionQueryService
        props.setProperty(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, StringUtil.EMPTY_STRING);
        // Create new ConnectionQueryServices so that we can set DROP_METADATA_ATTRIB
        String url = QueryUtil.getConnectionUrl(props, config, PRINCIPAL);
        return DriverManager.getConnection(url, props);
    }

    @Test
    public void testDropIndexTableHasSameNameWithDataTable() {
        String tableName = generateUniqueName();
        String indexName = "IDX_" + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String createTable = "CREATE TABLE " + tableName + "  (id varchar not null primary key, col integer)";
            conn.createStatement().execute(createTable);
            String createIndex = "CREATE INDEX " + indexName + " on " + tableName + "(col)";
            conn.createStatement().execute(createIndex);
            String dropIndex = "DROP INDEX " + indexName + " on " + indexName;
            conn.createStatement().execute(dropIndex);
            fail("should not execute successfully");
        } catch (SQLException e) {
            assertTrue(SQLExceptionCode.PARENT_TABLE_NOT_FOUND.getErrorCode() == e.getErrorCode());
        }
    }

    @Test
    public void testDropViewKeepsHTable() throws Exception {
        Connection conn = getConnection();
        Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        String hbaseNativeViewName = generateUniqueName();

        byte[] hbaseNativeBytes = SchemaUtil.getTableNameAsBytes(HBASE_NATIVE_SCHEMA_NAME, hbaseNativeViewName);
        try {
             TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(hbaseNativeBytes));
            ColumnFamilyDescriptor columnDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY_NAME)
                    .setKeepDeletedCells(KeepDeletedCells.TRUE).build();
            builder.addColumnFamily(columnDescriptor);
            admin.createTable(builder.build());
        } finally {
            admin.close();
        }
        
        conn.createStatement().execute("create view " + hbaseNativeViewName+
                "   (uint_key unsigned_int not null," +
                "    ulong_key unsigned_long not null," +
                "    string_key varchar not null,\n" +
                "    \"1\".uint_col unsigned_int," +
                "    \"1\".ulong_col unsigned_long" +
                "    CONSTRAINT pk PRIMARY KEY (uint_key, ulong_key, string_key))\n" +
                ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING + "='" + DataBlockEncoding.NONE + "'");
        conn.createStatement().execute("drop view " + hbaseNativeViewName);
        conn.close();
    }
}
        
