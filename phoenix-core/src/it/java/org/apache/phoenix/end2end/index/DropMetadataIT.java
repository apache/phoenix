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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

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
    public void testDropViewKeepsHTable() throws Exception {
        Connection conn = getConnection();
        HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        String hbaseNativeViewName = generateUniqueName();

        byte[] hbaseNativeBytes = SchemaUtil.getTableNameAsBytes(HBASE_NATIVE_SCHEMA_NAME, hbaseNativeViewName);
        try {
            @SuppressWarnings("deprecation")
            HTableDescriptor descriptor = new HTableDescriptor(hbaseNativeBytes);
            HColumnDescriptor columnDescriptor =  new HColumnDescriptor(FAMILY_NAME);
            columnDescriptor.setKeepDeletedCells(true);
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor);
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
                     HColumnDescriptor.DATA_BLOCK_ENCODING + "='" + DataBlockEncoding.NONE + "'");
        conn.createStatement().execute("drop view " + hbaseNativeViewName);
        conn.close();
    }
}
        
