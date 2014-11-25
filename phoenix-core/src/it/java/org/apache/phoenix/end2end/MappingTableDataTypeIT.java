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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class MappingTableDataTypeIT extends BaseHBaseManagedTimeIT {
    @Test
    public void testMappingHbaseTableToPhoenixTable() throws Exception {
        final TableName tableName = TableName.valueOf("MTEST");
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        
        HBaseAdmin admin = conn.getQueryServices().getAdmin();
        try {
            // Create table then get the single region for our new table.
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            HColumnDescriptor columnDescriptor =  new HColumnDescriptor(Bytes.toBytes("cf"));
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor);
            HTableInterface t = conn.getQueryServices().getTable(Bytes.toBytes("MTEST"));
            insertData(tableName.getName(), admin, t);
            t.close();
            try {
                testCreateTableMismatchedType();
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(),e.getErrorCode());
            }
        } finally {
            admin.close();
        }
    }

    private void insertData(final byte[] tableName, HBaseAdmin admin, HTableInterface t) throws IOException,
            InterruptedException {
        Put p = new Put(Bytes.toBytes("row"));
        p.add(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("value1"));
        t.put(p);
        t.flushCommits();
        admin.flush(tableName);
    }

    /**
     * Test create a table in Phoenix with mismatched data type UNSIGNED_LONG
     */
    private void testCreateTableMismatchedType() throws SQLException {
        String ddl = "create table IF NOT EXISTS MTEST (" + " id varchar NOT NULL primary key,"
                + " \"cf\".\"q1\" unsigned_long" + " ) ";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        conn.commit();
        String query = "select * from MTEST";
        ResultSet rs = conn.createStatement().executeQuery(query);
        rs.next();
        rs.getLong(2);
    }

}
