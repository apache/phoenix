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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class MappingTableDataTypeIT extends ParallelStatsDisabledIT {
    @Test
    public void testMappingHbaseTableToPhoenixTable() throws Exception {
        String mtest = generateUniqueName();
        final TableName tableName = TableName.valueOf(mtest);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        
        Admin admin = conn.getQueryServices().getAdmin();
        try {
            // Create table then get the single region for our new table.
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("cf1")))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("cf2")));
            admin.createTable(builder.build());
            Table t = conn.getQueryServices().getTable(Bytes.toBytes(mtest));
            insertData(tableName.getName(), admin, t);
            t.close();
            // create phoenix table that maps to existing HBase table
            createPhoenixTable(mtest);
            
            String selectSql = "SELECT * FROM " + mtest;
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            assertTrue("Expected single row", rs.next());
            // verify values from cf2 is not returned
            assertEquals("Number of columns", 2, rsMetaData.getColumnCount());
            assertEquals("Column Value", "value1", rs.getString(2));
            assertFalse("Expected single row ", rs.next());
            
            // delete the row
            String deleteSql = "DELETE FROM " + mtest + " WHERE id = 'row'";
            conn.createStatement().executeUpdate(deleteSql);
            conn.commit();
            
            // verify that no rows are returned when querying through phoenix
            rs = conn.createStatement().executeQuery(selectSql);
            assertFalse("Expected no row` ", rs.next());
            
            // verify that row with value for cf2 still exists when using hbase apis
            Scan scan = new Scan();
            ResultScanner results = t.getScanner(scan);
            Result result = results.next();
            assertNotNull("Expected single row", result);
            List<Cell> kvs = result.getColumnCells(Bytes.toBytes("cf2"), Bytes.toBytes("q2"));
            assertEquals("Expected single value ", 1, kvs.size());
            assertEquals("Column Value", "value2", Bytes.toString(kvs.get(0).getValueArray(), kvs.get(0).getValueOffset(), kvs.get(0).getValueLength()));
            assertNull("Expected single row", results.next());
        } finally {
            admin.close();
        }
    }

    private void insertData(final byte[] tableName, Admin admin, Table t) throws IOException,
            InterruptedException {
        Put p = new Put(Bytes.toBytes("row"));
        p.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("q1"), Bytes.toBytes("value1"));
        p.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("q2"), Bytes.toBytes("value2"));
        t.put(p);
        admin.flush(TableName.valueOf(tableName));
    }

    /**
     * Create a table in Phoenix that only maps column family cf1
     */
    private void createPhoenixTable(String tableName) throws SQLException {
        String ddl = "create table IF NOT EXISTS " + tableName+ " (" + " id varchar NOT NULL primary key,"
                + " \"cf1\".\"q1\" varchar" + " ) COLUMN_ENCODED_BYTES=NONE";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        conn.commit();
    }

}
