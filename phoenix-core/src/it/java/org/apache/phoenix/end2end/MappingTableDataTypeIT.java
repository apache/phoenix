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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;


public class MappingTableDataTypeIT extends ParallelStatsDisabledIT {
    @Test
    public void testMappingHbaseTableToPhoenixTable() throws Exception {
        String mtest = generateUniqueName();
        final TableName tableName = TableName.valueOf(mtest);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        
        HBaseAdmin admin = conn.getQueryServices().getAdmin();
        try {
            // Create table then get the single region for our new table.
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            HColumnDescriptor columnDescriptor1 =  new HColumnDescriptor(Bytes.toBytes("cf1"));
            HColumnDescriptor columnDescriptor2 =  new HColumnDescriptor(Bytes.toBytes("cf2"));
            descriptor.addFamily(columnDescriptor1);
            descriptor.addFamily(columnDescriptor2);
            admin.createTable(descriptor);
            HTableInterface t = conn.getQueryServices().getTable(Bytes.toBytes(mtest));
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
            List<KeyValue> kvs = result.getColumn(Bytes.toBytes("cf2"), Bytes.toBytes("q2"));
            assertEquals("Expected single value ", 1, kvs.size());
            assertEquals("Column Value", "value2", Bytes.toString(kvs.get(0).getValue()));
            assertNull("Expected single row", results.next());
        } finally {
            admin.close();
        }
    }

    private void insertData(final byte[] tableName, HBaseAdmin admin, HTableInterface t) throws IOException,
            InterruptedException {
        Put p = new Put(Bytes.toBytes("row"));
        p.add(Bytes.toBytes("cf1"), Bytes.toBytes("q1"), Bytes.toBytes("value1"));
        p.add(Bytes.toBytes("cf2"), Bytes.toBytes("q2"), Bytes.toBytes("value2"));
        t.put(p);
        t.flushCommits();
        admin.flush(tableName);
    }

    /**
     * Create a table in Phoenix that only maps column family cf1
     */
    private void createPhoenixTable(String tableName) throws SQLException {
        String ddl = "create table IF NOT EXISTS " + tableName+ " (" + " id varchar NOT NULL primary key,"
                + " \"cf1\".\"q1\" varchar" + " ) ";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        conn.commit();
    }

}
