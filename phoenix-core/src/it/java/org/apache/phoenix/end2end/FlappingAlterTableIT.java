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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;

public class FlappingAlterTableIT extends ParallelStatsDisabledIT {
    private String dataTableFullName;
    
    @Before
    public void setupTableNames() throws Exception {
        String schemaName = "";
        String dataTableName = generateUniqueName();
        dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
    }

    @Test
    public void testAddColumnForNewColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " ADD CF.STRING VARCHAR";
        conn1.createStatement().execute(ddl);
        try (HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName)).getColumnFamilies();
            assertEquals(2, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(HColumnDescriptor.DEFAULT_TTL, columnFamilies[0].getTimeToLive());
            assertEquals("CF", columnFamilies[1].getNameAsString());
            assertEquals(HColumnDescriptor.DEFAULT_TTL, columnFamilies[1].getTimeToLive());
        }
    }

    @Test
    public void testNewColumnFamilyInheritsTTLOfEmptyCF() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) SALT_BUCKETS = 8, TTL = 1000";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " ADD CF.STRING VARCHAR";
        conn1.createStatement().execute(ddl);
        try (HBaseAdmin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            HTableDescriptor tableDesc = admin.getTableDescriptor(Bytes.toBytes(dataTableFullName));
            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            assertEquals(2, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(1000, columnFamilies[0].getTimeToLive());
            assertEquals("CF", columnFamilies[1].getNameAsString());
            assertEquals(1000, columnFamilies[1].getTimeToLive());
        }
    }


}
