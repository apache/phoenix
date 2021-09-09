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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.compat.hbase.HbaseCompatCapabilities;
import org.apache.phoenix.compat.hbase.coprocessor.CompatBaseScannerRegionObserver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class SCNIT extends ParallelStatsDisabledIT {

    @Test
    public void testReadBeforeDelete() throws Exception {
        //we don't support reading earlier than a delete in HBase 2.0-2.2, only in 1.4+ and 2.3+
        if (!HbaseCompatCapabilities.isLookbackBeyondDeletesSupported()){
            return;
        }
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        long timeBeforeDelete;
        long timeAfterDelete;
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute("CREATE TABLE " + fullTableName + "(k VARCHAR PRIMARY KEY, v VARCHAR)");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','aa')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('c','cc')");
            conn.commit();
            timeBeforeDelete = EnvironmentEdgeManager.currentTime() + 1;
            Thread.sleep(2);
            conn.createStatement().execute("DELETE FROM " + fullTableName + " WHERE k = 'b'");
            conn.commit();
            timeAfterDelete = EnvironmentEdgeManager.currentTime() + 1;
        }

        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timeBeforeDelete));
        try (Connection connscn = DriverManager.getConnection(getUrl(), props)) {
            ResultSet rs = connscn.createStatement().executeQuery("select * from " + fullTableName);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertFalse(rs.next());
            rs.close();
        }
        props.clear();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(timeAfterDelete));
        try (Connection connscn = DriverManager.getConnection(getUrl(), props)) {
            ResultSet rs = connscn.createStatement().executeQuery("select * from " + fullTableName);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertFalse(rs.next());
            rs.close();
        }

    }

    @Test
    public void testSCNWithTTL() throws Exception {
        int ttl = 2;
        String fullTableName = createTableWithTTL(ttl);
        //sleep for one second longer than ttl
        Thread.sleep(ttl * 1000 + 1000);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(EnvironmentEdgeManager.currentTime() - 1000));
        try (Connection connscn = DriverManager.getConnection(getUrl(), props)) {
            ResultSet rs = connscn.createStatement().executeQuery("select * from " + fullTableName);
            assertFalse(rs.next());
            rs.close();
        }
    }

    private String createTableWithTTL(int ttl) throws SQLException, InterruptedException {
        String schemaName = generateUniqueName();
        String tableName = generateUniqueName();
        StringBuilder optionsBuilder = new StringBuilder();
        if (ttl > 0){
            optionsBuilder.append("TTL=");
            optionsBuilder.append(ttl);
        }
        String ddlOptions = optionsBuilder.toString();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute(String.format("CREATE TABLE %s" +
                            "(k VARCHAR PRIMARY KEY, f.v VARCHAR) %s", fullTableName, ddlOptions));
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('a','aa')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('b','bb')");
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES('c','cc')");
            conn.commit();
        }
        return fullTableName;
    }

}