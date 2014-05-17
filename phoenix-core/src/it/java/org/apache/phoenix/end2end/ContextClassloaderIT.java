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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ConfigUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class ContextClassloaderIT  {

    private static HBaseTestingUtility hbaseTestUtil;
    private static ClassLoader badContextClassloader;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        hbaseTestUtil = new HBaseTestingUtility();
        ConfigUtil.setReplicationConfigIfAbsent(hbaseTestUtil.getConfiguration());
        hbaseTestUtil.getConfiguration().setInt(QueryServices.MASTER_INFO_PORT_ATTRIB, -1);
        hbaseTestUtil.getConfiguration().setInt(QueryServices.REGIONSERVER_INFO_PORT_ATTRIB, -1);
        hbaseTestUtil.startMiniCluster();
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE test (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR)");
        stmt.execute("UPSERT INTO test VALUES (1, 'name1')");
        stmt.execute("UPSERT INTO test VALUES (2, 'name2')");
        stmt.close();
        conn.commit();
        conn.close();
        badContextClassloader = new URLClassLoader(new URL[] {
                File.createTempFile("invalid", ".jar").toURI().toURL() }, null);
    }

    private static String getUrl() {
        return "jdbc:phoenix:localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        hbaseTestUtil.shutdownMiniCluster();
    }

    @Test
    public void testQueryWithDifferentContextClassloader() throws SQLException, InterruptedException {
        Runnable target = new Runnable() {


            @Override
            public void run() {
                try {
                    Connection conn = DriverManager.getConnection(getUrl());
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("select * from test where name = 'name2'");
                    while (rs.next()) {
                        // Just make sure we run over all records
                    }
                    rs.close();
                    stmt.close();
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        BadContextClassloaderThread t = new BadContextClassloaderThread(target);
        t.start();
        t.join();
        assertFalse(t.failed);
    }

    @Test
    public void testGetDatabaseMetadataWithDifferentContextClassloader() throws InterruptedException {
        Runnable target = new Runnable() {
            @Override
            public void run() {
                try {
                    Connection conn = DriverManager.getConnection(getUrl());
                    ResultSet tablesRs = conn.getMetaData().getTables(null, null, null, null);
                    while (tablesRs.next()) {
                        // Just make sure we run over all records
                    }
                    tablesRs.close();
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        BadContextClassloaderThread t = new BadContextClassloaderThread(target);
        t.start();
        t.join();
        assertFalse(t.failed);
    }

    @Test
    public void testExecuteDdlWithDifferentContextClassloader() throws InterruptedException {
        Runnable target = new Runnable() {
            @Override
            public void run() {
                try {
                    Connection conn = DriverManager.getConnection(getUrl());
                    Statement stmt = conn.createStatement();
                    stmt.execute("CREATE TABLE T2 (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR)");
                    stmt.execute("UPSERT INTO T2 VALUES (1, 'name1')");
                    conn.commit();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM T2");
                    assertTrue(rs.next());
                    assertFalse(rs.next());
                    rs.close();
                    stmt.close();
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        BadContextClassloaderThread t = new BadContextClassloaderThread(target);
        t.start();
        t.join();
        assertFalse(t.failed);
    }

    static class BadContextClassloaderThread extends Thread {

        private final Runnable target;
        boolean failed = false;

        public BadContextClassloaderThread(Runnable target) {
            super("BadContextClassloaderThread");
            this.target = target;
            setContextClassLoader(badContextClassloader);
        }

        @Override
        public void run() {
            try {
                target.run();
            } catch (Throwable t) {
                failed = true;
                throw new RuntimeException(t);
            }
        }

    }
}
