/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.query.QueryServices;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test that verifies a user can read Phoenix tables with a minimal set of permissions.
 */
public class SystemTablePermissionsIT {
    private static String SUPERUSER;

    private static final Set<String> PHOENIX_SYSTEM_TABLES = new HashSet<>(Arrays.asList(
            "SYSTEM.CATALOG", "SYSTEM.SEQUENCE", "SYSTEM.STATS", "SYSTEM.FUNCTION",
                "SYSTEM.MUTEX"));
    // PHOENIX-XXXX SYSTEM.MUTEX isn't being created in the SYSTEM namespace as it should be.
    private static final Set<String> PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES = new HashSet<>(
            Arrays.asList("SYSTEM:CATALOG", "SYSTEM:SEQUENCE", "SYSTEM:STATS", "SYSTEM:FUNCTION",
                "SYSTEM.MUTEX"));

    private static final String TABLE_NAME =
        SystemTablePermissionsIT.class.getSimpleName().toUpperCase();
    private static final int NUM_RECORDS = 5;

    private HBaseTestingUtility testUtil = null;
    private Properties clientProperties = null;

    @BeforeClass
    public static void setup() throws Exception {
        SUPERUSER = System.getProperty("user.name");
    }

    private static void setCommonConfigProperties(Configuration conf) {
        conf.set("hbase.coprocessor.master.classes",
            "org.apache.hadoop.hbase.security.access.AccessController");
        conf.set("hbase.coprocessor.region.classes",
            "org.apache.hadoop.hbase.security.access.AccessController");
        conf.set("hbase.coprocessor.regionserver.classes",
            "org.apache.hadoop.hbase.security.access.AccessController");
        conf.set("hbase.security.exec.permission.checks", "true");
        conf.set("hbase.security.authorization", "true");
        conf.set("hbase.superuser", SUPERUSER);
    }

    @After
    public void cleanup() throws Exception {
        if (null != testUtil) {
          testUtil.shutdownMiniCluster();
          testUtil = null;
        }
    }

    @Test
    public void testSystemTablePermissions() throws Exception {
        testUtil = new HBaseTestingUtility();
        clientProperties = new Properties();
        Configuration conf = testUtil.getConfiguration();
        setCommonConfigProperties(conf);
        conf.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "false");
        clientProperties.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "false");
        testUtil.startMiniCluster(1);
        final UserGroupInformation superUser = UserGroupInformation.createUserForTesting(
            SUPERUSER, new String[0]);
        final UserGroupInformation regularUser = UserGroupInformation.createUserForTesting(
            "user", new String[0]);

        superUser.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createTable();
                readTable();
                return null;
            }
        });

        Set<String> tables = getHBaseTables();
        assertTrue("HBase tables do not include expected Phoenix tables: " + tables,
            tables.containsAll(PHOENIX_SYSTEM_TABLES));

        // Grant permission to the system tables for the unprivileged user
        superUser.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                try {
                    grantPermissions(regularUser.getShortUserName(), PHOENIX_SYSTEM_TABLES,
                        Action.EXEC, Action.READ);
                    grantPermissions(regularUser.getShortUserName(),
                        Collections.singleton(TABLE_NAME), Action.READ);
                } catch (Throwable e) {
                    if (e instanceof Exception) {
                        throw (Exception) e;
                    } else {
                        throw new Exception(e);
                    }
                }
                return null;
            }
        });

        // Make sure that the unprivileged user can read the table
        regularUser.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                // We expect this to not throw an error
                readTable();
                return null;
            }
        });
    }

    @Test
    public void testNamespaceMappedSystemTables() throws Exception {
        testUtil = new HBaseTestingUtility();
        clientProperties = new Properties();
        Configuration conf = testUtil.getConfiguration();
        setCommonConfigProperties(conf);
        testUtil.getConfiguration().set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        clientProperties.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        testUtil.startMiniCluster(1);
        final UserGroupInformation superUser =
            UserGroupInformation.createUserForTesting(SUPERUSER, new String[0]);
        final UserGroupInformation regularUser =
            UserGroupInformation.createUserForTesting("user", new String[0]);

        superUser.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createTable();
                readTable();
                return null;
            }
        });

        Set<String> tables = getHBaseTables();
        assertTrue("HBase tables do not include expected Phoenix tables: " + tables,
            tables.containsAll(PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES));

        // Grant permission to the system tables for the unprivileged user
        // An unprivileged user should only need to be able to Read and eXecute on them.
        superUser.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                try {
                    grantPermissions(regularUser.getShortUserName(),
                        PHOENIX_NAMESPACE_MAPPED_SYSTEM_TABLES, Action.EXEC, Action.READ);
                    grantPermissions(regularUser.getShortUserName(),
                        Collections.singleton(TABLE_NAME), Action.READ);
                } catch (Throwable e) {
                    if (e instanceof Exception) {
                        throw (Exception) e;
                    } else {
                        throw new Exception(e);
                    }
                }
                return null;
            }
        });

        regularUser.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                // We expect this to not throw an error
                readTable();
                return null;
            }
        });
    }

    private String getJdbcUrl() {
        return "jdbc:phoenix:localhost:" + testUtil.getZkCluster().getClientPort() + ":/hbase";
    }

    private void createTable() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), clientProperties);
            Statement stmt = conn.createStatement();) {
            assertFalse(stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME));
            assertFalse(stmt.execute("CREATE TABLE " + TABLE_NAME
                + "(pk INTEGER not null primary key, data VARCHAR)"));
            try (PreparedStatement pstmt = conn.prepareStatement("UPSERT INTO "
                + TABLE_NAME + " values(?, ?)")) {
                for (int i = 0; i < NUM_RECORDS; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, Integer.toString(i));
                    assertEquals(1, pstmt.executeUpdate());
                }
            }
            conn.commit();
        }
    }

    private void readTable() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), clientProperties);
            Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT pk, data FROM " + TABLE_NAME);
            assertNotNull(rs);
            int i = 0;
            while (rs.next()) {
                assertEquals(i, rs.getInt(1));
                assertEquals(Integer.toString(i), rs.getString(2));
                i++;
            }
            assertEquals(NUM_RECORDS, i);
        }
    }

    private void grantPermissions(String toUser, Set<String> tablesToGrant, Action... actions)
            throws Throwable {
          for (String table : tablesToGrant) {
              AccessControlClient.grant(testUtil.getConnection(), TableName.valueOf(table), toUser,
                  null, null, actions);
          }
    }

    private Set<String> getHBaseTables() throws IOException {
        Set<String> tables = new HashSet<>();
        for (TableName tn : testUtil.getHBaseAdmin().listTableNames()) {
            tables.add(tn.getNameAsString());
        }
        return tables;
    }
}
