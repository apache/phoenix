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

package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.junit.Test;

public class PhoenixEmbeddedDriverTest {

    @Test
    public void testGetZKConnectionInfo() throws SQLException {
        Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        String defaultQuorum = config.get(HConstants.ZOOKEEPER_QUORUM);

        for (String protocol : new String[] { "phoenix", "phoenix+zk" }) {
            String[] urls =
                    new String[] { null,
                            "",
                            "jdbc:" + protocol + "",
                            "jdbc:" + protocol + ";test=true",
                            "jdbc:" + protocol + ":localhost",
                            "localhost",
                            "localhost;",
                            "jdbc:" + protocol + ":localhost:123",
                            "jdbc:" + protocol + ":localhost:123;foo=bar",
                            "localhost:123",
                            "jdbc:" + protocol + ":localhost:123:/hbase",
                            "jdbc:" + protocol + ":localhost:123:/foo-bar",
                            "jdbc:" + protocol + ":localhost:123:/foo-bar;foo=bas",
                            "localhost:123:/foo-bar",
                            "jdbc:" + protocol + ":localhost:/hbase",
                            "jdbc:" + protocol + ":localhost:/foo-bar",
                            "jdbc:" + protocol + ":localhost:/foo-bar;test=true",
                            "localhost:/foo-bar",
                            "jdbc:" + protocol + ":v1,v2,v3",
                            "jdbc:" + protocol + ":v1,v2,v3;",
                            "jdbc:" + protocol + ":v1,v2,v3;test=true",
                            "v1,v2,v3",
                            "jdbc:" + protocol + ":v1,v2,v3:/hbase",
                            "jdbc:" + protocol + ":v1,v2,v3:/hbase;test=true",
                            "v1,v2,v3:/foo-bar",
                            "jdbc:" + protocol + ":v1,v2,v3:123:/hbase",
                            "v1,v2,v3:123:/hbase",
                            "jdbc:" + protocol + ":v1,v2,v3:123:/hbase;test=false",
                            "jdbc:" + protocol + ":v1,v2,v3:123:/hbase:user/principal:/user.keytab;test=false",
                            "jdbc:" + protocol + ":v1,v2,v3:123:/foo-bar:user/principal:/user.keytab;test=false",
                            "jdbc:" + protocol + ":v1,v2,v3:123:user/principal:/user.keytab;test=false",
                            "jdbc:" + protocol + ":v1,v2,v3:user/principal:/user.keytab;test=false",
                            "jdbc:" + protocol + ":v1,v2,v3:/hbase:user/principal:/user.keytab;test=false",
                            "jdbc:" + protocol + ":v1,v2,v3:LongRunningQueries;test=false",
                            "jdbc:" + protocol + ":v1,v2,v3:345:LongRunningQueries;test=false",
                            "jdbc:" + protocol + ":localhost:1234:user:C:\\user.keytab",
                            "jdbc:" + protocol + ":v1,v2,v3:345:/hbase:user1:C:\\Documents and Settings\\user1\\user1.keytab;test=false", };
            String[][] partsList =
                    new String[][] { { defaultQuorum + ":2181", null, "/hbase" },
                            { defaultQuorum + ":2181", null, "/hbase" },
                            { defaultQuorum + ":2181", null, "/hbase" }, {},
                            { "localhost:2181", null, "/hbase" },
                            { "localhost:2181", null, "/hbase" },
                            { "localhost:2181", null, "/hbase" },
                            { "localhost:123", null, "/hbase" },
                            { "localhost:123", null, "/hbase" },
                            { "localhost:123", null, "/hbase" },
                            { "localhost:123", null, "/hbase" },
                            { "localhost:123", null, "/foo-bar" },
                            { "localhost:123", null, "/foo-bar" },
                            { "localhost:123", null, "/foo-bar" },
                            { "localhost:2181", null, "/hbase" },
                            { "localhost:2181", null, "/foo-bar" },
                            { "localhost:2181", null, "/foo-bar" },
                            { "localhost:2181", null, "/foo-bar" },
                            { "v1:2181,v2:2181,v3:2181", null, "/hbase" },
                            { "v1:2181,v2:2181,v3:2181", null, "/hbase" },
                            { "v1:2181,v2:2181,v3:2181", null, "/hbase" },
                            { "v1:2181,v2:2181,v3:2181", null, "/hbase" },
                            { "v1:2181,v2:2181,v3:2181", null, "/hbase" },
                            { "v1:2181,v2:2181,v3:2181", null, "/hbase" },
                            { "v1:2181,v2:2181,v3:2181", null, "/foo-bar" },
                            { "v1:123,v2:123,v3:123", null, "/hbase" },
                            { "v1:123,v2:123,v3:123", null, "/hbase" },
                            { "v1:123,v2:123,v3:123", null, "/hbase" },
                            { "v1:123,v2:123,v3:123", null, "/hbase", "user/principal",
                                    "/user.keytab" },
                            { "v1:123,v2:123,v3:123", null, "/foo-bar", "user/principal",
                                    "/user.keytab" },
                            { "v1:123,v2:123,v3:123", null, "/hbase", "user/principal",
                                    "/user.keytab" },
                            { "v1:2181,v2:2181,v3:2181", null, "/hbase", "user/principal",
                                    "/user.keytab" },
                            { "v1:2181,v2:2181,v3:2181", null, "/hbase", "user/principal",
                                    "/user.keytab" },
                            { "v1:2181,v2:2181,v3:2181", null, "/hbase", "LongRunningQueries" },
                            { "v1:345,v2:345,v3:345", null, "/hbase", "LongRunningQueries" },
                            { "localhost:1234", null, "/hbase", "user", "C:\\user.keytab" },
                            { "v1:345,v2:345,v3:345", null, "/hbase", "user1",
                                    "C:\\Documents and Settings\\user1\\user1.keytab" }, };
            assertEquals(urls.length, partsList.length);
            for (int i = 0; i < urls.length; i++) {
                int pos = 0;
                try {
                    ZKConnectionInfo info =
                            (ZKConnectionInfo) ConnectionInfo.create(urls[i], null, null);
                    String[] parts = partsList[i];
                    if (parts.length > pos) {
                        assertEquals(parts[pos], info.getZkHosts());
                    }
                    if (parts.length > ++pos) {
                        assertEquals(parts[pos], info.getZkPort());
                    }
                    if (parts.length > ++pos) {
                        assertEquals(parts[pos], info.getZkRootNode());
                    }
                    if (parts.length > ++pos) {
                        assertEquals(parts[pos], info.getPrincipal());
                    }
                    if (parts.length > ++pos) {
                        assertEquals(parts[pos], info.getKeytab());
                    }
                } catch (AssertionError e) {
                    throw new AssertionError(
                            "For \"" + urls[i] + " at position: " + pos + "\": " + e.getMessage());
                }
            }
        }

    }

    @Test
    public void testGetMasterConnectionInfo() throws SQLException {
        assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.3.0")>=0);
        Configuration config =
                HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl", "org.apache.hadoop.hbase.client.MasterRegistry");
        String defaultMasters = "defaultmaster1:1243,defaultmaster2:2345";
        config.set("hbase.masters", defaultMasters);

        String[] urls = new String[] {
            null,
            "",
            "jdbc:phoenix+master",
            "jdbc:phoenix+master;test=true",
            "jdbc:phoenix",
            "jdbc:phoenix+master:localhost",
            "localhost",
            "localhost;",
            "localhost:123",
            "localhost,localhost2:123;",
            "localhost\\:123",
            "localhost\\:123:",
            "localhost\\:123::",
            "localhost\\:123:::",
            "localhost\\:123::::",
            "localhost\\:123:::::",
            "localhost\\:123:345::::",
            "localhost,localhost2\\:123;",
            "localhost,localhost2\\:123:456",
            "localhost,localhost2\\:123:456;test=false",
            "localhost\\:123:::user/principal:/user.keytab",
            "localhost\\:123:::LongRunningQueries",
            "localhost\\:123:::LongRunningQueries:",
            "localhost\\:123:::LongRunningQueries::",
            "localhost\\:123:::user/principal:C:\\user.keytab",
            "localhost\\:123:::user/principal:C:\\Documents and Settings\\user1\\user1.keytab",
        };
        String[][] partsList = new String[][] {
            {defaultMasters},
            {defaultMasters},
            {defaultMasters},
            {defaultMasters},
            {defaultMasters},
            {"localhost:"+HConstants.DEFAULT_MASTER_PORT},
            {"localhost:"+HConstants.DEFAULT_MASTER_PORT},
            {"localhost:"+HConstants.DEFAULT_MASTER_PORT},
            {"localhost:123"},
            {"localhost2:123,localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost2:123,localhost:16000"},
            {"localhost2:123,localhost:456"},
            {"localhost2:123,localhost:456"},
            {"localhost:123","user/principal","/user.keytab"},
            {"localhost:123","LongRunningQueries",null},
            {"localhost:123","LongRunningQueries",null},
            {"localhost:123","LongRunningQueries",null},
            {"localhost:123","user/principal","C:\\user.keytab"},
            {"localhost:123","user/principal","C:\\Documents and Settings\\user1\\user1.keytab"},
        };
        assertEquals(urls.length,partsList.length);
        for (int i = 0; i < urls.length; i++) {
            try {
                Configuration testConfig = new Configuration(config);
                MasterConnectionInfo info = (MasterConnectionInfo)ConnectionInfo.create(urls[i], testConfig, null, null);
                String[] parts = partsList[i];
                assertEquals(parts[0], info.getBoostrapServers());
                if(parts.length>1) {
                    assertEquals(parts[1], info.getPrincipal());
                } else {
                    assertNull(info.getPrincipal());
                }
                if(parts.length>2) {
                    assertEquals(parts[2], info.getKeytab());
                } else {
                    assertNull(info.getKeytab());
                }
            } catch (AssertionError e) {
                throw new AssertionError("For \"" + urls[i] + ": " + e.getMessage());
            }
        }
    }

    @Test
    public void testGetRPCConnectionInfo() throws SQLException {
        assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0")>=0);
        Configuration config =
                HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl", "org.apache.hadoop.hbase.client.RpcConnectionRegistry");
        String defaultBoostraps = "defaultmaster1:1243,defaultmaster2:2345";
        config.set("hbase.client.bootstrap.servers", defaultBoostraps);

        String[] urls = new String[] {
            null,
            "",
            "jdbc:phoenix+rpc",
            "jdbc:phoenix+rpc\";test=true",
            "jdbc:phoenix",
            "jdbc:phoenix+rpc\":localhost",
            "localhost",
            "localhost;",
            "localhost:123",
            "localhost,localhost2:123;",
            "localhost\\:123",
            "localhost\\:123:",
            "localhost\\:123::",
            "localhost\\:123:::",
            "localhost\\:123::::",
            "localhost\\:123:::::",
            "localhost\\:123:345::::",
            "localhost,localhost2\\:123;",
            "localhost,localhost2\\:123:456",
            "localhost,localhost2\\:123:456;test=false",
            "localhost\\:123:::user/principal:/user.keytab",
            "localhost\\:123:::LongRunningQueries",
            "localhost\\:123:::LongRunningQueries:",
            "localhost\\:123:::LongRunningQueries::",
            "localhost\\:123:::user/principal:C:\\user.keytab",
            "localhost\\:123:::user/principal:C:\\Documents and Settings\\user1\\user1.keytab",
        };
        String[][] partsList = new String[][] {
            {defaultBoostraps},
            {defaultBoostraps},
            {defaultBoostraps},
            {defaultBoostraps},
            {defaultBoostraps},
            {"localhost"},
            {"localhost"},
            {"localhost"},
            {"localhost:123"},
            {"localhost2:123,localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            {"localhost:123"},
            //No default port
            {"localhost,localhost2:123"},
            {"localhost2:123,localhost:456"},
            {"localhost2:123,localhost:456"},
            {"localhost:123","user/principal","/user.keytab"},
            {"localhost:123","LongRunningQueries",null},
            {"localhost:123","LongRunningQueries",null},
            {"localhost:123","LongRunningQueries",null},
            {"localhost:123","user/principal","C:\\user.keytab"},
            {"localhost:123","user/principal","C:\\Documents and Settings\\user1\\user1.keytab"},
        };
        assertEquals(urls.length,partsList.length);
        for (int i = 0; i < urls.length; i++) {
            try {
                Configuration testConfig = new Configuration(config);
                RPCConnectionInfo info = (RPCConnectionInfo)ConnectionInfo.create(urls[i], testConfig, null, null);
                String[] parts = partsList[i];
                assertEquals(parts[0], info.getBoostrapServers());
                if(parts.length>1) {
                    assertEquals(parts[1], info.getPrincipal());
                } else {
                    assertNull(info.getPrincipal());
                }
                if(parts.length>2) {
                    assertEquals(parts[2], info.getKeytab());
                } else {
                    assertNull(info.getKeytab());
                }
            } catch (AssertionError e) {
                throw new AssertionError("For \"" + urls[i] + ": " + e.getMessage());
            }
        }
    }

    @Test
    public void testNegativeGetConnectionInfo() throws SQLException {
        String[] urls = new String[] {
            //Reject unescaped ports in quorum string
            "jdbc:phoenix:v1:1,v2:2,v3:3",
            "jdbc:phoenix:v1:1,v2:2,v3:3;test=true",
            "jdbc:phoenix:v1,v2,v3:-1:/hbase;test=true",
            "jdbc:phoenix:v1,v2,v3:-1",
            "jdbc:phoenix+zk:v1:1,v2:2,v3:3",
            "jdbc:phoenix+zk:v1:1,v2:2,v3:3;test=true",
            "jdbc:phoenix+zk:v1,v2,v3:-1:/hbase;test=true",
            "jdbc:phoenix+zk:v1,v2,v3:-1"
        };
        for (String url : urls) {
            try {
                ConnectionInfo.create(url, null, null);
                throw new AssertionError("Expected exception for \"" + url + "\"");
            } catch (SQLException e) {
                try {
                    assertEquals(SQLExceptionCode.MALFORMED_CONNECTION_URL.getSQLState(), e.getSQLState());
                } catch (AssertionError ae) {
                    throw new AssertionError("For \"" + url + "\": " + ae.getMessage());
                }
            }
        }
    }

    @Test
    public void testRPCNegativeGetConnectionInfo() throws SQLException {
        assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0")>=0);
        String[] urls = new String[] {
            //Reject unescaped and invalid ports in quorum string
            "jdbc:phoenix+rpc:v1:1,v2:2,v3:3",
            "jdbc:phoenix+rpc:v1:1,v2:2,v3:3;test=true",
            "jdbc:phoenix+rpc:v1,v2,v3:-1:/hbase;test=true",
            "jdbc:phoenix+rpc:v1,v2,v3:-1",
            "jdbc:phoenix+master:v1:1,v2:2,v3:3",
            "jdbc:phoenix+master:v1:1,v2:2,v3:3;test=true",
            "jdbc:phoenix+master:v1,v2,v3:-1:/hbase;test=true",
            "jdbc:phoenix+master:v1,v2,v3:-1",
            //Reject rootnode and missing empty rootnode field
            "jdbc:phoenix+rpc:localhost,localhost2\\:123:456:rootNode",
            "jdbc:phoenix+rpc:localhost,localhost2\\:123:456:rootNode:prinicpial:keystore",
            "jdbc:phoenix+rpc:localhost,localhost2\\:123:456:prinicpial",
            "jdbc:phoenix+rpc:localhost,localhost2\\:123:456:prinicpial:keystore",
            "jdbc:phoenix+master:localhost,localhost2\\:123:456:rootNode",
            "jdbc:phoenix+master:localhost,localhost2\\:123:456:rootNode:prinicpial:keystore",
            "jdbc:phoenix+master:localhost,localhost2\\:123:456:prinicpial",
            "jdbc:phoenix+master:localhost,localhost2\\:123:456:prinicpial:keystore",

        };
        for (String url : urls) {
            try {
                ConnectionInfo.create(url, null, null);
                throw new AssertionError("Expected exception for \"" + url + "\"");
            } catch (SQLException e) {
                try {
                    assertEquals(SQLExceptionCode.MALFORMED_CONNECTION_URL.getSQLState(), e.getSQLState());
                } catch (AssertionError ae) {
                    throw new AssertionError("For \"" + url + "\": " + ae.getMessage());
                }
            }
        }
    }

    @Test
    public void testMasterDefaults() throws SQLException {
        assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.3.0") >= 0);
        try {
            Configuration config =
                    HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
            config.set("hbase.client.registry.impl",
                "org.apache.hadoop.hbase.client.MasterRegistry");
            ConnectionInfo.create("jdbc:phoenix+master", config, null, null);
            fail("Should have thrown exception");
        } catch (SQLException e) {
        }

        Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl", "org.apache.hadoop.hbase.client.MasterRegistry");
        config.set("hbase.master.hostname", "master.hostname");
        MasterConnectionInfo info =
                (MasterConnectionInfo) ConnectionInfo.create("jdbc:phoenix+master", config, null,
                    null);
        assertEquals(info.getBoostrapServers(), "master.hostname:16000");

        config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl", "org.apache.hadoop.hbase.client.MasterRegistry");
        config.set("hbase.master.hostname", "master.hostname");
        config.set("hbase.master.port", "17000");
        info = (MasterConnectionInfo) ConnectionInfo.create("jdbc:phoenix", config, null, null);
        assertEquals(info.getBoostrapServers(), "master.hostname:17000");

        config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl", "org.apache.hadoop.hbase.client.MasterRegistry");
        config.set("hbase.master.hostname", "master.hostname");
        config.set("hbase.master.port", "17000");
        config.set("hbase.masters", "master1:123,master2:234,master3:345");
        info = (MasterConnectionInfo) ConnectionInfo.create("jdbc:phoenix", config, null, null);
        assertEquals(info.getBoostrapServers(), "master1:123,master2:234,master3:345");

        config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl", "org.apache.hadoop.hbase.client.MasterRegistry");
        config.set("hbase.master.port", "17000");
        info =
                (MasterConnectionInfo) ConnectionInfo.create(
                    "jdbc:phoenix+master:master1.from.url,master2.from.url", config, null, null);
        assertEquals(info.getBoostrapServers(), "master1.from.url:17000,master2.from.url:17000");

        config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl", "org.apache.hadoop.hbase.client.MasterRegistry");
        config.set("hbase.master.port", "17000");
        info =
                (MasterConnectionInfo) ConnectionInfo.create(
                    "jdbc:phoenix+master:master1.from.url\\:123,master2.from.url", config, null,
                    null);
        assertEquals(info.getBoostrapServers(), "master1.from.url:123,master2.from.url:17000");

        config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl", "org.apache.hadoop.hbase.client.MasterRegistry");
        config.set("hbase.master.hostname", "master.hostname");
        config.set("hbase.master.port", "17000");
        config.set("hbase.masters", "master1:123,master2:234,master3:345");
        info =
                (MasterConnectionInfo) ConnectionInfo.create(
                    "jdbc:phoenix:master1.from.url\\:123,master2.from.url:18000", config, null,
                    null);
        assertEquals(info.getBoostrapServers(), "master1.from.url:123,master2.from.url:18000");

        config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl", "org.apache.hadoop.hbase.client.MasterRegistry");
        config.set("hbase.master.hostname", "master.hostname");
        config.set("hbase.master.port", "17000");
        config.set("hbase.masters", "master1:123,master2:234,master3:345");
        info =
                (MasterConnectionInfo) ConnectionInfo.create(
                    "jdbc:phoenix:master1.from.url\\:123,master2.from.url\\:234:18000", config,
                    null, null);
        assertEquals(info.getBoostrapServers(), "master1.from.url:123,master2.from.url:234");
    }

    @Test
    public void testRPCDefaults() throws SQLException {
        assumeTrue(VersionInfo.compareVersion(VersionInfo.getVersion(), "2.5.0") >= 0);
        try {
            Configuration config =
                    HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
            config.set("hbase.client.registry.impl",
                "org.apache.hadoop.hbase.client.RpcConnectionRegistry");
            ConnectionInfo.create("jdbc:phoenix+rpc", config, null, null);
            fail("Should have thrown exception");
        } catch (SQLException e) {
        }

        Configuration config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl",
            "org.apache.hadoop.hbase.client.RpcConnectionRegistry");
        config.set("hbase.client.bootstrap.servers", "bootstrap1\\:123,boostrap2\\:234");
        RPCConnectionInfo info =
                (RPCConnectionInfo) ConnectionInfo.create("jdbc:phoenix+rpc", config, null, null);
        assertEquals(info.getBoostrapServers(), "bootstrap1\\:123,boostrap2\\:234");

        config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl",
            "org.apache.hadoop.hbase.client.RpcConnectionRegistry");
        info =
                (RPCConnectionInfo) ConnectionInfo.create(
                    "jdbc:phoenix+rpc:bootstrap1.from.url,bootstrap2.from.url", config, null, null);
        // TODO looks like HBase doesn't do port replacement/check for RPC servers either ?
        assertEquals(info.getBoostrapServers(), "bootstrap1.from.url,bootstrap2.from.url");

        config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl",
            "org.apache.hadoop.hbase.client.RpcConnectionRegistry");
        info =
                (RPCConnectionInfo) ConnectionInfo.create(
                    "jdbc:phoenix+rpc:bootstrap1.from.url\\:123,bootstrap2.from.url\\::234", config,
                    null, null);
        // TODO looks like HBase doesn't do port replacement/check for RPC servers either ?
        assertEquals(info.getBoostrapServers(), "bootstrap1.from.url:123,bootstrap2.from.url:234");

        // Check fallback to master properties
        config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        config.set("hbase.client.registry.impl",
            "org.apache.hadoop.hbase.client.RpcConnectionRegistry");
        config.set("hbase.masters", "master1:123,master2:234,master3:345");
        info = (RPCConnectionInfo) ConnectionInfo.create("jdbc:phoenix+rpc", config, null, null);
        // TODO looks like HBase doesn't do port replacement/check for RPC servers either ?
        assertEquals(info.getBoostrapServers(), "master1:123,master2:234,master3:345");
    }

    @Test
    public void testNotAccept() throws Exception {
        Driver driver = new PhoenixDriver();
        assertFalse(driver.acceptsURL("jdbc:phoenix://localhost"));
        assertFalse(driver.acceptsURL("jdbc:phoenix:localhost;test=true;bar=foo"));
        assertFalse(driver.acceptsURL("jdbc:phoenix:localhost;test=true"));
        assertTrue(driver.acceptsURL("jdbc:phoenix:localhost:123"));
        assertTrue(driver.acceptsURL("jdbc:phoenix:localhost:123;untest=true"));
        assertTrue(driver.acceptsURL("jdbc:phoenix:localhost:123;untest=true;foo=bar"));
        DriverManager.deregisterDriver(driver);
    }

    @Test
    public void testPrincipalsMatching() throws Exception {
        assertTrue(ConnectionInfo.isSameName("user@EXAMPLE.COM", "user@EXAMPLE.COM"));
        assertTrue(ConnectionInfo.isSameName("user/localhost@EXAMPLE.COM", "user/localhost@EXAMPLE.COM"));
        // the user provided name might have a _HOST in it, which should be replaced by the hostname
        assertTrue(ConnectionInfo.isSameName("user/localhost@EXAMPLE.COM", "user/_HOST@EXAMPLE.COM", "localhost"));
        assertFalse(ConnectionInfo.isSameName("user/foobar@EXAMPLE.COM", "user/_HOST@EXAMPLE.COM", "localhost"));
        assertFalse(ConnectionInfo.isSameName("user@EXAMPLE.COM", "user/_HOST@EXAMPLE.COM", "localhost"));
        assertFalse(ConnectionInfo.isSameName("user@FOO", "user@BAR"));

        // NB: We _should_ be able to provide our or krb5.conf for this test to use, but this doesn't
        // seem to want to play nicely with the rest of the tests. Instead, we can just provide a default realm
        // by hand.

        // For an implied default realm, we should also match that. Users might provide a shortname
        // whereas UGI would provide the "full" name.
        assertTrue(ConnectionInfo.isSameName("user@APACHE.ORG", "user", null, "APACHE.ORG"));
        assertTrue(ConnectionInfo.isSameName("user/localhost@APACHE.ORG", "user/localhost", null, "APACHE.ORG"));
        assertFalse(ConnectionInfo.isSameName("user@APACHE.NET", "user", null, "APACHE.ORG"));
        assertFalse(ConnectionInfo.isSameName("user/localhost@APACHE.NET", "user/localhost", null, "APACHE.ORG"));
        assertTrue(ConnectionInfo.isSameName("user@APACHE.ORG", "user@APACHE.ORG", null, "APACHE.ORG"));
        assertTrue(ConnectionInfo.isSameName("user/localhost@APACHE.ORG", "user/localhost@APACHE.ORG", null, "APACHE.ORG"));

        assertTrue(ConnectionInfo.isSameName("user/localhost@APACHE.ORG", "user/_HOST", "localhost", "APACHE.ORG"));
        assertTrue(ConnectionInfo.isSameName("user/foobar@APACHE.ORG", "user/_HOST", "foobar", "APACHE.ORG"));
        assertFalse(ConnectionInfo.isSameName("user/localhost@APACHE.NET", "user/_HOST", "localhost", "APACHE.ORG"));
        assertFalse(ConnectionInfo.isSameName("user/foobar@APACHE.NET", "user/_HOST", "foobar", "APACHE.ORG"));
    }
}
