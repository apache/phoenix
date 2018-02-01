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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.http.ssl.KeyStoreTestUtil;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.queryserver.client.ThinClientUtil;
import org.apache.phoenix.queryserver.server.QueryServer;
import org.apache.phoenix.util.InstanceResolver;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class SecureQueryServerIT {
    private static final Log LOG = LogFactory.getLog(SecureQueryServerIT.class);

    private static final File TEMP_DIR = new File(getTempDirForClass());
    private static final File KEYTAB_DIR = new File(TEMP_DIR, "keytabs");
    private static final List<File> USER_KEYTAB_FILES = new ArrayList<>();

    private static final String SPNEGO_PRINCIPAL = "HTTP/localhost";
    private static final String PQS_PRINCIPAL = "phoenixqs/localhost";
    private static final String SERVICE_PRINCIPAL = "securecluster/localhost";
    private static File KEYTAB;

    private static MiniKdc KDC;
    private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
    private static LocalHBaseCluster HBASE_CLUSTER;
    private static int NUM_CREATED_USERS;

    private static ExecutorService PQS_EXECUTOR;
    private static QueryServer PQS;
    private static int PQS_PORT;
    private static String PQS_URL;

    private static String getTempDirForClass() {
        StringBuilder sb = new StringBuilder(32);
        sb.append(System.getProperty("user.dir")).append(File.separator);
        sb.append("target").append(File.separator);
        sb.append(SecureQueryServerIT.class.getSimpleName());
        return sb.toString();
    }

    private static void updateDefaultRealm() throws Exception {
        // (at least) one other phoenix test triggers the caching of this field before the KDC is up
        // which causes principal parsing to fail.
        Field f = KerberosName.class.getDeclaredField("defaultRealm");
        f.setAccessible(true);
        // Default realm for MiniKDC
        f.set(null, "EXAMPLE.COM");
    }

    private static void createUsers(int numUsers) throws Exception {
        assertNotNull("KDC is null, was setup method called?", KDC);
        NUM_CREATED_USERS = numUsers;
        for (int i = 1; i <= numUsers; i++) {
            String principal = "user" + i;
            File keytabFile = new File(KEYTAB_DIR, principal + ".keytab");
            KDC.createPrincipal(keytabFile, principal);
            USER_KEYTAB_FILES.add(keytabFile);
        }
    }

    private static Entry<String,File> getUser(int offset) {
        Preconditions.checkArgument(offset > 0 && offset <= NUM_CREATED_USERS);
        return Maps.immutableEntry("user" + offset, USER_KEYTAB_FILES.get(offset - 1));
    }

    /**
     * Setup the security configuration for hdfs.
     */
    private static void setHdfsSecuredConfiguration(Configuration conf) throws Exception {
        // Set principal+keytab configuration for HDFS
        conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, SERVICE_PRINCIPAL + "@" + KDC.getRealm());
        conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, KEYTAB.getAbsolutePath());
        conf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, SERVICE_PRINCIPAL + "@" + KDC.getRealm());
        conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, KEYTAB.getAbsolutePath());
        conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, SPNEGO_PRINCIPAL + "@" + KDC.getRealm());
        // Enable token access for HDFS blocks
        conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
        // Only use HTTPS (required because we aren't using "secure" ports)
        conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
        // Bind on localhost for spnego to have a chance at working
        conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
        conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

        // Generate SSL certs
        File keystoresDir = new File(UTIL.getDataTestDir("keystore").toUri().getPath());
        keystoresDir.mkdirs();
        String sslConfDir = KeyStoreTestUtil.getClasspathDir(SecureQueryServerIT.class);
        KeyStoreTestUtil.setupSSLConfig(keystoresDir.getAbsolutePath(), sslConfDir, conf, false);

        // Magic flag to tell hdfs to not fail on using ports above 1024
        conf.setBoolean("ignore.secure.ports.for.testing", true);
    }

    private static void ensureIsEmptyDirectory(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                FileUtils.deleteDirectory(f);
            } else {
                assertTrue("Failed to delete keytab directory", f.delete());
            }
        }
        assertTrue("Failed to create keytab directory", f.mkdirs());
    }

    /**
     * Setup and start kerberos, hbase
     */
    @BeforeClass
    public static void setUp() throws Exception {
        final Configuration conf = UTIL.getConfiguration();
        // Ensure the dirs we need are created/empty
        ensureIsEmptyDirectory(TEMP_DIR);
        ensureIsEmptyDirectory(KEYTAB_DIR);
        KEYTAB = new File(KEYTAB_DIR, "test.keytab");
        // Start a MiniKDC
        KDC = UTIL.setupMiniKdc(KEYTAB);
        // Create a service principal and spnego principal in one keytab
        // NB. Due to some apparent limitations between HDFS and HBase in the same JVM, trying to
        //     use separate identies for HBase and HDFS results in a GSS initiate error. The quick
        //     solution is to just use a single "service" principal instead of "hbase" and "hdfs"
        //     (or "dn" and "nn") per usual.
        KDC.createPrincipal(KEYTAB, SPNEGO_PRINCIPAL, PQS_PRINCIPAL, SERVICE_PRINCIPAL);
        // Start ZK by hand
        UTIL.startMiniZKCluster();

        // Create a number of unprivileged users
        createUsers(3);

        // Set configuration for HBase
        HBaseKerberosUtils.setPrincipalForTesting(SERVICE_PRINCIPAL + "@" + KDC.getRealm());
        HBaseKerberosUtils.setSecuredConfiguration(conf);
        setHdfsSecuredConfiguration(conf);
        UserGroupInformation.setConfiguration(conf);
        conf.setInt(HConstants.MASTER_PORT, 0);
        conf.setInt(HConstants.MASTER_INFO_PORT, 0);
        conf.setInt(HConstants.REGIONSERVER_PORT, 0);
        conf.setInt(HConstants.REGIONSERVER_INFO_PORT, 0);
        conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
            TokenProvider.class.getName());

        // Secure Phoenix setup
        conf.set("phoenix.queryserver.kerberos.http.principal", SPNEGO_PRINCIPAL + "@" + KDC.getRealm());
        conf.set("phoenix.queryserver.http.keytab.file", KEYTAB.getAbsolutePath());
        conf.set("phoenix.queryserver.kerberos.principal", PQS_PRINCIPAL + "@" + KDC.getRealm());
        conf.set("phoenix.queryserver.keytab.file", KEYTAB.getAbsolutePath());
        conf.setBoolean(QueryServices.QUERY_SERVER_DISABLE_KERBEROS_LOGIN, true);
        conf.setInt(QueryServices.QUERY_SERVER_HTTP_PORT_ATTRIB, 0);
        // Required so that PQS can impersonate the end-users to HBase
        conf.set("hadoop.proxyuser.phoenixqs.groups", "*");
        conf.set("hadoop.proxyuser.phoenixqs.hosts", "*");

        // Clear the cached singletons so we can inject our own.
        InstanceResolver.clearSingletons();
        // Make sure the ConnectionInfo doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return conf;
            }
            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });
        updateDefaultRealm();

        // Start HDFS
        UTIL.startMiniDFSCluster(1);
        // Use LocalHBaseCluster to avoid HBaseTestingUtility from doing something wrong
        // NB. I'm not actually sure what HTU does incorrect, but this was pulled from some test
        //     classes in HBase itself. I couldn't get HTU to work myself (2017/07/06)
        Path rootdir = UTIL.getDataTestDirOnTestFS(SecureQueryServerIT.class.getSimpleName());
        FSUtils.setRootDir(conf, rootdir);
        HBASE_CLUSTER = new LocalHBaseCluster(conf, 1);
        HBASE_CLUSTER.startup();

        // Then fork a thread with PQS in it.
        startQueryServer();
    }

    private static void startQueryServer() throws Exception {
        PQS = new QueryServer(new String[0], UTIL.getConfiguration());
        // Get the PQS ident for PQS to use
        final UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(PQS_PRINCIPAL, KEYTAB.getAbsolutePath());
        PQS_EXECUTOR = Executors.newSingleThreadExecutor();
        // Launch PQS, doing in the Kerberos login instead of letting PQS do it itself (which would
        // break the HBase/HDFS logins also running in the same test case).
        PQS_EXECUTOR.submit(new Runnable() {
            @Override public void run() {
                ugi.doAs(new PrivilegedAction<Void>() {
                    @Override public Void run() {
                        PQS.run();
                        return null;
                    }
                });
            }
        });
        PQS.awaitRunning();
        PQS_PORT = PQS.getPort();
        PQS_URL = ThinClientUtil.getConnectionUrl("localhost", PQS_PORT) + ";authentication=SPNEGO";
    }

    @AfterClass
    public static void stopKdc() throws Exception {
        // Remove our custom ConfigurationFactory for future tests
        InstanceResolver.clearSingletons();
        if (PQS_EXECUTOR != null) {
            PQS.stop();
            PQS_EXECUTOR.shutdown();
            if (!PQS_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.info("PQS didn't exit in 5 seconds, proceeding anyways.");
            }
        }
        if (HBASE_CLUSTER != null) {
            HBASE_CLUSTER.shutdown();
            HBASE_CLUSTER.join();
        }
        if (UTIL != null) {
            UTIL.shutdownMiniZKCluster();
        }
        if (KDC != null) {
            KDC.stop();
        }
    }

    @Test
    public void testBasicReadWrite() throws Exception {
        final Entry<String,File> user1 = getUser(1);
        UserGroupInformation user1Ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user1.getKey(), user1.getValue().getAbsolutePath());
        user1Ugi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override public Void run() throws Exception {
                // Phoenix
                final String tableName = "phx_table1";
                try (java.sql.Connection conn = DriverManager.getConnection(PQS_URL);
                        Statement stmt = conn.createStatement()) {
                    conn.setAutoCommit(true);
                    assertFalse(stmt.execute("CREATE TABLE " + tableName + "(pk integer not null primary key)"));
                    final int numRows = 5;
                    for (int i = 0; i < numRows; i++) {
                      assertEquals(1, stmt.executeUpdate("UPSERT INTO " + tableName + " values(" + i + ")"));
                    }

                    try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                        for (int i = 0; i < numRows; i++) {
                            assertTrue(rs.next());
                            assertEquals(i, rs.getInt(1));
                        }
                        assertFalse(rs.next());
                    }
                }
                return null;
            }
        });
    }

    byte[] copyBytes(byte[] src, int offset, int length) {
        byte[] dest = new byte[length];
        System.arraycopy(src, offset, dest, 0, length);
        return dest;
    }
}
