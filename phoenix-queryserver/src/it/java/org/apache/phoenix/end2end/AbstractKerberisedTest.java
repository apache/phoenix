/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.end2end;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Due to this bug https://bugzilla.redhat.com/show_bug.cgi?id=668830
 * We need to use `localhost.localdomain` as host name when running these tests on Jenkins (Centos)
 * but for Mac OS it should be `localhost` to pass.
 * The reason is kerberos principals in this tests are looked up from /etc/hosts
 * and a reverse DNS lookup of 127.0.0.1 is resolved to `localhost.localdomain` rather than `localhost` on Centos.
 * KDC sees `localhost` != `localhost.localdomain` and as the result test fails with authentication error.
 * It's also important to note these principals are shared between HDFs and HBase in this mini HBase cluster.
 * Some more reading https://access.redhat.com/solutions/57330
 */
public class AbstractKerberisedTest {
	private static final Log LOG = LogFactory.getLog(AbstractKerberisedTest.class);

	private static final File TEMP_DIR = new File(getTempDirForClass());
    private static final File KEYTAB_DIR = new File(TEMP_DIR, "keytabs");
    private static final List<File> USER_KEYTAB_FILES = new ArrayList<>();

    private static final String LOCAL_HOST_REVERSE_DNS_LOOKUP_NAME;

    static {
        try {
            LOCAL_HOST_REVERSE_DNS_LOOKUP_NAME = InetAddress.getByName("127.0.0.1").getCanonicalHostName();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static final String SPNEGO_PRINCIPAL = "HTTP/" + LOCAL_HOST_REVERSE_DNS_LOOKUP_NAME;
    protected static final String PQS_PRINCIPAL = "phoenixqs/" + LOCAL_HOST_REVERSE_DNS_LOOKUP_NAME;
    protected static final String SERVICE_PRINCIPAL = "securecluster/" + LOCAL_HOST_REVERSE_DNS_LOOKUP_NAME;
    protected static File KEYTAB;

	protected static MiniKdc KDC;
    protected static HBaseTestingUtility UTIL = new HBaseTestingUtility();
    protected static LocalHBaseCluster HBASE_CLUSTER;
    protected static int NUM_CREATED_USERS;

    protected static ExecutorService PQS_EXECUTOR;
    protected static QueryServer PQS;
    protected static int PQS_PORT;
    protected static String PQS_URL;

    private static String getTempDirForClass() {
        StringBuilder sb = new StringBuilder(32);
        sb.append(System.getProperty("user.dir")).append(File.separator);
        sb.append("target").append(File.separator);
        sb.append(AbstractKerberisedTest.class.getSimpleName());
        sb.append("-").append(UUID.randomUUID());
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

    protected static Map.Entry<String,File> getUser(int offset) {
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
        String sslConfDir = KeyStoreTestUtil.getClasspathDir(AbstractKerberisedTest.class);
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
     * Setup and start kerberosed, hbase
     */
    public static void configureAndStartQueryServer(final Configuration conf, int numberOfUsers) throws Exception {
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
        createUsers(numberOfUsers);

        // Set configuration for HBase
        HBaseKerberosUtils.setPrincipalForTesting(SERVICE_PRINCIPAL + "@" + KDC.getRealm());
        HBaseKerberosUtils.setSecuredConfiguration(conf);
        setHdfsSecuredConfiguration(conf);
        UserGroupInformation.setConfiguration(conf);
        conf.setInt(HConstants.MASTER_PORT, 0);
        conf.setInt(HConstants.MASTER_INFO_PORT, 0);
        conf.setInt(HConstants.REGIONSERVER_PORT, 0);
        conf.setInt(HConstants.REGIONSERVER_INFO_PORT, 0);

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
        Path rootdir = UTIL.getDataTestDirOnTestFS(AbstractKerberisedTest.class.getSimpleName());
        FSUtils.setRootDir(conf, rootdir);
        HBASE_CLUSTER = new LocalHBaseCluster(conf, 1);
        HBASE_CLUSTER.startup();

        // Then fork a thread with PQS in it.
        configureAndStartQueryServer();
    }

    private static void configureAndStartQueryServer() throws Exception {
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

    protected byte[] copyBytes(byte[] src, int offset, int length) {
        byte[] dest = new byte[length];
        System.arraycopy(src, offset, dest, 0, length);
        return dest;
    }
}
